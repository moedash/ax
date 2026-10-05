# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

SHELL := /bin/bash

# Configuration
AX_IMAGE_REPO ?= gcr.io/ax-substrate/ate-images
TASK_RUNNER_REPO ?= $(AX_IMAGE_REPO)/ax-task-runner
CONTAINER_CLI ?= $(shell which podman 2>/dev/null || which docker 2>/dev/null)

.PHONY: all build build-binaries build-task-runner install push push-task-runner deploy deploy-server deploy-redis apply-example test workflowcheck proto clean

# Pinned so that regenerating produces the same code as the committed files.
PROTOC_GEN_GO_VERSION ?= v1.36.11
PROTOC_GEN_GO_GRPC_VERSION ?= v1.6.0

# The determinism checker cannot be `go install`ed: its module carries a
# go 1.24 directive, so the binary it produces type-checks Go 1.27 packages
# with a 1.24 go/types, reports "package requires newer Go version" for much of
# the standard library, and exits 0 behind those errors. See docs/temporal.md.
WORKFLOWCHECK ?= $(shell go env GOPATH)/bin/workflowcheck-go1.27

all: build

## --------------------------------------
## Build Targets
## --------------------------------------

# Build all local binaries (ax CLI, server, task import)
build: build-binaries

build-binaries:
	@echo "==> Building local binaries (ax, ax-server, ax-migrate-tasks)..."
	@mkdir -p bin
	go build -trimpath -ldflags="-s -w" -o bin/ax ./cmd/ax
	go build -trimpath -ldflags="-s -w" -o bin/ax-server ./cmd/ax-server
	go build -trimpath -ldflags="-s -w" -o bin/ax-migrate-tasks ./cmd/ax-migrate-tasks

# Install the ax CLI into $(go env GOPATH)/bin
install:
	@echo "==> Installing ax CLI to $$(go env GOPATH)/bin..."
	go install -trimpath -ldflags="-s -w" ./cmd/ax

# Cross-compile ax-task-runner for Linux amd64 and build container image with Python, Antigravity, and git/curl
build-task-runner:
	@echo "==> Cross-compiling ax-task-runner for linux/amd64..."
	@mkdir -p bin/linux_amd64
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o bin/linux_amd64/ax-task-runner ./cmd/ax-task-runner
	@echo "==> Building container image $(TASK_RUNNER_REPO):latest using $(CONTAINER_CLI)..."
	$(CONTAINER_CLI) build --platform linux/amd64 -t $(TASK_RUNNER_REPO):latest -f Dockerfile.task-runner .

# Push task-runner container image to registry
push-task-runner: build-task-runner
	@echo "==> Pushing task runner image to $(TASK_RUNNER_REPO):latest..."
	$(CONTAINER_CLI) push $(TASK_RUNNER_REPO):latest
	@echo "==> Current pushed digest:"
	@gcloud container images list-tags $(TASK_RUNNER_REPO) --filter="tags=latest" --format="get(digest)"

# Build and push all images
push: push-task-runner

## --------------------------------------
## Deployment Targets
## --------------------------------------

# Deploy all AX components to Kubernetes (Redis, ax-server)
deploy: deploy-redis deploy-server

deploy-redis:
	@echo "==> Deploying Redis to ax-system namespace..."
	kubectl apply -f deploy/redis.yaml

deploy-server:
	@echo "==> Building and deploying ax-server using ko..."
	KO_DOCKER_REPO=$(AX_IMAGE_REPO) ko apply -f deploy/ax-server.yaml

# Apply example task and resources
apply-example:
	@echo "==> Applying example task and resources using bin/ax..."
	./bin/ax apply -f examples/task.yaml

## --------------------------------------
## Test & Verification Targets
## --------------------------------------

test:
	@echo "==> Running tests..."
	go test -v ./...

# Not part of `test`: the tool has to be built by hand until the released one
# works on Go 1.27.
workflowcheck:
	@echo "==> Checking workflow determinism..."
	@test -x "$(WORKFLOWCHECK)" || { \
		echo "$(WORKFLOWCHECK) is missing. Build it as described in docs/temporal.md."; \
		exit 1; \
	}
	$(WORKFLOWCHECK) -test=false ./internal/orchestration/...

## --------------------------------------
## Code Generation
## --------------------------------------

# Regenerate the API types. protoc itself is not pinned here, and the version
# it stamps into the generated header is whatever is on PATH.
proto:
	@echo "==> Generating pkg/apis/v1alpha1 from ax.proto..."
	go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)
	PATH="$$(go env GOPATH)/bin:$$PATH" protoc \
		--go_out=. --go_opt=module=github.com/google/ax \
		--go-grpc_out=. --go-grpc_opt=module=github.com/google/ax \
		pkg/apis/v1alpha1/ax.proto

clean:
	@echo "==> Cleaning build artifacts..."
	rm -rf bin/
