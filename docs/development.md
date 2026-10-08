# Development

## Prerequisites

- Go 1.27+
- [`ko`](https://ko.build/) for building and deploying control plane images
- Docker or Podman for the task runner image
- A Kubernetes cluster and kubeconfig

## Build

```bash
make build                 # bin/ax, bin/ax-server
make install               # install the ax CLI into $(go env GOPATH)/bin
make build-task-runner     # cross-compile the runner for linux/amd64 and build its image
make push-task-runner      # ...and push it (set TASK_RUNNER_REPO)
```

## Test

Runs everything, including the mock Substrate gRPC server, in-memory store validation, the API server tests, the task workflow against a mocked Agent Substrate, and a replay of the recorded task histories:

```bash
make test        # or: go test -v ./...
```

Workflow determinism is checked separately, and is worth running after any change to `internal/orchestration/workflows`:

```bash
make workflowcheck
```

The tool has to be built by hand once; `go install` does not give you a usable binary on Go 1.27. See [Temporal orchestration](temporal.md#versioning) for why and how, and for running the control plane on your machine with `--orchestrator=temporal`.

## Contributing

See [CONTRIBUTING.md](../CONTRIBUTING.md) for contribution guidelines, CLA requirements, and PR workflows.
