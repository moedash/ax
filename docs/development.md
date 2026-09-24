# Development

## Prerequisites

- Go 1.27+
- [`ko`](https://ko.build/) for building and deploying control plane images
- Docker or Podman for the task runner image
- A Kubernetes cluster and kubeconfig

## Build

```bash
make build                 # bin/ax, bin/ax-controller, bin/ax-server
make install               # install the ax CLI into $(go env GOPATH)/bin
make build-task-runner     # cross-compile the runner for linux/amd64 and build its image
make push-task-runner      # ...and push it (set TASK_RUNNER_REPO)
```

## Test

Runs everything: the task workflow against a mocked Agent Substrate, the
activities against an in-process fake of its Control API, a replay of a recorded
task history, and the API server:

```bash
make test        # or: go test -v ./...
```

Determinism is checked separately, and is worth running after any change to
`internal/orchestration/workflows`:

```bash
go install go.temporal.io/sdk/contrib/tools/workflowcheck@latest
workflowcheck ./internal/orchestration/...
```

To run the control plane on your machine, see
[Temporal orchestration](temporal.md#running-it-locally).

## Contributing

See [CONTRIBUTING.md](../CONTRIBUTING.md) for contribution guidelines, CLA requirements, and PR workflows.
