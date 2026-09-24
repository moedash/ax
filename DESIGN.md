# AX Design

## Architecture

Storing millions of short-lived tasks as Kubernetes CRDs pushes etcd past its comfort zone (single-digit GB storage limits, write-rate bottlenecks, control plane degradation). AX keeps each task in a Temporal workflow instead. The workflow is the task: it owns the task's desired state and its status, it drives Agent Substrate, and it survives a worker that dies mid-provisioning. The configuration kinds a task binds stay in Redis, where a plain key-value store is the right shape.

```
                      ax apply -f task.yaml
                                │
                                ▼
                            ax-server
                      (gRPC API + /healthz)
                                │
                ┌───────────────┴───────────────┐
                │                               │
        update / query                     get / save
        the task workflow               gateways, workspaces,
                │                             models
                ▼                               ▼
            Temporal                          Redis
      (one workflow per task)
                │
        task queue: ax-tasks
                │
                ▼
          ax-controller
     (Temporal workers, scale out)
                │
        gRPC (Control API)
                │
                ▼
         Agent Substrate
                ┌───────────────────────────────┐
                │ • Atespace Provisioning       │
                │ • Actor Creation & Activation │
                │ • Worker Assignment           │
                │ • Egress Policy Filtering     │
                └───────────────────────────────┘
```

A task's workflow ID is its business key, `<atespace>/<name>`, so the API server addresses a task without keeping a mapping and two callers cannot create the same task twice. Creating or changing a task is an update, reading one is a query, and listing them goes through Temporal visibility on the `AxAtespace` search attribute. See [Temporal orchestration](docs/temporal.md) for the whole mapping, the timeouts, and how to run it locally.

## Components

| Binary | Role |
|---|---|
| `ax` | Developer CLI. Applies manifests, inspects and watches resources, tunnels to the cluster. |
| `ax-server` | Stateless gRPC API on port 8080. Validates manifests, resolves what a task binds, and routes task calls to Temporal. The configuration kinds go to Redis. |
| `ax-controller` | Temporal workers. Run the task workflows and their activities: provision atespaces and actors on Agent Substrate, apply egress policy, and keep each task in the state its spec asks for. Scale by adding replicas. |
| `ax-task-runner` | Entrypoint inside every task container. Bootstraps the workspace, serves metadata, and runs the agent command. A thin wrapper over the `runner` package, which custom images can embed directly. |

## API reference

The control plane exposes the `ax.v1alpha1.AX` gRPC service. Health checks are plain HTTP: `GET /healthz` on the same port returns `200 OK`.

**Tasks**

| RPC | Description |
|---|---|
| `GetTask` | Get a task by atespace and name. |
| `ListTasks` | List tasks in an atespace, with pagination. |
| `UpdateTask` | Create or update a task. |
| `DeleteTask` | Delete a task. The task reports `Terminating` while its sandbox is torn down, then disappears. |
| `SuspendTask` | Checkpoint actor state and pause the task. |
| `ResumeTask` | Resume a suspended task. |
| `WatchTask` | Server-streaming RPC that emits status and condition transitions until the task is ready, has finished, or has failed. |

**Gateways**

| RPC | Description |
|---|---|
| `GetGateway` | Get a gateway by atespace and name. |
| `ListGateways` | List gateways in an atespace. |
| `UpdateGateway` | Create or update a gateway. |
| `DeleteGateway` | Delete a gateway. |

**Workspaces**

| RPC | Description |
|---|---|
| `GetWorkspace` | Get a workspace by atespace and name. |
| `ListWorkspaces` | List workspaces in an atespace. |
| `UpdateWorkspace` | Create or update a workspace. |
| `DeleteWorkspace` | Delete a workspace. |

**Models**

| RPC | Description |
|---|---|
| `GetModel` | Get a model configuration by atespace and name. |
| `ListModels` | List model configurations in an atespace. |
| `UpdateModel` | Create or update a model configuration. |
| `DeleteModel` | Delete a model configuration. |

Request and response types follow the `<Method>Request` / `<Method>Response` convention. Generated Go types live in [`pkg/apis/v1alpha1`](pkg/apis/v1alpha1).
