# Temporal orchestration

AX runs one Temporal workflow per task, for as long as the task exists. The
workflow owns the task: its desired state, its status, and every call AX makes
to Agent Substrate on its behalf. This page is the map of that design.

## Why

The control plane used to be a level-triggered loop over Redis Streams. The API
server saved a task and published an event; a pool of controllers read the
stream and reconciled. Four things were wrong with it, and all four are
structural rather than bugs to patch:

- **A failed reconcile was dropped.** The worker acknowledged every event right
  after processing it, whether or not reconciliation had succeeded, so a task
  that failed to provision was never revisited.
- **A crashed controller stranded work.** Entries read from the stream stayed
  pending forever. Nothing claimed them, because nothing implemented the reclaim
  the interface promised.
- **Provisioning was five calls with no memory.** Each call was idempotent, but a
  controller that died between two of them left the sandbox half-built and
  nobody re-drove the sequence.
- **Completion was invisible.** The runner logged the agent's exit code and threw
  it away. A task that had finished looked exactly like one still working.

Temporal replaces the loop with a durable execution: the sequence itself is the
state, so it resumes where it stopped.

## The mapping

| Then | Now |
|---|---|
| `SaveTask` plus a stream event | `UpdateWithStartWorkflow` with the `apply` update |
| `XREADGROUP` loop in `ax-controller` | Temporal worker polling the `ax-tasks` task queue |
| Reconcile function | `TaskWorkflow`, one execution per task |
| Five Substrate calls in a row | Five activities, each idempotent, rolled back by a saga |
| In-reconcile 15 second readiness poll | A heartbeating activity that polls in the background |
| `GetTask` from Redis | `task` query on the workflow |
| `ListTasks` from a Redis index | Visibility query on the `AxAtespace` search attribute |
| `spec.suspend` flipped in Redis | `suspend` and `resume` updates |
| Two-phase delete through Redis | `delete` update: teardown, then the workflow ends |
| Exit code logged and dropped | `complete` update from the runner, recorded as `status.exitCode` |
| Nothing revisited a stuck task | A resync every five minutes, plus Temporal's own retries |

## Package layout

| Package | Holds |
|---|---|
| `internal/orchestration/workflows` | `TaskWorkflow`, its handlers, the update and query names, the workflow ID helper, the search attribute |
| `internal/orchestration/activities` | The `Activities` struct, one method per side effect, and every timeout and retry policy |
| `internal/orchestration/taskclient` | The client the API server uses: updates, queries, visibility |
| `internal/orchestration` | The `Tasks` interface the API server depends on, and the errors it maps to status codes |
| `cmd/ax-controller` | Worker wiring: dependencies in, workflow and activities registered |

## The lifecycle of a task

1. **Apply.** `ax apply` reaches `UpdateTask`. The API server validates the spec,
   resolves the gateway and workspaces the task binds, and sends the `apply`
   update together with a workflow start. A task that already exists takes the
   update; a task whose workflow has ended is started again under the same name.
2. **Provision.** The workflow runs, in order: `EnsureAtespace`,
   `EnsureActorTemplate`, `EnsureActor`, `ApplyEgressPolicy`. Each compensation
   is registered before the step it undoes.
3. **Activate.** `SuspendActor` or `ResumeActor`, depending on `spec.suspend`.
   The resume answers with the worker's address, which becomes
   `status.workerIP`.
4. **Settle.** A background coroutine polls the sandbox until its workspace is
   set up, then flips `WorkspaceReady` and `Ready`. The caller that asked for the
   change is not held up by it.
5. **Live.** The workflow waits for updates, resyncing with Substrate every five
   minutes. Suspending, resuming, and reporting a command exit all land here.
6. **Delete.** The `delete` update tears the sandbox down and the workflow ends.
   Cancelling the workflow does the same thing through a disconnected context.

`status.phase` is derived from that state in one place, so a task cannot report
`Running` after its command has exited or `Completed` while its sandbox is
suspended.

## Updates, queries, and signals

| Kind | Name | Payload | Answers with |
|---|---|---|---|
| Update | `apply` | The task plus the resolved gateway and workspaces | The task, once the sandbox is in the state the spec asks for |
| Update | `suspend` | none | The task |
| Update | `resume` | none | The task |
| Update | `complete` | Exit code and an optional message | The task status |
| Update | `delete` | none | Nothing, once the record is gone |
| Signal | `complete` | Same as the update | Nothing |
| Query | `task` | none | Metadata, spec, and status |
| Query | `status` | none | Status only |

Every update has a validator, and every validator only reads state: it rejects a
change to a task that is being deleted, and a spec that cannot be applied, before
the request is written to history.

The `complete` signal exists for one reason: the runner is PID 1 of the sandbox
and may be shutting down when the command exits. An update needs a worker to
accept it; a signal only needs the service. The runner tries the update first,
because it wants to know the report landed, and falls back to the signal.

## Timeouts and retry policies

All of them live in `internal/orchestration/activities/options.go`.

| Setting | Value | Why |
|---|---|---|
| `StartToCloseTimeout` on control calls | 1 minute | One Substrate control call. The slowest, creating an actor while a previous one finishes deleting, waits about ten seconds inside the client. |
| `ScheduleToCloseTimeout` on provisioning | 15 minutes | The budget for provisioning to converge. A cluster that cannot serve a task within a quarter of an hour needs a human, not more retries. |
| `StartToCloseTimeout` on resume | 5 minutes | Placing an actor on a worker includes restoring its snapshot. |
| Workspace readiness poll | 10 minutes | A maiden run clones repositories and installs skills. Expiry leaves the task running with `WorkspaceReady` False rather than failing it. |
| `ScheduleToCloseTimeout` on teardown | 10 minutes | Template deletion is rejected while the actor still exists, so teardown has to outlast an actor that is slow to disappear. |
| `HeartbeatTimeout` | 1 minute | Well above the 10 second heartbeat interval, so a busy worker is not killed for being late. |
| Retry policy | 1 second initial, coefficient 2, 30 second maximum | Transient Substrate failures clear in seconds. |
| Resync interval | 5 minutes | A sandbox can crash or be rescheduled without anyone telling AX. |

Activities that block in a single control-plane call report liveness from a
side goroutine, so they can be detected as stuck and can learn that they were
cancelled. The ones that return as soon as Substrate accepts them carry no
heartbeat timeout.

### What is retried and what is not

Errors are classified once, in `activities/errors.go`, by the gRPC code Substrate
returns:

- **Not retried:** `InvalidArgument`, `PermissionDenied`, `NotFound`,
  `AlreadyExists`, `FailedPrecondition`, `OutOfRange`, `Unimplemented`, and any
  input an activity refuses outright. These are reported as
  `PermanentSubstrateError` or `InvalidSpec` and listed in every retry policy's
  `NonRetryableErrorTypes`.
- **Retried:** everything else, including `Unavailable`, `DeadlineExceeded`,
  `ResourceExhausted`, and `Aborted`. `Unauthenticated` is retried on purpose:
  the worker's bearer token is a projected service account token that is rotated
  in place, so a rejected call usually succeeds on the next attempt.

## What a caller waits for

A change to a task is durable as soon as Temporal has it, but it is only
*applied* by a worker, and placing an actor on a worker can take minutes. So
every call the API server makes has a bounded wait:

| Call | Waits for | If nothing answers in time |
|---|---|---|
| `UpdateTask` | The `apply` update to complete, up to 10 seconds | The task was created; it is reported `Pending` |
| `SuspendTask`, `ResumeTask`, `DeleteTask` | The update to be accepted, up to 10 seconds | `Unavailable`, with the task named |
| `GetTask` | The `task` query, up to 5 seconds | `Unavailable` |
| `ListTasks` | Visibility, then one query per task, up to 5 seconds each | The task is listed with what visibility knows and no status |

This is the one place where the new design is less forgiving than the old one.
Writing a task to Redis succeeded whether or not a controller was alive to act
on it, and the task then sat there untouched. Now a control plane with no
workers says so.

## Idempotency

Every activity is idempotent, and the keys they act on are derived in the
workflow, so a retry or a replay addresses exactly the same Substrate
resources:

- The **actor** carries the task's name. That is also why a task and its actor
  are interchangeable in the router's `ate-target-actor` header.
- The **actor template** is named `<task>-tmpl-<digest>`, where the digest covers
  the task spec and every workspace it binds. The same spec always maps to the
  same template, and a spec change makes a new one.
- The digest deliberately excludes the task's status and its `suspend` flag.
  Both change while a task runs and neither changes what the sandbox is made of,
  so including them stranded a new template on every status update and every
  suspend.
- Resolved credentials are excluded as well. The model API key is looked up
  inside the activity that builds the template, so a rotated key does not fork
  the template, and no secret is written to workflow history.

## Compensations

Provisioning is a saga. Each compensation is registered before the step it
undoes, because a step whose side effect landed can still fail on the way back.
They run in reverse on a disconnected context, so a rollback completes even when
the workflow itself is being cancelled:

| Step | Compensation |
|---|---|
| `EnsureAtespace` | none. The atespace is shared by every task in it. |
| `EnsureActorTemplate` | `DeleteActorTemplates` |
| `EnsureActor` | `DeleteActorIfExists` |
| `ApplyEgressPolicy` | `DeleteEgressPolicyIfExists` |

Activation is retried but never compensated. Deleting a sandbox because a resume
failed would throw away the workspace the task has been building, so a task that
cannot be activated stays `Failed` with its sandbox intact and can be resumed
again later.

A task bound to a gateway is rolled back when its egress policy cannot be
applied: it must not run without the allowlist it was given. A task with no
gateway keeps unrestricted egress either way, so there a failure is reported on
the `GatewayReady` condition and the task runs.

## Versioning

`TaskWorkflow` records a `provisioning` version marker on every run through
`workflow.GetVersion`. There is one supported version today, and the call is
kept rather than removed: a task provisioned by a version this worker does not
know fails fast instead of proceeding on the wrong assumptions, and the next
change to the sequence only has to raise `provisioningVersion` and branch on the
recorded value.

Two tests guard determinism:

- `workflowcheck` over `./internal/orchestration/...`. The one annotated
  exception is documented where it sits.
- A replay test over a recorded history of a task's whole life, in
  `internal/orchestration/workflows/testdata`. Re-record it with a dev server:

  ```bash
  temporal server start-dev --port 7466 --ui-port 8466 --headless
  AX_RECORD_HISTORY_ADDRESS=localhost:7466 \
    go test ./internal/orchestration/workflows/ -run TestRecordHistory
  ```

## Long-lived tasks

A task's workflow lives as long as the task, which for an agent can be days. The
workflow continues as new when the service suggests it, after draining anything
left in the completion channel, and carries the desired state and the status
across. The next run re-drives the provisioning sequence, which is idempotent and
cheap, and skips the workspace poll because `WorkspaceReady` came across with the
status.

## Running it locally

```bash
# 1. A Temporal dev server.
temporal server start-dev

# 2. The search attribute task listing needs, once per namespace.
temporal operator search-attribute create --name AxAtespace --type Keyword

# 3. Redis for the configuration kinds.
docker run -p 6379:6379 redis:7-alpine

# 4. The worker and the API server.
go run ./cmd/ax-controller --substrate-endpoint=<substrate> --substrate-plaintext
go run ./cmd/ax-server

# 5. Anything you would normally do.
ax apply -f examples/task.yaml
ax watch task task123
```

The Temporal UI at `http://localhost:8233` shows one workflow per task, its
whole history, and its pending updates.

## What changed for operators

- **`ax-controller` no longer talks to Redis.** It takes `--temporal-address`,
  `--temporal-namespace`, and `--task-queue`. The `--redis-*` flags are gone.
  `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, and `AX_TASK_QUEUE` override them.
- **`--template` and `--template-atespace` are gone.** Every task's template is
  built from the task's own spec, so there was no base template to point at. The
  flags only fed a fallback that produced a sandbox with none of the task's
  environment in it.
- **`ax-server` needs Temporal too.** Same three flags, plus `--watch-interval`
  for how often `WatchTask` asks a task for its state.
- **Redis stays, for gateways, workspaces, and models.** It is no longer on the
  path of a running task, so losing it does not stop tasks from being provisioned
  or torn down.
- **The `AxAtespace` search attribute must exist** in the namespace before a
  worker starts, or starting a task fails.
- **`--sandbox-temporal-address`** is the address task containers dial to report
  their command's exit. It defaults to `--temporal-address`, which is right when
  the workers and the sandboxes share a network and wrong when they do not.
- **A task is now a workflow.** `temporal workflow list --query "WorkflowType =
  'TaskWorkflow'"` lists them, `temporal workflow show -w <atespace>/<name>`
  shows everything that has happened to one, and a task stuck in `Pending` shows
  exactly which activity is failing and why.
- **With no worker running, task calls report `Unavailable`.** Reading or
  changing a task needs a worker to answer for it. A listing still works, and
  shows the tasks that exist with no status.
