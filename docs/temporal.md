# Temporal orchestration

By default `ax-server` reconciles a task with Agent Substrate inline: a task
RPC takes a Redis lock on the task, calls Substrate, writes the result to Redis,
and returns. That is the synchronous path, and it is what runs unless you ask
for something else.

`--orchestrator=temporal` is the something else. It is an add-on, not a
replacement: with the flag, `ax-server` runs each task as a Temporal workflow
and hosts the worker for it in-process. The API, the CLI, the manifests, and
the Redis store for workspaces and models are the same either way. What the
flag buys is durable execution for the part of the control plane that talks to
Substrate: a provisioning sequence that resumes where it stopped, retries with a
budget, a periodic look at every sandbox, and one history per task that says what
happened to it and why.

This page is the map of that path.

## When to use it

The synchronous path is the right default for a small cluster where the
operator is nearby. The Temporal path earns its keep when one of these hurts:

- **A half-built sandbox.** Provisioning is several Substrate calls in a row.
  If `ax-server` dies between two of them, the direct path has nothing that
  re-drives the rest; the task sits as `Failed` or, worse, as `Suspended` with
  an actor on the wrong template. The workflow is the state, so it picks up at
  the step it was on.
- **A sandbox that dies on its own.** The direct path learns about a crashed or
  rescheduled actor on the next call a client makes. The workflow looks every
  few minutes and acts: a crashed actor is reverted to its snapshot, a vanished
  one is provisioned again.
- **Retries you do not have to write.** Every Substrate call has a retry policy
  that knows which errors are worth retrying, and a budget after which the task
  reports `Failed` with the step that failed in its condition.
- **Seeing what happened.** `temporal workflow show -w default/task123` is the
  whole life of a task, call by call, including the attempts that failed.

## The mapping

| Synchronous path | Temporal path |
|---|---|
| Redis lock on the task, for the length of the RPC | The workflow ID `<atespace>/<name>`; Temporal serializes updates to it |
| Task record in Redis, status written after each call | The workflow's own state, answered through a query |
| `Reconcile` called inline from `CreateTask`, `SuspendTask`, `ResumeTask` | `apply`, `suspend`, and `resume` updates; the workflow drives Substrate |
| Five Substrate calls in a row | A look at what is there, then idempotent activities that build the sandbox, with what the pass itself created rolled back by a saga |
| 15 second readiness poll inside the RPC | A heartbeating activity that polls in the background |
| `GetTask` from Redis | `task` query on the workflow |
| `ListTasks` from a Redis index | A visibility list of the running task workflows, filtered by workflow ID |
| `WatchTask` from Redis pub/sub | A poll of the `task` query on an interval |
| `ReconcileDelete` inline from `DeleteTask` | `delete` update: teardown, then the workflow ends |
| Nothing revisits a task between calls | A resync every five minutes, plus Temporal's own retries |

## Package layout

| Package | Holds |
|---|---|
| `internal/orchestration/workflows` | `TaskWorkflow`, its handlers, the update and query names, the workflow ID helper |
| `internal/orchestration/activities` | The `Activities` struct, one method per side effect, and every timeout and retry policy |
| `internal/orchestration/taskclient` | The client the API server uses: updates, queries, and listing |
| `internal/orchestration` | The `Tasks` interface the API server depends on, and the errors it maps to status codes |
| `internal/server/tasks_temporal.go` | The task RPCs when `Options.Tasks` is set |
| `cmd/ax-server/temporal.go` | The flag group and the in-process worker |

## The lifecycle of a task

1. **Create.** `ax apply` reaches `CreateTask`. The API server validates the
   spec, resolves the workspaces the task binds, and sends the `apply` update
   together with a workflow start. The start uses
   `WorkflowIDConflictPolicy: FAIL`, so a name that is already running answers
   `FailedPrecondition` the way the synchronous path does, and
   `WorkflowIDReusePolicy: ALLOW_DUPLICATE`, so a deleted task's name can be
   used again.
2. **Provision.** The workflow looks first: `ObserveActor` says whether the
   task has an actor and which template it is on, `ObserveActorTemplate`
   whether the template this pass would build is already there. Then it runs,
   in order: `EnsureAtespace`, `EnsureActorTemplate`, `EnsureActor`. Each
   compensation is registered before the step it undoes, and only undoes what
   the observation said was not there before the pass. An actor found on some
   other template was not made by this workflow, so it is deleted and created
   again on this one.
3. **Suspend.** A task is created suspended, as on the synchronous path. The
   fresh actor is checkpointed and the task reports `Suspended`.
4. **Resume.** `ax resume` sends the `resume` update. `ResumeActor` answers with
   the worker's address, which becomes `status.workerIP`, and the task reports
   `Running`.
5. **Settle.** A background coroutine polls the sandbox until its workspace is
   set up, then flips `WorkspaceReady` and `Ready`. The caller that asked for
   the resume is not held up by it.
6. **Live.** The workflow waits for updates, resyncing with Substrate every five
   minutes. Suspending and resuming both land here.
7. **Delete.** The `delete` update tears the sandbox down and the workflow ends.
   The RPC returns once the sandbox is gone, as on the synchronous path.
   Cancelling the workflow does the same thing through a disconnected context.

`status.phase` is derived from that state in one place, so a task cannot report
`Running` while its sandbox is suspended.

### Tasks are immutable

The API has no update for tasks, and the workflow holds to that: the `apply`
update is accepted once, with the start, and refused afterwards. Substrate binds
an actor to the template it was created from, so there is nothing a second spec
could do but replace the sandbox behind the task's back.

### Sandbox generations

Each sandbox a task has had is a **generation**, counted in the workflow. The
generation is part of the template name, so a sandbox the task has replaced is
built from a template of its own. A task gets a new generation when its sandbox
vanished, or crashed and could not be reverted. Whatever the old sandbox had in
its workspace is discarded with it.

## Updates, queries, and signals

| Kind | Name | Payload | Answers with |
|---|---|---|---|
| Update | `apply` | The task plus the resolved workspaces | The task, once the sandbox is built and checkpointed |
| Update | `suspend` | none | The task |
| Update | `resume` | none | The task |
| Update | `delete` | none | Nothing, once the sandbox is gone |
| Query | `task` | none | Metadata, spec, and status |
| Query | `status` | none | Status only |

A workflow answers queries after it has closed, so a task that has been deleted,
cancelled, terminated, or failed would still describe itself for as long as its
history is retained. `GetTask` therefore sends the `task` query with
`QueryRejectCondition: NOT_OPEN`: a closed run rejects the query, and the
rejection is answered as `NotFound` whatever the run closed as. `ListTasks`
lists only the running task workflows.

Every update has a validator, and every validator only reads state: it rejects a
change to a task that is being deleted, a change before the first apply has
landed, a second apply, and a spec that cannot be applied or that names another
task, before the request is written to history.

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
| Resync interval | 5 minutes, `--resync-interval` | A sandbox can crash or be rescheduled without anyone telling AX. |

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
  the server's bearer token is a projected service account token that is
  rotated in place, so a rejected call usually succeeds on the next attempt.
- **One exception:** `FailedPrecondition` on a template deletion is retried,
  because Substrate answers that while the actor still derives from the
  template, and it clears as soon as the actor is gone.

## What a caller waits for

A change to a task is durable as soon as Temporal has it, but it is only
*applied* by a worker, and placing an actor on a worker can take minutes. So
every call the API server makes has a bounded wait:

| Call | Waits for | If nothing answers in time |
|---|---|---|
| `CreateTask` | The `apply` update to complete, up to 10 seconds | The task was created; it is reported `Pending` |
| `SuspendTask`, `ResumeTask` | The update to be accepted and then to complete, up to 10 seconds in all | `Unavailable` if nothing accepts it. `DeadlineExceeded` if a worker accepted it and is still on it. The workflow finishes it anyway |
| `DeleteTask` | The update to be accepted, up to 10 seconds, then the teardown for as long as it takes | `Unavailable` if nothing accepts it |
| `GetTask` | The `task` query, up to 5 seconds | `Unavailable` |
| `ListTasks` | A visibility list of the running workflows | Not applicable: no worker is involved |
| `WatchTask` | The `task` query on each poll | `Unavailable` after fifteen unanswered polls in a row, so a server restart does not end a watch |

The synchronous path holds a `CreateTask` or `ResumeTask` open for the whole
provisioning, readiness poll included. This path answers inside ten seconds
and lets the sandbox settle in the background. A client that wants to wait for
`Ready` uses `ax watch`, as it does on the synchronous path.

`ListTasks` reads visibility, which lags the workflow's own state by the
visibility store's indexing delay, so a listing right after a create can miss
the task. It carries no status, so `GetTask`, `SuspendTask`, `ResumeTask`, and
`CreateTask` read the workflow itself and see their own writes.

A delete interrupts a provisioning pass that is still retrying, so `ax delete`
does not wait out the fifteen minute budget of a pass that is not going to
succeed.

## Idempotency

Every activity is idempotent, and the keys they act on are derived in the
workflow, so a retry or a replay addresses exactly the same Substrate
resources:

- The **actor** carries the task's name. That is also why a task and its actor
  are interchangeable in the router's `ate-target-actor` header, on both paths.
- The **actor template** is named `<task>-tmpl-<digest>`, where the digest covers
  the task spec, every workspace it binds, and the sandbox generation. The same
  spec for the same generation always maps to the same template; a replacement
  sandbox makes a new one. The suffix takes fourteen characters of the
  sixty-three a Substrate name may have, which is why a task name is capped at
  forty-nine. The synchronous path names its templates the same way, from a
  different digest, so teardown on either path finds the other's templates.
- The digest deliberately excludes the task's status. It changes while a task
  runs and does not change what the sandbox is made of.
- Resolved credentials are excluded as well. The model API key is looked up
  inside the activity that builds the template, so no secret is written to
  workflow history. The consequence: rotating the key does not by itself
  produce a new template, so a sandbox keeps the key it was built with until
  the sandbox is replaced.

## Compensations

Provisioning is a saga. Each compensation is registered before the step it
undoes, because a step whose side effect landed can still fail on the way back.
They run in reverse on a disconnected context, so a rollback completes even when
the workflow itself is being cancelled:

| Step | Compensation |
|---|---|
| `EnsureAtespace` | none. The atespace is shared by every task in it. |
| `EnsureActorTemplate` | `DeleteActorTemplateIfExists`, when the template was not there before the pass |
| `EnsureActor` | `DeleteActorIfExists`, when the actor was not there before the pass, or the pass replaced it |

Whether a resource "was there before" is settled by the observation at the
start of the pass, not by what the create call answers. Activities run at
least once: a worker that dies after Substrate created the template but before
the result was recorded gets "found" on the retry, and a rollback that trusted
that answer would leave the template behind.

Activation is retried but never compensated. Deleting a sandbox because a resume
failed would throw away the workspace the task has been building, so a task that
cannot be activated stays `Failed` with its sandbox intact and can be resumed
again later.

## Crash recovery

The resync reads each task's actor every interval and reacts to what it finds:

- **Gone.** The sandbox is provisioned again, as a new generation.
- **Crashed.** `RevertActor` puts it back on its last snapshot, which keeps the
  workspace. The actor comes back suspended, and the next pass resumes it if
  the task is meant to run. This is what the synchronous path's `EnsureActor`
  does too. If the revert fails, the sandbox is given up on and provisioned
  again as a new generation.
- **Suspended when it should run, or running when it should be suspended.** The
  router resumes a suspended actor when a request reaches it, so this happens.
  The next pass puts the actor back where the task says.
- **Moved.** The worker address is corrected.

## Versioning

`TaskWorkflow` records a `provisioning` version marker on every run through
`workflow.GetVersion`. There is one supported version today, and the call is
kept rather than removed: a task provisioned by a version this worker does not
know fails fast instead of proceeding on the wrong assumptions, and the next
change to the sequence only has to raise `provisioningVersion` and branch on the
recorded value.

`make workflowcheck` guards determinism by running `workflowcheck` over
`./internal/orchestration/...`.

The released tool cannot be used as it ships. Its module carries a `go 1.24`
directive, so the binary `go install` produces type-checks Go 1.27 packages
with a 1.24 `go/types` and reports `package requires newer Go version` for much
of the standard library. It then exits 0 behind those errors, so it looks clean
while having checked nothing. Build it against a newer `x/tools`:

```bash
git clone https://github.com/temporalio/sdk-go
cd sdk-go/contrib/tools/workflowcheck
go mod edit -go=1.27.0
go get golang.org/x/tools@latest
go mod tidy
go build -o "$(go env GOPATH)/bin/workflowcheck-go1.27" .
```

### Configuration is bound when a task is created

The API server resolves a task's workspaces at create time and hands them to
the workflow. That is what keeps the worker off the configuration store, and it
means a later edit to a `Workspace` does not reach the tasks that already bind
it. Tasks are immutable, so there is no re-apply; a task that needs the new
workspace is a new task. The synchronous path reads the workspaces at the same
moment, so the two agree.

### The resync costs one read per task

Every interval, every settled task reads its actor. At ten thousand tasks and
the default five minutes that is about thirty reads a second against the
Control API, and it grows with the fleet. `--resync-interval` is the knob; a
Substrate watch, so that AX is told about a crashed or moved actor instead of
asking, is the follow-up that removes the tradeoff.

## Long-lived tasks

A task's workflow lives as long as the task, which for an agent can be days. The
workflow continues as new when the service suggests it, and carries the desired
state, the status, whether the task is meant to be suspended, the sandbox
generation, and a teardown failure across. The next run re-drives the
provisioning sequence,
which is idempotent and cheap: the observation finds the actor on the template
of the carried generation, so nothing is replaced. The workspace poll is
skipped because `WorkspaceReady` came across with the status.

## Running it locally

```bash
# 1. A Temporal dev server.
temporal server start-dev

# 2. Redis for the configuration kinds.
docker run -p 6379:6379 redis:7-alpine

# 3. Agent Substrate, or the fake that stands in for it.
go run ./internal/substrate/substratetest/cmd/fakecontrol

# 4. The API server, with the worker inside it.
go run ./cmd/ax-server --orchestrator=temporal \
  --substrate-endpoint=127.0.0.1:9001 --substrate-plaintext

# 5. Anything you would normally do.
ax --server=localhost:8080 apply -f examples/task.yaml
ax --server=localhost:8080 resume task task123
ax --server=localhost:8080 get tasks
ax --server=localhost:8080 delete task task123
```

The fake Control API accepts every call and reports actors as running on a
sandbox it also serves, so tasks reach `Running` with `WorkspaceReady` True.
Point step 4 at a real Substrate to do the same thing for real.

The Temporal UI at `http://localhost:8233` shows one workflow per task, its
whole history, and its pending updates.

## Switching a deployment

- **The flag.** Add `--orchestrator=temporal` to `ax-server`, with
  `--temporal-address`, `--temporal-namespace`, and `--task-queue` as needed.
  `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, `AX_TASK_QUEUE`, and
  `AX_ORCHESTRATOR` override them. `deploy/ax-server.yaml` carries the lines,
  commented out.
- **Tasks the synchronous path created are not seen** by the Temporal path:
  they are records in Redis, and this path does not read those. `ax get tasks`
  lists none of them and `ax delete` answers `NotFound`, while their actors
  keep running on Substrate. Delete every task before switching.
- **Switching back** has the mirror-image gap: workflows keep running and
  their actors keep running, and the synchronous path does not see them.
  Delete every task first, with `ax delete`, while the Temporal path is still
  on.
- **`--template` and `--template-atespace` are not read** on this path. Every
  task's template is built from the task's own spec, so there is no base
  template to point at.
- **Redis stays** for workspaces and models, and for the lock the synchronous
  path takes. It is no longer on the path of a running task, so losing it does
  not stop tasks from being provisioned or torn down.
- **A task name is at most 49 characters**, on both paths. Its actor template
  is named after it with a fourteen character suffix, and Substrate names are
  DNS labels of at most 63.
- **A listing no longer asks each task.** `ax get tasks` lists the running task
  workflows in one call however many tasks there are. It carries their names and
  atespaces, not their status, so the phase and worker address show only in
  `ax get task` and `ax describe task`.
- **A task is a workflow.** `temporal workflow list --query "WorkflowType =
  'TaskWorkflow'"` lists them, `temporal workflow show -w <atespace>/<name>`
  shows everything that has happened to one, and a task stuck in `Pending`
  shows exactly which activity is failing and why.
- **To remove a task from the Temporal side, cancel its workflow, never
  terminate it.** `temporal workflow cancel -w <atespace>/<name>` runs the same
  teardown a delete does, through a disconnected context, and the sandbox goes
  with the task. `temporal workflow terminate` ends the run without running any
  workflow code, so the actor and its templates stay on Substrate with nothing
  pointing at them, and `ax get task` answers `NotFound` as if the task were
  gone. `ax delete` is still the tool for normal use.
- **With no worker running, task calls report `Unavailable`.** The worker lives
  in `ax-server`, so this means every replica is down or cannot reach the task
  queue. A listing still works, and shows the tasks that exist with no status.
- **Shutdown takes longer.** The worker is given six minutes to finish the
  activity it is in, so a resume that is restoring a snapshot is not abandoned
  halfway. Raise the pod's `terminationGracePeriodSeconds` if you want that
  budget honored; an activity cut short is retried by another replica either
  way.

## Known gaps

- The Redis lock is not taken on this path. The workflow ID serializes changes
  to one task, which is what the lock was for, but a deployment that runs
  replicas with different `--orchestrator` values against the same Redis has
  two control planes that do not see each other's tasks.
- `WatchTask` is a poll of the `task` query, two seconds apart by default. The
  synchronous path pushes from Redis pub/sub.
- `RevertActor` is the first thing tried on a crashed sandbox, and the fallback
  is a replacement that loses the workspace. Nothing surfaces which of the two
  happened beyond the `Ready` condition's reason.
- Tasks created on one path are invisible to the other; see
  [Switching a deployment](#switching-a-deployment).
