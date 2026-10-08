# ax control-plane demos

Three laptop demos that put the two ax orchestrators side by side. Each one runs
the same task through `--orchestrator=direct` (the synchronous path that
reconciles inline under a Redis lock) and through `--orchestrator=temporal` (each
task is a Temporal workflow), and shows the direct path behaving worse under a
control-plane fault.

## The backend is fake. The ax behavior is real.

There is no Kubernetes and no real Agent Substrate here. The Substrate Control
API is served by `internal/substrate/substratetest/cmd/fakecontrol`, an
in-process fake that records calls and answers them with the minimum a caller
needs. It also serves a readiness endpoint that stands in for a sandbox.

What is real is everything on the ax side. The same `ax-server`, the same
`TaskReconciler` on the direct path, the same `TaskWorkflow` and activities on
the Temporal path, the same `ax` CLI, the same Redis store. The fake is only
there so the whole control plane fits on a laptop, and so a fault can be injected
on demand. Read the fault as a stand-in for a Substrate that is restarting, slow,
or rejecting a call, not as a claim about how Substrate behaves.

## What you need

- `go` (the demos build the binaries)
- `redis-server` (the store both paths use)
- `temporal` (the dev server, for the Temporal path)
- `curl` (to read and poke the fake)

Everything runs on loopback. Nothing is pushed anywhere.

## Running

```bash
demo/scenario-a-transient-blip.sh
demo/scenario-b-crash-midprovision.sh
demo/scenario-c-silent-suspend.sh
```

Each script builds the binaries, runs the direct phase, tears it down, runs the
Temporal phase, tears it down, and prints a verdict. Every process is killed on
exit through a trap, including when you `Ctrl-C`. Logs and manifests land under
`/tmp/ax-demo/<scenario>/`.

Ports default to values off the beaten path and can be overridden. `NO_COLOR=1`
turns off color.

```bash
AX_DEMO_REDIS_PORT=6399 AX_DEMO_SERVER_PORT=8111 \
AX_DEMO_FAKE_CONTROL_PORT=9101 AX_DEMO_TEMPORAL_PORT=7244 \
  demo/scenario-a-transient-blip.sh
```

## The scenarios

### A. A transient Substrate blip

The fake fails the first `CreateActor` call and serves every call after it
(`--fail-create-actor-times 1`).

- **direct.** `ax apply` reconciles inline. The one failed call ends the task as
  `Failed`, and nothing re-drives it. It stays `Failed`.
- **temporal.** The `EnsureActor` activity is retried, the second attempt
  succeeds, and the task settles. `ax resume` then takes it to `Running`.

```
direct:   Failed (gave up)
temporal: Running (retried, recovered)
```

### B. A crash mid-provision

The fake makes every resume slow (`--delay-resume`), which opens a window to kill
`ax-server` while it is placing the actor on a worker.

- **direct.** The resume runs inline inside the RPC. Kill `ax-server` mid-resume
  and nothing re-drives the half-finished placement. After a restart the task
  sits where it was, `Suspended`.
- **temporal.** The workflow is the state. Kill `ax-server` mid-resume, restart
  it, and the restarted worker picks the resume back up and the task reaches
  `Running`.

Recovery on the Temporal path waits out the abandoned activity's heartbeat
timeout before Temporal reschedules it, which is about a minute. The script
allows up to `AX_DEMO_RECOVER_TIMEOUT` seconds (150 by default). Once the crash
window has been used, the script turns the slow resume off, so the recovery you
watch is the Temporal retry and not the artificial delay.

```
direct:   stuck Suspended (no re-drive)
temporal: Running (workflow resumed)
```

### C. A silent suspend divergence

A task is driven to `Running`, then a suspend is asked for while `SuspendActor`
fails (`--fail-suspend`).

- **direct.** The reconciler logs the failed suspend and reports `Suspended`
  anyway. `ax suspend` exits 0. The record now says `Suspended` while the actor
  is still `RUNNING` on the fake and was never in the suspended set. The record
  lies.
- **temporal.** The suspend is an activity whose failure is surfaced. `ax suspend`
  exits non-zero, and the phase does not become `Suspended`. No false success.

The fault timing differs on purpose. On the direct path the fault is on the whole
time and the path just logs it. The Temporal path refuses to report a suspend it
did not perform, so it cannot even be driven into a running state with the fault
on. The fault is turned on only once the task is running, which is itself the
point in Temporal's favor.

```
direct:   reported Suspended, actor still RUNNING (divergence)
temporal: suspend failed loudly, no false success
```

## The fake's fault knobs

Added to `substratetest.ControlServer` and exposed as flags on `fakecontrol`.

| Flag | Effect |
|---|---|
| `--fail-create-actor-times N` | Fail the first `N` `CreateActor` calls with `Unavailable`, then serve them |
| `--fail-suspend` | Fail every `SuspendActor` call |
| `--delay-resume <dur>` | Sleep `<dur>` inside every `ResumeActor` call |

`fakecontrol` also serves a small HTTP API (`--control-addr`, default
`127.0.0.1:9003`) so a demo can flip a fault on while a task is already running
and read back what the fake really holds.

| Request | Effect |
|---|---|
| `GET /introspect` | JSON of actor states and the created, resumed, suspended, and reverted sets |
| `POST /knobs?fail-suspend=true` | Turn the suspend fault on or off at runtime |
| `POST /knobs?fail-create-actor-times=N` | Set the create fault count at runtime |
| `POST /knobs?delay-resume=<dur>` | Set the resume delay at runtime |
