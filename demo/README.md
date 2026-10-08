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

## How a verdict is decided

Each verdict is computed from what the run observed: the phase `ax` reports, the
actor state the fake holds, CLI exit codes, and retry attempts in the
`ax-server` log. A verdict line is green when the observation matches the
expected contrast and red when it doesn't. If any check misses, the script says
so and exits non-zero, so a bad run can't pass for a good one.

## The scenarios

### A. A transient Substrate blip

The fake fails the first `CreateActor` call and serves every call after it
(`--fail-create-actor-times 1`).

- **direct.** `ax apply` reconciles inline. The one failed call ends the task as
  `Failed`, and nothing re-drives it. Applying the task again is rejected,
  because a task is immutable. Recovery needs a human to delete it and start
  over.
- **temporal.** The `EnsureActor` activity is retried, the second attempt
  succeeds, and the task settles. `ax resume` then takes it to `Running`.

```
direct:   Failed after one blip. Re-applying is rejected, so a human must delete and redo it.
temporal: Running. EnsureActor failed once, was retried, and the task recovered on its own.
```

### B. A crash mid-provision

The fake makes every resume slow (`--delay-resume`), which opens a window to kill
`ax-server` while it is placing the actor on a worker.

- **direct.** The resume runs inline inside the RPC. Kill `ax-server` mid-resume
  and the resume dies with the process. After a restart the task is still
  `Suspended` and the actor never moved. Nothing picks the resume back up, so
  someone has to notice and run it again.
- **temporal.** The resume is a step of the task's workflow. Kill `ax-server`
  mid-resume, restart it, and the restarted worker finishes the resume. The task
  reaches `Running`.

In both modes the CLI that asked for the resume sees the crash. The difference
is whether the work it asked for still happens.

Recovery on the Temporal path waits out the abandoned activity's heartbeat
timeout before Temporal reschedules it, which is about a minute. The script
allows up to `AX_DEMO_RECOVER_TIMEOUT` seconds (150 by default). Once the crash
window has been used, the script turns the slow resume off, so the recovery you
watch is the Temporal retry and not the artificial delay.

```
direct:   Suspended. The resume died with ax-server and nothing finished it.
temporal: Running. The restarted worker finished the resume that was in flight.
```

### C. A silent suspend divergence

Both paths run the same steps. Drive a task to `Running`, make `SuspendActor`
fail, ask for a suspend, then turn the fault off as if Substrate recovered.

- **direct.** The reconciler logs the failed suspend and reports `Suspended`
  anyway. `ax suspend` exits 0. The record says `Suspended` while the actor is
  still `RUNNING` on the fake. After Substrate recovers, nothing reconciles the
  two. The record stays wrong.
- **temporal.** The suspend is an activity, and it keeps retrying. `ax suspend`
  stops waiting after 10s and exits non-zero, and the phase stays `Running`.
  Once the fault is off, the next retry lands, and the task ends `Suspended`
  with the actor `SUSPENDED`. Record and actor agree.

`ax suspend` waits 10 seconds. The worker accepted the suspend, so the CLI
reports it as pending (`DeadlineExceeded`, "accepted and is still running"), not
as a worker that never answered. The workflow keeps retrying it.

```
direct:   said yes. The record says Suspended, but the actor kept RUNNING and still is.
temporal: claimed nothing while Substrate failed, then finished. Record and actor agree.
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
