# ax demos: ax on its own vs ax on Temporal

Each scenario runs the same task through `ax-server --orchestrator=direct`
(upstream, which reconciles inline under a Redis lock) and
`ax-server --orchestrator=temporal` (each task is a Temporal workflow), injects
one Substrate fault, and prints a verdict computed from what it observed.

The Substrate backend is a fake (`fakecontrol`), so everything runs on a laptop.
The ax code is real on both paths.

## Run

```bash
demo/scenario-a-transient-blip.sh       # about 1 minute
demo/scenario-b-crash-midprovision.sh   # about 5 minutes
demo/scenario-c-silent-suspend.sh       # about 3 minutes
```

You need `go`, `redis-server`, the `temporal` CLI, and `curl`. Logs go to
`/tmp/ax-demo/<scenario>/`. A run that doesn't show the contrast exits non-zero.

## What each one shows

| Fault | ax on its own | ax on Temporal |
|---|---|---|
| A. The first `CreateActor` call fails once | The task ends `Failed`, and applying it again is rejected. A human has to delete and redo it. | `EnsureActor` is retried and the task reaches `Running`. |
| B. `ax-server` is killed while a resume is in flight | The resume dies with the process, and the task stays `Suspended`. | The restarted worker finishes the resume and the task reaches `Running`. |
| C. `SuspendActor` fails, then recovers | `ax suspend` succeeds and the record says `Suspended` while the actor keeps running. It stays wrong after Substrate recovers. | `ax suspend` reports the change as pending (`DeadlineExceeded`), the record stays `Running`, and the workflow finishes the suspend once Substrate recovers. |

In B, Temporal notices the dead worker when its one-minute heartbeat lapses, so
recovery takes about a minute. Both paths get the same 120-second wait, so the
direct path isn't judged on a shorter one. Set `AX_DEMO_RECOVER_WAIT` to shorten
it when presenting.

`fakecontrol` takes faults over HTTP while it runs. Its package comment lists the
endpoints.
