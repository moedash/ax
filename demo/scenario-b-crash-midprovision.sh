#!/usr/bin/env bash
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

# Scenario B. The control plane crashes mid-provision.
#
# The fake makes every resume slow, so there is a window to kill ax-server while
# it is placing the actor on a worker. In both modes the CLI that asked for the
# resume sees the crash. What differs is the work. On the direct path the resume
# ran inside the RPC and died with the process, so the task stays Suspended until
# someone notices and asks again. On the Temporal path the resume is a workflow
# step, so the restarted worker finishes it.

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

TASK="crash-task"
# DELAY_RESUME is the window the resume stays in flight. KILL_AFTER is when we
# crash into it. Keep KILL_AFTER well under DELAY_RESUME.
DELAY_RESUME="${AX_DEMO_DELAY_RESUME:-25s}"
KILL_AFTER="${AX_DEMO_KILL_AFTER:-6}"
# RECOVER_TIMEOUT covers Temporal's dead-worker detection. The abandoned resume
# is rescheduled after its heartbeat timeout, which is about a minute.
RECOVER_TIMEOUT="${AX_DEMO_RECOVER_TIMEOUT:-150}"
DIRECT_RESULT=""
DIRECT_ACTOR=""
TEMPORAL_RESULT=""
TEMPORAL_ACTOR=""

# drive_to_resume applies the task, waits for Suspended, then kicks off a resume
# in the background and crashes ax-server while that resume is in flight.
drive_to_resume() {
  write_task "${TASK}" "${WORKDIR}/task.yaml"
  sub "apply the task and wait for Suspended"
  run ax apply -f "${WORKDIR}/task.yaml" >/dev/null || true
  wait_for_phase "${TASK}" Suspended 30 \
    || note "phase is $(task_phase "${TASK}")"

  sub "resume in the background; the resume will block in the slow placement"
  ( ax resume task "${TASK}" >"${WORKDIR}/resume.log" 2>&1 ) &
  DEMO_PIDS+=("$!")

  sleep "${KILL_AFTER}"
  sub "the resume is mid-flight on the fake: $(introspect)"
  sub "crash ax-server now (SIGKILL, pid ${SERVER_PID})"
  crash_server
  ok "ax-server is down with a half-finished resume"
}

run_direct() {
  step "DIRECT path (--orchestrator=direct)"
  start_redis
  start_fake --delay-resume "${DELAY_RESUME}"
  start_server direct
  drive_to_resume

  sub "restart ax-server (direct)"
  start_server direct
  sub "watch the task for a while; nothing picks the resume back up"
  local waited=0
  while (( waited < 12 )); do
    echo "  t=${waited}s phase=$(task_phase "${TASK}") actor=$(actor_state "${TASK}")"
    sleep 3; waited=$((waited + 3))
  done
  note "the client that asked for the resume saw: $(tail -1 "${WORKDIR}/resume.log")"
  note "fake: $(introspect)"

  DIRECT_RESULT="$(task_phase "${TASK}")"
  DIRECT_ACTOR="$(actor_state "${TASK}")"
  stop_stack
}

run_temporal() {
  step "TEMPORAL path (--orchestrator=temporal)"
  start_redis
  start_fake --delay-resume "${DELAY_RESUME}"
  start_temporal
  start_server temporal
  drive_to_resume

  # The slow placement did its job (it gave us a window). Turn it off so the
  # retried resume finishes quickly and the recovery is the Temporal retry, not
  # the artificial delay.
  knob delay-resume=0
  sub "restart ax-server (temporal); the worker reconnects"
  start_server temporal
  sub "Temporal reruns the resume once the dead worker's heartbeat lapses (about a minute)"
  local t0=${SECONDS}
  if wait_for_phase "${TASK}" Running "${RECOVER_TIMEOUT}"; then
    ok "the task reached Running $((SECONDS - t0))s after the restart"
  else
    err "task did not recover inside ${RECOVER_TIMEOUT}s; phase is $(task_phase "${TASK}")"
  fi
  note "the client that asked for the resume saw: $(tail -1 "${WORKDIR}/resume.log")"
  note "fake: $(introspect)"
  note "highest ResumeActor attempt in the ax-server log: $(max_attempt ResumeActor)"

  TEMPORAL_RESULT="$(task_phase "${TASK}")"
  TEMPORAL_ACTOR="$(actor_state "${TASK}")"
  stop_stack
}

main() {
  printf '%s\nScenario B: a crash mid-provision (ax-server killed during resume)%s\n' \
    "${BOLD}" "${RESET}"
  demo_init scenario-b
  demo_build
  run_direct
  run_temporal

  step "VERDICT"
  check direct "${DIRECT_RESULT}/${DIRECT_ACTOR}" '^Suspended/ACTOR_STATE_SUSPENDED$' \
    "Suspended. The resume died with ax-server and nothing finished it." \
    "got ${DIRECT_RESULT:-none}/${DIRECT_ACTOR:-none}, expected Suspended/SUSPENDED"
  check temporal "${TEMPORAL_RESULT}/${TEMPORAL_ACTOR}" '^Running/ACTOR_STATE_RUNNING$' \
    "Running. The restarted worker finished the resume that was in flight." \
    "got ${TEMPORAL_RESULT:-none}/${TEMPORAL_ACTOR:-none}, expected Running/RUNNING"
  summarize "both clients saw the crash. Only Temporal finished the work it had accepted."
}

main "$@"
