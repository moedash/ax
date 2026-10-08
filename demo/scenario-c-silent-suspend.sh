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

# Scenario C. A silent suspend divergence.
#
# Both paths run the same steps. Drive a task to Running, make SuspendActor fail,
# ask for a suspend, then let Substrate recover. The direct path says the suspend
# worked while the actor keeps running, and the record stays wrong after Substrate
# comes back. The Temporal path makes no claim it can't back, keeps retrying the
# suspend, and finishes it once Substrate recovers.

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

TASK="suspend-task"
# Set by suspend_under_fault: what each side reported while the fault was on.
SUSPEND_EXIT=""
PHASE_DURING=""
ACTOR_DURING=""
# Each result reads exit/phase-during/actor-during/phase-after/actor-after.
D_RESULT=""
T_RESULT=""

# drive_to_running applies the task and resumes it to Running.
drive_to_running() {
  write_task "${TASK}" "${WORKDIR}/task.yaml"
  run ax apply -f "${WORKDIR}/task.yaml" >/dev/null || true
  wait_for_phase "${TASK}" Suspended 30 || true
  run ax resume task "${TASK}" >/dev/null || true
  wait_for_phase "${TASK}" Running 30 \
    || note "phase is $(task_phase "${TASK}")"
}

# suspend_under_fault runs the steps both paths share: reach Running, make
# SuspendActor fail, ask for a suspend, and record what each side then reports.
suspend_under_fault() {
  sub "drive the task to Running"
  drive_to_running
  ok "task phase is $(task_phase "${TASK}"), actor is $(actor_state "${TASK}")"

  sub "make SuspendActor fail on the fake"
  knob fail-suspend=true

  sub "ask for a suspend while SuspendActor is failing"
  SUSPEND_EXIT=0
  run ax suspend task "${TASK}" || SUSPEND_EXIT=$?
  PHASE_DURING="$(task_phase "${TASK}")"
  ACTOR_DURING="$(actor_state "${TASK}")"
  echo "suspend exit=${SUSPEND_EXIT}; ax says phase=${PHASE_DURING};" \
    "fake says actor=${ACTOR_DURING}"
}

run_direct() {
  step "DIRECT path (--orchestrator=direct)"
  start_redis
  start_fake
  start_server direct
  suspend_under_fault

  sub "Substrate recovers (fault off); watch whether the record catches up"
  knob fail-suspend=false
  local waited=0
  while (( waited < 15 )); do
    echo "  t=${waited}s phase=$(task_phase "${TASK}") actor=$(actor_state "${TASK}")"
    sleep 5; waited=$((waited + 5))
  done
  note "fake: $(introspect)"

  D_RESULT="${SUSPEND_EXIT}/${PHASE_DURING}/${ACTOR_DURING}"
  D_RESULT+="/$(task_phase "${TASK}")/$(actor_state "${TASK}")"
  stop_stack
}

run_temporal() {
  step "TEMPORAL path (--orchestrator=temporal)"
  start_redis
  start_fake
  start_temporal
  start_server temporal
  suspend_under_fault
  if (( SUSPEND_EXIT != 0 )); then
    # The CLI waits 10s for the suspend and then gives up. Its message says no
    # worker answered, but the worker is fine and still retrying the suspend.
    note "the CLI stopped waiting; the workflow is still retrying SuspendActor"
  fi

  sub "Substrate recovers (fault off); the workflow's next retry lands"
  knob fail-suspend=false
  if wait_for_phase "${TASK}" Suspended 90; then
    ok "the suspend finished on its own"
  else
    err "the suspend did not finish; phase is $(task_phase "${TASK}")"
  fi
  note "fake: $(introspect)"
  note "highest SuspendActor attempt in the ax-server log: $(max_attempt SuspendActor)"

  T_RESULT="${SUSPEND_EXIT}/${PHASE_DURING}/${ACTOR_DURING}"
  T_RESULT+="/$(task_phase "${TASK}")/$(actor_state "${TASK}")"
  stop_stack
}

main() {
  printf '%s\nScenario C: a silent suspend divergence (SuspendActor fails)%s\n' \
    "${BOLD}" "${RESET}"
  demo_init scenario-c
  demo_build
  run_direct
  run_temporal

  step "VERDICT"
  check direct "${D_RESULT}" \
    '^0/Suspended/ACTOR_STATE_RUNNING/Suspended/ACTOR_STATE_RUNNING$' \
    "said yes. The record says Suspended, but the actor kept RUNNING and still is." \
    "got ${D_RESULT}, expected 0/Suspended/RUNNING/Suspended/RUNNING"
  check temporal "${T_RESULT}" \
    '^[1-9][0-9]*/Running/ACTOR_STATE_RUNNING/Suspended/ACTOR_STATE_SUSPENDED$' \
    "claimed nothing while Substrate failed, then finished. Record and actor agree." \
    "got ${T_RESULT}, expected nonzero/Running/RUNNING/Suspended/SUSPENDED"
  summarize "the direct record lies about the actor. The Temporal record never does."
}

main "$@"
