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
# A task is driven to Running, then a suspend is asked for while SuspendActor
# fails. The direct path swallows the failure, reports Suspended, and the record
# now disagrees with the actor, which is still running. The Temporal path surfaces
# the failure and does not report a suspend it did not perform.
#
# The suspend fault is on the whole time for direct, which just logs it. Temporal
# refuses to report a suspend it could not do, so it cannot even be driven into a
# running state with the fault on. The fault is therefore turned on only once the
# task is running, which is itself the point in Temporal's favor.

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

TASK="suspend-task"
DIRECT_RESULT=""
TEMPORAL_RESULT=""

# drive_to_running applies the task and resumes it to Running.
drive_to_running() {
  write_task "${TASK}" "${WORKDIR}/task.yaml"
  run ax apply -f "${WORKDIR}/task.yaml" >/dev/null || true
  wait_for_phase "${TASK}" Suspended 30 || true
  run ax resume task "${TASK}" >/dev/null || true
  wait_for_phase "${TASK}" Running 30 \
    || note "phase is $(task_phase "${TASK}")"
}

run_direct() {
  step "DIRECT path (--orchestrator=direct)"
  start_redis
  # The suspend fault is on the whole time. The direct path only logs it.
  start_fake --fail-suspend
  start_server direct

  sub "drive the task to Running"
  drive_to_running
  ok "task phase is $(task_phase "${TASK}"), actor is $(actor_state "${TASK}")"

  sub "suspend the task while SuspendActor is failing"
  if run ax suspend task "${TASK}"; then
    note "suspend returned success (exit 0)"
  else
    note "suspend returned an error"
  fi

  local phase; phase="$(task_phase "${TASK}")"
  local state; state="$(actor_state "${TASK}")"
  echo "ax says phase=${phase}; fake says actor=${state}"
  note "fake: $(introspect)"

  DIRECT_RESULT="ax=${phase}, actor=${state}"
  stop_stack
}

run_temporal() {
  step "TEMPORAL path (--orchestrator=temporal)"
  start_redis
  # No fault yet, so the task can be driven to Running cleanly.
  start_fake
  start_temporal
  start_server temporal

  sub "drive the task to Running"
  drive_to_running
  ok "task phase is $(task_phase "${TASK}"), actor is $(actor_state "${TASK}")"

  sub "turn the suspend fault on now that the task is running"
  knob fail-suspend=true

  sub "suspend the task while SuspendActor is failing"
  if run ax suspend task "${TASK}"; then
    note "suspend returned success (exit 0)"
  else
    note "suspend returned an error (exit non-zero), which is the point"
  fi

  local phase; phase="$(task_phase "${TASK}")"
  local state; state="$(actor_state "${TASK}")"
  echo "ax says phase=${phase}; fake says actor=${state}"
  note "fake: $(introspect)"
  # Let the background retry stop mattering before teardown.
  knob fail-suspend=false

  TEMPORAL_RESULT="ax=${phase}, actor=${state}"
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
  echo "direct:   ${DIRECT_RESULT} -> reported Suspended, actor still RUNNING (divergence)"
  echo "temporal: ${TEMPORAL_RESULT} -> suspend failed loudly, no false success"
}

main "$@"
