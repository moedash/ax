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

# Scenario A. A transient Substrate blip.
#
# The fake fails the first CreateActor call and serves every call after it. On
# the direct path the one failed call fails the task for good, and applying it
# again is rejected because a task is immutable. On the Temporal path the
# activity is retried and the task recovers.

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

TASK="blip-task"
DIRECT_RESULT=""
DIRECT_REAPPLY=""
TEMPORAL_RESULT=""
TEMPORAL_ATTEMPT=""

run_direct() {
  step "DIRECT path (--orchestrator=direct)"
  start_redis
  # The first CreateActor fails with Unavailable, then the fake serves the rest.
  start_fake --fail-create-actor-times 1
  start_server direct

  write_task "${TASK}" "${WORKDIR}/task.yaml"
  sub "apply the task; the first CreateActor hits the blip"
  if run ax apply -f "${WORKDIR}/task.yaml"; then
    note "apply returned success"
  else
    note "apply returned an error (the direct path gives up inline)"
  fi

  sub "ax get task right after apply"
  run ax get task "${TASK}" | grep -E 'phase:|actor:' || true
  sleep 2
  sub "and again a moment later, to show nothing re-drives it"
  DIRECT_RESULT="$(task_phase "${TASK}")"
  echo "phase stays ${DIRECT_RESULT}"

  # The blip is over, so this is the first fix an operator would reach for.
  sub "the blip is over; try the obvious fix and apply the task again"
  if run ax apply -f "${WORKDIR}/task.yaml"; then
    DIRECT_REAPPLY="accepted"
  else
    DIRECT_REAPPLY="rejected"
  fi
  note "fake: $(introspect)"
  stop_stack
}

run_temporal() {
  step "TEMPORAL path (--orchestrator=temporal)"
  start_redis
  start_fake --fail-create-actor-times 1
  start_temporal
  start_server temporal

  write_task "${TASK}" "${WORKDIR}/task.yaml"
  sub "apply the task; EnsureActor hits the same blip and is retried"
  run ax apply -f "${WORKDIR}/task.yaml" || true
  if wait_for_phase "${TASK}" Suspended 30; then
    ok "apply recovered; the task settled Suspended (a task is created suspended)"
  else
    err "apply did not settle; phase is $(task_phase "${TASK}")"
  fi

  sub "resume the task to put its actor on a worker"
  run ax resume task "${TASK}" || true
  if wait_for_phase "${TASK}" Running 30; then
    ok "task reached Running"
  else
    err "task did not reach Running; phase is $(task_phase "${TASK}")"
  fi
  note "fake: $(introspect)"
  TEMPORAL_ATTEMPT="$(max_attempt EnsureActor)"
  note "highest EnsureActor attempt in the ax-server log: ${TEMPORAL_ATTEMPT:-none}"

  TEMPORAL_RESULT="$(task_phase "${TASK}")"
  stop_stack
}

main() {
  printf '%s\nScenario A: a transient Substrate blip (first CreateActor fails)%s\n' \
    "${BOLD}" "${RESET}"
  demo_init scenario-a
  demo_build
  run_direct
  run_temporal

  step "VERDICT"
  check direct "${DIRECT_RESULT}/${DIRECT_REAPPLY}" '^Failed/rejected$' \
    "Failed after one blip. Re-applying is rejected, so a human must delete and redo it." \
    "got ${DIRECT_RESULT:-none}/${DIRECT_REAPPLY:-none}, expected Failed/rejected"
  check temporal "${TEMPORAL_RESULT}/${TEMPORAL_ATTEMPT:-0}" '^Running/([2-9]|[1-9][0-9]+)$' \
    "Running. EnsureActor failed once, was retried, and the task recovered on its own." \
    "got ${TEMPORAL_RESULT:-none} with ${TEMPORAL_ATTEMPT:-0} attempts, expected Running and 2+"
  summarize "the same blip ended the task on direct and was absorbed on Temporal."
}

main "$@"
