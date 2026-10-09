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
# The first CreateActor call fails once with Unavailable. On its own, ax fails the
# task, and applying it again is rejected because a task is immutable. On
# Temporal the activity is retried and the task runs.

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
TASK=blip-task

play() {
  step "$1 path"
  boot "$1"
  fault create-actor-failures=1
  write_task
  sub "apply the task while the first CreateActor fails"
  run ax apply -f "${WORKDIR}/task.yaml" || true
  if [[ "$1" == direct ]]; then
    sub "the blip is over; apply the task again"
    local reapply=accepted
    run ax apply -f "${WORKDIR}/task.yaml" || reapply=rejected
    printf -v RESULT_direct '%s, re-apply %s' "$(phase)" "${reapply}"
  else
    wait_until 30 phase_is Suspended || true
    sub "resume the task"
    run ax resume task "${TASK}" || true
    wait_until 30 phase_is Running || true
    local retried=no tries
    tries="$(attempts EnsureActor)"
    if ((${tries:-0} >= 2)); then retried=yes; fi
    printf -v RESULT_temporal '%s, retried %s' "$(phase)" "${retried}"
  fi
  note "ax says $(phase), the fake holds actor $(actor)"
  teardown
}

setup scenario-a
play direct
play temporal
step "verdict"
check direct "${RESULT_direct}" "Failed, re-apply rejected" \
  "Failed after one blip, and re-applying is rejected. A human has to delete and redo it."
check temporal "${RESULT_temporal}" "Running, retried yes" \
  "Running. EnsureActor was retried and the task recovered on its own."
finish "the same blip ended the task on direct and was absorbed on Temporal."
