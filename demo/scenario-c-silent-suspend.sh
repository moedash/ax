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

# Scenario C. A failed suspend.
#
# Both paths run the same steps. Drive a task to Running, make SuspendActor fail,
# ask for a suspend, then clear the fault and give both paths the same time to
# settle. On its own, ax reports the suspend as done while the actor keeps
# running, and the record stays wrong. On Temporal the suspend is reported as
# pending, and the workflow finishes it once Substrate recovers.

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
TASK=suspend-task
WAIT="${AX_DEMO_SETTLE_WAIT:-45}"

# settled is true once the record and the actor agree the task is suspended.
settled() { phase_is Suspended && [[ "$(actor)" == SUSPENDED ]]; }

play() {
  step "$1 path"
  boot "$1"
  write_task
  sub "drive the task to Running"
  run ax apply -f "${WORKDIR}/task.yaml" || true
  wait_until 30 phase_is Suspended || true
  run ax resume task "${TASK}" || true
  wait_until 30 phase_is Running || true
  note "ax says $(phase), the fake holds actor $(actor)"

  sub "make SuspendActor fail, then ask for a suspend"
  fault suspend=fail
  local code=0
  run ax suspend task "${TASK}" || code=$?
  local during
  during="exit ${code}, $(phase), actor $(actor)"
  note "${during}"

  sub "Substrate recovers; wait up to ${WAIT}s for the record and the actor to agree"
  fault suspend=ok
  wait_until "${WAIT}" settled || true
  printf -v "RESULT_$1" '%s -> %s, actor %s' "${during}" "$(phase)" "$(actor)"
  note "now $(phase), the fake holds actor $(actor)"
  teardown
}

setup scenario-c
play direct
play temporal
step "verdict"
check direct "${RESULT_direct}" \
  "exit 0, Suspended, actor RUNNING -> Suspended, actor RUNNING" \
  "said the suspend worked while the actor kept RUNNING, and the record stayed wrong."
check temporal "${RESULT_temporal}" \
  "exit 1, Running, actor RUNNING -> Suspended, actor SUSPENDED" \
  "reported the suspend as pending, then finished it. Record and actor agree."
finish "the direct record lies about the actor. The Temporal record never does."
