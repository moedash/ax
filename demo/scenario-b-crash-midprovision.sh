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

# Scenario B. ax-server crashes while it is placing an actor on a worker.
#
# The fake holds every resume open, so ax-server can be killed with one in
# flight. Both clients see the crash. On its own, ax loses the resume and the
# task stays Suspended. On Temporal the restarted worker finishes it. Temporal
# notices the dead worker when its one-minute heartbeat lapses, so both paths
# get the same, longer wait.

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
TASK=crash-task
WAIT="${AX_DEMO_RECOVER_WAIT:-120}"

play() {
  step "$1 path"
  boot "$1"
  write_task
  run ax apply -f "${WORKDIR}/task.yaml" || true
  wait_until 30 phase_is Suspended || true

  sub "resume in the background while the fake holds the resume open"
  fault resume-delay=25s
  ax resume task "${TASK}" >"${WORKDIR}/resume.log" 2>&1 &
  PIDS+=("$!")
  sleep 6
  sub "kill ax-server with the resume in flight, then restart it with Substrate healthy"
  crash_server
  fault resume-delay=0
  start_server "$1"
  note "the client that asked for the resume saw: $(tail -1 "${WORKDIR}/resume.log")"

  sub "wait up to ${WAIT}s for the task to reach Running"
  local start=${SECONDS}
  if wait_until "${WAIT}" phase_is Running; then
    note "Running $((SECONDS - start))s after the restart"
  else
    note "still $(phase) after ${WAIT}s"
  fi
  printf -v "RESULT_$1" '%s, actor %s' "$(phase)" "$(actor)"
  teardown
}

setup scenario-b
play direct
play temporal
step "verdict"
check direct "${RESULT_direct}" "Suspended, actor SUSPENDED" \
  "Suspended. The resume died with ax-server, and nothing finished it."
check temporal "${RESULT_temporal}" "Running, actor RUNNING" \
  "Running. The restarted worker finished the resume that was in flight."
finish "both clients saw the crash. Only Temporal finished the work it had accepted."
