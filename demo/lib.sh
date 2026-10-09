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

# Shared setup for the ax demos. Each scenario runs the real ax control plane on a
# laptop against a fake Substrate backend (fakecontrol), injects one Substrate
# fault, and compares --orchestrator=direct with --orchestrator=temporal.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${REPO_ROOT}/bin"

# The ports sit away from the usual ones so a demo doesn't collide with servers
# you already run. fakecontrol uses FAKE_PORT and the two ports after it.
REDIS_PORT="${AX_DEMO_REDIS_PORT:-6399}"
SERVER_ADDR="127.0.0.1:${AX_DEMO_SERVER_PORT:-8111}"
TEMPORAL_PORT="${AX_DEMO_TEMPORAL_PORT:-7244}"
FAKE_PORT="${AX_DEMO_FAKE_PORT:-9101}"
FAULT_URL="http://127.0.0.1:$((FAKE_PORT + 2))"

if [[ -t 1 && -z "${NO_COLOR:-}" ]]; then
  BOLD=$'\e[1m' DIM=$'\e[2m' GREEN=$'\e[32m' RED=$'\e[31m' YELLOW=$'\e[33m' RESET=$'\e[0m'
else
  BOLD="" DIM="" GREEN="" RED="" YELLOW="" RESET=""
fi

step() { printf '\n%s== %s%s\n' "${BOLD}" "$*" "${RESET}"; }
sub() { printf '%s-- %s%s\n' "${BOLD}" "$*" "${RESET}"; }
note() { printf '%s%s%s\n' "${YELLOW}" "$*" "${RESET}"; }
run() { printf '%s$ %s%s\n' "${DIM}" "$*" "${RESET}"; "$@"; }
fail() { printf '%s%s%s\n' "${RED}" "$*" "${RESET}" >&2; exit 1; }

# ax runs the CLI against the demo's ax-server, so commands read as you'd type them.
ax() { "${BIN}/ax" --server="${SERVER_ADDR}" "$@"; }

WORKDIR=""
SERVER_PID=""
PIDS=()
RESULT_direct=""
RESULT_temporal=""
MISSED=0

# setup checks the tools, builds the binaries, and arms a trap that stops every
# process the demo started, however it ends.
setup() {
  local tool
  for tool in go redis-server redis-cli temporal curl; do
    command -v "${tool}" >/dev/null || fail "missing ${tool}; see demo/README.md"
  done
  WORKDIR="${AX_DEMO_WORKDIR:-/tmp/ax-demo}/$1"
  rm -rf "${WORKDIR}"
  mkdir -p "${WORKDIR}" "${BIN}"
  trap teardown EXIT
  trap 'exit 1' INT TERM
  step "Building ax, ax-server, and fakecontrol (logs in ${WORKDIR})"
  (cd "${REPO_ROOT}" && go build -o "${BIN}/" ./cmd/ax ./cmd/ax-server \
    ./internal/substrate/substratetest/cmd/fakecontrol)
}

# wait_until retries a command until it succeeds or $1 seconds pass.
wait_until() {
  local deadline=$((SECONDS + $1))
  shift
  until "$@" >/dev/null 2>&1; do
    ((SECONDS < deadline)) || return 1
    sleep 0.5
  done
}

# boot starts Redis, fakecontrol, a Temporal dev server for the temporal path, and
# ax-server in the given mode.
boot() {
  redis-server --port "${REDIS_PORT}" --save '' --appendonly no >"${WORKDIR}/redis.log" 2>&1 &
  PIDS+=("$!")
  "${BIN}/fakecontrol" --addr "127.0.0.1:${FAKE_PORT}" \
    --sandbox-addr "127.0.0.1:$((FAKE_PORT + 1))" \
    --fault-addr "127.0.0.1:$((FAKE_PORT + 2))" >"${WORKDIR}/fakecontrol.log" 2>&1 &
  PIDS+=("$!")
  if [[ "$1" == temporal ]]; then
    temporal server start-dev --headless --port "${TEMPORAL_PORT}" \
      --http-port $((TEMPORAL_PORT + 1)) --metrics-port $((TEMPORAL_PORT + 2)) \
      --log-level error >"${WORKDIR}/temporal.log" 2>&1 &
    PIDS+=("$!")
    wait_until 90 temporal operator cluster health --address "127.0.0.1:${TEMPORAL_PORT}" \
      || fail "the Temporal dev server did not start; see ${WORKDIR}/temporal.log"
  fi
  wait_until 10 redis-cli -p "${REDIS_PORT}" ping || fail "redis did not start"
  wait_until 10 curl -sf "${FAULT_URL}/actors" || fail "fakecontrol did not start"
  start_server "$1"
}

# start_server starts ax-server on its own, so a scenario can crash and restart it.
start_server() {
  local args=(--orchestrator="$1" --addr "${SERVER_ADDR}"
    --redis-addr "127.0.0.1:${REDIS_PORT}"
    --substrate-endpoint "127.0.0.1:${FAKE_PORT}" --substrate-plaintext)
  if [[ "$1" == temporal ]]; then
    args+=(--temporal-address "127.0.0.1:${TEMPORAL_PORT}")
  fi
  "${BIN}/ax-server" "${args[@]}" >>"${WORKDIR}/ax-server.log" 2>&1 &
  SERVER_PID="$!"
  wait_until 30 curl -sf "http://${SERVER_ADDR}/healthz" \
    || fail "ax-server did not start; see ${WORKDIR}/ax-server.log"
}

# crash_server kills ax-server without a graceful shutdown, so work in flight is
# abandoned rather than finished.
crash_server() {
  kill -9 "${SERVER_PID}" 2>/dev/null || true
  wait "${SERVER_PID}" 2>/dev/null || true
}

# teardown stops everything boot started, so the next path starts clean on the
# same ports.
teardown() {
  local pid
  if [[ -n "${SERVER_PID}" ]]; then crash_server; fi
  for pid in ${PIDS[@]+"${PIDS[@]}"}; do kill "${pid}" 2>/dev/null || true; done
  for pid in ${PIDS[@]+"${PIDS[@]}"}; do wait "${pid}" 2>/dev/null || true; done
  PIDS=()
  SERVER_PID=""
}

# write_task writes a minimal task. The fake accepts any image, so no container
# runs.
write_task() {
  cat >"${WORKDIR}/task.yaml" <<YAML
apiVersion: ax.io/v1alpha1
kind: Task
metadata:
  name: ${TASK}
  atespace: default
spec:
  image: "example.com/ax-task-runner:demo"
YAML
}

# phase is what ax recorded for the task. actor is what the fake, standing in for
# Substrate, really holds. The demos compare the two.
phase() { ax describe task "${TASK}" 2>/dev/null | awk '/^Phase:/{print $2}' || true; }
actor() {
  local state
  state="$(curl -s "${FAULT_URL}/actors" | grep -oE "\"${TASK}\":\"[A-Z_]+\"" \
    | cut -d'"' -f4 || true)"
  echo "${state:-none}"
}
phase_is() { [[ "$(phase)" == "$1" ]]; }
fault() { curl -sf -X POST "${FAULT_URL}/fault?$1" >/dev/null || fail "fakecontrol rejected $1"; }

# attempts prints the highest attempt ax-server logged for an activity, which is
# the evidence that Temporal retried it.
attempts() {
  grep -h "ActivityType=$1" "${WORKDIR}/ax-server.log" 2>/dev/null \
    | grep -oE 'Attempt=[0-9]+' | cut -d= -f2 | sort -n | tail -1 || true
}

# check prints one verdict line by comparing what was observed with what was
# expected, and remembers a miss so a scenario can't claim a contrast it didn't
# see.
check() {
  if [[ "$2" == "$3" ]]; then
    printf '%s%-9s %s%s\n' "${GREEN}" "$1:" "$4" "${RESET}"
  else
    printf '%s%-9s expected "%s", got "%s"%s\n' "${RED}" "$1:" "$3" "$2" "${RESET}"
    MISSED=1
  fi
}

# finish ends a scenario, and exits non-zero if any check missed.
finish() {
  if ((MISSED)); then fail "did not reproduce; see the log above"; fi
  printf '%sreproduced: %s%s\n' "${GREEN}" "$1" "${RESET}"
}
