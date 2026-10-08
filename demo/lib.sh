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

# Shared boot and teardown for the ax demos. It runs the whole ax control plane
# on a laptop against a FAKE Substrate backend. The ax behavior is real on both
# paths. Only the cluster is a stand-in, so the demos stay honest about which
# part is being shown.

set -euo pipefail

DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${DEMO_DIR}/.." && pwd)"

# Ports are off the beaten path so a demo does not fight a server you already
# run. Override any of them from the environment.
REDIS_PORT="${AX_DEMO_REDIS_PORT:-6399}"
FAKE_CONTROL_PORT="${AX_DEMO_FAKE_CONTROL_PORT:-9101}"
FAKE_SANDBOX_PORT="${AX_DEMO_FAKE_SANDBOX_PORT:-9102}"
FAKE_HTTP_PORT="${AX_DEMO_FAKE_HTTP_PORT:-9103}"
SERVER_PORT="${AX_DEMO_SERVER_PORT:-8111}"
TEMPORAL_PORT="${AX_DEMO_TEMPORAL_PORT:-7244}"

SERVER_ADDR="127.0.0.1:${SERVER_PORT}"
FAKE_CONTROL_ADDR="127.0.0.1:${FAKE_CONTROL_PORT}"
FAKE_SANDBOX_ADDR="127.0.0.1:${FAKE_SANDBOX_PORT}"
FAKE_HTTP="http://127.0.0.1:${FAKE_HTTP_PORT}"
TEMPORAL_ADDR="127.0.0.1:${TEMPORAL_PORT}"

if [[ -t 1 && -z "${NO_COLOR:-}" ]]; then
  BOLD=$'\e[1m'; DIM=$'\e[2m'; CYAN=$'\e[36m'; GREEN=$'\e[32m'
  YELLOW=$'\e[33m'; RED=$'\e[31m'; RESET=$'\e[0m'
else
  BOLD=""; DIM=""; CYAN=""; GREEN=""; YELLOW=""; RED=""; RESET=""
fi

WORKDIR=""
AX=""; AX_SERVER=""; FAKECONTROL=""
SERVER_PID=""
SERVER_MODE=""
declare -a DEMO_PIDS=()

# demo_init prepares a clean log directory and arms the trap that cleans up every
# process on exit, however the script ends.
demo_init() {
  WORKDIR="${AX_DEMO_WORKDIR:-/tmp/ax-demo}/$1"
  rm -rf "${WORKDIR}"
  mkdir -p "${WORKDIR}"
  trap demo_cleanup EXIT INT TERM
  note "logs and manifests under ${WORKDIR}"
}

# demo_build builds the three binaries the demos drive. gopls diagnostics for
# this clone are noise, so the shell build is the source of truth.
demo_build() {
  step "Building ax, ax-server, and the fake control plane"
  ( cd "${REPO_ROOT}" \
      && go build -o bin/ax ./cmd/ax \
      && go build -o bin/ax-server ./cmd/ax-server \
      && go build -o bin/fakecontrol ./internal/substrate/substratetest/cmd/fakecontrol )
  AX="${REPO_ROOT}/bin/ax"
  AX_SERVER="${REPO_ROOT}/bin/ax-server"
  FAKECONTROL="${REPO_ROOT}/bin/fakecontrol"
  ok "binaries built"
}

# Presentation helpers.
step() { printf '\n%s%s== %s%s\n' "${BOLD}" "${CYAN}" "$*" "${RESET}"; }
sub()  { printf '%s-- %s%s\n' "${BOLD}" "$*" "${RESET}"; }
run()  { printf '%s$ %s%s\n' "${DIM}" "$*" "${RESET}"; "$@"; }
ok()   { printf '%s* %s%s\n' "${GREEN}" "$*" "${RESET}"; }
note() { printf '%s%s%s\n' "${YELLOW}" "$*" "${RESET}"; }
err()  { printf '%s! %s%s\n' "${RED}" "$*" "${RESET}"; }

# ax runs the CLI against the demo server, so the commands read the way you would
# type them.
ax() { "${AX}" --server="${SERVER_ADDR}" "$@"; }

# write_task writes a minimal task manifest. The fake accepts any image, so the
# image is a placeholder and no real container runs.
write_task() {
  local name="$1" file="$2"
  cat >"${file}" <<YAML
apiVersion: ax.io/v1alpha1
kind: Task
metadata:
  name: ${name}
  atespace: default
spec:
  image: "example.com/ax-task-runner:demo"
YAML
}

# --- backend lifecycle ---

start_redis() {
  redis-server --port "${REDIS_PORT}" --save '' --appendonly no \
    >"${WORKDIR}/redis.log" 2>&1 &
  DEMO_PIDS+=("$!")
  for _ in $(seq 1 50); do
    if redis-cli -p "${REDIS_PORT}" ping >/dev/null 2>&1; then return 0; fi
    sleep 0.2
  done
  err "redis did not come up on ${REDIS_PORT}"; return 1
}

# start_fake starts the fake control plane. Extra arguments are fault flags, for
# example --fail-create-actor-times 1.
start_fake() {
  "${FAKECONTROL}" \
    --addr "${FAKE_CONTROL_ADDR}" \
    --sandbox-addr "${FAKE_SANDBOX_ADDR}" \
    --control-addr "127.0.0.1:${FAKE_HTTP_PORT}" \
    "$@" >"${WORKDIR}/fakecontrol.log" 2>&1 &
  DEMO_PIDS+=("$!")
  for _ in $(seq 1 50); do
    if curl -sf "${FAKE_HTTP}/introspect" >/dev/null 2>&1; then return 0; fi
    sleep 0.2
  done
  err "fake control plane did not come up"; return 1
}

start_temporal() {
  temporal server start-dev \
    --headless \
    --port "${TEMPORAL_PORT}" \
    --http-port "$((TEMPORAL_PORT + 1))" \
    --metrics-port "$((TEMPORAL_PORT + 2))" \
    --log-level error \
    >"${WORKDIR}/temporal.log" 2>&1 &
  DEMO_PIDS+=("$!")
  for _ in $(seq 1 90); do
    if temporal operator cluster health --address "${TEMPORAL_ADDR}" \
        >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  err "temporal dev server did not come up on ${TEMPORAL_ADDR}"; return 1
}

# start_server starts ax-server in the given mode and records its PID on its own,
# so a scenario can kill and restart it to model a crash.
start_server() {
  SERVER_MODE="$1"
  local args=(
    --orchestrator="${SERVER_MODE}"
    --addr "${SERVER_ADDR}"
    --redis-addr "127.0.0.1:${REDIS_PORT}"
    --substrate-endpoint "${FAKE_CONTROL_ADDR}"
    --substrate-plaintext
  )
  if [[ "${SERVER_MODE}" == "temporal" ]]; then
    args+=(--temporal-address "${TEMPORAL_ADDR}")
  fi
  "${AX_SERVER}" "${args[@]}" >>"${WORKDIR}/ax-server.log" 2>&1 &
  SERVER_PID="$!"
  for _ in $(seq 1 60); do
    if curl -sf "http://${SERVER_ADDR}/healthz" >/dev/null 2>&1; then return 0; fi
    if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
      err "ax-server exited early; see ${WORKDIR}/ax-server.log"; return 1
    fi
    sleep 0.5
  done
  err "ax-server did not come up on ${SERVER_ADDR}"; return 1
}

# crash_server kills ax-server the hard way, so an in-flight call is abandoned
# rather than finished during a graceful shutdown.
crash_server() {
  [[ -n "${SERVER_PID}" ]] && kill -9 "${SERVER_PID}" 2>/dev/null || true
  wait "${SERVER_PID}" 2>/dev/null || true
  SERVER_PID=""
}

# stop_stack tears down everything a phase started, so the next phase gets a
# fresh backend on the same ports.
stop_stack() {
  crash_server
  for pid in "${DEMO_PIDS[@]:-}"; do
    [[ -n "${pid}" ]] && kill "${pid}" 2>/dev/null || true
  done
  for pid in "${DEMO_PIDS[@]:-}"; do
    [[ -n "${pid}" ]] && wait "${pid}" 2>/dev/null || true
  done
  DEMO_PIDS=()
}

demo_cleanup() {
  trap - EXIT INT TERM
  stop_stack
}

# --- fake control plane introspection ---

# introspect prints what the fake really holds.
introspect() { curl -s "${FAKE_HTTP}/introspect"; }

# actor_state prints the fake's state for one actor, for example
# ACTOR_STATE_RUNNING. It reads the key inside the "actors" map, which is the one
# place a state string follows an actor name.
actor_state() {
  introspect | grep -oE "\"$1\":\"ACTOR_STATE_[A-Z]+\"" \
    | grep -oE "ACTOR_STATE_[A-Z]+" | head -1
}

# knob flips a fault on the running fake, for example knob fail-suspend=true.
knob() { curl -s -X POST "${FAKE_HTTP}/knobs?$1" >/dev/null; }

# --- task helpers ---

# task_phase prints the phase ax reports for a task, on either path. The list
# view carries no phase on the Temporal path, so this reads the task itself.
task_phase() {
  ax describe task "$1" 2>/dev/null | awk '/^Phase:/{print $2}'
}

# wait_for_phase polls until a task reaches a phase or the timeout runs out.
wait_for_phase() {
  local name="$1" want="$2" timeout="${3:-60}" waited=0
  while (( waited < timeout )); do
    if [[ "$(task_phase "${name}")" == "${want}" ]]; then return 0; fi
    sleep 2; waited=$((waited + 2))
  done
  return 1
}
