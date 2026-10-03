#!/usr/bin/env bash

# Copyright 2026 checkpointd authors
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

# Runs an interactive chat with chat-agent, which self-crashes on roughly half the turns (at most once per turn), so you can watch checkpointd recover it live.
#
# Usage: examples/agent-crash-demo/demo.sh (type a message and press enter, Ctrl-C to quit)
#
# Env overrides (must match whatever setup.sh was actually run with):
#   KIND_CLUSTER_NAME   kind cluster to talk to (default: checkpointd-agent-crash-demo)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

log() { echo "[agent-crash-demo] $*" >&2; }
fail() { echo "[agent-crash-demo] FAIL: $*" >&2; exit 1; }

NS="checkpointd-agent-crash-demo"
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-checkpointd-agent-crash-demo}"
KUBECTL_CONTEXT="kind-${KIND_CLUSTER_NAME}"
LOCAL_PORT=""
# Substrate labels each WorkerPool's own pods ate.dev/worker-pool=<name>, not the workload= label on the WorkerPool object itself.
WORKER_LABEL="ate.dev/worker-pool=checkpointd-agent-crash-demo-harness"

run_kubectl() { kubectl --context "$KUBECTL_CONTEXT" "$@"; }

command -v kubectl >/dev/null 2>&1 || fail "kubectl not found on PATH"
command -v go >/dev/null 2>&1 || fail "go not found on PATH"
command -v jq >/dev/null 2>&1 || fail "jq not found on PATH"
command -v curl >/dev/null 2>&1 || fail "curl not found on PATH"
run_kubectl get namespace "$NS" >/dev/null 2>&1 \
  || fail "namespace $NS not found in context $KUBECTL_CONTEXT -- run setup.sh first"

# --- Install the a2a CLI, pinned to this repo's own a2a-go version -----
A2A_CLI_VERSION="v2.5.0"
A2A_CLI_BIN_DIR="$(mktemp -d -t checkpointd-agent-crash-demo-a2a-cli.XXXXXXXX)"
log "installing the a2a CLI (github.com/a2aproject/a2a-go/v2/cmd/a2a@$A2A_CLI_VERSION)"
GOBIN="$A2A_CLI_BIN_DIR" go install "github.com/a2aproject/a2a-go/v2/cmd/a2a@$A2A_CLI_VERSION" \
  || fail "could not go install the a2a CLI"
A2A_CLI="$A2A_CLI_BIN_DIR/a2a"

PORT_FORWARD_PID=""
CRASH_WATCH_PID=""
# startCrashWatch writes its running tally here as it observes each crash live, since a fresh kubectl logs query
# at exit time can race the container runtime and miss the just-printed last line.
CRASH_COUNT_FILE="$(mktemp)"
echo 0 >"$CRASH_COUNT_FILE"
cleanup() {
  if [[ -n "$PORT_FORWARD_PID" ]]; then
    kill "$PORT_FORWARD_PID" 2>/dev/null || true
    wait "$PORT_FORWARD_PID" 2>/dev/null || true
  fi
  if [[ -n "$CRASH_WATCH_PID" ]]; then
    kill "$CRASH_WATCH_PID" 2>/dev/null || true
    wait "$CRASH_WATCH_PID" 2>/dev/null || true
  fi

  # This is a lower bound, since a crash in the last ~2s before exit may not have been polled yet.
  crash_count="$(cat "$CRASH_COUNT_FILE" 2>/dev/null || echo 0)"
  log "chat-agent self-crashed at least $crash_count time(s) during this session"
  rm -f "$CRASH_COUNT_FILE"
  rm -rf "$A2A_CLI_BIN_DIR"
}
trap cleanup EXIT
# A plain command substitution defers a pending trap until it finishes, so Ctrl-C would otherwise wait out the current a2a call; killing CMD_PID directly makes it immediate.
trap '[[ -n "$CMD_PID" ]] && kill "$CMD_PID" 2>/dev/null; exit 130' INT

# runA2A runs its arguments in the background and sets CMD_OUT to their combined output, so CMD_PID is always known and killable.
CMD_PID=""
CMD_OUT=""
runA2A() {
  local tmp status
  tmp="$(mktemp)"
  "$@" >"$tmp" 2>&1 &
  CMD_PID=$!
  wait "$CMD_PID"
  status=$?
  CMD_PID=""
  CMD_OUT="$(cat "$tmp")"
  rm -f "$tmp"
  return "$status"
}

# startCrashWatch prints chat-agent's own "CRASH:" marker in real time, best-effort, by tailing each worker pod's log since the last line already seen from it.
startCrashWatch() {
  (
    # A quiet poll makes grep exit 1, which would kill this loop under pipefail, so both are disabled here.
    set +e +o pipefail
    declare -A since
    declare -A seen
    count=0
    watchStart="$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
    while true; do
      # Pods are re-listed every iteration so a replacement pod after a crash is picked up too.
      for pod in $(run_kubectl -n "$NS" get pods -l "$WORKER_LABEL" -o name 2>/dev/null); do
        out="$(run_kubectl -n "$NS" logs "$pod" --all-containers --timestamps \
          --since-time="${since[$pod]:-$watchStart}" --ignore-errors 2>/dev/null)"
        [[ -z "$out" ]] && continue
        since[$pod]="$(tail -1 <<<"$out" | awk '{print $1}')"
        # Triggered on the runtime's own "panic: <message>" line alone: Go only prints it on an unrecovered
        # panic, so by itself it's independent confirmation the actor actually died, and it always follows the
        # app's own CRASH log line, so there's no need to also match and print that one.
        while IFS= read -r line; do
          [[ "$line" == *"panic: "* ]] || continue
          key="$pod $line"
          [[ -n "${seen[$key]:-}" ]] && continue
          seen[$key]=1
          json="${line#* }"
          container="$(jq -r '.labels["ate.actor.container.name"] // empty' <<<"$json" 2>/dev/null)"
          # Only chat-agent ever panics by design; a panic from anything else isn't this demo's crash.
          [[ "$container" == "chat-agent" ]] || continue
          msg="$(jq -r '.message' <<<"$json" 2>/dev/null)"
          count=$((count + 1))
          echo "$count" >"$CRASH_COUNT_FILE"
          echo "[agent-crash-demo] Detected crash of chat-agent: \"$msg\""
        done <<<"$out"
      done
      sleep 2
    done
  ) &
  CRASH_WATCH_PID=$!
}

# start_port_forward retries the whole `kubectl port-forward` process, up to
# 3 times, in case the target pod happens to be mid-recreate.
start_port_forward() {
  local target="svc/checkpointd-server"
  local attempt pf_log
  pf_log="$(mktemp)"
  for attempt in 1 2 3; do
    if [[ -n "$PORT_FORWARD_PID" ]]; then
      kill "$PORT_FORWARD_PID" 2>/dev/null || true
      wait "$PORT_FORWARD_PID" 2>/dev/null || true
    fi
    log "port-forwarding $target (attempt $attempt/3)"
    : >"$pf_log"
    run_kubectl -n "$NS" port-forward "$target" "${LOCAL_PORT:-}:80" \
      >"$pf_log" 2>&1 &
    PORT_FORWARD_PID=$!
    local i
    for i in $(seq 1 50); do
      LOCAL_PORT="$(sed -nE 's#^Forwarding from 127\.0\.0\.1:([0-9]+) ->.*#\1#p' "$pf_log" | tail -1)"
      if [[ -n "$LOCAL_PORT" ]] && (exec 3<>"/dev/tcp/127.0.0.1/$LOCAL_PORT") 2>/dev/null; then
        exec 3<&- 3>&-
        AGENT_URL="http://127.0.0.1:$LOCAL_PORT/agents/chat-agent/"
        log "reachable on localhost:$LOCAL_PORT"
        rm -f "$pf_log"
        return 0
      fi
      sleep 0.2
    done
    log "never became reachable within 10s"
  done
  rm -f "$pf_log"
  fail "port-forward to $target never became reachable after 3 attempts"
}

AGENT_URL=""
TASK_ID=""
TENANT=""

# waitForAgentReady polls chat-agent's AgentCard endpoint until it responds, or fails after 3 minutes.
waitForAgentReady() {
  local i
  for i in $(seq 1 180); do
    curl -fsS -o /dev/null "http://127.0.0.1:$LOCAL_PORT/agents/chat-agent/.well-known/agent-card.json" 2>/dev/null && return 0
    sleep 1
  done
  fail "chat-agent's own AgentCard never became reachable through checkpointd-server within 3 minutes"
}

# pollUntilStopped polls GetTask every 3s, up to 5 minutes, until the task reaches a stopping state, and sets POLL_OUT to its JSON.
# Must be called as a plain statement, never in a subshell, or runA2A's CMD_PID won't be visible to the INT trap.
POLL_OUT=""
pollUntilStopped() {
  local i cur state
  for i in $(seq 1 100); do
    runA2A "$A2A_CLI" --transport jsonrpc --timeout 30s get task "$AGENT_URL" "$TASK_ID" --tenant "$TENANT" -o json \
      || { start_port_forward; sleep 2; continue; }
    cur="$CMD_OUT"
    state="$(echo "$cur" | jq -r '.status.state // empty' 2>/dev/null)"
    case "$state" in
      TASK_STATE_INPUT_REQUIRED | TASK_STATE_COMPLETED | TASK_STATE_FAILED | TASK_STATE_CANCELED | TASK_STATE_REJECTED)
        POLL_OUT="$cur"
        return 0
        ;;
    esac
    sleep 3
  done
  return 1
}

# SEND_TIMEOUT is generous enough for a cold turn with two actor starts, two completions, and a possible crash-and-recovery cycle.
SEND_TIMEOUT="240s"

# send sets REPLY to chat-agent's reply text and updates TASK_ID/TENANT, and must be called as a plain statement, never in a subshell.
REPLY=""
send() {
  local text=$1 out attempt
  local -a cmd=("$A2A_CLI" --transport jsonrpc --timeout "$SEND_TIMEOUT" send "$AGENT_URL" -o json)
  [[ -n "$TASK_ID" ]] && cmd+=(--task "$TASK_ID" --tenant "$TENANT")
  cmd+=("$text")
  log "running: ${cmd[*]}"

  # A brand-new send can't be safely retried, since checkpointd may already have created the session before timing out.
  if [[ -z "$TASK_ID" ]]; then
    runA2A "${cmd[@]}" \
      || fail "first send(\"$text\") got no reply within $SEND_TIMEOUT: $(tr '\n' ' ' <<<"$CMD_OUT")"
    out="$CMD_OUT"
    TASK_ID="$(echo "$out" | jq -r '.id // empty')"
    TENANT="$(echo "$out" | jq -r '.metadata["checkpointd-tenant"] // empty')"
    [[ -n "$TASK_ID" ]] || fail "send(\"$text\") returned no task id: $(tr '\n' ' ' <<<"$out")"
    REPLY="$(echo "$out" | jq -r '.status.message.parts[0].text')"
    return
  fi

  for attempt in $(seq 1 4); do
    if runA2A "${cmd[@]}"; then
      out="$CMD_OUT"
      REPLY="$(echo "$out" | jq -r '.status.message.parts[0].text')"
      return
    fi
    out="$CMD_OUT"
    if [[ "$out" == *"is already being processed"* ]]; then
      # The send landed server-side even though this client lost the response, so poll instead of resending.
      pollUntilStopped || fail "task $TASK_ID never reached a stopping state within 5 minutes after \"$text\""
      REPLY="$(echo "$POLL_OUT" | jq -r '.status.message.parts[0].text')"
      return
    fi
    log "send failed (attempt $attempt/4), retrying in 5s: $(tr '\n' ' ' <<<"$out")"
    sleep 5
    start_port_forward
  done
  fail "send(\"$text\") never succeeded after 4 attempts: $(tr '\n' ' ' <<<"$out")"
}

start_port_forward
waitForAgentReady
startCrashWatch

log "=== interactive demo: type a message and press enter, Ctrl-C to quit ==="
while true; do
  printf '> ' >&2
  IFS= read -r prompt || break
  [[ -z "$prompt" ]] && continue

  turn_start_time="$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
  send "$prompt"
  echo "  reply:"
  echo "$REPLY" | sed 's/^/    /'

  crashed_this_turn=0
  for pod in $(run_kubectl -n "$NS" get pods -l "$WORKER_LABEL" -o name 2>/dev/null); do
    n="$(run_kubectl -n "$NS" logs "$pod" --all-containers --since-time="$turn_start_time" --ignore-errors 2>/dev/null | grep -c "CRASH: chat-agent self-destructing" || true)"
    crashed_this_turn=$((crashed_this_turn + n))
  done
  [[ "$crashed_this_turn" -eq 0 ]] && log "crash hasn't been detected in this turn"
done
