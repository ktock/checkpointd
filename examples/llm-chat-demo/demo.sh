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

# Drives chat-agent from outside the cluster: greet, ask a follow-up, restart every kind worker node (checkpointd-server included), ask again, and confirm the conversation log survived.
#
# Usage: examples/llm-chat-demo/demo.sh
#
# Env overrides (must match whatever setup.sh was actually run with):
#   KIND_CLUSTER_NAME   kind cluster to talk to (default: checkpointd-llm-chat-demo)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

log() { echo "[llm-chat-demo] $*" >&2; }
fail() { echo "[llm-chat-demo] FAIL: $*" >&2; exit 1; }

NS="checkpointd-llm-chat-demo"
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-checkpointd-llm-chat-demo}"
KUBECTL_CONTEXT="kind-${KIND_CLUSTER_NAME}"
LOCAL_PORT=""

run_kubectl() { kubectl --context "$KUBECTL_CONTEXT" "$@"; }

command -v kubectl >/dev/null 2>&1 || fail "kubectl not found on PATH"
command -v go >/dev/null 2>&1 || fail "go not found on PATH"
command -v jq >/dev/null 2>&1 || fail "jq not found on PATH"
command -v curl >/dev/null 2>&1 || fail "curl not found on PATH"
run_kubectl get namespace "$NS" >/dev/null 2>&1 \
  || fail "namespace $NS not found in context $KUBECTL_CONTEXT -- run setup.sh first"

# --- Install the a2a CLI, pinned to this repo's own a2a-go version -----
A2A_CLI_VERSION="v2.5.0"
A2A_CLI_BIN_DIR="$(mktemp -d -t checkpointd-llm-chat-demo-a2a-cli.XXXXXXXX)"
log "installing the a2a CLI (github.com/a2aproject/a2a-go/v2/cmd/a2a@$A2A_CLI_VERSION)"
GOBIN="$A2A_CLI_BIN_DIR" go install "github.com/a2aproject/a2a-go/v2/cmd/a2a@$A2A_CLI_VERSION" \
  || fail "could not go install the a2a CLI"
A2A_CLI="$A2A_CLI_BIN_DIR/a2a"

PORT_FORWARD_PID=""
cleanup() {
  if [[ -n "$PORT_FORWARD_PID" ]]; then
    kill "$PORT_FORWARD_PID" 2>/dev/null || true
    wait "$PORT_FORWARD_PID" 2>/dev/null || true
  fi
  rm -rf "$A2A_CLI_BIN_DIR"
}
trap cleanup EXIT

# start_port_forward retries the whole `kubectl port-forward` process, up to 3 times, so a mid-restart target gets re-resolved.
start_port_forward() {
  local attempt pf_log
  pf_log="$(mktemp)"
  for attempt in 1 2 3; do
    if [[ -n "$PORT_FORWARD_PID" ]]; then
      kill "$PORT_FORWARD_PID" 2>/dev/null || true
      wait "$PORT_FORWARD_PID" 2>/dev/null || true
    fi
    log "port-forwarding svc/checkpointd-server (attempt $attempt/3)"
    : >"$pf_log"
    run_kubectl -n "$NS" port-forward svc/checkpointd-server "${LOCAL_PORT:-}:80" \
      >"$pf_log" 2>&1 &
    PORT_FORWARD_PID=$!
    local i
    for i in $(seq 1 50); do
      LOCAL_PORT="$(sed -nE 's#^Forwarding from 127\.0\.0\.1:([0-9]+) ->.*#\1#p' "$pf_log" | tail -1)"
      if [[ -n "$LOCAL_PORT" ]] && (exec 3<>"/dev/tcp/127.0.0.1/$LOCAL_PORT") 2>/dev/null; then
        exec 3<&- 3>&-
        AGENT_URL="http://127.0.0.1:$LOCAL_PORT/agents/chat-agent/"
        log "  reachable on localhost:$LOCAL_PORT"
        rm -f "$pf_log"
        return 0
      fi
      sleep 0.2
    done
    log "  never became reachable within 10s"
  done
  rm -f "$pf_log"
  fail "port-forward to svc/checkpointd-server never became reachable after 3 attempts"
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

# currentReply prints this task's current reply text, or empty if there is no task yet.
currentReply() {
  [[ -n "$TASK_ID" ]] || return 0
  "$A2A_CLI" --transport jsonrpc --timeout 30s get task "$AGENT_URL" "$TASK_ID" --tenant "$TENANT" -o json 2>/dev/null \
    | jq -r '.status.message.parts[0].text // empty' 2>/dev/null || true
}

# send sets REPLY to chat-agent's reply text and updates TASK_ID/TENANT, and must be called as a plain statement, never in a subshell.
REPLY=""
send() {
  local text=$1 out attempt before ok=0
  before="$(currentReply)"
  local -a cmd=("$A2A_CLI" --transport jsonrpc --timeout 120s send "$AGENT_URL" -o json)
  [[ -n "$TASK_ID" ]] && cmd+=(--task "$TASK_ID" --tenant "$TENANT")
  cmd+=("$text")
  log "  running: ${cmd[*]}"
  for attempt in $(seq 1 6); do
    out="$("${cmd[@]}" 2>&1)" && { ok=1; break; }
    if [[ "$out" == *"is already being processed"* ]]; then
      log "  a request for this task is already in flight; polling GetTask instead of resending (up to 5 minutes)"
      if out="$(pollForReplyChange "$before")"; then
        ok=1
        break
      fi
      log "  gave up polling after 5 minutes; trying a fresh send"
    else
      log "  send failed (attempt $attempt/6), retrying in 10s: $(tr '\n' ' ' <<<"$out")"
      sleep 10
      start_port_forward
    fi
  done
  # Must fail here, before parsing $out, or a non-JSON error string silently produces an empty TASK_ID.
  [[ "$ok" == "1" ]] || fail "send(\"$text\") never succeeded after 6 attempts: $(tr '\n' ' ' <<<"$out")"
  TASK_ID="$(echo "$out" | jq -r '.id')"
  TENANT="$(echo "$out" | jq -r '.metadata["checkpointd-tenant"]')"
  REPLY="$(echo "$out" | jq -r '.status.message.parts[0].text')"
}

# pollForReplyChange polls GetTask every 5s until the reply text differs from before, requiring the same new reply on two consecutive polls.
pollForReplyChange() {
  local before=$1 i cur cur_reply prev_seen=""
  for i in $(seq 1 60); do
    sleep 5
    cur="$("$A2A_CLI" --transport jsonrpc --timeout 30s get task "$AGENT_URL" "$TASK_ID" --tenant "$TENANT" -o json 2>/dev/null)" \
      || { start_port_forward; continue; }
    cur_reply="$(echo "$cur" | jq -r '.status.message.parts[0].text // empty' 2>/dev/null)"
    if [[ -n "$cur_reply" && "$cur_reply" != "$before" ]]; then
      if [[ "$cur_reply" == "$prev_seen" ]]; then
        echo "$cur"
        return 0
      fi
      prev_seen="$cur_reply"
    fi
  done
  return 1
}

start_port_forward
waitForAgentReady

log "=== send: Hi, I'm Foo. ==="
send "Hi, I'm Foo."
echo "  reply:"
echo "$REPLY" | sed 's/^/    /'

log "=== send: What is your name? ==="
send "What is your name?"
echo "  reply:"
echo "$REPLY" | sed 's/^/    /'

# Restarts every worker node rather than the control-plane node.
log "=== restarting every kind *worker* node (simulating a real node crash+reboot, not just a pod delete) ==="
mapfile -t worker_nodes < <(docker ps --format '{{.Names}}' --filter "name=${KIND_CLUSTER_NAME}-worker")
[[ "${#worker_nodes[@]}" -gt 0 ]] || fail "no kind worker node containers found for cluster '$KIND_CLUSTER_NAME' -- was it created with KIND_WORKER_NODES=0?"

# Snapshots taken before the restart, so we can later confirm the restart
# actually happened, instead of only checking that kubectl reports
# everything Ready again afterward -- which would stay true even if a node
# or pod was silently left untouched.
log "  snapshotting node container start times, checkpointd-server-0's pod uid, and the harness WorkerPool's own worker pod uids, before restarting anything"
declare -A node_started_before
for n in "${worker_nodes[@]}"; do
  node_started_before["$n"]="$(docker inspect -f '{{.State.StartedAt}}' "$n")"
  [[ -n "${node_started_before[$n]}" ]] || fail "could not read container start time for kind node $n before restarting it"
done
checkpointd_uid_before="$(run_kubectl -n "$NS" get pod checkpointd-server-0 -o jsonpath='{.metadata.uid}')"
[[ -n "$checkpointd_uid_before" ]] || fail "could not read checkpointd-server-0's pod uid before the node restart"
harness_uids_before="$(run_kubectl -n "$NS" get pods -o json 2>/dev/null \
  | jq -r --arg prefix "${NS}-harness-" '[.items[] | select(.metadata.name | startswith($prefix)) | .metadata.uid] | sort | join(",")')"
[[ -n "$harness_uids_before" ]] || fail "found no harness WorkerPool worker pods (name prefix ${NS}-harness-) in $NS before the node restart"

log "  restarting: ${worker_nodes[*]}"
docker restart "${worker_nodes[@]}" >/dev/null

log "  confirming every restarted node container actually stopped and started again, not a silent no-op"
for n in "${worker_nodes[@]}"; do
  node_started_after="$(docker inspect -f '{{.State.StartedAt}}' "$n")"
  [[ -n "$node_started_after" && "$node_started_after" != "${node_started_before[$n]}" ]] \
    || fail "kind node $n's container start time did not change after 'docker restart' (before: ${node_started_before[$n]}, after: ${node_started_after:-<none>}) -- it was never actually restarted"
done
log "  every restarted node container's start time changed -- confirmed real restarts"

log "  waiting for every node to report Ready again (up to 3 minutes)"
run_kubectl wait --for=condition=Ready nodes --all --timeout=180s \
  || fail "not every node became Ready again after the restart"

log "  recreating pods that were already running on any restarted worker node"
# Force-deleting every pod that was on one of these nodes breaks the "Pod sandbox changed" cycle instead of waiting it out.
stale_pods="" stale_ate_pods=""
for n in "${worker_nodes[@]}"; do
  stale_pods+=" $(run_kubectl -n "$NS" get pods --field-selector "spec.nodeName=$n" -o jsonpath='{.items[*].metadata.name}')"
  # ate-system also lands on these same nodes by default, so it needs the same recovery to avoid adding latency or resetting egress calls.
  stale_ate_pods+=" $(run_kubectl -n ate-system get pods --field-selector "spec.nodeName=$n" -o jsonpath='{.items[*].metadata.name}')"
done
stale_pods="$(echo $stale_pods)"
stale_ate_pods="$(echo $stale_ate_pods)"
if [[ -n "$stale_pods" ]]; then
  log "    deleting ($NS): $stale_pods"
  # shellcheck disable=SC2086
  run_kubectl -n "$NS" delete pod $stale_pods --grace-period=0 --force --wait=false
fi
if [[ -n "$stale_ate_pods" ]]; then
  log "    deleting (ate-system): $stale_ate_pods"
  # shellcheck disable=SC2086
  run_kubectl -n ate-system delete pod $stale_ate_pods --grace-period=0 --force --wait=false
  log "  waiting for ate-system's control plane to be ready again (up to 2 minutes each)"
  for d in ate-api-server atenet-egress atenet-router; do
    run_kubectl -n ate-system rollout status "deployment/$d" --timeout=120s \
      || fail "ate-system's $d never became Ready again after the node restart"
  done
  run_kubectl -n ate-system rollout status daemonset/atelet --timeout=120s \
    || fail "ate-system's atelet never became Ready again after the node restart"
fi

log "  waiting for the WorkerPool to report Ready again (up to 3 minutes)"
workerpool_ready=0
for _ in $(seq 1 90); do
  ready="$(run_kubectl -n "$NS" get workerpool checkpointd-llm-chat-demo-harness -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)"
  want="$(run_kubectl -n "$NS" get workerpool checkpointd-llm-chat-demo-harness -o jsonpath='{.spec.replicas}' 2>/dev/null || true)"
  if [[ -n "$ready" && -n "$want" && "$ready" == "$want" ]]; then
    workerpool_ready=1
    break
  fi
  sleep 2
done
[[ "$workerpool_ready" == "1" ]] || fail "WorkerPool never became fully Ready again after recreating the restarted nodes' own pods"

log "  waiting for llama-completion to be reachable again (up to 2 minutes)"
run_kubectl -n "$NS" rollout status deployment/llama-completion --timeout=120s \
  || fail "llama-completion never became Ready again after the node restart"

log "  waiting for checkpointd-server to be reachable again (up to 2 minutes)"
run_kubectl -n "$NS" rollout status statefulset/checkpointd-server --timeout=120s \
  || fail "checkpointd-server never became Ready again after the node restart"

log "  confirming checkpointd-server-0 and every harness worker pod were actually recreated, not left running untouched"
checkpointd_uid_after="$(run_kubectl -n "$NS" get pod checkpointd-server-0 -o jsonpath='{.metadata.uid}')"
[[ -n "$checkpointd_uid_after" ]] || fail "could not read checkpointd-server-0's pod uid after the node restart"
[[ "$checkpointd_uid_after" != "$checkpointd_uid_before" ]] \
  || fail "checkpointd-server-0 still has the same pod uid ($checkpointd_uid_before) after the node restart -- it was never actually force-deleted/recreated"
harness_uids_after="$(run_kubectl -n "$NS" get pods -o json 2>/dev/null \
  | jq -r --arg prefix "${NS}-harness-" '[.items[] | select(.metadata.name | startswith($prefix)) | .metadata.uid] | sort | join(",")')"
[[ -n "$harness_uids_after" ]] || fail "found no harness WorkerPool worker pods (name prefix ${NS}-harness-) in $NS after the node restart"
common_uids="$(comm -12 <(tr ',' '\n' <<<"$harness_uids_before" | sort) <(tr ',' '\n' <<<"$harness_uids_after" | sort))"
[[ -z "$common_uids" ]] \
  || fail "some harness WorkerPool worker pod(s) survived the node restart unchanged (uid(s): $(tr '\n' ' ' <<<"$common_uids")) -- expected every worker pod to have been recreated along with its node"
log "  checkpointd-server-0 (uid $checkpointd_uid_before -> $checkpointd_uid_after) and all ${#worker_nodes[@]} restarted node(s)' harness worker pods were genuinely recreated -- confirmed real restarts throughout, not assumed ones"

# Picked to not depend on SmolLM2's actual reasoning, since the check below only needs the log to have survived.
log "=== send: What is the weather? (same task -- reply's own log should still carry both messages above) ==="
start_port_forward
waitForAgentReady
send "What is the weather?"
echo "  reply:"
echo "$REPLY" | sed 's/^/    /'

[[ "$REPLY" == *"Hi, I'm Foo."* ]] || fail "post-crash reply's own conversation log is missing the pre-crash message 'Hi, I'm Foo.' -- the running history did not survive the node restart"
[[ "$REPLY" == *"What is your name?"* ]] || fail "post-crash reply's own conversation log is missing the pre-crash message 'What is your name?' -- the running history did not survive the node restart"

log "PASS -- chat-agent's full conversation log survived every kind worker node restarting, checkpointd-server included."
