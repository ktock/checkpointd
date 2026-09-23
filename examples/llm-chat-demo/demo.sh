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

# Drives chat-agent from outside the cluster to show checkpointd's two
# separate resilience properties in turn:
#   1. any instance can correctly handle a request for an existing session --
#      turn 1 goes to checkpointd-server-0, turn 2 is sent directly to
#      checkpointd-server-1 instead and still continues the same session.
#   2. the conversation survives an unclean crash of both instances at once --
#      both replicas' own checkpointd process is SIGKILLed (not the pod, not
#      the node); restartPolicy: Never means neither restarts in place, so
#      Kubernetes replaces both with fresh pods instead, and turn 3, sent via
#      the round-robin Service once the replacements are Ready, proves the
#      session still resumes correctly -- its state lives in the shared
#      Postgres-backed event log and the agent's own actor checkpoint, not
#      in whichever specific checkpointd-server process last touched it.
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

POD_0="checkpointd-server-0"
POD_1="checkpointd-server-1"
run_kubectl -n "$NS" get pod "$POD_1" >/dev/null 2>&1 \
  || fail "$POD_1 not found -- run setup.sh first (needs at least 2 checkpointd-server replicas)"

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

# start_port_forward retries the whole `kubectl port-forward` process, up to
# 3 times, so a mid-restart target gets re-resolved. With no argument, it
# reuses whatever target was last set (default: the round-robin Service) --
# this is what lets send()'s and pollForReplyChange()'s own internal retries
# (which call this bare) preserve a caller's explicit target (e.g.
# "pod/$POD_1") across a retry instead of silently falling back to the
# Service.
CURRENT_PF_TARGET="svc/checkpointd-server"
start_port_forward() {
  [[ $# -gt 0 ]] && CURRENT_PF_TARGET="$1"
  local target="$CURRENT_PF_TARGET"
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
        log "  reachable on localhost:$LOCAL_PORT"
        rm -f "$pf_log"
        return 0
      fi
      sleep 0.2
    done
    log "  never became reachable within 10s"
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

log "=== send: Hi, I'm Foo. (targeted directly at $POD_0) ==="
start_port_forward "pod/$POD_0"
waitForAgentReady
send "Hi, I'm Foo."
echo "  reply:"
echo "$REPLY" | sed 's/^/    /'

# Proves property 1: any instance can pick up an existing session, not just
# the one that started it. $POD_1 has never seen this session before now.
log "=== send: What is your name? (targeted directly at $POD_1, a different instance from turn 1) ==="
start_port_forward "pod/$POD_1"
waitForAgentReady
send "What is your name?"
echo "  reply:"
echo "$REPLY" | sed 's/^/    /'

log "=== crashing checkpointd's own process on both $POD_0 and $POD_1 (even though restartPolicy: Never means neither pod restarts in place, the conversation still recovers correctly) ==="

crashCheckpointd() {
  local pod=$1 node helper
  node="$(run_kubectl -n "$NS" get pod "$pod" -o jsonpath='{.spec.nodeName}')"
  [[ -n "$node" ]] || fail "could not read $pod's own node name"
  helper="crash-helper-$pod"
  log "  crashing $pod's own checkpointd process (SIGKILL) via a short-lived hostPID helper pod on $node"
  run_kubectl -n "$NS" delete pod "$helper" --ignore-not-found --wait=true >/dev/null 2>&1
  run_kubectl -n "$NS" run "$helper" --restart=Never --image=busybox:1.36 \
    --overrides="{\"spec\":{\"hostPID\":true,\"nodeName\":\"$node\"}}" \
    --command -- sh -c '
      self=$$
      for f in /proc/[0-9]*/cmdline; do
        pid=${f#/proc/}; pid=${pid%/cmdline}
        [ "$pid" = "$self" ] && continue
        cmd=$(tr "\0" " " < "$f" 2>/dev/null)
        case "$cmd" in
          */checkpointd-app/checkpointd\ *--pod-name='"$pod"'\ *)
            kill -9 "$pid"
            exit 0
            ;;
        esac
      done
      echo "no checkpointd process found for pod '"$pod"'" >&2
      exit 1
    ' >/dev/null \
    || fail "could not create the crash helper pod for $pod"
  run_kubectl -n "$NS" wait --for=jsonpath='{.status.phase}'=Succeeded "pod/$helper" --timeout=60s \
    || fail "crash helper pod for $pod never completed -- could not confirm SIGKILL was delivered to $pod's own checkpointd process"
  run_kubectl -n "$NS" delete pod "$helper" --wait=false >/dev/null 2>&1 || true
}

# Captured before the crash: restartPolicy: Never means the StatefulSet
# controller replaces each pod (new uid) rather than kubelet restarting the
# same one in place, but that replacement -- like any Failed pod's cleanup
# under a StatefulSet -- can happen faster than polling status.phase can
# reliably observe (confirmed live: the transient Failed phase is often too
# narrow a window to catch). A uid change is a permanent, reliably-pollable
# fact instead, so that's what's checked below, not the phase.
uid_before_0="$(run_kubectl -n "$NS" get pod "$POD_0" -o jsonpath='{.metadata.uid}')"
uid_before_1="$(run_kubectl -n "$NS" get pod "$POD_1" -o jsonpath='{.metadata.uid}')"

crashCheckpointd "$POD_0"
crashCheckpointd "$POD_1"

log "  waiting for checkpointd-server's own StatefulSet to bring both crashed replicas back (restartPolicy: Never forbids an in-place restart)"
run_kubectl -n "$NS" rollout status statefulset/checkpointd-server --timeout=180s \
  || fail "checkpointd-server's own StatefulSet never finished bringing both crashed replicas back"

for pod in "$POD_0" "$POD_1"; do
  before_var="uid_before_${pod##*-}"
  uid_after=""
  for i in $(seq 1 30); do
    uid_after="$(run_kubectl -n "$NS" get pod "$pod" -o jsonpath='{.metadata.uid}' 2>/dev/null || true)"
    [[ -z "$uid_after" || "$uid_after" != "${!before_var}" ]] && break
    sleep 2
  done
  [[ -z "$uid_after" || "$uid_after" != "${!before_var}" ]] \
    || fail "$pod still has its pre-crash uid (${!before_var}) -- expected restartPolicy: Never to prevent an in-place restart"
  log "  $pod confirmed NOT restarted in place (uid ${!before_var} -> ${uid_after:-gone}) -- Kubernetes replaced it with a fresh pod instead of reusing it"
done

log "=== send: What is the weather? (via svc/checkpointd-server, now that both replacement instances are Ready) ==="
start_port_forward "svc/checkpointd-server"
waitForAgentReady
send "What is the weather?"
echo "  reply:"
echo "$REPLY" | sed 's/^/    /'

[[ "$REPLY" == *"Hi, I'm Foo."* ]] || fail "post-crash reply's own conversation log is missing the pre-crash message 'Hi, I'm Foo.' -- the running history did not survive both instances' checkpointd process crashing"
[[ "$REPLY" == *"What is your name?"* ]] || fail "post-crash reply's own conversation log is missing the pre-crash message 'What is your name?' -- the running history did not survive both instances' checkpointd process crashing"

log "PASS -- turn 1 ($POD_0) and turn 2 ($POD_1) prove any instance can handle a request for an existing session; turn 3 proves the conversation still recovers fully correct after both original instances' own checkpointd process was killed with SIGKILL and Kubernetes replaced them with fresh pods instead of restarting them in place."
