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

# Shared helpers for script/test-k8s/test.sh and subtests/*.sh.

log() {
  echo "[test-k8s] $*" >&2
}

fail() {
  echo "[test-k8s] FAIL: $*" >&2
  exit 1
}

run_kubectl() {
  kubectl --context="$KUBECTL_CONTEXT" "$@"
}

# actorName computes the CRD name Agent Substrate assigns to a private actor.
actorName() {
  local sessionID=$1 agent=$2
  python3 -c '
import hashlib, base64, sys
session_id, agent = sys.argv[1], sys.argv[2]
prefix = agent[:10]
digest = hashlib.sha256(f"checkpointd-{session_id}-{agent}".encode()).digest()
h = base64.b32encode(digest).decode().rstrip("=").lower()
print(f"{prefix}-{h}")
' "$sessionID" "$agent"
}

run_kubectl_ate() {
  "$KUBECTL_ATE_BIN" --context="$KUBECTL_CONTEXT" "$@"
}

dump_checkpointd_server_logs() {
  log "--- checkpointd-server-0 logs ---"
  run_kubectl -n "$NS" logs pod/checkpointd-server-0 2>&1 | sed 's/^/[checkpointd-server] /' >&2 || true
}

# dump_testserver_diagnostics prints testserver's pod status and logs.
dump_testserver_diagnostics() {
  log "--- testserver pod status ---"
  run_kubectl -n "$NS" get pods -l app=testserver -o wide 2>&1 >&2 || true
  run_kubectl -n "$NS" get pods -l app=testserver -o json 2>/dev/null \
    | jq -r '.items[] | "pod \(.metadata.name): restartCount=\(.status.containerStatuses[0].restartCount // "?") ready=\(.status.containerStatuses[0].ready // "?") lastState=\(.status.containerStatuses[0].lastState // {})"' >&2 || true
  log "--- testserver logs ---"
  run_kubectl -n "$NS" logs -l app=testserver --all-containers --prefix 2>&1 | sed 's/^/[testserver] /' >&2 || true
  run_kubectl -n "$NS" logs -l app=testserver --all-containers --prefix --previous 2>&1 | sed 's/^/[testserver-previous] /' >&2 || true
}

# dump_harness_worker_logs prints every harness worker pod's stdout.
dump_harness_worker_logs() {
  log "--- checkpointd-k8s-test-harness worker pod logs ---"
  local pod
  for pod in $(run_kubectl -n "$NS" get pods -o name 2>/dev/null | grep '^pod/checkpointd-k8s-test-harness-'); do
    run_kubectl -n "$NS" logs "$pod" 2>&1 | sed "s/^/[${pod#pod/}] /" >&2 || true
    run_kubectl -n "$NS" logs "$pod" --previous 2>&1 | sed "s/^/[${pod#pod/}-previous] /" >&2 || true
  done
}

# start_port_forward (re-)establishes the port-forward tunnel to
# svc/checkpointd-server on a dynamically assigned local port.
start_port_forward() {
  if [[ -n "$PORT_FORWARD_PID" ]]; then
    kill "$PORT_FORWARD_PID" 2>/dev/null || true
    wait "$PORT_FORWARD_PID" 2>/dev/null || true
  fi
  # Force-kills any leftover kubectl port-forward process still holding the port.
  pkill -9 -f -- "context=$KUBECTL_CONTEXT.*port-forward svc/checkpointd-server" 2>/dev/null || true
  local pf_log="$WORKDIR/port-forward.log"
  : >"$pf_log"
  (
    trap 'kill -9 "$pid" 2>/dev/null; wait "$pid" 2>/dev/null; exit 0' TERM INT
    target="${CHECKPOINTD_SERVER_LOCAL_PORT:-}:80"
    while true; do
      run_kubectl -n "$NS" port-forward svc/checkpointd-server "$target" &
      pid=$!
      wait "$pid"
      port="$(sed -nE 's#^Forwarding from 127\.0\.0\.1:([0-9]+) ->.*#\1#p' "$pf_log" | tail -1)"
      [[ -n "$port" ]] && target="$port:80"
      sleep 1
    done
  ) >"$pf_log" 2>&1 &
  PORT_FORWARD_PID=$!
  for i in $(seq 1 50); do
    CHECKPOINTD_SERVER_LOCAL_PORT="$(sed -nE 's#^Forwarding from 127\.0\.0\.1:([0-9]+) ->.*#\1#p' "$pf_log" | tail -1)"
    if [[ -n "$CHECKPOINTD_SERVER_LOCAL_PORT" ]] && (exec 3<>"/dev/tcp/127.0.0.1/$CHECKPOINTD_SERVER_LOCAL_PORT") 2>/dev/null; then
      exec 3<&- 3>&-
      log "port-forwarding svc/checkpointd-server to localhost:$CHECKPOINTD_SERVER_LOCAL_PORT (the host reaching into the cluster)"
      return 0
    fi
    sleep 0.2
  done
  fail "port-forward to svc/checkpointd-server never became reachable"
}

# restart_checkpointd_server_pod deletes checkpointd-server-0, waits for
# it to become Ready again, then re-establishes the port-forward.
restart_checkpointd_server_pod() {
  run_kubectl -n "$NS" delete pod checkpointd-server-0 --wait=true
  for i in $(seq 1 60); do
    run_kubectl -n "$NS" get pod checkpointd-server-0 >/dev/null 2>&1 && break
    sleep 2
  done
  run_kubectl -n "$NS" wait --for=condition=Ready pod/checkpointd-server-0 --timeout=180s \
    || { dump_checkpointd_server_logs; fail "checkpointd-server-0 never became Ready again after being deleted"; }
  start_port_forward
}

# a2a_cli runs the pinned a2a CLI over jsonrpc, retrying transport-level failures a few times.
A2A_CLI_MAX_ATTEMPTS=5
a2a_cli() {
  local attempt out errfile rc
  errfile="$(mktemp)"
  for attempt in $(seq 1 "$A2A_CLI_MAX_ATTEMPTS"); do
    if out="$("$A2A_CLI" --transport jsonrpc --timeout 120s "$@" 2>"$errfile")"; then
      rm -f "$errfile"
      echo "$out"
      return 0
    fi
    rc=$?
    if ! grep -qE "context deadline exceeded|connection refused|connection reset|EOF" "$errfile"; then
      cat "$errfile" >&2
      rm -f "$errfile"
      return "$rc"
    fi
    log "  a2a_cli: transport-level failure (attempt $attempt/$A2A_CLI_MAX_ATTEMPTS): $(tr '\n' ' ' <"$errfile") -- retrying"
    sleep 2
  done
  cat "$errfile" >&2
  rm -f "$errfile"
  log "  a2a_cli: exhausted all $A2A_CLI_MAX_ATTEMPTS attempts, still transport-level failures -- dumping diagnostics"
  dump_checkpointd_server_logs
  dump_harness_worker_logs
  return "$rc"
}

# pollTaskState polls GetTask until it reaches want, storing the last response in LAST_GET_OUT.
LAST_GET_OUT=""
pollTaskState() {
  local url=$1 id=$2 tenant=$3 want=$4 i state
  for i in $(seq 1 120); do
    LAST_GET_OUT="$(a2a_cli get task "$url" "$id" --tenant "$tenant" -o json 2>/dev/null)" || true
    state="$(echo "$LAST_GET_OUT" | jq -r '.status.state // empty')"
    [[ "$state" == "$want" ]] && return 0
    sleep 3
  done
  return 1
}

# taskWithArtifacts fetches id's Task including Artifacts via `a2a list tasks --with-artifacts`.
taskWithArtifacts() {
  local url=$1 id=$2 tenant=$3 i out
  for i in $(seq 1 5); do
    out="$(a2a_cli list tasks "$url" --with-artifacts --tenant "$tenant" -o json 2>/dev/null | jq -c --arg id "$id" '.tasks[] | select(.id == $id)')" || true
    [[ -n "$out" ]] && { echo "$out"; return 0; }
    sleep 1
  done
  return 1
}

# taskTenant extracts the tenant checkpointd stamps into a Task's own Metadata.
taskTenant() {
  echo "$1" | jq -r '.metadata["checkpointd-tenant"] // empty'
}

# pollTaskNotTerminal polls GetTask until it observes any non-terminal state.
pollTaskNotTerminal() {
  local url=$1 id=$2 tenant=$3 i state
  for i in $(seq 1 60); do
    LAST_GET_OUT="$(a2a_cli get task "$url" "$id" --tenant "$tenant" -o json 2>/dev/null)" || true
    state="$(echo "$LAST_GET_OUT" | jq -r '.status.state // empty')"
    case "$state" in
      TASK_STATE_COMPLETED | TASK_STATE_FAILED | TASK_STATE_CANCELED | TASK_STATE_REJECTED | "") ;;
      *) return 0 ;;
    esac
    sleep 3
  done
  return 1
}

# resolveWorkerPod prints the Kubernetes worker pod currently backing agent's actor.
resolveWorkerPod() {
  local url=$1 id=$2 tenant=$3 agent=$4
  local task_out session_id actor_name a worker_ns worker_pod
  for i in $(seq 1 300); do
    task_out="$(a2a_cli get task "$url" "$id" --tenant "$tenant" -o json 2>/dev/null)" || true
    session_id="$(echo "$task_out" | jq -r '.metadata["checkpointd-tenant"] // empty' 2>/dev/null)"
    if [[ -n "$session_id" ]]; then
      actor_name="$(actorName "$session_id" "$agent")"
      a="$(run_kubectl_ate get actors -a "$NS" -o json 2>/dev/null | jq -c --arg name "$actor_name" '[.actors[] | select(.metadata.name == $name)] | .[0] // empty' 2>/dev/null)"
      if [[ -n "$a" && "$a" != "empty" && "$a" != "null" ]]; then
        worker_ns="$(echo "$a" | jq -r '.status.workerAssignment.workerNamespace // empty')"
        worker_pod="$(echo "$a" | jq -r '.status.workerAssignment.workerPod // empty')"
        if [[ -n "$worker_ns" && -n "$worker_pod" ]]; then
          echo "$worker_ns $worker_pod"
          return 0
        fi
      fi
    fi
    sleep 0.3
  done
  return 1
}

# confirmPodGone polls until the pod (ns/name) that had prevUID is actually
# gone -- either no longer found, or found with a different uid (name reuse)
# -- rather than trusting a --wait=false delete took effect just because the
# kubectl command returned.
confirmPodGone() {
  local ns=$1 name=$2 prevUID=$3 i uid
  for i in $(seq 1 60); do
    uid="$(run_kubectl -n "$ns" get pod "$name" -o jsonpath='{.metadata.uid}' 2>/dev/null || true)"
    [[ -z "$uid" || "$uid" != "$prevUID" ]] && return 0
    sleep 2
  done
  return 1
}

# checkEnteredOnce asserts a workflow was entered from the top exactly once.
checkEnteredOnce() {
  local id=$1 label=$2
  local count
  count="$(curl -sf "http://127.0.0.1:$TESTSERVER_LOCAL_PORT/count/${id}-entered")" \
    || fail "$label: could not read its own entered-count from pubsub (id: $id)"
  [[ "$count" == "1" ]] \
    || fail "$label: its own workflow was entered $count times (want exactly 1) -- recovery discarded the actor's healthy in-flight state and restarted it from a blank snapshot instead of resuming"
}

# countRelaysTo counts how many turns checkpointd dispatched to targetActorID,
# via its own per-turn "Suspending SubstrATE actor" JSON log line.
countRelaysTo() {
  local targetActorID=$1
  run_kubectl -n "$NS" logs pod/checkpointd-server-0 2>/dev/null \
    | jq -R -r --arg id "$targetActorID" \
        'fromjson? | select(.msg == "Suspending SubstrATE actor" and .conversation_id == $id) | .conversation_id' \
    | wc -l | tr -d ' '
}

# checkCallNotRepeated asserts checkpointd dispatched exactly want turns to one private actor.
checkCallNotRepeated() {
  local tenant=$1 target=$2 want=$3 label=$4
  local targetActorID
  targetActorID="$(actorName "$tenant" "$target")"
  local count
  count="$(countRelaysTo "$targetActorID")"
  [[ "$count" == "$want" ]] \
    || fail "$label: checkpointd dispatched $count turns to $targetActorID (want exactly $want) -- recovery redid an already-completed hop instead of resuming past it"
}
