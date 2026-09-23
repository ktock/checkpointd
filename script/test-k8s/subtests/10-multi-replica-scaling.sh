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

# subtest_multi_replica_scaling runs checkpointd-server as a multi-replica
# StatefulSet on shared Postgres (see 90-postgres-statefulset.yaml and the
# "-postgres" 91/92 manifest variants) and confirms:
#
#   A) a session that starts on one specific instance and parks at
#      InputRequired can be explicitly, deterministically continued by a
#      different specific instance (direct pod-to-pod port-forwards, not
#      round-robin luck);
#   B) a session parked on an instance that then gets scaled in (removed
#      entirely) can still be continued afterward by a surviving instance;
#   C) the broader scale-up/scale-down cycle preserves both a parked
#      (InputRequired) and an actively-RUNNING session, reached the normal
#      way through the plain round-robin svc/checkpointd-server throughout;
#   D) a *non-graceful* force-kill of the owning instance's own pod (not a
#      graceful kubectl scale) while it actively owns a RUNNING session
#      still recovers correctly and exactly once -- either through the
#      same-named StatefulSet replacement's own startup self-recovery
#      (resumeServerSessions, matching by pod name) or a surviving
#      instance's orphan-session salvage loop, whichever wins the race.
#      Either is a correct outcome; this only asserts it's never both at
#      once.
subtest_multi_replica_scaling() {
  log "confirming the initial checkpointd-server StatefulSet is fully rolled out"
  run_kubectl -n "$NS" rollout status statefulset/checkpointd-server --timeout=120s \
    || { dumpAllCheckpointdServerLogs; fail "checkpointd-server StatefulSet never became fully Ready before the test began"; }
  local initial_replicas
  initial_replicas="$(run_kubectl -n "$NS" get statefulset checkpointd-server -o jsonpath='{.spec.replicas}')"
  log "  starting from $initial_replicas replica(s)"
  [[ "$initial_replicas" -ge 2 ]] || fail "this subtest needs at least 2 initial replicas (got $initial_replicas) to exercise explicit cross-instance continuation"

  # --- Scenario A: explicit cross-instance continuation, no scale change ----

  log "=== scenario A: session starts on checkpointd-server-0, continuation explicitly targeted at checkpointd-server-1 ==="
  local fwd0 port0 pid0
  fwd0="$(portForwardToPod checkpointd-server-0 80)" || fail "could not port-forward directly to checkpointd-server-0"
  port0="${fwd0%% *}"; pid0="${fwd0##* }"
  local directURL0="http://127.0.0.1:$port0/agents/agent-b/"

  local a_out a_task_id a_tenant
  a_out="$(a2a_cli send --immediate "$directURL0" -o json "scenario-a")" || { kill "$pid0" 2>/dev/null; fail "a2a send directly to checkpointd-server-0 failed"; }
  a_task_id="$(echo "$a_out" | jq -r '.id // empty')"
  a_tenant="$(taskTenant "$a_out")"
  [[ -n "$a_task_id" && "$a_task_id" != "null" ]] || { kill "$pid0" 2>/dev/null; fail "a2a send directly to checkpointd-server-0 did not return a task id (output: $a_out)"; }
  pollTaskState "$directURL0" "$a_task_id" "$a_tenant" TASK_STATE_INPUT_REQUIRED \
    || { kill "$pid0" 2>/dev/null; dumpAllCheckpointdServerLogs; fail "scenario A: task $a_task_id never reached TASK_STATE_INPUT_REQUIRED on checkpointd-server-0"; }
  log "  task $a_task_id started on checkpointd-server-0 and parked at InputRequired"
  kill "$pid0" 2>/dev/null; wait "$pid0" 2>/dev/null || true

  local fwd1 port1 pid1
  fwd1="$(portForwardToPod checkpointd-server-1 80)" || fail "could not port-forward directly to checkpointd-server-1"
  port1="${fwd1%% *}"; pid1="${fwd1##* }"
  local directURL1="http://127.0.0.1:$port1/agents/agent-b/"

  a2a_cli send --immediate "$directURL1" -o json --task "$a_task_id" --tenant "$a_tenant" "hello" >/dev/null \
    || { kill "$pid1" 2>/dev/null; dumpAllCheckpointdServerLogs; fail "scenario A: continuation explicitly targeted at checkpointd-server-1 failed"; }
  pollTaskState "$directURL1" "$a_task_id" "$a_tenant" TASK_STATE_INPUT_REQUIRED \
    || { kill "$pid1" 2>/dev/null; dumpAllCheckpointdServerLogs; fail "scenario A: task $a_task_id did not correctly continue on checkpointd-server-1 (last response: $LAST_GET_OUT)"; }
  log "  task $a_task_id correctly continued on checkpointd-server-1 -- a different instance than the one that started it"
  kill "$pid1" 2>/dev/null; wait "$pid1" 2>/dev/null || true
  # 3, not 2: agent-b's own doc comment says turns 1 and 2 (Working, then
  # InputRequired) both run inside its first call -- that's 2 relay hops
  # before the client-visible response even returns -- plus 1 more hop for
  # the "hello" continuation = 3 total. This only guards against a
  # *fourth*, redundant relay (e.g. double-driven from both instances at
  # once), not against agent-b's own expected turn count. Both pods are
  # still up here, so the multi-pod log scrape sees everything.
  checkCallNotRepeatedAcrossPods "$a_tenant" "agent-b" 3 "scenario A"

  # --- Scenario B: the owning instance is scaled in before continuation ----

  log "=== scenario B: session starts on checkpointd-server-1, that instance is then scaled in, continuation lands on a surviving instance ==="
  # A StatefulSet's own scale-down is deterministic (unlike a Deployment's):
  # it always removes the highest ordinal first, so checkpointd-server-1
  # (the last of $initial_replicas) is exactly the one a scale-down to
  # $((initial_replicas - 1)) will remove -- no need to guess or discover.
  local fwd1b port1b pid1b
  fwd1b="$(portForwardToPod checkpointd-server-1 80)" || fail "could not port-forward directly to checkpointd-server-1"
  port1b="${fwd1b%% *}"; pid1b="${fwd1b##* }"
  local directURL1b="http://127.0.0.1:$port1b/agents/agent-b/"

  local b_out b_task_id b_tenant
  b_out="$(a2a_cli send --immediate "$directURL1b" -o json "scenario-b")" || { kill "$pid1b" 2>/dev/null; fail "a2a send directly to checkpointd-server-1 failed"; }
  b_task_id="$(echo "$b_out" | jq -r '.id // empty')"
  b_tenant="$(taskTenant "$b_out")"
  [[ -n "$b_task_id" && "$b_task_id" != "null" ]] || { kill "$pid1b" 2>/dev/null; fail "a2a send directly to checkpointd-server-1 did not return a task id (output: $b_out)"; }
  pollTaskState "$directURL1b" "$b_task_id" "$b_tenant" TASK_STATE_INPUT_REQUIRED \
    || { kill "$pid1b" 2>/dev/null; dumpAllCheckpointdServerLogs; fail "scenario B: task $b_task_id never reached TASK_STATE_INPUT_REQUIRED on checkpointd-server-1"; }
  log "  task $b_task_id started on checkpointd-server-1 and parked at InputRequired"
  kill "$pid1b" 2>/dev/null; wait "$pid1b" 2>/dev/null || true

  local removed_uid
  removed_uid="$(run_kubectl -n "$NS" get pod checkpointd-server-1 -o jsonpath='{.metadata.uid}')"
  [[ -n "$removed_uid" ]] || fail "could not read checkpointd-server-1's uid before scaling it in"
  log "  scaling checkpointd-server down by 1: $initial_replicas -> $((initial_replicas - 1))"
  run_kubectl -n "$NS" scale statefulset/checkpointd-server --replicas="$((initial_replicas - 1))"
  confirmPodGone "$NS" checkpointd-server-1 "$removed_uid" \
    || fail "scenario B: checkpointd-server-1 (uid $removed_uid) was not actually removed by the scale-in within the expected time"
  log "  checkpointd-server-1 confirmed gone -- its session's owner_pod can now only ever have been released (InputRequired) or reclaimed on restart, never re-owned by a ghost"

  # The long-lived svc/checkpointd-server port-forward (used by $AGENT_B_URL
  # below) may happen to be attached to checkpointd-server-1 itself, and
  # checkpointd closes its own HTTP listener immediately on SIGTERM while it
  # drains in-flight sessions, well before the Pod object actually
  # disappears -- so that tunnel can silently start refusing new requests
  # without ever dying itself. Explicitly re-establish it now.
  start_port_forward

  log "  continuing task $b_task_id through the plain round-robin Service -- only checkpointd-server-0 remains, so this deterministically exercises a surviving instance picking up a session whose original owner no longer exists"
  a2a_cli send --immediate "$AGENT_B_URL" -o json --task "$b_task_id" --tenant "$b_tenant" "hello" >/dev/null \
    || { dumpAllCheckpointdServerLogs; fail "scenario B: continuation after the owning instance was scaled in failed"; }
  pollTaskState "$AGENT_B_URL" "$b_task_id" "$b_tenant" TASK_STATE_INPUT_REQUIRED \
    || { dumpAllCheckpointdServerLogs; fail "scenario B: task $b_task_id did not correctly continue after its owning instance was scaled in (last response: $LAST_GET_OUT)"; }
  log "  task $b_task_id correctly continued on a surviving instance after checkpointd-server-1 was scaled in"
  # No relay-count check here: checkpointd-server-1's own logs -- which
  # would have recorded the first turn's relay -- are gone along with the
  # pod itself. The functional assertions above (correct state transition,
  # no error) are this scenario's real proof.

  # --- Scenario C: the broader scale-up/scale-down cycle, via round-robin --

  local current_replicas=$((initial_replicas - 1))
  log "=== scenario C: broader scale-up/scale-down cycle with one parked and one actively-RUNNING session, reached only through svc/checkpointd-server ==="

  log "sending a message directly to agent-b (parks at TaskStateInputRequired, owner_pod released once paused)"
  local c_out c_task_id c_tenant
  c_out="$(a2a_cli send --immediate "$AGENT_B_URL" -o json "scenario-c")" || fail "a2a send to agent-b failed"
  c_task_id="$(echo "$c_out" | jq -r '.id // empty')"
  [[ -n "$c_task_id" && "$c_task_id" != "null" ]] || fail "a2a send to agent-b did not return a task id (output: $c_out)"
  c_tenant="$(taskTenant "$c_out")"
  pollTaskState "$AGENT_B_URL" "$c_task_id" "$c_tenant" TASK_STATE_INPUT_REQUIRED \
    || { dumpAllCheckpointdServerLogs; fail "agent-b task $c_task_id never reached TASK_STATE_INPUT_REQUIRED (last response: $LAST_GET_OUT)"; }
  log "  task $c_task_id parked at InputRequired"

  log "sending a long-poll request (stays RUNNING, actively owned, until notified below)"
  local LONG_WAIT_ID="multi-replica-scaling-$(date +%s)"
  local lp_out lp_task_id lp_tenant
  lp_out="$(a2a_cli send --immediate "$LONG_POLL_URL" -o json "$LONG_WAIT_ID")" || fail "a2a send to long-poll failed"
  lp_task_id="$(echo "$lp_out" | jq -r '.id // empty')"
  [[ -n "$lp_task_id" && "$lp_task_id" != "null" ]] || fail "a2a send to long-poll did not return a task id (output: $lp_out)"
  lp_tenant="$(taskTenant "$lp_out")"
  pollTaskNotTerminal "$LONG_POLL_URL" "$lp_task_id" "$lp_tenant" \
    || { dumpAllCheckpointdServerLogs; fail "long-poll task $lp_task_id never reached a non-terminal state"; }
  log "  task $lp_task_id is in progress (long-poll's own retry loop against long-wait)"

  local up_replicas=$((current_replicas + 2))
  log "scaling checkpointd-server up: $current_replicas -> $up_replicas"
  run_kubectl -n "$NS" scale statefulset/checkpointd-server --replicas="$up_replicas"
  run_kubectl -n "$NS" rollout status statefulset/checkpointd-server --timeout=180s \
    || { dumpAllCheckpointdServerLogs; fail "checkpointd-server never finished scaling up to $up_replicas replicas"; }
  log "  now at $up_replicas replicas"

  log "continuing agent-b's task through the round-robin Service -- may land on a different replica than the one that originally owned it"
  a2a_cli send --immediate "$AGENT_B_URL" -o json --task "$c_task_id" --tenant "$c_tenant" "hello" >/dev/null \
    || { dumpAllCheckpointdServerLogs; fail "continuing agent-b's task after scale-up failed"; }
  pollTaskState "$AGENT_B_URL" "$c_task_id" "$c_tenant" TASK_STATE_INPUT_REQUIRED \
    || { dumpAllCheckpointdServerLogs; fail "agent-b task $c_task_id never returned to TaskStateInputRequired after scale-up (last response: $LAST_GET_OUT)"; }
  log "  agent-b's task correctly reattached and progressed after scale-up"

  pollTaskNotTerminal "$LONG_POLL_URL" "$lp_task_id" "$lp_tenant" \
    || fail "long-poll task $lp_task_id unexpectedly left its non-terminal state during/after the scale-up"
  log "  long-poll's still-RUNNING task was undisturbed by the scale-up"

  log "finishing agent-b's conversation (hello -> world -> Completed) and long-poll's task, before scaling back down"
  a2a_cli send --immediate "$AGENT_B_URL" -o json --task "$c_task_id" --tenant "$c_tenant" "world" >/dev/null \
    || { dumpAllCheckpointdServerLogs; fail "final continuation to agent-b failed"; }
  pollTaskState "$AGENT_B_URL" "$c_task_id" "$c_tenant" TASK_STATE_COMPLETED \
    || { dumpAllCheckpointdServerLogs; fail "agent-b task $c_task_id never reached TASK_STATE_COMPLETED"; }

  log "confirming agent-b's own in-memory received-message history is exactly [\"hello\", \"world\"] -- proves the actor that received \"hello\" earlier (before the scale-up moved this session's ownership) is the same continuously-running actor process that just received \"world\", not a freshly started one"
  local c_final_out c_history i
  for i in $(seq 1 15); do
    c_final_out="$(a2a_cli get task "$AGENT_B_URL" "$c_task_id" --tenant "$c_tenant" -o json 2>/dev/null)" || true
    c_history="$(echo "$c_final_out" | jq -c '.status.message.parts[0].data.received_history // empty')"
    [[ -n "$c_history" ]] && break
    sleep 1
  done
  [[ "$c_history" == '["hello","world"]' ]] \
    || { dumpAllCheckpointdServerLogs; fail "agent-b's own received_history = '$c_history', want [\"hello\",\"world\"] -- the actor may have been recreated fresh instead of resuming its own checkpoint across the session's move to a different instance (full response: $c_final_out)"; }
  log "  agent-b's own accumulated history confirms the same actor process handled the whole conversation: $c_history"

  curl -sf -X POST "http://127.0.0.1:$TESTSERVER_LOCAL_PORT/notify/$LONG_WAIT_ID" -d "notified" >/dev/null \
    || fail "failed to POST the long-poll notification"
  pollTaskState "$LONG_POLL_URL" "$lp_task_id" "$lp_tenant" TASK_STATE_COMPLETED \
    || { dumpAllCheckpointdServerLogs; fail "long-poll task $lp_task_id never reached TASK_STATE_COMPLETED"; }
  log "  both sessions completed while running multi-replica"

  log "scaling checkpointd-server down: $up_replicas -> 1"
  # A StatefulSet's own scale-down always removes the highest ordinals
  # first, so the exact set of pods about to disappear is known up front --
  # capture their uids before scaling down, so each can be individually
  # confirmed actually gone afterward, rather than just trusting a Running
  # pod count (which can't tell "already gone" apart from "never existed").
  local ordinal
  declare -A down_uids
  for ((ordinal = up_replicas - 1; ordinal >= 1; ordinal--)); do
    down_uids[$ordinal]="$(run_kubectl -n "$NS" get pod "checkpointd-server-$ordinal" -o jsonpath='{.metadata.uid}' 2>/dev/null || true)"
  done
  run_kubectl -n "$NS" scale statefulset/checkpointd-server --replicas=1
  for ((ordinal = up_replicas - 1; ordinal >= 1; ordinal--)); do
    [[ -n "${down_uids[$ordinal]}" ]] || continue
    confirmPodGone "$NS" "checkpointd-server-$ordinal" "${down_uids[$ordinal]}" \
      || fail "checkpointd-server-$ordinal (uid ${down_uids[$ordinal]}) was not actually removed by the scale-down within the expected time"
  done
  log "  scaled down to 1 replica; every removed ordinal confirmed actually gone"

  # As in scenario B: the long-lived svc/checkpointd-server port-forward may
  # be attached to one of the replicas this scale-down just removed.
  start_port_forward

  log "confirming both scenario C sessions are still correctly resolvable after the scale-down"
  local final_c final_lp
  final_c="$(a2a_cli get task "$AGENT_B_URL" "$c_task_id" --tenant "$c_tenant" -o json)" || fail "GetTask for agent-b after scale-down failed"
  [[ "$(echo "$final_c" | jq -r '.status.state')" == "TASK_STATE_COMPLETED" ]] \
    || fail "agent-b task $c_task_id status after scale-down = '$(echo "$final_c" | jq -r '.status.state')', want TASK_STATE_COMPLETED"
  final_lp="$(a2a_cli get task "$LONG_POLL_URL" "$lp_task_id" --tenant "$lp_tenant" -o json)" || fail "GetTask for long-poll after scale-down failed"
  [[ "$(echo "$final_lp" | jq -r '.status.state')" == "TASK_STATE_COMPLETED" ]] \
    || fail "long-poll task $lp_task_id status after scale-down = '$(echo "$final_lp" | jq -r '.status.state')', want TASK_STATE_COMPLETED"
  log "  both sessions survived the full scale-up-then-scale-down cycle, reachable via svc/checkpointd-server's plain round-robin throughout"

  # --- Scenario D: non-graceful kill of the owning pod itself ---------------

  log "scaling checkpointd-server back up to $initial_replicas for scenario D"
  run_kubectl -n "$NS" scale statefulset/checkpointd-server --replicas="$initial_replicas"
  run_kubectl -n "$NS" rollout status statefulset/checkpointd-server --timeout=180s \
    || { dumpAllCheckpointdServerLogs; fail "checkpointd-server never finished scaling back up to $initial_replicas replicas for scenario D"; }

  log "=== scenario D: checkpointd-server-0 is force-killed (not scaled out) while it owns a RUNNING long-poll session ==="
  local fwd1d port1d pid1d
  fwd1d="$(portForwardToPod checkpointd-server-0 80)" || fail "could not port-forward directly to checkpointd-server-0"
  port1d="${fwd1d%% *}"; pid1d="${fwd1d##* }"
  local directURL1d="http://127.0.0.1:$port1d/agents/long-poll/"

  local LONG_WAIT_ID_D="multi-replica-force-kill-$(date +%s)"
  local d_out d_task_id d_tenant
  d_out="$(a2a_cli send --immediate "$directURL1d" -o json "$LONG_WAIT_ID_D")" || { kill "$pid1d" 2>/dev/null; fail "a2a send directly to checkpointd-server-0 failed"; }
  d_task_id="$(echo "$d_out" | jq -r '.id // empty')"
  d_tenant="$(taskTenant "$d_out")"
  [[ -n "$d_task_id" && "$d_task_id" != "null" ]] || { kill "$pid1d" 2>/dev/null; fail "a2a send directly to checkpointd-server-0 did not return a task id (output: $d_out)"; }
  pollTaskNotTerminal "$directURL1d" "$d_task_id" "$d_tenant" \
    || { kill "$pid1d" 2>/dev/null; dumpAllCheckpointdServerLogs; fail "scenario D: task $d_task_id never reached a non-terminal (actively RUNNING) state on checkpointd-server-0"; }
  log "  task $d_task_id started on checkpointd-server-0 and is actively RUNNING (long-poll's own retry loop against long-wait), owner_pod = checkpointd-server-0"
  kill "$pid1d" 2>/dev/null; wait "$pid1d" 2>/dev/null || true

  local killed_uid
  killed_uid="$(run_kubectl -n "$NS" get pod checkpointd-server-0 -o jsonpath='{.metadata.uid}')"
  [[ -n "$killed_uid" ]] || fail "could not read checkpointd-server-0's uid before force-killing it"
  log "  force-deleting checkpointd-server-0 itself (uid $killed_uid) -- NOT a graceful kubectl scale, so no drain ever runs and owner_pod/owner_uid are left stale in the sessions table"
  run_kubectl -n "$NS" delete pod checkpointd-server-0 --grace-period=0 --force --wait=false
  confirmPodGone "$NS" checkpointd-server-0 "$killed_uid" \
    || fail "scenario D: checkpointd-server-0 (uid $killed_uid) was not actually removed by the force-delete within the expected time"
  log "  checkpointd-server-0 (uid $killed_uid) confirmed gone -- task $d_task_id's session is now a genuine orphan (owner_pod=checkpointd-server-0, owner_uid=$killed_uid, nobody driving it)"

  log "  releasing long-wait's notification so checkpointd-server-0's orphaned session can complete once recovered"
  curl -sf -X POST "http://127.0.0.1:$TESTSERVER_LOCAL_PORT/notify/$LONG_WAIT_ID_D" -d "notified" >/dev/null \
    || fail "failed to POST the scenario D long-poll notification"

  # As in scenario B/C: the long-lived svc/checkpointd-server port-forward
  # may be attached to the now-gone checkpointd-server-0.
  start_port_forward

  log "  polling for task $d_task_id to recover and complete -- either checkpointd-server-0's own StatefulSet-recreated replacement (same name) self-recovers it at its own startup via resumeServerSessions, or a surviving instance's periodic salvage sweep claims it first, whichever wins the race"
  pollTaskState "$LONG_POLL_URL" "$d_task_id" "$d_tenant" TASK_STATE_COMPLETED \
    || { dumpAllCheckpointdServerLogs; fail "scenario D: task $d_task_id was never recovered and completed after checkpointd-server-0 was force-killed (last response: $LAST_GET_OUT)"; }

  # Reports which path actually won this run, without hard-asserting
  # either: both are correct outcomes, and which one wins a given race is
  # inherently timing-dependent (see this file's own top comment).
  local recovered_via="checkpointd-server-0's own same-named replacement (self-recovery)" pod
  for pod in $(run_kubectl -n "$NS" get pods -l app=checkpointd-server -o name 2>/dev/null); do
    if run_kubectl -n "$NS" logs "$pod" 2>/dev/null | grep -q "salvage: session $d_tenant orphaned"; then
      recovered_via="a surviving instance's orphan-session salvage loop"
      break
    fi
  done
  log "  task $d_task_id was recovered via $recovered_via"

  log "confirming long-poll's own workflow was entered exactly once -- proves it was resumed from its own actor checkpoint, by exactly one recovery path, never both at once"
  checkEnteredOnce "$LONG_WAIT_ID_D" "long-poll (scenario D, post-recovery)"
  log "  long-poll was entered exactly once, not re-entered or double-driven"
  # checkpointd-server-0's own StatefulSet-recreated replacement (same
  # name, new uid) comes back on its own -- no explicit scale-back needed
  # here, unlike scenario B's deliberate scale-in.
}
