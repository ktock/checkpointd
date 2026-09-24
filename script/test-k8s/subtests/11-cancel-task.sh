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

# subtest_cancel_task confirms CancelTask against a task that's actively
# RUNNING (not paused at InputRequired) actually stops its relay loop and
# cleans up every actor it visited.
subtest_cancel_task() {
  local CANCEL_WAIT_ID="cancel-task-$(date +%s)"

  log "sending a long-poll request that never gets notified -- it stays actively RUNNING (long-poll's own retry loop against long-wait), not paused at InputRequired, until it's canceled below"
  local send_out task_id tenant
  send_out="$(a2a_cli send --immediate "$LONG_POLL_URL" -o json "$CANCEL_WAIT_ID")" || fail "a2a send failed"
  task_id="$(echo "$send_out" | jq -r '.id // empty')"
  [[ -n "$task_id" && "$task_id" != "null" ]] || fail "a2a send did not return a task id (output: $send_out)"
  tenant="$(taskTenant "$send_out")"
  [[ -n "$tenant" ]] || fail "a2a send did not return a session id in metadata (output: $send_out)"
  log "  task $task_id submitted (session $tenant)"

  pollTaskNotTerminal "$LONG_POLL_URL" "$task_id" "$tenant" \
    || { dump_checkpointd_server_logs; fail "task $task_id never reached a non-terminal (actively RUNNING) state"; }
  log "  task $task_id is actively RUNNING"

  log "confirming both actors this task visits (long-poll itself, and long-wait) actually exist before canceling -- otherwise their disappearance below wouldn't prove anything"
  local poll_actor wait_actor
  poll_actor="$(actorName "$tenant" long-poll)"
  wait_actor="$(actorName "$tenant" long-wait)"
  local found=0 i actors_json
  for i in $(seq 1 60); do
    actors_json="$(run_kubectl_ate get actors -a "$NS" -o json 2>/dev/null)"
    if echo "$actors_json" | jq -e --arg a "$poll_actor" --arg b "$wait_actor" \
        '(.actors // []) as $actors | ($actors | any(.metadata.name == $a)) and ($actors | any(.metadata.name == $b))' >/dev/null 2>&1; then
      found=1
      break
    fi
    sleep 2
  done
  [[ "$found" == "1" ]] || fail "task $task_id: expected both actors $poll_actor and $wait_actor to exist before cancellation, but at least one never appeared"
  log "  both actors ($poll_actor, $wait_actor) confirmed present"

  log "canceling task $task_id while it's still actively RUNNING (not paused) -- the owning instance's own relay loop must notice and stop, not just this call's own instance recording Canceled"
  local cancel_out
  cancel_out="$(a2a_cli cancel "$LONG_POLL_URL" "$task_id" --tenant "$tenant" -o json)" || { dumpAllCheckpointdServerLogs; fail "a2a cancel failed"; }
  [[ "$(echo "$cancel_out" | jq -r '.status.state')" == "TASK_STATE_CANCELED" ]] \
    || fail "CancelTask's own response status.state = '$(echo "$cancel_out" | jq -r '.status.state')', want TASK_STATE_CANCELED (full response: $cancel_out)"
  log "  CancelTask durably recorded Canceled; now waiting for the actively-driving relay loop to notice and actually stop"

  log "polling for both actors to actually be deleted -- this only happens once driveRelayLoop's own next per-hop check notices the session is TERMINATING and runs finishTermination/cleanupActors, proving the relay loop actually stopped rather than continuing to drive already-canceled work"
  local gone=0
  for i in $(seq 1 60); do
    actors_json="$(run_kubectl_ate get actors -a "$NS" -o json 2>/dev/null)"
    if ! echo "$actors_json" | jq -e --arg a "$poll_actor" --arg b "$wait_actor" \
        '(.actors // []) as $actors | ($actors | any(.metadata.name == $a)) or ($actors | any(.metadata.name == $b))' >/dev/null 2>&1; then
      gone=1
      break
    fi
    sleep 3
  done
  [[ "$gone" == "1" ]] \
    || { dumpAllCheckpointdServerLogs; fail "task $task_id: actors $poll_actor/$wait_actor were not both cleaned up within the expected time after cancellation -- the relay loop never noticed TERMINATING and stopped"; }
  log "  both actors were cleaned up -- the relay loop stopped and finishTermination ran"

  log "confirming GetTask still reports Canceled after cleanup"
  local final_out
  final_out="$(a2a_cli get task "$LONG_POLL_URL" "$task_id" --tenant "$tenant" -o json)" || fail "GetTask after cancellation failed"
  [[ "$(echo "$final_out" | jq -r '.status.state')" == "TASK_STATE_CANCELED" ]] \
    || fail "task $task_id status after cleanup = '$(echo "$final_out" | jq -r '.status.state')', want TASK_STATE_CANCELED (full response: $final_out)"
  log "  task $task_id correctly stayed Canceled"
}
