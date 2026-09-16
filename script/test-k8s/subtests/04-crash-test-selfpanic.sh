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

# subtest_crash_test_selfpanic has the agent process panic itself, with no external pod deletion, and confirms it still recovers exactly once.
subtest_crash_test_selfpanic() {
  log "sending a message to crash-test-selfpanic"
  local CRASH_SELFPANIC_ID="crash-selfpanic-$(date +%s)"
  local crash_sp_out crash_sp_task_id crash_sp_tenant
  # --immediate: crash-test-selfpanic's own workflow doesn't reach a
  # terminal/interrupted state until well after its self-panic and recovery
  # -- without it, this call would now block for that whole recovery (a2a-go's
  # own reference default) rather than the more generous pollTaskState below.
  crash_sp_out="$(a2a_cli send --immediate "$CRASH_SELFPANIC_URL" -o json "$CRASH_SELFPANIC_ID")" || fail "a2a send to crash-test-selfpanic failed"
  crash_sp_task_id="$(echo "$crash_sp_out" | jq -r '.id // empty')"
  [[ -n "$crash_sp_task_id" && "$crash_sp_task_id" != "null" ]] || fail "a2a send to crash-test-selfpanic did not return a task id (output: $crash_sp_out)"
  crash_sp_tenant="$(taskTenant "$crash_sp_out")"
  log "  task $crash_sp_task_id submitted -- this agent panics itself right after signaling readiness, with no external crash injection at all"

  log "  polling for crash-test-selfpanic's task to recover and complete"
  pollTaskState "$CRASH_SELFPANIC_URL" "$crash_sp_task_id" "$crash_sp_tenant" TASK_STATE_COMPLETED \
    || { dump_checkpointd_server_logs; fail "crash-test-selfpanic task $crash_sp_task_id never recovered/completed after its own self-panic (last task response: $LAST_GET_OUT)"; }
  checkCallNotRepeated "$crash_sp_tenant" "echo-agent" 3 "crash-test-selfpanic"
  checkEnteredOnce "$CRASH_SELFPANIC_ID" "crash-test-selfpanic"
  log "  crash-test-selfpanic recovered from its own self-panic and completed, with echo-agent invoked exactly 3 times total (call-1 confirmed not repeated) and its own workflow entered exactly once (no full-refresh discard)"
}
