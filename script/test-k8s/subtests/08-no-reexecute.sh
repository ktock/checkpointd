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

# subtest_no_reexecute confirms a completed task is not re-driven by a later checkpointd-server restart.
subtest_no_reexecute() {
  local send_resp task_id tenant
  send_resp="$(a2a_cli send "$ECHO_URL" -o json "hello-no-reexecute")" || fail "a2a send failed"
  task_id="$(echo "$send_resp" | jq -r '.id')"
  [[ -n "$task_id" && "$task_id" != "null" ]] || fail "SendMessage did not return a task id (full response: $send_resp)"
  tenant="$(taskTenant "$send_resp")"
  log "  task $task_id submitted"

  pollTaskState "$ECHO_URL" "$task_id" "$tenant" TASK_STATE_COMPLETED \
    || { dump_checkpointd_server_logs; fail "task $task_id never reached TASK_STATE_COMPLETED before the restart (last response: $LAST_GET_OUT)"; }
  local reply_text
  reply_text="$(echo "$LAST_GET_OUT" | jq -r '.status.message.parts[0].text')"
  [[ "$reply_text" == *"hello-no-reexecute"* ]] || fail "reply text = '$reply_text', want it to contain 'hello-no-reexecute' (full response: $LAST_GET_OUT)"

  log "  restarting checkpointd-server-0"
  restart_checkpointd_server_pod

  # Resuming an already-completed session is a safe no-op, so the real check is that echo-agent's own actor wasn't invoked again.
  checkCallNotRepeated "$tenant" "echo-agent" 0 "no-reexecute"

  local rerun_resp rerun_state rerun_text
  rerun_resp="$(a2a_cli get task "$ECHO_URL" "$task_id" --tenant "$tenant" -o json)" || fail "a2a get task failed after restart"
  rerun_state="$(echo "$rerun_resp" | jq -r '.status.state // empty')"
  [[ "$rerun_state" == "TASK_STATE_COMPLETED" ]] || fail "GetTask after restart: status.state = '$rerun_state', want TASK_STATE_COMPLETED (full response: $rerun_resp)"
  rerun_text="$(echo "$rerun_resp" | jq -r '.status.message.parts[0].text')"
  [[ "$rerun_text" == "$reply_text" ]] || fail "GetTask after restart returned a different result: got '$rerun_text', want '$reply_text' (task was re-executed, not replayed)"
  log "  restart correctly left the completed task alone"
}
