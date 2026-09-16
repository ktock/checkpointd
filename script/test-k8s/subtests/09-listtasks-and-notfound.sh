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

# subtest_listtasks_and_notfound checks ListTasks and GetTask for an unknown id against a real cluster.
subtest_listtasks_and_notfound() {
  local send_resp task_id tenant
  send_resp="$(a2a_cli send "$ECHO_URL" -o json "hello-listtasks")" || fail "a2a send failed"
  task_id="$(echo "$send_resp" | jq -r '.id')"
  [[ -n "$task_id" && "$task_id" != "null" ]] || fail "SendMessage did not return a task id (full response: $send_resp)"
  tenant="$(taskTenant "$send_resp")"
  pollTaskState "$ECHO_URL" "$task_id" "$tenant" TASK_STATE_COMPLETED \
    || { dump_checkpointd_server_logs; fail "task $task_id never reached TASK_STATE_COMPLETED (last response: $LAST_GET_OUT)"; }

  local list_resp list_state
  list_resp="$(a2a_cli list tasks "$ECHO_URL" -o json --limit 50)" || fail "a2a list tasks failed"
  list_state="$(echo "$list_resp" | jq -r --arg id "$task_id" '.tasks[] | select(.id == $id) | .status.state')"
  [[ "$list_state" == "TASK_STATE_COMPLETED" ]] || fail "ListTasks: task $task_id status.state = '$list_state', want TASK_STATE_COMPLETED (full response: $list_resp)"
  log "  ListTasks reports task $task_id as completed"

  local notfound_out notfound_rc=0
  notfound_out="$("$A2A_CLI" --transport jsonrpc get task "$ECHO_URL" bogus-task-id -o json 2>&1)" || notfound_rc=$?
  [[ "$notfound_rc" -ne 0 ]] || fail "GetTask for a bogus id unexpectedly succeeded (output: $notfound_out)"
  echo "$notfound_out" | grep -qi "not found" \
    || fail "GetTask for a bogus id failed for an unexpected reason, want a not-found error (output: $notfound_out)"
  log "  unknown task id correctly rejected"
}
