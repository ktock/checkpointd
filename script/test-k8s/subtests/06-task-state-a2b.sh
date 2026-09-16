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

# subtest_task_state_a2b drives agent-a, which drives agent-b through nested SendMessage calls, and checks agent-a's own PASS/FAIL verdict.
subtest_task_state_a2b() {
  log "sending a message to agent-a (drives agent-b through nested SendMessage calls)"
  local out task_id tenant
  out="$(a2a_cli send "$AGENT_A_URL" -o json "start")" || fail "a2a send to agent-a failed"
  task_id="$(echo "$out" | jq -r '.id // empty')"
  [[ -n "$task_id" && "$task_id" != "null" ]] || fail "a2a send to agent-a did not return a task id (output: $out)"
  tenant="$(taskTenant "$out")"
  log "  task $task_id submitted"

  pollTaskState "$AGENT_A_URL" "$task_id" "$tenant" TASK_STATE_COMPLETED \
    || { dump_checkpointd_server_logs; fail "agent-a task $task_id never reached TASK_STATE_COMPLETED (last response: $LAST_GET_OUT)"; }
  # Retried, since status.state can reach COMPLETED slightly before status.message settles.
  local final_out final_text i
  for i in $(seq 1 15); do
    final_out="$(a2a_cli get task "$AGENT_A_URL" "$task_id" --tenant "$tenant" -o json 2>/dev/null)" || true
    final_text="$(echo "$final_out" | jq -r '.status.message.parts[0].text // empty')"
    [[ -n "$final_text" ]] && break
    sleep 1
  done
  case "$final_text" in
    PASS:*) log "  agent-a: $final_text" ;;
    *) dump_checkpointd_server_logs; fail "agent-a's own final reply = '$final_text', want it to start with 'PASS:' (agent-a's own internal assertions on agent-b's Task-state transitions failed)" ;;
  esac
}
