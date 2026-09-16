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

# subtest_task_state_client2b drives agent-b directly from an external client instead of through agent-a, polling and continuing across each turn.
subtest_task_state_client2b() {
  log "sending a message directly to agent-b (external client, no agent-a involved)"
  local out task_id tenant initial_state
  out="$(a2a_cli send "$AGENT_B_URL" -o json "start")" || fail "a2a send to agent-b failed"
  task_id="$(echo "$out" | jq -r '.id // empty')"
  [[ -n "$task_id" && "$task_id" != "null" ]] || fail "a2a send to agent-b did not return a task id (output: $out)"
  tenant="$(taskTenant "$out")"
  log "  task $task_id submitted"

  # A plain blocking SendMessage (no ReturnImmediately) waits past agent-b's
  # own Working self-loop and only unblocks once it reaches its first
  # genuinely interrupted state, matching a2a-go's own reference
  # defaultRequestHandler.SendMessage default behavior.
  initial_state="$(echo "$out" | jq -r '.status.state // empty')"
  [[ "$initial_state" == "TASK_STATE_INPUT_REQUIRED" ]] \
    || fail "a2a send to agent-b returned status.state = '$initial_state', want TASK_STATE_INPUT_REQUIRED (should wait past agent-b's own Working self-continuation)"
  log "  send waited past agent-b's own Working self-continuation, unblocking only at TaskStateInputRequired, as expected"
  # Metadata from the earlier Working self-continuing hop is already visible here, merged forward.

  local origin
  origin="$(echo "$out" | jq -r '.metadata["agent-b/origin"] // empty')"
  [[ "$origin" == "turn1" ]] \
    || fail "agent-b's first reply has Metadata[agent-b/origin] = '$origin', want 'turn1' (full response: $out)"

  log "polling until agent-b reaches TaskStateInputRequired"
  pollTaskState "$AGENT_B_URL" "$task_id" "$tenant" TASK_STATE_INPUT_REQUIRED \
    || { dump_checkpointd_server_logs; fail "agent-b task $task_id never reached TASK_STATE_INPUT_REQUIRED (last response: $LAST_GET_OUT)"; }
  log "  agent-b reached TaskStateInputRequired"
  checkClient2bArtifact "$task_id" "$tenant" "turn1-" "turn 2 (first InputRequired)"
  checkClient2bMetadata "$LAST_GET_OUT" "agent-b/origin" "turn1" "turn 2 (first InputRequired)"

  log "sending the first continuation message (hello) against the same task"
  a2a_cli send "$AGENT_B_URL" -o json --task "$task_id" --tenant "$tenant" "hello" >/dev/null \
    || fail "a2a send (first continuation) to agent-b failed"

  log "polling until agent-b reaches TaskStateInputRequired a second time"
  pollTaskState "$AGENT_B_URL" "$task_id" "$tenant" TASK_STATE_INPUT_REQUIRED \
    || { dump_checkpointd_server_logs; fail "agent-b task $task_id never reached TaskStateInputRequired a second time (last response: $LAST_GET_OUT)"; }
  log "  agent-b reached TaskStateInputRequired a second time"
  # Turn 3 appended a second chunk to the artifact and a second Metadata key alongside turn 1's.
  checkClient2bArtifact "$task_id" "$tenant" "turn1-turn3-" "turn 3 (second InputRequired)"
  checkClient2bMetadata "$LAST_GET_OUT" "agent-b/origin" "turn1" "turn 3 (second InputRequired)"
  checkClient2bMetadata "$LAST_GET_OUT" "agent-b/turn3" "seen" "turn 3 (second InputRequired)"

  log "sending the second continuation message (world) against the same task"
  a2a_cli send "$AGENT_B_URL" -o json --task "$task_id" --tenant "$tenant" "world" >/dev/null \
    || fail "a2a send (second continuation) to agent-b failed"

  pollTaskState "$AGENT_B_URL" "$task_id" "$tenant" TASK_STATE_COMPLETED \
    || { dump_checkpointd_server_logs; fail "agent-b task $task_id never reached TASK_STATE_COMPLETED (last response: $LAST_GET_OUT)"; }
  # Retried, since status.state can reach COMPLETED slightly before status.message settles.
  local final_out final_data i
  for i in $(seq 1 15); do
    final_out="$(a2a_cli get task "$AGENT_B_URL" "$task_id" --tenant "$tenant" -o json 2>/dev/null)" || true
    final_data="$(echo "$final_out" | jq -r '.status.message.parts[0].data.data // empty')"
    [[ -n "$final_data" ]] && break
    sleep 1
  done
  [[ "$final_data" == "B received: world" ]] \
    || fail "agent-b's own final data = '$final_data', want 'B received: world' (full response: $final_out)"
  log "  agent-b completed with the correct data: $final_data"
  # Turn 4 touches neither the artifact nor Metadata, yet both must still carry forward unchanged.
  checkClient2bArtifact "$task_id" "$tenant" "turn1-turn3-" "turn 4 (Completed)"
  checkClient2bMetadata "$final_out" "agent-b/origin" "turn1" "turn 4 (Completed)"
  checkClient2bMetadata "$final_out" "agent-b/turn3" "seen" "turn 4 (Completed)"
  log "  Artifacts and Metadata both correctly preserved across all turns"
}

# checkClient2bArtifact confirms task_id's Task carries exactly one artifact, under "b-notes", whose Parts join into wantText.
checkClient2bArtifact() {
  local task_id=$1 tenant=$2 want_text=$3 label=$4
  local task got
  task="$(taskWithArtifacts "$AGENT_B_URL" "$task_id" "$tenant")" \
    || { dump_checkpointd_server_logs; fail "$label: could not fetch task $task_id with artifacts (a2a list tasks --with-artifacts)"; }
  local count
  count="$(echo "$task" | jq '.artifacts | length')"
  [[ "$count" == "1" ]] || fail "$label: Artifacts has $count entries, want 1 (task: $task)"
  local id
  id="$(echo "$task" | jq -r '.artifacts[0].artifactId // empty')"
  [[ "$id" == "b-notes" ]] || fail "$label: Artifacts[0].artifactId = '$id', want 'b-notes' (task: $task)"
  got="$(echo "$task" | jq -j '.artifacts[0].parts[].text // empty')"
  [[ "$got" == "$want_text" ]] \
    || fail "$label: artifact 'b-notes' text = '$got', want '$want_text' (turn 1's and turn 3's own chunks must merge into one artifact, not replace or duplicate each other; task: $task)"
}

# checkClient2bMetadata confirms task_json's Metadata[key] == want.
checkClient2bMetadata() {
  local task_json=$1 key=$2 want=$3 label=$4
  local got
  got="$(echo "$task_json" | jq -r --arg k "$key" '.metadata[$k] // empty')"
  [[ "$got" == "$want" ]] \
    || fail "$label: Metadata[$key] = '$got', want '$want' (task: $task_json)"
}
