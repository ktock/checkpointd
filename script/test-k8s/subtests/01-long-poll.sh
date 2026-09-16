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

# subtest_long_poll runs two concurrent long-poll tasks, crashing an actor
# and then the whole server pod before either is notified, confirming the
# pod restart resumes checkpointd's own relay loop from the event log rather
# than re-entering either workflow from the beginning.
subtest_long_poll() {
  local LONG_WAIT_ID_1="demo-long-wait-1"
  local LONG_WAIT_ID_2="demo-long-wait-2"

  log "sending the first long-poll request via the a2a CLI, from the host, outside the cluster"
  local send_out_1 task_id_1 tenant_1
  # --immediate: long-poll's own workflow stays Working until notified below,
  # arbitrarily later -- without it, this call would now block for the whole
  # workflow (a2a-go's own reference default), well past its own timeout.
  send_out_1="$(a2a_cli send --immediate "$LONG_POLL_URL" -o json "$LONG_WAIT_ID_1")" || fail "a2a send (1) failed"
  task_id_1="$(echo "$send_out_1" | jq -r '.id')"
  [[ -n "$task_id_1" && "$task_id_1" != "null" ]] || fail "a2a send (1) did not return a task id (output: $send_out_1)"
  tenant_1="$(taskTenant "$send_out_1")"
  [[ -n "$tenant_1" ]] || fail "a2a send (1) did not return a session id in metadata (output: $send_out_1)"
  log "  task $task_id_1 submitted"

  log "sending the second, concurrent long-poll request"
  local send_out_2 task_id_2 tenant_2
  send_out_2="$(a2a_cli send --immediate "$LONG_POLL_URL" -o json "$LONG_WAIT_ID_2")" || fail "a2a send (2) failed"
  task_id_2="$(echo "$send_out_2" | jq -r '.id')"
  [[ -n "$task_id_2" && "$task_id_2" != "null" ]] || fail "a2a send (2) did not return a task id (output: $send_out_2)"
  tenant_2="$(taskTenant "$send_out_2")"
  [[ -n "$tenant_2" ]] || fail "a2a send (2) did not return a session id in metadata (output: $send_out_2)"
  log "  task $task_id_2 submitted"

  log "polling for both tasks to still be in progress (non-terminal)"
  pollTaskNotTerminal "$LONG_POLL_URL" "$task_id_1" "$tenant_1" \
    || { dump_checkpointd_server_logs; fail "task $task_id_1 never reached a non-terminal state (last response: $LAST_GET_OUT)"; }
  pollTaskNotTerminal "$LONG_POLL_URL" "$task_id_2" "$tenant_2" \
    || { dump_checkpointd_server_logs; fail "task $task_id_2 never reached a non-terminal state (last response: $LAST_GET_OUT)"; }
  log "  both tasks are in progress -- injecting crashes before ever notifying either one"

  log "confirming both tasks hold genuinely distinct, isolated long-wait actors"
  local wait_actor_count=0 wait_actor_names="" actors_json i
  local expect_name_1 expect_name_2
  expect_name_1="$(actorName "$tenant_1" long-wait)"
  expect_name_2="$(actorName "$tenant_2" long-wait)"
  for i in $(seq 1 60); do
    actors_json="$(run_kubectl_ate get actors -a "$NS" -o json 2>/dev/null)"
    wait_actor_count="$(echo "$actors_json" | jq -r '[.actors[] | select(.actorTemplateName=="long-wait-template")] | length')"
    wait_actor_names="$(echo "$actors_json" | jq -r '[.actors[] | select(.actorTemplateName=="long-wait-template")] | .[].metadata.name' | tr '\n' ' ')"
    [[ "$wait_actor_count" == "2" ]] && break
    sleep 3
  done
  [[ "$wait_actor_count" == "2" ]] \
    || fail "expected 2 distinct long-wait actors (one per concurrent task), found $wait_actor_count (names seen: [$wait_actor_names]; expected $expect_name_1 for tenant_1=$tenant_1 and $expect_name_2 for tenant_2=$tenant_2) -- the two tasks may be sharing one actor"
  log "  confirmed: 2 distinct long-wait actors, one per task"

  log "durability test 1/2: killing task $task_id_1's own long-wait actor mid-poll"
  # Each task gets its own randomly generated instance id, exposed via GetTask's Metadata, used to reconstruct the actor name.
  local wait_actor
  wait_actor="checkpointd-${tenant_1}-long-wait"
  if run_kubectl_ate get actors -a "$NS" -o json 2>/dev/null | jq -e --arg name "$wait_actor" '.actors[] | select(.metadata.name == $name)' >/dev/null 2>&1; then
    run_kubectl_ate delete actor "$wait_actor" -a "$NS" --any-state || true
    log "  deleted actor $wait_actor"
  else
    log "  no currently-assigned actor $wait_actor found (caught it between polls); continuing"
  fi
  log "  confirming task $task_id_1 recovers and keeps progressing after the actor crash"
  local recovered=0
  for i in $(seq 1 40); do
    if run_kubectl -n "$NS" logs pod/checkpointd-server-0 --tail=20 2>/dev/null | grep -q "relaying"; then
      recovered=1
      break
    fi
    sleep 3
  done
  [[ "$recovered" == "1" ]] || { dump_checkpointd_server_logs; fail "task $task_id_1 did not recover after the actor crash within the expected time"; }
  pollTaskNotTerminal "$LONG_POLL_URL" "$task_id_1" "$tenant_1" \
    || { dump_checkpointd_server_logs; fail "task $task_id_1 did not return to a non-terminal state after its actor crash (last response: $LAST_GET_OUT)"; }
  log "  task $task_id_1 recovered"

  log "  confirming task $task_id_2 was undisturbed by task $task_id_1's actor crash"
  pollTaskNotTerminal "$LONG_POLL_URL" "$task_id_2" "$tenant_2" \
    || fail "task $task_id_2 unexpectedly left its non-terminal state after task $task_id_1's actor crash"

  log "durability test 2/2: deleting the whole checkpointd-server-0 pod (affects both concurrent tasks at once, recovered via the PVC-backed event log and resumeServerSessions)"
  restart_checkpointd_server_pod
  log "  pod is back -- confirming both tasks resumed"
  pollTaskNotTerminal "$LONG_POLL_URL" "$task_id_1" "$tenant_1" \
    || { dump_checkpointd_server_logs; fail "task $task_id_1 did not resume after the whole-pod crash (last response: $LAST_GET_OUT)"; }
  pollTaskNotTerminal "$LONG_POLL_URL" "$task_id_2" "$tenant_2" \
    || { dump_checkpointd_server_logs; fail "task $task_id_2 did not resume after the whole-pod crash (last response: $LAST_GET_OUT)"; }
  log "  both tasks survived the whole-pod crash"

  log "  confirming checkpointd recovered its relay loop for both tasks from the event log, not by restarting either long-poll workflow from the beginning"
  checkEnteredOnce "$LONG_WAIT_ID_1" "long-poll (task 1)"
  checkEnteredOnce "$LONG_WAIT_ID_2" "long-poll (task 2)"

  log "sending both notifications (POST /notify/$LONG_WAIT_ID_1, POST /notify/$LONG_WAIT_ID_2)"
  run_kubectl -n "$NS" run "long-wait-curl-1-$(date +%s)" --image=curlimages/curl:latest --restart=Never --rm -i --command -- \
    curl -sf -X POST "http://testserver.$NS.svc:8080/notify/$LONG_WAIT_ID_1" -d "notified-by-test-1" \
    || fail "failed to POST the first notification"
  run_kubectl -n "$NS" run "long-wait-curl-2-$(date +%s)" --image=curlimages/curl:latest --restart=Never --rm -i --command -- \
    curl -sf -X POST "http://testserver.$NS.svc:8080/notify/$LONG_WAIT_ID_2" -d "notified-by-test-2" \
    || fail "failed to POST the second notification"

  log "polling for both tasks to complete"
  pollTaskState "$LONG_POLL_URL" "$task_id_1" "$tenant_1" TASK_STATE_COMPLETED \
    || { dump_checkpointd_server_logs; fail "task $task_id_1 never reached TASK_STATE_COMPLETED (last response: $LAST_GET_OUT)"; }
  pollTaskState "$LONG_POLL_URL" "$task_id_2" "$tenant_2" TASK_STATE_COMPLETED \
    || { dump_checkpointd_server_logs; fail "task $task_id_2 never reached TASK_STATE_COMPLETED (last response: $LAST_GET_OUT)"; }
  log "  both long-poll tasks completed, triggered entirely from outside the cluster"

  log "checking ListTasks against long-poll's own path reports both completed tasks"
  local list_out list_state id
  list_out="$(a2a_cli list tasks "$LONG_POLL_URL" -o json --limit 10)" || fail "a2a list tasks failed"
  for id in "$task_id_1" "$task_id_2"; do
    list_state="$(echo "$list_out" | jq -r --arg id "$id" '.tasks[] | select(.id == $id) | .status.state')"
    [[ "$list_state" == "TASK_STATE_COMPLETED" ]] || fail "ListTasks: task $id status.state = '$list_state', want TASK_STATE_COMPLETED (full response: $list_out)"
  done
  log "  ListTasks correctly reports both completed tasks"
}
