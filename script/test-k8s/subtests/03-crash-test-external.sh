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

# subtest_crash_test_external force-deletes crash-test-external's worker pod mid-turn and confirms it recovers without repeating a call.
subtest_crash_test_external() {
  log "sending a message to crash-test-external"
  local CRASH_EXT_ID="crash-ext-$(date +%s)"
  local crash_ext_out crash_ext_task_id crash_ext_tenant
  # --immediate: this test needs the task id back before the workflow
  # completes, to inject the worker-pod crash mid-turn -- without it, this
  # call would now block for the whole workflow (a2a-go's own reference
  # default) and deadlock against the crash injection below.
  crash_ext_out="$(a2a_cli send --immediate "$CRASH_EXT_URL" -o json "$CRASH_EXT_ID")" || fail "a2a send to crash-test-external failed"
  crash_ext_task_id="$(echo "$crash_ext_out" | jq -r '.id // empty')"
  [[ -n "$crash_ext_task_id" && "$crash_ext_task_id" != "null" ]] || fail "a2a send to crash-test-external did not return a task id (output: $crash_ext_out)"
  crash_ext_tenant="$(taskTenant "$crash_ext_out")"
  log "  task $crash_ext_task_id submitted"

  log "  waiting for crash-test-external to signal readiness via testserver (up to 180s)"
  local crash_ext_ready_code
  crash_ext_ready_code="$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$TESTSERVER_LOCAL_PORT/wait/${CRASH_EXT_ID}-ready?timeout=180")" || true
  [[ "$crash_ext_ready_code" == "200" ]] || fail "crash-test-external never signaled readiness within 180s (id: $CRASH_EXT_ID, last status: $crash_ext_ready_code)"
  log "  crash-test-external signaled readiness -- it's now retrying against long-wait for its own go signal (see script/test-k8s/agents/crash_test_agent), so there's no rush -- resolving its own worker pod"

  local crash_ext_worker crash_ext_worker_ns crash_ext_worker_pod
  crash_ext_worker="$(resolveWorkerPod "$CRASH_EXT_URL" "$crash_ext_task_id" "$crash_ext_tenant" "crash-test-external")" \
    || fail "could not resolve crash-test-external's own worker pod within the expected time"
  crash_ext_worker_ns="${crash_ext_worker%% *}"
  crash_ext_worker_pod="${crash_ext_worker##* }"
  log "  force-deleting worker pod $crash_ext_worker_ns/$crash_ext_worker_pod (crash-test-external's own worker, mid-turn) -- this is Substrate's own worker-pod-gone crash detection, not checkpointd's"
  run_kubectl -n "$crash_ext_worker_ns" delete pod "$crash_ext_worker_pod" --grace-period=0 --force --wait=false

  log "  releasing crash-test-external to proceed with its remaining calls (POST /notify/${CRASH_EXT_ID}-go) -- posted now, not after confirming the crash: testserver's own notification is durable and idempotent, so whenever the recovered actor re-polls for it, it'll already be there"
  curl -sf -X POST "http://127.0.0.1:$TESTSERVER_LOCAL_PORT/notify/${CRASH_EXT_ID}-go" -d "go" >/dev/null \
    || fail "failed to POST the go signal to crash-test-external"

  log "  polling for crash-test-external's task to recover (via recoverCrashedActor) and complete"
  pollTaskState "$CRASH_EXT_URL" "$crash_ext_task_id" "$crash_ext_tenant" TASK_STATE_COMPLETED \
    || { dump_checkpointd_server_logs; dump_testserver_diagnostics; dump_harness_worker_logs; fail "crash-test-external task $crash_ext_task_id never recovered/completed after its worker pod was force-deleted (last response: $LAST_GET_OUT)"; }
  checkCallNotRepeated "$crash_ext_tenant" "echo-agent" 3 "crash-test-external"
  checkEnteredOnce "$CRASH_EXT_ID" "crash-test-external"
  log "  crash-test-external recovered from a real worker-pod crash and completed, with echo-agent invoked exactly 3 times total (call-1 confirmed not repeated) and its own workflow entered exactly once (no full-refresh discard)"
}
