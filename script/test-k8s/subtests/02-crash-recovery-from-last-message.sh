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

# subtest_crash_recovery_from_last_message is a minimal version of
# crash-test-external: it force-deletes crash-recovery's own worker pod
# while it's blocked on a plain wait, after crash-recovery has already
# called echo-agent and resumed with its reply; confirms the pod (by uid)
# and the harness WorkerPool actually went down and came back, not just
# that the delete command returned; and confirms the task completes by
# resuming past that reply rather than repeating the call to echo-agent or
# replaying its workflow from the beginning. This exercises the exact
# scenario in README.md's "When does it checkpoint and resume an agent".
subtest_crash_recovery_from_last_message() {
  log "sending a message to crash-recovery"
  local CRASH_RECOVERY_ID="crash-recovery-$(date +%s)"
  local crash_recovery_out crash_recovery_task_id crash_recovery_tenant
  # --immediate: this test needs the task id back before the workflow
  # completes, to inject the worker-pod crash mid-turn.
  crash_recovery_out="$(a2a_cli send --immediate "$CRASH_RECOVERY_URL" -o json "$CRASH_RECOVERY_ID")" || fail "a2a send to crash-recovery failed"
  crash_recovery_task_id="$(echo "$crash_recovery_out" | jq -r '.id // empty')"
  [[ -n "$crash_recovery_task_id" && "$crash_recovery_task_id" != "null" ]] || fail "a2a send to crash-recovery did not return a task id (output: $crash_recovery_out)"
  crash_recovery_tenant="$(taskTenant "$crash_recovery_out")"
  log "  task $crash_recovery_task_id submitted"

  log "  waiting for crash-recovery to call echo-agent, resume with its reply, and signal readiness via testserver (up to 180s)"
  local crash_recovery_ready_code
  crash_recovery_ready_code="$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$TESTSERVER_LOCAL_PORT/wait/${CRASH_RECOVERY_ID}-ready?timeout=180")" || true
  [[ "$crash_recovery_ready_code" == "200" ]] || fail "crash-recovery never signaled readiness within 180s (id: $CRASH_RECOVERY_ID, last status: $crash_recovery_ready_code)"
  log "  crash-recovery signaled readiness -- it's now blocked waiting for its own go signal, so there's no rush -- resolving its own worker pod"

  local crash_recovery_worker crash_recovery_worker_ns crash_recovery_worker_pod
  crash_recovery_worker="$(resolveWorkerPod "$CRASH_RECOVERY_URL" "$crash_recovery_task_id" "$crash_recovery_tenant" "crash-recovery")" \
    || fail "could not resolve crash-recovery's own worker pod within the expected time"
  crash_recovery_worker_ns="${crash_recovery_worker%% *}"
  crash_recovery_worker_pod="${crash_recovery_worker##* }"
  local crash_recovery_worker_uid
  crash_recovery_worker_uid="$(run_kubectl -n "$crash_recovery_worker_ns" get pod "$crash_recovery_worker_pod" -o jsonpath='{.metadata.uid}')"
  [[ -n "$crash_recovery_worker_uid" ]] || fail "could not read the uid of worker pod $crash_recovery_worker_ns/$crash_recovery_worker_pod before deleting it"
  log "  force-deleting worker pod $crash_recovery_worker_ns/$crash_recovery_worker_pod (uid $crash_recovery_worker_uid, crash-recovery's own worker, mid-turn) -- this is Substrate's own worker-pod-gone crash detection, not checkpointd's"
  run_kubectl -n "$crash_recovery_worker_ns" delete pod "$crash_recovery_worker_pod" --grace-period=0 --force --wait=false

  log "  confirming the worker pod was actually removed, not just a --wait=false no-op"
  confirmPodGone "$crash_recovery_worker_ns" "$crash_recovery_worker_pod" "$crash_recovery_worker_uid" \
    || fail "worker pod $crash_recovery_worker_ns/$crash_recovery_worker_pod is still present with the same uid $crash_recovery_worker_uid 120s after the force-delete -- the crash injection never actually took effect"
  log "  worker pod $crash_recovery_worker_pod (uid $crash_recovery_worker_uid) confirmed gone"

  log "  confirming the harness WorkerPool self-healed back to its desired ready replica count (a real replacement worker was scheduled to take its place)"
  local crash_recovery_ready crash_recovery_want i2
  for i2 in $(seq 1 60); do
    crash_recovery_ready="$(run_kubectl -n "$NS" get workerpool "${NS}-harness" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)"
    crash_recovery_want="$(run_kubectl -n "$NS" get workerpool "${NS}-harness" -o jsonpath='{.spec.replicas}' 2>/dev/null || true)"
    [[ -n "$crash_recovery_ready" && "$crash_recovery_ready" == "$crash_recovery_want" ]] && break
    sleep 2
  done
  [[ -n "${crash_recovery_ready:-}" && "$crash_recovery_ready" == "${crash_recovery_want:-}" ]] \
    || fail "harness WorkerPool ${NS}-harness never returned to $crash_recovery_want ready replicas after the force-delete (got: ${crash_recovery_ready:-0}) -- the pool never recovered from the simulated crash"
  # Best-effort only: identifies the newest worker pod (by creation time, excluding the deleted uid) purely for diagnostic visibility -- never fails the test on its own.
  local crash_recovery_replacement_pod crash_recovery_replacement_restarts
  crash_recovery_replacement_pod="$(run_kubectl -n "$NS" get pods -o json 2>/dev/null \
    | jq -r --arg prefix "${NS}-harness-" --arg uid "$crash_recovery_worker_uid" \
        '[.items[] | select(.metadata.name | startswith($prefix)) | select(.metadata.uid != $uid)] | sort_by(.metadata.creationTimestamp) | last | .metadata.name // empty' 2>/dev/null)" || true
  if [[ -n "$crash_recovery_replacement_pod" ]]; then
    crash_recovery_replacement_restarts="$(run_kubectl -n "$NS" get pod "$crash_recovery_replacement_pod" -o jsonpath='{.status.containerStatuses[0].restartCount}' 2>/dev/null || echo "?")"
    log "  harness WorkerPool back to $crash_recovery_ready/$crash_recovery_want ready replicas -- newest worker pod is $crash_recovery_replacement_pod (container restart count: $crash_recovery_replacement_restarts -- a freshly scheduled pod, not the crashed one restarted in place)"
  fi

  log "  releasing crash-recovery to complete (POST /notify/${CRASH_RECOVERY_ID}-go) -- posted now, not after confirming the crash: testserver's own notification is durable and idempotent, so whenever the recovered actor re-polls for it, it'll already be there"
  curl -sf -X POST "http://127.0.0.1:$TESTSERVER_LOCAL_PORT/notify/${CRASH_RECOVERY_ID}-go" -d "go" >/dev/null \
    || fail "failed to POST the go signal to crash-recovery"

  log "  polling for crash-recovery's task to recover and complete"
  pollTaskState "$CRASH_RECOVERY_URL" "$crash_recovery_task_id" "$crash_recovery_tenant" TASK_STATE_COMPLETED \
    || { dump_checkpointd_server_logs; dump_testserver_diagnostics; dump_harness_worker_logs; fail "crash-recovery task $crash_recovery_task_id never recovered/completed after its worker pod was force-deleted (last response: $LAST_GET_OUT)"; }

  # Retried, since status.state can reach COMPLETED slightly before status.message settles.
  local crash_recovery_final_out crash_recovery_final_text i
  for i in $(seq 1 15); do
    crash_recovery_final_out="$(a2a_cli get task "$CRASH_RECOVERY_URL" "$crash_recovery_task_id" --tenant "$crash_recovery_tenant" -o json 2>/dev/null)" || true
    crash_recovery_final_text="$(echo "$crash_recovery_final_out" | jq -r '.status.message.parts[0].text // empty')"
    [[ -n "$crash_recovery_final_text" ]] && break
    sleep 1
  done
  [[ "$crash_recovery_final_text" == "echo-agent received $CRASH_RECOVERY_ID" ]] \
    || fail "crash-recovery's completed task message = '$crash_recovery_final_text', want 'echo-agent received $CRASH_RECOVERY_ID' -- recovery lost the echo-agent reply it had already resumed with"

  checkCallNotRepeated "$crash_recovery_tenant" "echo-agent" 1 "crash-recovery-from-last-message"
  checkEnteredOnce "$CRASH_RECOVERY_ID" "crash-recovery-from-last-message"

  # Direct proof of README.md's "resumes this agent from (1) directly from
  # the checkpoint" claim: the line right after the checkpointed send to
  # echo-agent is plain (non-a2a) code, so it is not itself covered by
  # checkpointd's own durable relay log. If recovery genuinely replays from
  # the checkpoint forward (rather than restoring exact pre-crash process
  # state), this line runs once before the crash and once again on replay.
  local crash_recovery_resumed_count
  crash_recovery_resumed_count="$(curl -sf "http://127.0.0.1:$TESTSERVER_LOCAL_PORT/count/${CRASH_RECOVERY_ID}-resumed")" \
    || fail "crash-recovery-from-last-message: could not read its own resumed-count from testserver (id: $CRASH_RECOVERY_ID)"
  [[ "$crash_recovery_resumed_count" == "2" ]] \
    || fail "crash-recovery-from-last-message: the line right after resuming with echo-agent's reply ran $crash_recovery_resumed_count times (want exactly 2) -- this contradicts README.md's claim that checkpointd resumes this agent directly from the checkpoint after echo-agent's reply, replaying forward from there rather than some other point"

  log "  crash-recovery recovered from a real worker-pod crash and completed by resuming from echo-agent's reply, with echo-agent invoked exactly once (not repeated), its own workflow entered exactly once (no full-refresh discard), and the code right after the checkpoint re-run exactly twice (direct proof of resuming from that checkpoint, not an exact pre-crash snapshot restore)"
}
