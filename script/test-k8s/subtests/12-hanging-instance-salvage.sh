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

# subtest_hanging_instance_salvage confirms Kubernetes' own livenessProbe and
# container-level restartPolicy: Never -- not any explicit crash or exit from
# checkpointd itself -- are what recover a genuinely HUNG instance: one whose
# process never crashes or exits on its own, just stops responding to
# everything, including its own probes.
#
# signalCheckpointd STOP pauses (SIGSTOP) checkpointd's own process directly,
# without touching the pod or container -- the process keeps existing, just
# answers nothing. readinessProbe pulls the pod out of the Service quickly;
# once livenessProbe's own failureThreshold trips, kubelet kills the (still
# paused) container, and because restartPolicy: Never forbids an in-place
# restart, the container stays dead instead of coming back with the same
# uid -- unlike the default restartPolicy, which would keep restarting it in
# place forever, permanently stranding its sessions (a live checkpointd
# process can never reclaim ownership it never actually released).
#
# Once the container actually dies, the StatefulSet controller notices and
# recreates that same ordinal (same name, new uid)
# so fast that its brief Failed phase isn't reliably observable by polling
# -- not by this test, and not by another instance's own salvage sweep
# either (clientsetPodExistenceChecker.PodExists is a single point-in-time
# Get, not a watch). So this hangs the *highest* ordinal and, once it's
# confirmed unhealthy, scales the StatefulSet down by one -- StatefulSet
# scale-down always targets the highest ordinal too, so this prevents the
# controller from ever recreating it, giving both this test and any
# salvage sweep a stable, genuinely-gone pod to observe instead of racing
# a sub-second recreation window.
subtest_hanging_instance_salvage() {
  log "confirming the initial checkpointd-server StatefulSet is fully rolled out"
  run_kubectl -n "$NS" rollout status statefulset/checkpointd-server --timeout=120s \
    || { dumpAllCheckpointdServerLogs; fail "checkpointd-server StatefulSet never became fully Ready before the test began"; }
  local initial_replicas
  initial_replicas="$(run_kubectl -n "$NS" get statefulset checkpointd-server -o jsonpath='{.spec.replicas}')"
  [[ "$initial_replicas" -ge 2 ]] || fail "this subtest needs at least 2 initial replicas (got $initial_replicas) to exercise salvage by a surviving instance"

  local target_pod="checkpointd-server-$((initial_replicas - 1))"
  local target_uid
  target_uid="$(run_kubectl -n "$NS" get pod "$target_pod" -o jsonpath='{.metadata.uid}')"

  local fwdh porth pidh directURLh
  fwdh="$(portForwardToPod "$target_pod" 80)" || fail "could not port-forward directly to $target_pod"
  porth="${fwdh%% *}"; pidh="${fwdh##* }"
  directURLh="http://127.0.0.1:$porth/agents/long-poll/"

  local HANG_WAIT_ID="hanging-instance-$(date +%s)"
  local h_out h_task_id h_tenant
  h_out="$(a2a_cli send --immediate "$directURLh" -o json "$HANG_WAIT_ID")" || { kill "$pidh" 2>/dev/null; fail "a2a send directly to $target_pod failed"; }
  h_task_id="$(echo "$h_out" | jq -r '.id // empty')"
  h_tenant="$(taskTenant "$h_out")"
  [[ -n "$h_task_id" && "$h_task_id" != "null" ]] || { kill "$pidh" 2>/dev/null; fail "a2a send directly to $target_pod did not return a task id (output: $h_out)"; }
  pollTaskNotTerminal "$directURLh" "$h_task_id" "$h_tenant" \
    || { kill "$pidh" 2>/dev/null; dumpAllCheckpointdServerLogs; fail "hanging-instance-salvage: task $h_task_id never reached a non-terminal (actively RUNNING) state on $target_pod"; }
  log "  task $h_task_id started on $target_pod and is actively RUNNING (long-poll's own retry loop against long-wait), owner_pod = $target_pod"
  kill "$pidh" 2>/dev/null; wait "$pidh" 2>/dev/null || true

  signalCheckpointd "$target_pod" STOP
  log "  $target_pod's own checkpointd process is now paused (SIGSTOP) -- it still exists, but answers nothing, including /healthz and /ready"

  log "  waiting for readinessProbe to pull $target_pod out of the Service (should be fast: periodSeconds=3)"
  local i ready=""
  for i in $(seq 1 60); do
    ready="$(run_kubectl -n "$NS" get pod "$target_pod" -o jsonpath='{.status.containerStatuses[0].ready}' 2>/dev/null || true)"
    [[ "$ready" == "false" ]] && break
    sleep 2
  done
  [[ "$ready" == "false" ]] || fail "$target_pod's own readinessProbe never reported NotReady after its checkpointd process was paused"
  log "  $target_pod is NotReady (readinessProbe failing against the paused process)"

  log "  scaling checkpointd-server down by 1 -- StatefulSet scale-down always removes the highest ordinal first, so this targets $target_pod specifically and stops the StatefulSet controller from ever recreating it once livenessProbe kills it"
  run_kubectl -n "$NS" scale statefulset/checkpointd-server --replicas="$((initial_replicas - 1))"

  log "  waiting for livenessProbe's own failureThreshold to trip, kubelet to kill the paused container, and $target_pod to actually disappear (can take a couple of minutes: probe failureThreshold, then the full terminationGracePeriodSeconds before SIGKILL finally lands on a process that's ignoring SIGTERM while stopped)"
  confirmPodGone "$NS" "$target_pod" "$target_uid" \
    || fail "$target_pod (uid $target_uid) never actually disappeared -- livenessProbe + restartPolicy: Never never kicked in as expected"
  log "  $target_pod confirmed gone -- restartPolicy: Never prevented any in-place restart, and the scale-down stopped the StatefulSet from recreating it"

  # As in 10-multi-replica-scaling.sh's own scenarios: the long-lived
  # svc/checkpointd-server port-forward may still be attached to $target_pod.
  start_port_forward

  log "  releasing long-wait's notification so $target_pod's orphaned session can complete once salvaged"
  curl -sf -X POST "http://127.0.0.1:$TESTSERVER_LOCAL_PORT/notify/$HANG_WAIT_ID" -d "notified" >/dev/null \
    || fail "failed to POST the notification"

  log "  polling for a surviving instance's salvage sweep to claim and complete task $h_task_id (no client request re-targets it, and $target_pod is never coming back under its own name -- this must happen purely from the periodic/startup salvage loop, not from any client-driven ClaimSession path or same-name self-recovery)"
  pollTaskState "$LONG_POLL_URL" "$h_task_id" "$h_tenant" TASK_STATE_COMPLETED \
    || { dumpAllCheckpointdServerLogs; fail "hanging-instance-salvage: task $h_task_id was never salvaged and completed by a surviving instance after $target_pod hung (last response: $LAST_GET_OUT)"; }
  log "  task $h_task_id was salvaged by a surviving instance's orphan-session salvage loop and completed correctly, even though $target_pod's own checkpointd process never crashed, panicked, or exited on its own"

  log "confirming long-poll's own workflow was entered exactly once -- proves the salvaging instance resumed long-poll's own actor checkpoint rather than restarting its workflow from scratch"
  checkEnteredOnce "$HANG_WAIT_ID" "hanging-instance-salvage"
  log "  long-poll was entered exactly once, not re-entered after salvage"
}
