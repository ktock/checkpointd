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

# subtest_tck runs the real A2A TCK suite as a throwaway in-cluster pod against checkpointd-server's tck-agent path.
subtest_tck() {
  log "running the A2A TCK (MUST level, jsonrpc transport) against checkpointd-server's tck-agent"
  local TCK_POD="tck-run-$(date +%s)"
  # A plain Pod manifest, not `kubectl run`, so it can set explicit resource requests.
  run_kubectl -n "$NS" create -f - <<EOF || fail "could not start $TCK_POD"
apiVersion: v1
kind: Pod
metadata:
  name: $TCK_POD
spec:
  restartPolicy: Never
  containers:
    - name: $TCK_POD
      image: $TCK_RUNNER_IMAGE
      command:
        - python3
        - run_tck.py
        - --sut-host
        - http://checkpointd-server.$NS.svc:80/agents/tck-agent
        - --transport
        - jsonrpc
        - --level
        - must
        - -v
      resources:
        requests:
          cpu: 500m
          memory: 256Mi
EOF
  local tck_phase="" i
  for i in $(seq 1 60); do
    tck_phase="$(run_kubectl -n "$NS" get pod "$TCK_POD" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    [[ "$tck_phase" == "Succeeded" || "$tck_phase" == "Failed" ]] && break
    sleep 5
  done
  log "--- TCK output ---"
  run_kubectl -n "$NS" logs "pod/$TCK_POD" 2>&1 | sed 's/^/[tck] /' >&2 || true
  run_kubectl -n "$NS" delete pod "$TCK_POD" --wait=false >/dev/null 2>&1 || true
  if [[ "$tck_phase" == "Succeeded" ]]; then
    log "TCK: all MUST-level jsonrpc requirements passed"
  elif [[ "$tck_phase" == "Failed" ]]; then
    dump_checkpointd_server_logs
    fail "TCK: some MUST-level jsonrpc requirements failed -- see [tck] output above for the real gap(s), and [checkpointd-server] above for what checkpointd itself was doing at the time"
  else
    dump_checkpointd_server_logs
    fail "TCK: run did not finish within the expected time (last phase: $tck_phase)"
  fi
}
