#!/usr/bin/env bash

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

# Tears down what setup.sh created: the kind cluster, plus any leftover
# `kubectl port-forward` from demo.sh.
#
# Usage: examples/llm-chat-demo/cleanup.sh
#
# Env overrides (must match whatever setup.sh was actually run with):
#   KIND_CLUSTER_NAME   kind cluster to delete (default: checkpointd-llm-chat-demo)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

log() { echo "[llm-chat-demo cleanup] $*" >&2; }

KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-checkpointd-llm-chat-demo}"
KUBECTL_CONTEXT="kind-${KIND_CLUSTER_NAME}"

command -v kind >/dev/null 2>&1 || { log "kind not found on PATH"; exit 1; }

# Catches a demo.sh run that was interrupted before its own cleanup trap fired.
log "killing any leftover 'kubectl ... port-forward' against $KUBECTL_CONTEXT"
pkill -f "kubectl --context $KUBECTL_CONTEXT .*port-forward" 2>/dev/null || true

if ! kind get clusters 2>/dev/null | grep -qx "$KIND_CLUSTER_NAME"; then
  log "no kind cluster named '$KIND_CLUSTER_NAME' found, nothing to delete"
  exit 0
fi

# Also removes every namespace, Deployment, WorkerPool, ActorTemplate, and
# StatefulSet inside it -- nothing else in this demo needs cleaning up
# individually.
log "deleting kind cluster '$KIND_CLUSTER_NAME'"
KIND_CLUSTER_NAME="$KIND_CLUSTER_NAME" "$REPO_ROOT/script/lib/delete-kind-cluster.sh"

log "PASS -- cluster '$KIND_CLUSTER_NAME' deleted. Run examples/llm-chat-demo/setup.sh to start over."
