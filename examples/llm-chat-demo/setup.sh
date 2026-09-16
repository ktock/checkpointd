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

# Sets up a kind cluster with Agent Substrate, checkpointd, and both demo agents (chat-agent, reviewer-agent) deployed and discovered.
#
# Deliberately not ephemeral, unlike script/test-k8s/test.sh: this leaves the cluster running when it exits.
#
# Assumes: docker, kind, kubectl, go, git, and envsubst are installed and working.
#
# Usage: examples/llm-chat-demo/setup.sh
#
# Env overrides:
#   KIND_CLUSTER_NAME   kind cluster to create (default: checkpointd-llm-chat-demo)
#   KIND_WORKER_NODES   Worker nodes alongside the one control-plane (default: 1)
#   SUBSTRATE_REPO      Agent Substrate git remote to clone (default: upstream)
#   SUBSTRATE_BRANCH    Branch to clone (default: main)
#   SUBSTRATE_COMMIT    Commit to check out (default: pinned)
#   ATE_INSTALL_ROLLOUT_TIMEOUT  Per-component `kubectl rollout status` timeout (default: 180s)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
K8S_DIR="$SCRIPT_DIR/manifests"
cd "$REPO_ROOT"

log() { echo "[llm-chat-demo setup] $*" >&2; }
fail() { echo "[llm-chat-demo setup] FAIL: $*" >&2; exit 1; }

NS="checkpointd-llm-chat-demo"
IMAGE_TAG="dev"

KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-checkpointd-llm-chat-demo}"
export KIND_CLUSTER_NAME
KIND_WORKER_NODES="${KIND_WORKER_NODES:-1}"
export KIND_WORKER_NODES
KUBECTL_CONTEXT="kind-${KIND_CLUSTER_NAME}"

SUBSTRATE_REPO="${SUBSTRATE_REPO:-https://github.com/agent-substrate/substrate}"
SUBSTRATE_BRANCH="${SUBSTRATE_BRANCH:-main}"
SUBSTRATE_COMMIT="${SUBSTRATE_COMMIT:-d909d690532b3e2e06496cfdd9e1200e56b2c27b}"
ATE_INSTALL_ROLLOUT_TIMEOUT="${ATE_INSTALL_ROLLOUT_TIMEOUT:-180s}"
export ATE_INSTALL_ROLLOUT_TIMEOUT

log "checking docker, kind, kubectl, go, git, envsubst"
command -v docker >/dev/null 2>&1 || fail "docker not found on PATH"
docker info >/dev/null 2>&1 || fail "docker is installed but not usable (daemon not reachable) -- please fix docker first"
command -v kind >/dev/null 2>&1 || fail "kind not found on PATH"
command -v kubectl >/dev/null 2>&1 || fail "kubectl not found on PATH"
command -v go >/dev/null 2>&1 || fail "go not found on PATH"
command -v git >/dev/null 2>&1 || fail "git not found on PATH"
command -v envsubst >/dev/null 2>&1 || fail "envsubst not found on PATH (part of gettext-base, used to render manifests)"

SUBSTRATE_DIR="$(mktemp -d -t checkpointd-llm-chat-demo-substrate.XXXXXXXX)"
WORKDIR="$(mktemp -d -t checkpointd-llm-chat-demo-manifests.XXXXXXXX)"
cleanup() {
  rm -rf "$SUBSTRATE_DIR" "$WORKDIR"
}
trap cleanup EXIT

log "cloning $SUBSTRATE_REPO@$SUBSTRATE_BRANCH into $SUBSTRATE_DIR"
git clone --branch "$SUBSTRATE_BRANCH" --single-branch --quiet "$SUBSTRATE_REPO" "$SUBSTRATE_DIR" >&2 \
  || fail "could not clone $SUBSTRATE_REPO@$SUBSTRATE_BRANCH"
git -C "$SUBSTRATE_DIR" checkout --quiet "$SUBSTRATE_COMMIT" \
  || fail "commit $SUBSTRATE_COMMIT not found on $SUBSTRATE_REPO@$SUBSTRATE_BRANCH"

log "creating kind cluster '$KIND_CLUSTER_NAME' ($KIND_WORKER_NODES worker node(s)) and its own registry"
# create-kind-cluster.sh prints this cluster's own registry address as its sole stdout line.
REGISTRY="$(KIND_BIN="$SUBSTRATE_DIR/hack/kind.sh" SUBSTRATE_DIR="$SUBSTRATE_DIR" \
  "$REPO_ROOT/script/lib/create-kind-cluster.sh")" || fail "kind cluster creation failed"

log "checking the image registry ($REGISTRY) is reachable from this host"
if ! curl -fsS "http://$REGISTRY/v2/_catalog" >/dev/null 2>&1; then
  fail "can't reach http://$REGISTRY/v2/_catalog right after creating the cluster -- the 'kind with local registry' setup didn't come up as expected"
fi

log "installing Agent Substrate (ate-system, atenet) -- this is the slow part (rollout timeout: $ATE_INSTALL_ROLLOUT_TIMEOUT)"
# Agent Substrate's own install script waits on several rollouts in turn, typically ~2 minutes total.
ate_installed=0
for attempt in 1 2 3; do
  if (cd "$SUBSTRATE_DIR" && NO_DEV_ENV=true KO_DOCKER_REPO="$REGISTRY" \
    ./hack/install-ate-kind.sh --deploy-ate-system --deploy-atenet); then
    ate_installed=1
    break
  fi
  log "Agent Substrate install attempt $attempt/3 failed -- retrying"
  sleep 5
done
[[ "$ate_installed" == "1" ]] || fail "Agent Substrate install failed after 3 attempts"

log "resolving and pushing the ateom-gvisor worker image via ko"
# run-tool.sh runs Agent Substrate's own pinned `ko` build, rather than relying on a system-installed one.
ATEOM_IMAGE="$(cd "$SUBSTRATE_DIR" && KO_DOCKER_REPO="$REGISTRY" ./hack/run-tool.sh ko build ./cmd/ateom-gvisor)"
[[ "$ATEOM_IMAGE" == "$REGISTRY"/* ]] || fail "ko build didn't print a resolved image reference (got: $ATEOM_IMAGE)"
log "  using ateomImage: $ATEOM_IMAGE"

build_and_push() {
  local dockerfile=$1 target=$2 name=$3
  local ref="$REGISTRY/checkpointd-llm-chat-demo-$name:$IMAGE_TAG"
  log "  building $name ($target)"
  docker build --target "$target" -f "$dockerfile" -t "$ref" "$REPO_ROOT" >&2
  log "  pushing $name"
  local digest
  digest="$(docker push "$ref" 2>&1 | tee >(cat >&2) | grep -oE 'digest: sha256:[0-9a-f]+' | awk '{print $2}')"
  [[ -n "$digest" ]] || fail "could not determine pushed digest for $ref"
  echo "$REGISTRY/checkpointd-llm-chat-demo-$name@$digest"
}
log "building and pushing images (tag: $IMAGE_TAG)"
CHECKPOINTD_IMAGE="$(build_and_push "$REPO_ROOT/cmd/Dockerfile" checkpointd checkpointd)"
CHAT_AGENT_IMAGE="$(build_and_push "$SCRIPT_DIR/agents.Dockerfile" chat-agent chat-agent)"
REVIEWER_AGENT_IMAGE="$(build_and_push "$SCRIPT_DIR/agents.Dockerfile" reviewer-agent reviewer-agent)"

log "rendering manifests"
export CHECKPOINTD_IMAGE ATEOM_IMAGE CHAT_AGENT_IMAGE REVIEWER_AGENT_IMAGE
render_vars='${CHECKPOINTD_IMAGE} ${ATEOM_IMAGE} ${CHAT_AGENT_IMAGE} ${REVIEWER_AGENT_IMAGE}'
for f in "$K8S_DIR"/*.yaml; do
  envsubst "$render_vars" < "$f" > "$WORKDIR/$(basename "$f")"
done

run_kubectl() { kubectl --context "$KUBECTL_CONTEXT" "$@"; }

run_kubectl apply -f "$WORKDIR/00-namespace.yaml"
run_kubectl apply -f "$WORKDIR/10-workerpool.yaml"
run_kubectl apply -f "$WORKDIR/05-llama-completion.yaml"

log "waiting for the WorkerPool to report Ready (up to 5 minutes)"
workerpool_ready=0
for _ in $(seq 1 150); do
  ready="$(run_kubectl -n "$NS" get workerpool checkpointd-llm-chat-demo-harness -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)"
  want="$(run_kubectl -n "$NS" get workerpool checkpointd-llm-chat-demo-harness -o jsonpath='{.spec.replicas}' 2>/dev/null || true)"
  if [[ -n "$ready" && -n "$want" && "$ready" == "$want" ]]; then
    workerpool_ready=1
    break
  fi
  sleep 2
done
[[ "$workerpool_ready" == "1" ]] || fail "WorkerPool never became Ready"

log "waiting for llama-completion to become Ready (up to 3 minutes -- downloading the model)"
run_kubectl -n "$NS" rollout status deployment/llama-completion --timeout=180s \
  || fail "llama-completion never became Ready -- check 'kubectl -n $NS logs deployment/llama-completion -c fetch-model' and '... -c llama-server'"

# Applied before checkpointd-server, so its first synchronous discovery pass finds both agents immediately.
log "applying ActorTemplates (chat-agent, reviewer-agent) -- before checkpointd-server"
run_kubectl apply -f "$WORKDIR/20-actortemplate-chat-agent.yaml"
run_kubectl apply -f "$WORKDIR/21-actortemplate-reviewer-agent.yaml"
run_kubectl -n "$NS" wait --for=condition=Ready actortemplate/chat-agent-template --timeout=180s \
  || fail "chat-agent-template never became Ready"
run_kubectl -n "$NS" wait --for=condition=Ready actortemplate/reviewer-agent-template --timeout=180s \
  || fail "reviewer-agent-template never became Ready"

log "applying checkpointd-server (ConfigMap + StatefulSet)"
run_kubectl apply -f "$WORKDIR/85-checkpointd-configmap.yaml"
run_kubectl apply -f "$WORKDIR/90-checkpointd-server.yaml"

log "waiting for checkpointd-server to discover both agents (readinessProbe, up to 3 minutes)"
run_kubectl -n "$NS" rollout status statefulset/checkpointd-server --timeout=180s \
  || fail "checkpointd-server's readinessProbe never passed -- see 'kubectl -n $NS logs checkpointd-server-0' and 'kubectl -n $NS get actortemplates'"

log "PASS -- chat-agent and reviewer-agent are deployed and discovered. Next: examples/llm-chat-demo/demo.sh"
