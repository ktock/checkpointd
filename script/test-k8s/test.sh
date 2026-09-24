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

# Fully standalone end-to-end test against a real Agent Substrate cluster.
#
# This file does shared setup (cluster, Agent Substrate install, image
# build/push, manifest apply, the a2a CLI, port-forwards), then dispatches
# to whichever subtests are selected -- see subtests/*.sh for what each one does:
#   long-poll                two concurrent long-poll tasks, crashed twice
#                            (an actor, then the whole checkpointd-server
#                            pod) before either agent's long-wait poll is
#                            ever notified -- checkpointd's own recovery
#                            test for long-polling/long-waiting agents, not
#                            a test of any real human-approval workflow
#   crash-recovery-from-last-message  minimal single-hop version of
#                             crash-test-external: one call to echo-agent,
#                             then a worker-pod force-delete, confirming
#                             recovery resumes from that call's reply
#                             instead of replaying the workflow
#   crash-test-external      multi-hop turn crashed via an external
#                             worker-pod force-delete
#   crash-test-selfpanic     multi-hop turn crashed via the agent panicking
#                             itself
#   tck                      the real A2A TCK suite (MUST level, jsonrpc) --
#                            one subtest, not split further, even though it
#                            runs ~235 individual test cases internally
#   task-state-a2b            agent_a drives agent_b through its own
#                             Task-state transitions via nested SendMessage
#                             calls (also exercises real AgentCard
#                             discovery -- see agent_a's own resolveCard)
#   task-state-client2b       the same agent-b driven directly by an
#                             external client instead
#   no-reexecute              a completed echo-agent task survives a
#                             checkpointd-server restart unchanged, not re-driven
#   listtasks-and-notfound    ListTasks and GetTask-for-an-unknown-id, against echo-agent
#   multi-replica-scaling     checkpointd-server run as a multi-replica
#                             StatefulSet on shared Postgres: sessions survive
#                             cross-instance continuation, scale-up/down, and
#                             a non-graceful pod force-kill self-recovered by
#                             the same-named replacement
#   cancel-task               CancelTask against an actively RUNNING task
#                             stops its relay loop and cleans up every actor
#                             it visited, not just records Canceled
#   hanging-instance-salvage  an instance's own process is paused (SIGSTOP,
#                             never crashes) while owning a RUNNING session --
#                             confirms Kubernetes' livenessProbe and
#                             restartPolicy: Never move it to Failed, then,
#                             scaled out so it can't self-recover under its
#                             own name, that a surviving instance salvages it
#
# Assumes only docker, kind, kubectl, go, make, git, jq, and envsubst are installed.
#
# Usage: script/test-k8s/test.sh
#
# Env overrides:
#   CHECKPOINTD_K8S_NAMESPACE       Namespace to deploy into (default: checkpointd-k8s-test)
#   CHECKPOINTD_K8S_REGISTRY        Registry to push images to (default: the shared "kind-registry")
#   KIND_CLUSTER_NAME            kind cluster to create (default: checkpointd-test-k8s)
#   KIND_WORKER_NODES            Worker nodes alongside the one control-plane (default: 3)
#   SUBSTRATE_REPO               Agent Substrate git remote to clone (default: upstream)
#   SUBSTRATE_BRANCH             Branch to clone (default: main)
#   SUBSTRATE_COMMIT             Commit to check out and verify (default: pinned)
#   ATE_INSTALL_ROLLOUT_TIMEOUT  Per-component rollout timeout (default: 180s)
#   KEEP_CLUSTER                 If "1", don't delete the cluster or scratch checkout on exit
#   TEST_K8S_TARGET              Space-separated subtest ids to run (default: all)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
K8S_DIR="$REPO_ROOT/script/test-k8s/manifests"
cd "$REPO_ROOT"

NS="${CHECKPOINTD_K8S_NAMESPACE:-checkpointd-k8s-test}"
REGISTRY="${CHECKPOINTD_K8S_REGISTRY:-}"
IMAGE_TAG="dev"

KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-checkpointd-test-k8s}"
export KIND_CLUSTER_NAME
KUBECTL_CONTEXT="kind-${KIND_CLUSTER_NAME}"
export KUBECTL_CONTEXT

SUBSTRATE_REPO="${SUBSTRATE_REPO:-https://github.com/agent-substrate/substrate}"
SUBSTRATE_BRANCH="${SUBSTRATE_BRANCH:-main}"
# Pinned to a recent agent-substrate/substrate commit for reproducibility.
SUBSTRATE_COMMIT="${SUBSTRATE_COMMIT:-d909d690532b3e2e06496cfdd9e1200e56b2c27b}"

# Longer than install-ate-kind.sh's own default, to give a kind cluster's first install more room.
ATE_INSTALL_ROLLOUT_TIMEOUT="${ATE_INSTALL_ROLLOUT_TIMEOUT:-180s}"
export ATE_INSTALL_ROLLOUT_TIMEOUT

PORT_FORWARD_PID=""
TESTSERVER_PORT_FORWARD_PID=""
CHECKPOINTD_SERVER_LOCAL_PORT=""
TESTSERVER_LOCAL_PORT=""

source "$SCRIPT_DIR/lib.sh"

# --- Resolve which subtests to run, and which images/manifests they need ----

SUBTEST_ORDER=(long-poll crash-recovery-from-last-message crash-test-external crash-test-selfpanic tck task-state-a2b task-state-client2b no-reexecute listtasks-and-notfound multi-replica-scaling cancel-task hanging-instance-salvage)
declare -A SUBTESTS=(
  [long-poll]=subtest_long_poll
  [crash-recovery-from-last-message]=subtest_crash_recovery_from_last_message
  [crash-test-external]=subtest_crash_test_external
  [crash-test-selfpanic]=subtest_crash_test_selfpanic
  [tck]=subtest_tck
  [task-state-a2b]=subtest_task_state_a2b
  [task-state-client2b]=subtest_task_state_client2b
  [no-reexecute]=subtest_no_reexecute
  [listtasks-and-notfound]=subtest_listtasks_and_notfound
  [multi-replica-scaling]=subtest_multi_replica_scaling
  [cancel-task]=subtest_cancel_task
  [hanging-instance-salvage]=subtest_hanging_instance_salvage
)

target="${TEST_K8S_TARGET:-}"
if [[ -z "$target" ]]; then
  run_ids=("${SUBTEST_ORDER[@]}")
else
  read -r -a run_ids <<<"$target"
  for id in "${run_ids[@]}"; do
    [[ -n "${SUBTESTS[$id]:-}" ]] || fail "unknown subtest '$id' in TEST_K8S_TARGET (known: ${SUBTEST_ORDER[*]})"
  done
fi

# Each agent image "key" (matching build_and_push's own naming) alongside the
# ActorTemplate manifest file and template name(s) it produces, so a selected
# subtest can pull in only what it needs -- saves build time and disk,
# especially when CI runs one subtest per job (see .github/workflows/tests.yml).
declare -A AGENT_MANIFEST=(
  [echo]="20-actortemplate-echo.yaml"
  [long-wait]="62-actortemplate-long-wait.yaml"
  [long-poll]="63-actortemplate-long-poll.yaml"
  [tck-agent]="64-actortemplate-tck-agent.yaml"
  [crash-test-agent]="65-actortemplate-crash-test.yaml"
  [crash-recovery-agent]="66-actortemplate-crash-recovery.yaml"
  [agent-a]="70-actortemplate-agent-a.yaml"
  [agent-b]="71-actortemplate-agent-b.yaml"
)
declare -A AGENT_TEMPLATES=(
  [echo]="echo-template"
  [long-wait]="long-wait-template"
  [long-poll]="long-poll-template"
  [tck-agent]="tck-agent-template"
  [crash-test-agent]="crash-test-external-template crash-test-selfpanic-template"
  [crash-recovery-agent]="crash-recovery-template"
  [agent-a]="agent-a-template"
  [agent-b]="agent-b-template"
)
# Which agent keys (see AGENT_MANIFEST/AGENT_TEMPLATES above) each subtest needs.
# crash-test-external and crash-test-selfpanic share one image and one
# manifest file (with both ActorTemplates), so selecting either one brings in
# both templates -- there's no image to save by splitting them further.
declare -A SUBTEST_AGENTS=(
  [long-poll]="long-wait long-poll"
  [crash-recovery-from-last-message]="crash-recovery-agent echo"
  [crash-test-external]="crash-test-agent echo long-wait"
  [crash-test-selfpanic]="crash-test-agent echo"
  [tck]="tck-agent"
  [task-state-a2b]="agent-a agent-b"
  [task-state-client2b]="agent-b"
  [no-reexecute]="echo"
  [listtasks-and-notfound]="echo"
  [multi-replica-scaling]="long-wait long-poll agent-b"
  [cancel-task]="long-wait long-poll"
  [hanging-instance-salvage]="long-wait long-poll"
)
# Subtests that also need the testserver Deployment/Service (its /notify and
# /wait endpoints).
declare -A SUBTEST_NEEDS_TESTSERVER=(
  [long-poll]=1
  [crash-recovery-from-last-message]=1
  [crash-test-external]=1
  [crash-test-selfpanic]=1
  [multi-replica-scaling]=1
  [cancel-task]=1
  [hanging-instance-salvage]=1
)
# Subtests that need checkpointd-server run multi-replica on shared Postgres
# instead of the plain single-replica/SQLite deployment every other subtest
# uses -- see 90-postgres-statefulset.yaml and the 91/92 "-postgres" manifest
# variants.
declare -A SUBTEST_NEEDS_POSTGRES=(
  [multi-replica-scaling]=1
  [hanging-instance-salvage]=1
)

declare -A NEEDED_AGENTS=()
NEEDED_TESTSERVER=0
NEEDED_TCK_RUNNER=0
NEEDED_POSTGRES=0
for id in "${run_ids[@]}"; do
  for a in ${SUBTEST_AGENTS[$id]:-}; do
    NEEDED_AGENTS[$a]=1
  done
  if [[ "${SUBTEST_NEEDS_TESTSERVER[$id]:-0}" == "1" ]]; then
    NEEDED_TESTSERVER=1
  fi
  if [[ "$id" == "tck" ]]; then
    NEEDED_TCK_RUNNER=1
  fi
  if [[ "${SUBTEST_NEEDS_POSTGRES[$id]:-0}" == "1" ]]; then
    NEEDED_POSTGRES=1
  fi
done

# --- Preflight: everything this script shells out to must actually work ------

log "checking docker, kind, kubectl, go, make, git, jq, envsubst"
command -v docker >/dev/null 2>&1 || fail "docker not found on PATH"
docker info >/dev/null 2>&1 || fail "docker is installed but not usable (daemon not reachable) -- please fix docker first"
command -v kind >/dev/null 2>&1 || fail "kind not found on PATH"
command -v kubectl >/dev/null 2>&1 || fail "kubectl not found on PATH"
command -v go >/dev/null 2>&1 || fail "go not found on PATH (needed to build Agent Substrate and kubectl-ate from source)"
command -v make >/dev/null 2>&1 || fail "make not found on PATH (Agent Substrate's own install scripts use it)"
command -v git >/dev/null 2>&1 || fail "git not found on PATH (needed to clone Agent Substrate)"
command -v envsubst >/dev/null 2>&1 || fail "envsubst not found on PATH (part of gettext-base, used to render manifests)"
command -v jq >/dev/null 2>&1 || fail "jq not found on PATH (used to parse kubectl-ate JSON output)"

# --- Clone Agent Substrate at a pinned commit into a scratch checkout --------

# reclaim_stale_scratch_dirs removes scratch directories left behind by an earlier, killed run.
STALE_SCRATCH_AGE_MIN="${STALE_SCRATCH_AGE_MIN:-120}"
reclaim_stale_scratch_dirs() {
  local stale
  stale="$(find /tmp -maxdepth 1 -name 'checkpointd-test-k8s-*' -mmin "+$STALE_SCRATCH_AGE_MIN" 2>/dev/null)"
  [[ -z "$stale" ]] && return
  log "reclaiming stale scratch dir(s) left behind by an earlier, abandoned run (older than ${STALE_SCRATCH_AGE_MIN}m):"
  while IFS= read -r d; do
    log "  $d"
    rm -rf "$d"
  done <<<"$stale"
}
reclaim_stale_scratch_dirs

SUBSTRATE_DIR="$(mktemp -d -t checkpointd-test-k8s-substrate.XXXXXXXX)"
WORKDIR="$(mktemp -d -t checkpointd-test-k8s-workdir.XXXXXXXX)"
KIND_CLUSTER_UP=0

cleanup() {
  # Always killed here, even under KEEP_CLUSTER=1, since this is only our own port-forward.
  if [[ -n "$PORT_FORWARD_PID" ]]; then
    kill "$PORT_FORWARD_PID" 2>/dev/null || true
    wait "$PORT_FORWARD_PID" 2>/dev/null || true
  fi
  if [[ -n "$TESTSERVER_PORT_FORWARD_PID" ]]; then
    kill "$TESTSERVER_PORT_FORWARD_PID" 2>/dev/null || true
    wait "$TESTSERVER_PORT_FORWARD_PID" 2>/dev/null || true
  fi
  if [[ "${KEEP_CLUSTER:-0}" == "1" ]]; then
    log "KEEP_CLUSTER=1: leaving cluster '$KIND_CLUSTER_NAME' and scratch dirs $SUBSTRATE_DIR, $WORKDIR in place"
    return
  fi
  if [[ "$KIND_CLUSTER_UP" == "1" ]]; then
    log "deleting kind cluster '$KIND_CLUSTER_NAME'"
    # Not agent-substrate's own hack/delete-kind-cluster.sh, which also deletes the shared registry.
    KIND_BIN="$SUBSTRATE_DIR/hack/kind.sh" SUBSTRATE_DIR="$SUBSTRATE_DIR" \
      "$REPO_ROOT/script/lib/delete-kind-cluster.sh" || true
  fi
  rm -rf "$SUBSTRATE_DIR" "$WORKDIR"
}
# Trapping INT/TERM too ensures a Ctrl-C still runs cleanup.
trap cleanup EXIT INT TERM

log "cloning $SUBSTRATE_REPO@$SUBSTRATE_BRANCH into $SUBSTRATE_DIR"
git clone --branch "$SUBSTRATE_BRANCH" --single-branch --quiet "$SUBSTRATE_REPO" "$SUBSTRATE_DIR" >&2 \
  || fail "could not clone $SUBSTRATE_REPO@$SUBSTRATE_BRANCH"
git -C "$SUBSTRATE_DIR" checkout --quiet "$SUBSTRATE_COMMIT" \
  || fail "commit $SUBSTRATE_COMMIT not found on $SUBSTRATE_REPO@$SUBSTRATE_BRANCH -- has the branch been rebased or deleted?"
actual_commit="$(git -C "$SUBSTRATE_DIR" rev-parse HEAD)"
[[ "$actual_commit" == "$SUBSTRATE_COMMIT" ]] \
  || fail "checked out $actual_commit, expected $SUBSTRATE_COMMIT"
log "  checked out $SUBSTRATE_COMMIT"

# kubectl-ate finds and deletes actors directly, since they aren't plain Kubernetes objects.
KUBECTL_ATE_BIN="$SUBSTRATE_DIR/bin/kubectl-ate"
log "building kubectl-ate (scratch, used only by this run)"
(cd "$SUBSTRATE_DIR" && go build -o "$KUBECTL_ATE_BIN" ./cmd/kubectl-ate) \
  || fail "could not build kubectl-ate from $SUBSTRATE_DIR"

# --- Create the kind cluster and install Agent Substrate into it -------------

log "creating kind cluster '$KIND_CLUSTER_NAME'"
# Set before the call, since a failed creation can still leave a partial cluster to delete.
KIND_CLUSTER_UP=1
# KIND_BIN routes this through Agent Substrate's own pinned kind binary, not whatever "kind" is on PATH.
created_registry="$(KIND_BIN="$SUBSTRATE_DIR/hack/kind.sh" SUBSTRATE_DIR="$SUBSTRATE_DIR" \
  "$REPO_ROOT/script/lib/create-kind-cluster.sh")" || fail "kind cluster creation failed"
# CHECKPOINTD_K8S_REGISTRY, if the caller set it, overrides the one create-kind-cluster.sh created.
REGISTRY="${REGISTRY:-$created_registry}"

log "checking the image registry ($REGISTRY) is reachable from this host"
if ! curl -fsS "http://$REGISTRY/v2/_catalog" >/dev/null 2>&1; then
  fail "can't reach http://$REGISTRY/v2/_catalog right after creating the cluster -- the 'kind with local registry' setup didn't come up as expected"
fi

log "installing Agent Substrate (ate-system, atenet) -- this is the slow part (rollout timeout: $ATE_INSTALL_ROLLOUT_TIMEOUT)"
(cd "$SUBSTRATE_DIR" && NO_DEV_ENV=true KO_DOCKER_REPO="$REGISTRY" \
  ./hack/install-ate-kind.sh --deploy-ate-system --deploy-atenet) \
  || fail "Agent Substrate install failed"

log "resolving and pushing the ateom-gvisor worker image via ko"
# run-tool.sh runs Agent Substrate's own pinned `ko` build, rather than relying on a system-installed one.
ATEOM_IMAGE="$(cd "$SUBSTRATE_DIR" && KO_DOCKER_REPO="$REGISTRY" ./hack/run-tool.sh ko build ./cmd/ateom-gvisor)"
[[ "$ATEOM_IMAGE" == "$REGISTRY"/* ]] || fail "ko build didn't print a resolved image reference (got: $ATEOM_IMAGE)"
log "  using ateomImage: $ATEOM_IMAGE"

# --- Build and push images ----------------------------------------------------

log "building and pushing images (tag: $IMAGE_TAG) -- only what's needed for: ${run_ids[*]}"

build_and_push() {
  local dockerfile=$1 target=$2 name=$3
  local ref="$REGISTRY/checkpointd-k8s-test-$name:$IMAGE_TAG"
  log "  building $name ($target)"
  local target_flag=()
  [[ -n "$target" ]] && target_flag=(--target "$target")
  docker build "${target_flag[@]}" -f "$dockerfile" -t "$ref" "$REPO_ROOT" >&2
  log "  pushing $name"
  local digest
  digest="$(docker push "$ref" 2>&1 | tee >(cat >&2) | grep -oE 'digest: sha256:[0-9a-f]+' | awk '{print $2}')"
  [[ -n "$digest" ]] || fail "could not determine pushed digest for $ref"
  echo "$REGISTRY/checkpointd-k8s-test-$name@$digest"
}

CHECKPOINTD_IMAGE="$(build_and_push "$REPO_ROOT/cmd/Dockerfile" checkpointd checkpointd)"
ECHO_IMAGE="" LONG_WAIT_IMAGE="" LONG_POLL_IMAGE="" TESTSERVER_IMAGE="" TCK_AGENT_IMAGE="" \
  TCK_RUNNER_IMAGE="" CRASH_TEST_AGENT_IMAGE="" CRASH_RECOVERY_AGENT_IMAGE="" AGENT_A_IMAGE="" AGENT_B_IMAGE=""
if [[ -n "${NEEDED_AGENTS[echo]:-}" ]]; then ECHO_IMAGE="$(build_and_push "$K8S_DIR/agents.Dockerfile" echo echo)"; fi
if [[ -n "${NEEDED_AGENTS[long-wait]:-}" ]]; then LONG_WAIT_IMAGE="$(build_and_push "$K8S_DIR/agents.Dockerfile" long-wait long-wait)"; fi
if [[ -n "${NEEDED_AGENTS[long-poll]:-}" ]]; then LONG_POLL_IMAGE="$(build_and_push "$K8S_DIR/agents.Dockerfile" long-poll long-poll)"; fi
if [[ "$NEEDED_TESTSERVER" == "1" ]]; then TESTSERVER_IMAGE="$(build_and_push "$K8S_DIR/agents.Dockerfile" testserver testserver)"; fi
if [[ -n "${NEEDED_AGENTS[tck-agent]:-}" ]]; then TCK_AGENT_IMAGE="$(build_and_push "$K8S_DIR/agents.Dockerfile" tck-agent tck-agent)"; fi
if [[ "$NEEDED_TCK_RUNNER" == "1" ]]; then TCK_RUNNER_IMAGE="$(build_and_push "$K8S_DIR/tck.Dockerfile" "" tck-runner)"; fi
if [[ -n "${NEEDED_AGENTS[crash-test-agent]:-}" ]]; then CRASH_TEST_AGENT_IMAGE="$(build_and_push "$K8S_DIR/agents.Dockerfile" crash-test-agent crash-test-agent)"; fi
if [[ -n "${NEEDED_AGENTS[crash-recovery-agent]:-}" ]]; then CRASH_RECOVERY_AGENT_IMAGE="$(build_and_push "$K8S_DIR/agents.Dockerfile" crash-recovery-agent crash-recovery-agent)"; fi
if [[ -n "${NEEDED_AGENTS[agent-a]:-}" ]]; then AGENT_A_IMAGE="$(build_and_push "$K8S_DIR/agents.Dockerfile" agent-a agent-a)"; fi
if [[ -n "${NEEDED_AGENTS[agent-b]:-}" ]]; then AGENT_B_IMAGE="$(build_and_push "$K8S_DIR/agents.Dockerfile" agent-b agent-b)"; fi

log "  checkpointd image:          $CHECKPOINTD_IMAGE"
log "  echo image:              ${ECHO_IMAGE:-(skipped, not needed)}"
log "  long-wait image:         ${LONG_WAIT_IMAGE:-(skipped, not needed)}"
log "  long-poll image:         ${LONG_POLL_IMAGE:-(skipped, not needed)}"
log "  testserver image:        ${TESTSERVER_IMAGE:-(skipped, not needed)}"
log "  tck-agent image:         ${TCK_AGENT_IMAGE:-(skipped, not needed)}"
log "  tck-runner image:        ${TCK_RUNNER_IMAGE:-(skipped, not needed)}"
log "  crash-test-agent image:  ${CRASH_TEST_AGENT_IMAGE:-(skipped, not needed)}"
log "  crash-recovery-agent image: ${CRASH_RECOVERY_AGENT_IMAGE:-(skipped, not needed)}"
log "  agent-a image:           ${AGENT_A_IMAGE:-(skipped, not needed)}"
log "  agent-b image:           ${AGENT_B_IMAGE:-(skipped, not needed)}"

# --- Render and apply manifests ------------------------------------------------

CONFIGMAP_FILE="91-checkpointd-configmap.yaml"
SERVER_MANIFEST_FILE="92-checkpointd-server-statefulset.yaml"
if [[ "$NEEDED_POSTGRES" == "1" ]]; then
  CONFIGMAP_FILE="91-checkpointd-configmap-postgres.yaml"
  SERVER_MANIFEST_FILE="92-checkpointd-server-statefulset-postgres.yaml"
fi

NEEDED_MANIFEST_FILES=(00-namespace.yaml 10-workerpool.yaml "$CONFIGMAP_FILE" "$SERVER_MANIFEST_FILE")
if [[ "$NEEDED_TESTSERVER" == "1" ]]; then
  NEEDED_MANIFEST_FILES+=(60-testserver.yaml)
fi
if [[ "$NEEDED_POSTGRES" == "1" ]]; then
  NEEDED_MANIFEST_FILES+=(90-postgres-statefulset.yaml)
fi
for a in "${!NEEDED_AGENTS[@]}"; do
  NEEDED_MANIFEST_FILES+=("${AGENT_MANIFEST[$a]}")
done

# Only meaningful (and only actually referenced by the manifest templates)
# when NEEDED_POSTGRES=1; harmless, unused envsubst inputs otherwise.
CHECKPOINTD_REPLICAS="${CHECKPOINTD_REPLICAS:-2}"
CHECKPOINTD_TERMINATION_GRACE_SECONDS="${CHECKPOINTD_TERMINATION_GRACE_SECONDS:-60}"
# Short by default so a test doesn't have to wait a real deployment's
# 1-minute default interval for the salvage loop to pick up an orphan.
CHECKPOINTD_SALVAGE_SWEEP_INTERVAL="${CHECKPOINTD_SALVAGE_SWEEP_INTERVAL:-5s}"

log "rendering manifests (workdir: $WORKDIR)"
export NS CHECKPOINTD_IMAGE ECHO_IMAGE LONG_WAIT_IMAGE LONG_POLL_IMAGE TESTSERVER_IMAGE \
  TCK_AGENT_IMAGE CRASH_TEST_AGENT_IMAGE CRASH_RECOVERY_AGENT_IMAGE AGENT_A_IMAGE AGENT_B_IMAGE ATEOM_IMAGE \
  CHECKPOINTD_REPLICAS CHECKPOINTD_TERMINATION_GRACE_SECONDS CHECKPOINTD_SALVAGE_SWEEP_INTERVAL
render_vars='${NS} ${CHECKPOINTD_IMAGE} ${ECHO_IMAGE} ${LONG_WAIT_IMAGE} ${LONG_POLL_IMAGE} ${TESTSERVER_IMAGE} ${TCK_AGENT_IMAGE} ${CRASH_TEST_AGENT_IMAGE} ${CRASH_RECOVERY_AGENT_IMAGE} ${AGENT_A_IMAGE} ${AGENT_B_IMAGE} ${ATEOM_IMAGE} ${CHECKPOINTD_REPLICAS} ${CHECKPOINTD_TERMINATION_GRACE_SECONDS} ${CHECKPOINTD_SALVAGE_SWEEP_INTERVAL}'
for f in "${NEEDED_MANIFEST_FILES[@]}"; do
  envsubst "$render_vars" < "$K8S_DIR/$f" > "$WORKDIR/$f"
done

log "applying namespace"
run_kubectl apply -f "$WORKDIR/00-namespace.yaml"

log "applying WorkerPool"
run_kubectl apply -f "$WORKDIR/10-workerpool.yaml"

ACTOR_TEMPLATES=()
for a in "${!NEEDED_AGENTS[@]}"; do
  for t in ${AGENT_TEMPLATES[$a]}; do
    ACTOR_TEMPLATES+=("$t")
  done
done

log "applying ActorTemplates (${ACTOR_TEMPLATES[*]})"
for a in "${!NEEDED_AGENTS[@]}"; do
  run_kubectl apply -f "$WORKDIR/${AGENT_MANIFEST[$a]}"
done

log "waiting for ActorTemplates to become Ready"
for t in "${ACTOR_TEMPLATES[@]}"; do
  run_kubectl -n "$NS" wait --for=condition=Ready "actortemplate/$t" --timeout=180s
done

log "waiting for the WorkerPool to reach its desired replicas"
for i in $(seq 1 60); do
  ready="$(run_kubectl -n "$NS" get workerpool checkpointd-k8s-test-harness -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)"
  want="$(run_kubectl -n "$NS" get workerpool checkpointd-k8s-test-harness -o jsonpath='{.spec.replicas}' 2>/dev/null || true)"
  [[ -n "$ready" && "$ready" == "$want" ]] && break
  sleep 5
done
[[ -n "${ready:-}" && "$ready" == "${want:-}" ]] || fail "WorkerPool checkpointd-k8s-test-harness never reached $want ready replicas (got: ${ready:-0})"

if [[ "$NEEDED_TESTSERVER" == "1" ]]; then
  log "applying the testserver Deployment/Service"
  run_kubectl apply -f "$WORKDIR/60-testserver.yaml"
  run_kubectl -n "$NS" rollout status deployment/testserver --timeout=120s
fi

if [[ "$NEEDED_POSTGRES" == "1" ]]; then
  log "applying Postgres (shared eventlog/session store for a multi-replica checkpointd-server)"
  run_kubectl apply -f "$WORKDIR/90-postgres-statefulset.yaml"
  run_kubectl -n "$NS" rollout status statefulset/postgres --timeout=120s
fi

log "applying ConfigMap (checkpointd-config)"
run_kubectl apply -f "$WORKDIR/$CONFIGMAP_FILE"

log "applying the checkpointd-server StatefulSet"
run_kubectl apply -f "$WORKDIR/$SERVER_MANIFEST_FILE"

# --- Wait for checkpointd-server to become Ready, then reach it via the a2a CLI over a kubectl port-forward. ---

# Both the single-replica/SQLite and multi-replica/Postgres manifests are a
# StatefulSet, so its own rollout status is the direct, correct wait for
# "every initial replica is Ready" either way.
log "waiting for every initial checkpointd-server replica to become Ready"
run_kubectl -n "$NS" rollout status statefulset/checkpointd-server --timeout=180s \
  || { dumpAllCheckpointdServerLogs; fail "checkpointd-server StatefulSet never finished its initial rollout"; }

A2A_CLI_VERSION="v2.5.0" # pinned to the same a2a-go version this repo's go.mod uses (see go.mod)
log "installing the a2a CLI (github.com/a2aproject/a2a-go/v2/cmd/a2a@$A2A_CLI_VERSION) to a scratch location"
A2A_CLI_BIN_DIR="$WORKDIR/a2a-cli-bin"
mkdir -p "$A2A_CLI_BIN_DIR"
GOBIN="$A2A_CLI_BIN_DIR" go install "github.com/a2aproject/a2a-go/v2/cmd/a2a@$A2A_CLI_VERSION" \
  || fail "could not go install the a2a CLI"
A2A_CLI="$A2A_CLI_BIN_DIR/a2a"

start_port_forward

if [[ "$NEEDED_TESTSERVER" == "1" ]]; then
  # A second tunnel to svc/testserver, on its own dynamically assigned local port.
  TESTSERVER_PORT_FORWARD_LOG="$WORKDIR/testserver-port-forward.log"
  : >"$TESTSERVER_PORT_FORWARD_LOG"
  (
    # -9, and waited for, since kubectl's own shutdown on a lost pod can be slow.
    trap 'kill -9 "$pid" 2>/dev/null; wait "$pid" 2>/dev/null; exit 0' TERM INT
    target=":8080"
    while true; do
      run_kubectl -n "$NS" port-forward svc/testserver "$target" &
      pid=$!
      wait "$pid"
      port="$(sed -nE 's#^Forwarding from 127\.0\.0\.1:([0-9]+) ->.*#\1#p' "$TESTSERVER_PORT_FORWARD_LOG" | tail -1)"
      [[ -n "$port" ]] && target="$port:8080"
      sleep 1
    done
  ) >"$TESTSERVER_PORT_FORWARD_LOG" 2>&1 &
  TESTSERVER_PORT_FORWARD_PID=$!
  testserver_reachable=0
  for i in $(seq 1 50); do
    TESTSERVER_LOCAL_PORT="$(sed -nE 's#^Forwarding from 127\.0\.0\.1:([0-9]+) ->.*#\1#p' "$TESTSERVER_PORT_FORWARD_LOG" | tail -1)"
    if [[ -n "$TESTSERVER_LOCAL_PORT" ]] && (exec 3<>"/dev/tcp/127.0.0.1/$TESTSERVER_LOCAL_PORT") 2>/dev/null; then
      exec 3<&- 3>&-
      testserver_reachable=1
      break
    fi
    sleep 0.2
  done
  [[ "$testserver_reachable" == "1" ]] && log "port-forwarding svc/testserver to localhost:$TESTSERVER_LOCAL_PORT" \
    || fail "port-forward to svc/testserver never became reachable"
fi

ECHO_URL="http://127.0.0.1:$CHECKPOINTD_SERVER_LOCAL_PORT/agents/echo-agent/"
LONG_POLL_URL="http://127.0.0.1:$CHECKPOINTD_SERVER_LOCAL_PORT/agents/long-poll/"
CRASH_RECOVERY_URL="http://127.0.0.1:$CHECKPOINTD_SERVER_LOCAL_PORT/agents/crash-recovery/"
CRASH_EXT_URL="http://127.0.0.1:$CHECKPOINTD_SERVER_LOCAL_PORT/agents/crash-test-external/"
CRASH_SELFPANIC_URL="http://127.0.0.1:$CHECKPOINTD_SERVER_LOCAL_PORT/agents/crash-test-selfpanic/"
AGENT_A_URL="http://127.0.0.1:$CHECKPOINTD_SERVER_LOCAL_PORT/agents/agent-a/"
AGENT_B_URL="http://127.0.0.1:$CHECKPOINTD_SERVER_LOCAL_PORT/agents/agent-b/"

# --- Subtest dispatch --------------------------------------

for f in "$SCRIPT_DIR"/subtests/*.sh; do
  source "$f"
done

for id in "${run_ids[@]}"; do
  log "=== subtest: $id ==="
  "${SUBTESTS[$id]}"
done

# Reaching here means every selected subtest already passed.
log "PASS (ran: ${run_ids[*]})"
exit 0
