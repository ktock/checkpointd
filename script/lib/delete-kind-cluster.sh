#!/usr/bin/env bash

# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Forked from Agent Substrate's hack/delete-kind-cluster.sh, minus the
# registry cleanup: create-kind-cluster.sh's own registry is shared across
# multiple clusters here, so deleting one cluster must not delete it.

set -o errexit -o nounset -o pipefail

KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-kind}"
KIND_BIN="${KIND_BIN:-kind}"

# Runs $KIND_BIN with the cwd it needs when it's a checkout-relative wrapper
# rather than a plain binary on PATH -- see create-kind-cluster.sh's own
# matching run_kind.
run_kind() {
  if [ -n "${SUBSTRATE_DIR:-}" ]; then
    (cd "${SUBSTRATE_DIR}" && "${KIND_BIN}" "$@")
  else
    "${KIND_BIN}" "$@"
  fi
}

if [ "$#" -gt 0 ]; then
  case "$1" in
    -h|--help)
      echo "Usage: $0"
      echo "Deletes the kind cluster '${KIND_CLUSTER_NAME}'. Does not touch the shared registry container, since other clusters may still be using it."
      echo
      echo "Configured through the environment:"
      echo "  KIND_CLUSTER_NAME  Name of the cluster to delete (default: kind)."
      echo "  KIND_BIN           kind binary/wrapper to use (default: kind on PATH)."
      echo "  SUBSTRATE_DIR      Agent Substrate checkout KIND_BIN needs as its cwd, if any."
      exit 0
      ;;
  esac
fi

echo "Deleting kind cluster '${KIND_CLUSTER_NAME}'..."
run_kind delete cluster --name "${KIND_CLUSTER_NAME}"
