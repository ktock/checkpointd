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

# Helpers for creating Agent Substrate ActorTemplates through kubectl-ate, sourced by scripts that define log() and fail().

# ate_create_actor_templates <kubectl-ate> <kube-context> <manifest> creates the atespace and every ActorTemplate document in manifest.
# A template that already exists is kept, since templates are immutable.
ate_create_actor_templates() {
  local kubectl_ate=$1 context=$2 manifest=$3
  local docs doc atespace name out
  docs="$(mktemp -d)"
  awk -v dir="$docs" 'BEGIN { n = 0 } /^---$/ { n++; next } { print > sprintf("%s/doc-%02d.yaml", dir, n) }' "$manifest"
  for doc in "$docs"/doc-*.yaml; do
    atespace="$(sed -n 's/^  atespace: *//p' "$doc" | head -1)"
    name="$(sed -n 's/^  name: *//p' "$doc" | head -1)"
    [[ -n "$atespace" && -n "$name" ]] || { rm -rf "$docs"; fail "$manifest has a template without metadata.atespace and metadata.name"; }

    if ! out="$("$kubectl_ate" --context="$context" create atespace "$atespace" 2>&1)" && [[ "$out" != *"AlreadyExists"* ]]; then
      rm -rf "$docs"
      fail "creating atespace $atespace: $out"
    fi
    if out="$("$kubectl_ate" --context="$context" create actor-template -f "$doc" 2>&1)"; then
      log "  created ActorTemplate $atespace/$name"
    elif [[ "$out" == *"AlreadyExists"* ]]; then
      log "  ActorTemplate $atespace/$name already exists; keeping it"
    else
      rm -rf "$docs"
      fail "creating ActorTemplate $atespace/$name: $out"
    fi
  done
  rm -rf "$docs"
}

# ate_wait_actor_template_golden <kubectl-ate> <kube-context> <atespace> <name> <timeout-seconds> waits until the template's golden snapshot is built.
# It fails fast if Substrate reports an error building it.
ate_wait_actor_template_golden() {
  local kubectl_ate=$1 context=$2 atespace=$3 name=$4 timeout=$5
  local deadline=$((SECONDS + timeout)) out golden err
  while ((SECONDS < deadline)); do
    if out="$("$kubectl_ate" --context="$context" get actor-template "$name" -a "$atespace" -o json 2>/dev/null)"; then
      golden="$(jq -r '(.actorTemplates[0] // .) | .status.goldenSnapshotStatus.goldenTag.name // empty' <<<"$out")"
      [[ -n "$golden" ]] && return 0
      err="$(jq -r '(.actorTemplates[0] // .) | .status.goldenSnapshotStatus.errorMessage // empty' <<<"$out")"
      [[ -z "$err" ]] || fail "ActorTemplate $atespace/$name failed to build its golden snapshot: $err"
    fi
    sleep 5
  done
  fail "ActorTemplate $atespace/$name never built its golden snapshot within ${timeout}s"
}
