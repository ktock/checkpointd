// Copyright 2026 Google LLC
// Copyright 2026 checkpointd authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/ktock/checkpointd/internal/harness/substrate"
	"google.golang.org/protobuf/encoding/protojson"
	"gopkg.in/yaml.v3"
)

const validCard = `{"name":"Test Agent","description":"for discovery_test.go"}`

// envTemplate builds a single-container ActorTemplate for agentsFromTemplates
// tests, with env as that container's env vars.
func envTemplate(atespace, name string, env map[string]string) *ateapipb.ActorTemplate {
	var vars []*ateapipb.EnvVar
	for k, v := range env {
		vars = append(vars, &ateapipb.EnvVar{Name: k, Value: v})
	}
	return &ateapipb.ActorTemplate{
		Metadata:   &ateapipb.ResourceMetadata{Atespace: atespace, Name: name},
		Containers: []*ateapipb.Container{{Name: "main", Env: vars}},
	}
}

func TestAgentsFromTemplates_SkipsNonCheckpointdTemplates(t *testing.T) {
	templates := []*ateapipb.ActorTemplate{
		envTemplate("ns", "unrelated-actor", map[string]string{"SOME_OTHER_ENV": "x"}),
		envTemplate("ns", "opted-out", map[string]string{checkpointdAgentEnv: "false"}),
		envTemplate("ns", "no-env-at-all", nil),
	}
	harnesses, cards := agentsFromTemplates(templates, "", substrate.ControlAPIOptions{})
	if len(harnesses) != 0 {
		t.Errorf("harnesses = %v, want empty", harnesses)
	}
	if len(cards) != 0 {
		t.Errorf("cards = %v, want empty", cards)
	}
}

func TestAgentsFromTemplates_DefaultsIDToTemplateName(t *testing.T) {
	templates := []*ateapipb.ActorTemplate{
		envTemplate("ns", "my-agent", map[string]string{checkpointdAgentEnv: "true", checkpointdAgentCardEnv: validCard}),
	}
	harnesses, _ := agentsFromTemplates(templates, "", substrate.ControlAPIOptions{})
	if _, ok := harnesses["my-agent"]; !ok {
		t.Errorf("harnesses = %v, want key %q", harnesses, "my-agent")
	}
}

func TestAgentsFromTemplates_IDOverride(t *testing.T) {
	templates := []*ateapipb.ActorTemplate{
		envTemplate("ns", "my-agent", map[string]string{
			checkpointdAgentEnv:     "true",
			checkpointdAgentIDEnv:   "custom-id",
			checkpointdAgentCardEnv: validCard,
		}),
	}
	harnesses, _ := agentsFromTemplates(templates, "", substrate.ControlAPIOptions{})
	if _, ok := harnesses["custom-id"]; !ok {
		t.Errorf("harnesses = %v, want key %q", harnesses, "custom-id")
	}
	if _, ok := harnesses["my-agent"]; ok {
		t.Errorf("harnesses = %v, want no key %q (overridden)", harnesses, "my-agent")
	}
}

func TestAgentsFromTemplates_InvalidPort(t *testing.T) {
	templates := []*ateapipb.ActorTemplate{
		envTemplate("ns", "my-agent", map[string]string{
			checkpointdAgentEnv:       "true",
			checkpointdHarnessPortEnv: "not-a-port",
			checkpointdAgentCardEnv:   validCard,
		}),
	}
	harnesses, _ := agentsFromTemplates(templates, "", substrate.ControlAPIOptions{})
	if _, ok := harnesses["my-agent"]; ok {
		t.Errorf("harnesses = %v, want no key %q because of invalid port", harnesses, "my-agent")
	}
}

func TestAgentsFromTemplates_CardRegistered(t *testing.T) {
	templates := []*ateapipb.ActorTemplate{
		envTemplate("ns", "external", map[string]string{
			checkpointdAgentEnv:     "true",
			checkpointdAgentCardEnv: `{"name":"External Agent","description":"does things"}`,
		}),
	}
	harnesses, cards := agentsFromTemplates(templates, "", substrate.ControlAPIOptions{})
	if len(harnesses) != 1 {
		t.Fatalf("harnesses = %v, want 1 entry", harnesses)
	}
	card, ok := cards["external"]
	if !ok {
		t.Fatalf("cards = %v, want an entry for external", cards)
	}
	if card.Name != "External Agent" || card.Description != "does things" {
		t.Errorf("cards[external] = %+v, want Name=%q Description=%q", card, "External Agent", "does things")
	}
}

func TestAgentsFromTemplates_CardRegisteredYAML(t *testing.T) {
	templates := []*ateapipb.ActorTemplate{
		envTemplate("ns", "external", map[string]string{
			checkpointdAgentEnv: "true",
			checkpointdAgentCardEnv: "name: External Agent\n" +
				"description: does things\n" +
				"skills:\n" +
				"  - id: thing\n" +
				"    name: Do a thing\n" +
				"    description: does the thing\n",
		}),
	}
	harnesses, cards := agentsFromTemplates(templates, "", substrate.ControlAPIOptions{})
	if len(harnesses) != 1 {
		t.Fatalf("harnesses = %v, want 1 entry", harnesses)
	}
	card, ok := cards["external"]
	if !ok {
		t.Fatalf("cards = %v, want an entry for external", cards)
	}
	if card.Name != "External Agent" || card.Description != "does things" {
		t.Errorf("cards[external] = %+v, want Name=%q Description=%q", card, "External Agent", "does things")
	}
	if len(card.Skills) != 1 || card.Skills[0].ID != "thing" {
		t.Errorf("cards[external].Skills = %+v, want one skill with ID %q", card.Skills, "thing")
	}
}

// TestAgentsFromTemplates_NoCardSkipsAgentEntirely confirms agent_card is
// mandatory for a checkpointd-managed agent.
func TestAgentsFromTemplates_NoCardSkipsAgentEntirely(t *testing.T) {
	templates := []*ateapipb.ActorTemplate{
		envTemplate("ns", "no-card", map[string]string{checkpointdAgentEnv: "true"}),
	}
	harnesses, cards := agentsFromTemplates(templates, "", substrate.ControlAPIOptions{})
	if _, ok := harnesses["no-card"]; ok {
		t.Errorf("harnesses = %v, want no entry for no-card (agent_card is required)", harnesses)
	}
	if len(cards) != 0 {
		t.Errorf("cards = %v, want empty", cards)
	}
}

func TestAgentsFromTemplates_MalformedCardSkipsAgentEntirely(t *testing.T) {
	templates := []*ateapipb.ActorTemplate{
		envTemplate("ns", "my-agent", map[string]string{
			checkpointdAgentEnv:     "true",
			checkpointdAgentCardEnv: `not valid json`,
		}),
	}
	harnesses, cards := agentsFromTemplates(templates, "", substrate.ControlAPIOptions{})
	if _, ok := harnesses["my-agent"]; ok {
		t.Errorf("harnesses = %v, want no entry for my-agent (malformed card)", harnesses)
	}
	if len(cards) != 0 {
		t.Errorf("cards = %v, want empty", cards)
	}
}

func TestAgentsFromTemplates_DuplicateIDKeepsFirst(t *testing.T) {
	templates := []*ateapipb.ActorTemplate{
		envTemplate("ns-a", "template-a", map[string]string{
			checkpointdAgentEnv:     "true",
			checkpointdAgentIDEnv:   "shared-id",
			checkpointdAgentCardEnv: validCard,
		}),
		envTemplate("ns-b", "template-b", map[string]string{
			checkpointdAgentEnv:   "true",
			checkpointdAgentIDEnv: "shared-id",
		}),
	}
	harnesses, _ := agentsFromTemplates(templates, "", substrate.ControlAPIOptions{})
	if len(harnesses) != 1 {
		t.Errorf("harnesses = %v, want exactly 1 entry for the duplicated id", harnesses)
	}
}

func TestAgentsFromTemplates_NoNameNoOverrideSkipped(t *testing.T) {
	templates := []*ateapipb.ActorTemplate{
		envTemplate("ns", "", map[string]string{checkpointdAgentEnv: "true"}),
	}
	harnesses, _ := agentsFromTemplates(templates, "", substrate.ControlAPIOptions{})
	if len(harnesses) != 0 {
		t.Errorf("harnesses = %v, want empty (no name, no CHECKPOINTD_AGENT_ID override)", harnesses)
	}
}

// TestDiscoveryState_ChangedOnlyOnRealChange confirms changed reports true
// only when the sorted agent id set actually differs from last time.
func TestDiscoveryState_ChangedOnlyOnRealChange(t *testing.T) {
	s := &discoveryState{}
	if !s.changed([]string{"a", "b"}) {
		t.Error("first call: changed = false, want true (nothing seen yet)")
	}
	if s.changed([]string{"a", "b"}) {
		t.Error("same set again: changed = true, want false")
	}
	if !s.changed([]string{"a", "b", "c"}) {
		t.Error("agent added: changed = false, want true")
	}
	if s.changed([]string{"a", "b", "c"}) {
		t.Error("same set again after the addition: changed = true, want false")
	}
	if !s.changed(nil) {
		t.Error("all agents removed: changed = false, want true")
	}
	if s.changed(nil) {
		t.Error("still empty: changed = true, want false")
	}
}

func TestCheckpointdAgentEnvVars(t *testing.T) {
	tmpl := envTemplate("ns", "my-agent", map[string]string{
		checkpointdAgentEnv:   "true",
		checkpointdAgentIDEnv: "override",
	})
	env, ok := checkpointdAgentEnvVars(tmpl)
	if !ok {
		t.Fatal("checkpointdAgentEnvVars: ok = false, want true")
	}
	if env[checkpointdAgentIDEnv] != "override" {
		t.Errorf("env[%s] = %q, want %q", checkpointdAgentIDEnv, env[checkpointdAgentIDEnv], "override")
	}

	other := envTemplate("ns", "other", nil)
	if _, ok := checkpointdAgentEnvVars(other); ok {
		t.Error("checkpointdAgentEnvVars(no env): ok = true, want false")
	}
}

// fakeLister serves a fixed template set and records the atespace it was asked for.
type fakeLister struct {
	templates []*ateapipb.ActorTemplate
	err       error
	asked     []string
}

func (f *fakeLister) ListActorTemplates(_ context.Context, atespace string) ([]*ateapipb.ActorTemplate, error) {
	f.asked = append(f.asked, atespace)
	return f.templates, f.err
}

func TestDiscoverAgents_ListsTheGivenAtespace(t *testing.T) {
	lister := &fakeLister{templates: []*ateapipb.ActorTemplate{
		envTemplate("my-atespace", "agent-a", map[string]string{checkpointdAgentEnv: "true", checkpointdAgentCardEnv: validCard}),
		envTemplate("my-atespace", "unrelated", nil),
	}}
	harnesses, cards, names, err := discoverAgents(context.Background(), lister, "my-atespace", "", substrate.ControlAPIOptions{})
	if err != nil {
		t.Fatalf("discoverAgents: %v", err)
	}
	if !slices.Equal(lister.asked, []string{"my-atespace"}) {
		t.Errorf("listed atespaces = %v, want [my-atespace]", lister.asked)
	}
	if _, ok := harnesses["agent-a"]; !ok || len(harnesses) != 1 {
		t.Errorf("harnesses = %v, want only agent-a", harnesses)
	}
	if _, ok := cards["agent-a"]; !ok {
		t.Errorf("cards = %v, want agent-a's card", cards)
	}
	if want := []string{"my-atespace/agent-a", "my-atespace/unrelated"}; !slices.Equal(names, want) {
		t.Errorf("reported names = %v, want every listed template %v", names, want)
	}
}

func TestDiscoverAgents_EmptyAtespaceListsEveryAtespace(t *testing.T) {
	lister := &fakeLister{}
	if _, _, _, err := discoverAgents(context.Background(), lister, "", "", substrate.ControlAPIOptions{}); err != nil {
		t.Fatalf("discoverAgents: %v", err)
	}
	if !slices.Equal(lister.asked, []string{""}) {
		t.Errorf("listed atespaces = %v, want [\"\"] (all atespaces)", lister.asked)
	}
}

func TestDiscoverAgents_ListErrorIsReturned(t *testing.T) {
	lister := &fakeLister{err: errors.New("control API down")}
	if _, _, _, err := discoverAgents(context.Background(), lister, "", "", substrate.ControlAPIOptions{}); err == nil {
		t.Fatal("discoverAgents: got nil error, want the list failure so the previous agent set is kept")
	}
}

func TestAgentsFromTemplates_EgressPolicyFromEnv(t *testing.T) {
	templates := []*ateapipb.ActorTemplate{
		envTemplate("ns", "needs-egress", map[string]string{
			checkpointdAgentEnv:        "true",
			checkpointdAgentCardEnv:    validCard,
			checkpointdEgressPolicyEnv: `{"rules":[{"hostnames":{"patterns":["llama.ns.svc"]}}]}`,
		}),
		envTemplate("ns", "no-egress", map[string]string{checkpointdAgentEnv: "true", checkpointdAgentCardEnv: validCard}),
	}
	harnesses, _ := agentsFromTemplates(templates, "", substrate.ControlAPIOptions{})
	needs := harnesses["needs-egress"].(*substrate.SubstrateHarness)
	if rules := needs.EgressPolicy().GetRules(); len(rules) != 1 || rules[0].GetHostnames().GetPatterns()[0] != "llama.ns.svc" {
		t.Errorf("needs-egress policy rules = %v, want the configured rule", rules)
	}
	if got := harnesses["no-egress"].(*substrate.SubstrateHarness).EgressPolicy(); got != nil {
		t.Errorf("no-egress policy = %v, want none", got)
	}
}

func TestAgentsFromTemplates_SkipsAgentWithInvalidEgressPolicy(t *testing.T) {
	templates := []*ateapipb.ActorTemplate{
		envTemplate("ns", "bad-policy", map[string]string{
			checkpointdAgentEnv:        "true",
			checkpointdAgentCardEnv:    validCard,
			checkpointdEgressPolicyEnv: `{"rulez":[]}`,
		}),
	}
	harnesses, cards := agentsFromTemplates(templates, "", substrate.ControlAPIOptions{})
	if len(harnesses) != 0 || len(cards) != 0 {
		t.Errorf("harnesses = %v, cards = %v, want the agent skipped rather than running with an unintended policy", harnesses, cards)
	}
}

// parseTemplateManifest parses each YAML document in data the way kubectl-ate does, rejecting unknown fields.
func parseTemplateManifest(t *testing.T, data string) []*ateapipb.ActorTemplate {
	t.Helper()
	dec := yaml.NewDecoder(bytes.NewReader([]byte(data)))
	var templates []*ateapipb.ActorTemplate
	for {
		var doc any
		if err := dec.Decode(&doc); err != nil {
			if err.Error() == "EOF" {
				return templates
			}
			t.Fatalf("invalid YAML: %v", err)
		}
		if doc == nil {
			continue
		}
		jsonData, err := json.Marshal(doc)
		if err != nil {
			t.Fatalf("converting to JSON: %v", err)
		}
		tmpl := &ateapipb.ActorTemplate{}
		if err := protojson.Unmarshal(jsonData, tmpl); err != nil {
			t.Fatalf("not a valid ActorTemplate: %v", err)
		}
		templates = append(templates, tmpl)
	}
}

const templateManifest = `
metadata:
  atespace: team-atespace
  name: chat-template
containers:
  - name: chat
    image: "registry.example/chat@sha256:0000000000000000000000000000000000000000000000000000000000000000"
    command: ["/app/chat", "--harness-addr", "0.0.0.0:80", "--llama-addr", "llama.team-ns.svc:8080"]
    env:
      - name: CHECKPOINTD_AGENT
        value: "true"
      - name: CHECKPOINTD_AGENT_ID
        value: chat
      - name: CHECKPOINTD_AGENT_CARD
        value: |
          name: Chat Agent
          description: Answers chat messages
          version: 1.0.0
      - name: CHECKPOINTD_EGRESS_POLICY
        value: |
          rules:
            - hostnames:
                patterns: ["llama.team-ns.svc"]
snapshotConfig:
  onPause: SNAPSHOT_CONTENT_SCOPE_FULL
  onCommit: SNAPSHOT_CONTENT_SCOPE_FULL
  storageLocation: "gs://ate-snapshots/team-ns/chat/"
sandboxConfig:
  sandboxClass: SANDBOX_CLASS_GVISOR
  configName: gvisor-default
---
metadata:
  atespace: team-atespace
  name: plain-template
containers:
  - name: plain
    image: "registry.example/plain@sha256:0000000000000000000000000000000000000000000000000000000000000000"
`

// TestAgentsFromTemplates_ManifestYAMLRoundTrip confirms an ActorTemplate written as a kubectl-ate YAML manifest is discovered as an agent with its card and egress policy, and that a template without the opt-in is ignored.
func TestAgentsFromTemplates_ManifestYAMLRoundTrip(t *testing.T) {
	templates := parseTemplateManifest(t, templateManifest)
	if len(templates) != 2 {
		t.Fatalf("parsed %d templates, want 2", len(templates))
	}

	harnesses, cards := agentsFromTemplates(templates, "", substrate.ControlAPIOptions{})
	if len(harnesses) != 1 || len(cards) != 1 {
		t.Fatalf("discovered %d harness(es) and %d card(s), want exactly the opted-in one of each", len(harnesses), len(cards))
	}
	chat, ok := harnesses["chat"].(*substrate.SubstrateHarness)
	if !ok {
		t.Fatalf("harnesses = %v, want one named chat", harnesses)
	}
	if cards["chat"].Name != "Chat Agent" {
		t.Errorf("card = %+v, want the manifest's AgentCard", cards["chat"])
	}
	rules := chat.EgressPolicy().GetRules()
	if len(rules) != 1 || !slices.Equal(rules[0].GetHostnames().GetPatterns(), []string{"llama.team-ns.svc"}) {
		t.Errorf("egress policy rules = %v, want the manifest's llama hostname rule", rules)
	}
}
