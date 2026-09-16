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
	"testing"

	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/ktock/checkpointd/internal/harness/substrate"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const validCard = `{"name":"Test Agent","description":"for discovery_test.go"}`

// envTemplate builds a single-container ActorTemplate for agentsFromTemplates
// tests, with env as that container's env vars.
func envTemplate(namespace, name string, env map[string]string) atev1alpha1.ActorTemplate {
	var vars []atev1alpha1.EnvVar
	for k, v := range env {
		vars = append(vars, atev1alpha1.EnvVar{Name: k, Value: v})
	}
	return atev1alpha1.ActorTemplate{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec: atev1alpha1.ActorTemplateSpec{
			Containers: []atev1alpha1.Container{{Name: "main", Env: vars}},
		},
	}
}

func TestAgentsFromTemplates_SkipsNonCheckpointdTemplates(t *testing.T) {
	templates := []atev1alpha1.ActorTemplate{
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
	templates := []atev1alpha1.ActorTemplate{
		envTemplate("ns", "my-agent", map[string]string{checkpointdAgentEnv: "true", checkpointdAgentCardEnv: validCard}),
	}
	harnesses, _ := agentsFromTemplates(templates, "", substrate.ControlAPIOptions{})
	if _, ok := harnesses["my-agent"]; !ok {
		t.Errorf("harnesses = %v, want key %q", harnesses, "my-agent")
	}
}

func TestAgentsFromTemplates_IDOverride(t *testing.T) {
	templates := []atev1alpha1.ActorTemplate{
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
	templates := []atev1alpha1.ActorTemplate{
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
	templates := []atev1alpha1.ActorTemplate{
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
	templates := []atev1alpha1.ActorTemplate{
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
	templates := []atev1alpha1.ActorTemplate{
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
	templates := []atev1alpha1.ActorTemplate{
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
	templates := []atev1alpha1.ActorTemplate{
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
	templates := []atev1alpha1.ActorTemplate{
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
	env, ok := checkpointdAgentEnvVars(&tmpl)
	if !ok {
		t.Fatal("checkpointdAgentEnvVars: ok = false, want true")
	}
	if env[checkpointdAgentIDEnv] != "override" {
		t.Errorf("env[%s] = %q, want %q", checkpointdAgentIDEnv, env[checkpointdAgentIDEnv], "override")
	}

	other := envTemplate("ns", "other", nil)
	if _, ok := checkpointdAgentEnvVars(&other); ok {
		t.Error("checkpointdAgentEnvVars(no env): ok = true, want false")
	}
}
