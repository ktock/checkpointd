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
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"sort"
	"strconv"
	"time"

	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	ateclientset "github.com/agent-substrate/substrate/pkg/client/clientset/versioned"
	config "github.com/ktock/checkpointd/internal/config/checkpointd"
	"github.com/ktock/checkpointd/internal/controller"
	"github.com/ktock/checkpointd/internal/harness"
	"github.com/ktock/checkpointd/internal/harness/substrate"
	"gopkg.in/yaml.v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

const (
	// checkpointdAgentEnv being true allows this template to be a target of
	// checkpointd's agent discovery. Default is false.
	checkpointdAgentEnv = "CHECKPOINTD_AGENT"

	// checkpointdAgentIDEnv optionally overrides the agent's ID. ActorTemplate's
	// own name is used when unset.
	checkpointdAgentIDEnv = "CHECKPOINTD_AGENT_ID"

	// checkpointdAgentCardEnv carries this agent's config.AgentCardConfig
	// in JSON or YAML. An actor that allows discovery (checkpointdAgentEnv) but
	// omits this or malformed is logged and skipped entirely.
	checkpointdAgentCardEnv = "CHECKPOINTD_AGENT_CARD"

	// checkpointdHarnessPortEnv optionally overrides defaultHarnessPort with the
	// port this agent's own HarnessService actually listens on.
	checkpointdHarnessPortEnv = "CHECKPOINTD_HARNESS_PORT"

	// defaultHarnessPort is used when checkpointdHarnessPortEnv is unset.
	defaultHarnessPort = 80

	// checkpointdNamespaceEnv is the target namespace to discover agents.
	checkpointdNamespaceEnv = "CHECKPOINTD_KUBERNETES_NAMESPACE"
)

// newDiscoveryClient builds the typed Kubernetes clientset discoverAgents
// uses to list ActorTemplate CRDs. Note that Substrate Control API's own
// ListActorTemplates RPC only reads agents populated by an explicit
// CreateActorTemplate call.
func newDiscoveryClient() (ateclientset.Interface, error) {
	// authenticates as checkpointd-server pod's ServiceAccount.
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("loading in-cluster Kubernetes config (--discover only works in a real cluster): %w", err)
	}
	return ateclientset.NewForConfig(cfg)
}

// discoverAgents lists every ActorTemplate and returns the harness.Harness
// set, per-agent AgentCard content checkpointd needs, and the raw
// ActorTemplate names Kubernetes reported.
func discoverAgents(ctx context.Context, cs ateclientset.Interface, endpoint string, ctrlOpts substrate.ControlAPIOptions) (map[string]harness.Harness, map[string]*config.AgentCardConfig, []string, error) {
	list, err := cs.ApiV1alpha1().ActorTemplates(os.Getenv(checkpointdNamespaceEnv)).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("listing ActorTemplates: %w", err)
	}
	var names []string
	for _, t := range list.Items {
		names = append(names, t.Namespace+"/"+t.Name)
	}
	harnesses, cards := agentsFromTemplates(list.Items, endpoint, ctrlOpts)
	return harnesses, cards, names, nil
}

func agentsFromTemplates(templates []atev1alpha1.ActorTemplate, endpoint string, ctrlOpts substrate.ControlAPIOptions) (map[string]harness.Harness, map[string]*config.AgentCardConfig) {
	harnesses := make(map[string]harness.Harness)
	cards := make(map[string]*config.AgentCardConfig)
	for i := range templates {
		tmpl := &templates[i]
		env, ok := checkpointdAgentEnvVars(tmpl)
		if !ok {
			continue
		}

		id := env[checkpointdAgentIDEnv]
		if id == "" {
			id = tmpl.Name
		}
		if id == "" {
			log.Infof("discovery: ActorTemplate in namespace %q has no name and no %s override, skipping", tmpl.Namespace, checkpointdAgentIDEnv)
			continue
		}
		if _, dup := harnesses[id]; dup {
			log.Infof("discovery: agent %q already discovered from another ActorTemplate, skipping duplicate", id)
			continue
		}

		raw := env[checkpointdAgentCardEnv]
		if raw == "" {
			log.Infof("discovery: agent %q has no %s, skipping (agent_card is required)", id, checkpointdAgentCardEnv)
			continue
		}
		card, err := parseAgentCardEnv(raw)
		if err != nil {
			log.Infof("discovery: agent %q: parsing %s: %v", id, checkpointdAgentCardEnv, err)
			continue
		}

		port := defaultHarnessPort
		if raw, ok := env[checkpointdHarnessPortEnv]; ok {
			p, err := strconv.Atoi(raw)
			if err != nil || p <= 0 {
				log.Infof("discovery: agent %q has invalid %s %q, skipping", id, checkpointdHarnessPortEnv, raw)
				continue
			} else {
				port = p
			}
		}

		h, err := substrate.New(id, endpoint, tmpl.Namespace, tmpl.Name, port, 0, ctrlOpts)
		if err != nil {
			log.Infof("discovery: agent %q: building harness: %v", id, err)
			continue
		}
		harnesses[id] = h
		cards[id] = card
	}
	return harnesses, cards
}

func parseAgentCardEnv(raw string) (*config.AgentCardConfig, error) {
	var card config.AgentCardConfig
	jsonErr := json.Unmarshal([]byte(raw), &card)
	if jsonErr == nil {
		return &card, nil
	}
	if yamlErr := yaml.Unmarshal([]byte(raw), &card); yamlErr == nil {
		return &card, nil
	}
	return nil, fmt.Errorf("not valid JSON (%w) or YAML", jsonErr)
}

// checkpointdAgentEnvVars returns the env vars of tmpl's first container that
// sets checkpointdAgentEnv to "true".
func checkpointdAgentEnvVars(tmpl *atev1alpha1.ActorTemplate) (map[string]string, bool) {
	for _, c := range tmpl.Spec.Containers {
		env := make(map[string]string, len(c.Env))
		for _, e := range c.Env {
			env[e.Name] = e.Value
		}
		if env[checkpointdAgentEnv] == "true" {
			return env, true
		}
	}
	return nil, false
}

// discoveryState remembers the most recently applied agent set, so
// discoverAndApply only logs when the harness set actually changes.
type discoveryState struct {
	lastAgentIDs []string
}

// changed reports whether ids (sorted) differs from the last-seen set,
// updating state to ids either way.
func (s *discoveryState) changed(ids []string) bool {
	same := slices.Equal(ids, s.lastAgentIDs)
	s.lastAgentIDs = ids
	return !same
}

// discoverAndApply runs one discoverAgents pass and, on success, applies it.
// live-swaps reg's harness set via Registry.Replace and stores the result in
// cards for buildDynamicServerMux's routing. Only logs when the discovered
// agent set differs from state's own last-seen one.
func discoverAndApply(ctx context.Context, cs ateclientset.Interface, endpoint string, ctrlOpts substrate.ControlAPIOptions, reg *controller.Registry, cards *agentCardStore, state *discoveryState) error {
	harnesses, newCards, names, err := discoverAgents(ctx, cs, endpoint, ctrlOpts)
	if err != nil {
		return err
	}
	if err := reg.Replace(harnesses, ""); err != nil {
		return err
	}
	cards.store(newCards)

	agentIDs := make([]string, 0, len(harnesses))
	for id := range harnesses {
		agentIDs = append(agentIDs, id)
	}
	sort.Strings(agentIDs)

	if state.changed(agentIDs) {
		log.Infof("discovery: Kubernetes reported %d ActorTemplate(s): %v", len(names), names)
		log.Infof("discovery: found %d agent(s): %v", len(agentIDs), agentIDs)
	}
	return nil
}

func runDiscoveryLoop(ctx context.Context, cs ateclientset.Interface, endpoint string, ctrlOpts substrate.ControlAPIOptions, reg *controller.Registry, cards *agentCardStore, interval time.Duration, state *discoveryState) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := discoverAndApply(ctx, cs, endpoint, ctrlOpts, reg, cards, state); err != nil {
				log.Infof("discovery: %v, keeping previous agent set", err)
			}
		}
	}
}
