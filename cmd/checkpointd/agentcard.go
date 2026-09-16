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
	"github.com/a2aproject/a2a-go/v2/a2a"
	config "github.com/ktock/checkpointd/internal/config/checkpointd"
	"github.com/ktock/checkpointd/internal/hop"
)

func buildAgentCard(content *config.AgentCardConfig, id, baseURL string) *a2a.AgentCard {
	card := &a2a.AgentCard{
		Name:             content.Name,
		Description:      content.Description,
		Version:          content.Version,
		IconURL:          content.IconURL,
		DocumentationURL: content.DocumentationURL,
		// DefaultInputModes, DefaultOutputModes, and Skills below are all
		// non-nil since none has `omitempty`, so a nil slice would
		// otherwise marshal as JSON null, which the AgentCard schema rejects.
		DefaultInputModes:  nonNil(content.DefaultInputModes),
		DefaultOutputModes: nonNil(content.DefaultOutputModes),
		Skills:             []a2a.AgentSkill{},
		SupportedInterfaces: []*a2a.AgentInterface{
			// Agent-to-agent communication picks this interface
			a2a.NewAgentInterface(hop.CheckpointdURLPrefix+id, hop.CheckpointdTransportProtocol),

			// External-client-to-agent communication picks this interface
			a2a.NewAgentInterface(baseURL, a2a.TransportProtocolJSONRPC),
		},
		// Additional capabilities are unsupported
		Capabilities: a2a.AgentCapabilities{Streaming: false, PushNotifications: false},
	}
	if content.Provider != nil {
		card.Provider = &a2a.AgentProvider{Org: content.Provider.Organization, URL: content.Provider.URL}
	}
	for _, s := range content.Skills {
		card.Skills = append(card.Skills, a2a.AgentSkill{
			ID:          s.ID,
			Name:        s.Name,
			Description: s.Description,
			Tags:        nonNil(s.Tags),
			Examples:    s.Examples,
			InputModes:  s.InputModes,
			OutputModes: s.OutputModes,
		})
	}
	return card
}

// nonNil returns s unchanged, or a non-nil empty slice if s is nil.
func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}
