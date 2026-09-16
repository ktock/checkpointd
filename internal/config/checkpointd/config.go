// Copyright 2026 Google LLC
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

// Package checkpointd provides configuration for checkpointd --
// the subset of internal/config's own schema checkpointd actually uses,
// with no dependency on internal/config or any of its other, ax-specific
// fields (antigravity harnesses, skills, telemetry, the ax gRPC server).
package checkpointd

import (
	"fmt"
	"os"
	"time"

	"github.com/ktock/checkpointd/internal/harness"
	"github.com/ktock/checkpointd/internal/harness/substrate"
	"gopkg.in/yaml.v3"
)

// The port for harnesses running as substrate actors. Substrate's actor
// networking DNATs inbound workerPodIP:80 to the actor.
const substrateDefaultPort = 80

// Config represents checkpointd's own configuration.
type Config struct {
	Version  string         `yaml:"version"`
	EventLog EventLogConfig `yaml:"eventlog"`
	Registry RegistryConfig `yaml:"registry,omitempty"`
}

// SQLiteConfig configures the SQLite event log file.
type SQLiteConfig struct {
	Filename string `yaml:"filename"` // SQLite file for event log storage
}

// PostgresConfig configures the Postgres event log.
type PostgresConfig struct {
	DSN string `yaml:"dsn"` // Postgres connection DSN
}

// EventLogConfig configures the event log storage.
type EventLogConfig struct {
	SQLiteConfig   SQLiteConfig   `yaml:"sqlite,omitempty"`
	PostgresConfig PostgresConfig `yaml:"postgres,omitempty"`
}

// RegistryConfig groups the custom harnesses to serve, on substrate, whose
// implementation and container image are provided by the user via their
// own ActorTemplate.
type RegistryConfig struct {
	Substrate []SubstrateHarnessConfig `yaml:"substrate,omitempty"`
}

// SubstrateHarnessConfig configures one custom harness deployed as a
// substrate actor.
type SubstrateHarnessConfig struct {
	ID        string           `yaml:"id"`                   // Unique harness identifier
	Namespace string           `yaml:"namespace"`            // ActorTemplate namespace
	Template  string           `yaml:"template"`             // ActorTemplate name
	Port      int              `yaml:"port,omitempty"`       // HarnessService port
	Default   bool             `yaml:"default,omitempty"`    // Default harness or not
	AgentCard *AgentCardConfig `yaml:"agent_card,omitempty"` // AgentCard of this agent
	Endpoint  string           `yaml:"endpoint,omitempty"`   // Custom substrate address
	SuspendActorTimeout string `yaml:"suspend_actor_timeout,omitempty"` // SuspendActor timeout
}

// AgentCardConfig is the user-authored subset of a2a.AgentCard.
type AgentCardConfig struct {
	Name               string                   `yaml:"name" json:"name"`
	Description        string                   `yaml:"description" json:"description"`
	Version            string                   `yaml:"version" json:"version"`
	IconURL            string                   `yaml:"icon_url,omitempty" json:"icon_url"`
	DocumentationURL   string                   `yaml:"documentation_url,omitempty" json:"documentation_url"`
	DefaultInputModes  []string                 `yaml:"default_input_modes,omitempty" json:"default_input_modes"`
	DefaultOutputModes []string                 `yaml:"default_output_modes,omitempty" json:"default_output_modes"`
	Provider           *AgentCardProviderConfig `yaml:"provider,omitempty" json:"provider"`
	Skills             []AgentCardSkillConfig   `yaml:"skills,omitempty" json:"skills"`
}

// AgentCardProviderConfig is AgentCardConfig's own Provider field.
type AgentCardProviderConfig struct {
	Organization string `yaml:"organization" json:"organization"`
	URL          string `yaml:"url" json:"url"`
}

// AgentCardSkillConfig is one entry of AgentCardConfig's own Skills field.
type AgentCardSkillConfig struct {
	ID          string   `yaml:"id" json:"id"`
	Name        string   `yaml:"name" json:"name"`
	Description string   `yaml:"description" json:"description"`
	Tags        []string `yaml:"tags,omitempty" json:"tags"`
	Examples    []string `yaml:"examples,omitempty" json:"examples"`
	InputModes  []string `yaml:"input_modes,omitempty" json:"input_modes"`
	OutputModes []string `yaml:"output_modes,omitempty" json:"output_modes"`
}

// NewHarness builds the custom harness. Custom harnesses always run as substrate
// actors from the user's own ActorTemplate.
func (c SubstrateHarnessConfig) NewHarness(endpoint string, ctrlOpts substrate.ControlAPIOptions) (harness.Harness, error) {
	port := c.Port
	if port == 0 {
		port = substrateDefaultPort
	}
	suspendActorTimeout, err := c.suspendActorTimeout()
	if err != nil {
		return nil, fmt.Errorf("substrate harness %q: %w", c.ID, err)
	}
	return newSubstrateHarness(c.ID, endpoint, c.Namespace, c.Template, port, suspendActorTimeout, ctrlOpts)
}

// suspendActorTimeout parses c.SuspendActorTimeout.
func (c SubstrateHarnessConfig) suspendActorTimeout() (time.Duration, error) {
	if c.SuspendActorTimeout == "" {
		// means use substrate.DefaultSuspendActorTimeout
		return 0, nil
	}
	d, err := time.ParseDuration(c.SuspendActorTimeout)
	if err != nil {
		return 0, fmt.Errorf("invalid suspend_actor_timeout %q: %w", c.SuspendActorTimeout, err)
	}
	return d, nil
}

// newSubstrateHarness brings up a harness that is deployed as a substrate actor.
func newSubstrateHarness(harnessID, endpoint, namespace, template string, port int, suspendActorTimeout time.Duration, ctrlOpts substrate.ControlAPIOptions) (harness.Harness, error) {
	sh, err := substrate.New(harnessID, endpoint, namespace, template, port, suspendActorTimeout, ctrlOpts)
	if err != nil {
		return nil, err
	}
	return sh, nil
}

// LoadFromFile loads configuration from a YAML file.
func LoadFromFile(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}
	return LoadFromBytes(data)
}

// LoadFromBytes parses configuration from YAML bytes and applies defaults.
func LoadFromBytes(data []byte) (*Config, error) {
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	cfg.setDefaults()

	return &cfg, nil
}

// DefaultConfig returns a configuration with default values set.
func DefaultConfig() *Config {
	var cfg Config
	cfg.setDefaults()
	return &cfg
}

// setDefaults sets default values for optional fields.
func (c *Config) setDefaults() {
	if c.EventLog.SQLiteConfig.Filename == "" {
		c.EventLog.SQLiteConfig.Filename = "eventlog/log.sqlite"
	}
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	if c.EventLog.PostgresConfig.DSN == "" && c.EventLog.SQLiteConfig.Filename == "" {
		return fmt.Errorf("eventlog requires either postgres.dsn or sqlite.filename")
	}

	var defaultCount int
	for _, sc := range c.Registry.Substrate {
		if sc.ID == "" {
			return fmt.Errorf("substrate harness id is required")
		}
		if sc.Namespace == "" {
			return fmt.Errorf("substrate harness %q: namespace is required", sc.ID)
		}
		if sc.Template == "" {
			return fmt.Errorf("substrate harness %q: template is required", sc.ID)
		}
		if _, err := sc.suspendActorTimeout(); err != nil {
			return fmt.Errorf("substrate harness %q: %w", sc.ID, err)
		}
		if sc.Default {
			defaultCount++
		}
	}

	if defaultCount > 1 {
		return fmt.Errorf("multiple harnesses marked as default")
	}

	return nil
}
