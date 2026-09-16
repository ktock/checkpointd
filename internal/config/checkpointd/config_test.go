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

package checkpointd

import (
	"strings"
	"testing"

	"github.com/ktock/checkpointd/internal/harness/substrate"
	"gopkg.in/yaml.v3"
)

func TestSubstrateNewHarness(t *testing.T) {
	h, err := SubstrateHarnessConfig{ID: "c", Namespace: "team-ns", Template: "custom-template"}.NewHarness("api.ate-system.svc:443", substrate.ControlAPIOptions{})
	if err != nil {
		t.Fatalf("NewHarness: %v", err)
	}
	if h == nil {
		t.Fatal("expected non-nil harness")
	}
}

// validConfig returns a config that passes Validate, that tests can mutate.
func validConfig() *Config {
	c := DefaultConfig()
	c.Registry = RegistryConfig{
		Substrate: []SubstrateHarnessConfig{
			{ID: "custom", Namespace: "team-ns", Template: "custom-template", Default: true},
		},
	}
	return c
}

func TestValidate_ValidConfig(t *testing.T) {
	if err := validConfig().Validate(); err != nil {
		t.Fatalf("Validate() = %v, want nil", err)
	}
}

func TestValidate_CustomIDRequired(t *testing.T) {
	c := validConfig()
	c.Registry.Substrate[0].ID = ""
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "substrate harness id") {
		t.Fatalf("Validate() = %v, want substrate id error", err)
	}
}

func TestValidate_CustomNamespaceRequired(t *testing.T) {
	c := validConfig()
	c.Registry.Substrate[0].Namespace = ""
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "namespace is required") {
		t.Fatalf("Validate() = %v, want namespace-required error", err)
	}
}

func TestValidate_CustomTemplateRequired(t *testing.T) {
	c := validConfig()
	c.Registry.Substrate[0].Template = ""
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "template is required") {
		t.Fatalf("Validate() = %v, want template-required error", err)
	}
}

func TestValidate_SuspendActorTimeoutInvalid(t *testing.T) {
	c := validConfig()
	c.Registry.Substrate[0].SuspendActorTimeout = "not-a-duration"
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "suspend_actor_timeout") {
		t.Fatalf("Validate() = %v, want suspend_actor_timeout error", err)
	}
}

func TestValidate_SuspendActorTimeoutValid(t *testing.T) {
	c := validConfig()
	c.Registry.Substrate[0].SuspendActorTimeout = "11m"
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate() = %v, want nil", err)
	}
}

func TestSubstrateNewHarness_InvalidSuspendActorTimeout(t *testing.T) {
	_, err := SubstrateHarnessConfig{ID: "c", Namespace: "team-ns", Template: "custom-template", SuspendActorTimeout: "not-a-duration"}.NewHarness("api.ate-system.svc:443", substrate.ControlAPIOptions{})
	if err == nil || !strings.Contains(err.Error(), "suspend_actor_timeout") {
		t.Fatalf("NewHarness() = %v, want suspend_actor_timeout error", err)
	}
}

func TestSubstrateNewHarness_ValidSuspendActorTimeout(t *testing.T) {
	h, err := SubstrateHarnessConfig{ID: "c", Namespace: "team-ns", Template: "custom-template", SuspendActorTimeout: "11m"}.NewHarness("api.ate-system.svc:443", substrate.ControlAPIOptions{})
	if err != nil {
		t.Fatalf("NewHarness: %v", err)
	}
	if h == nil {
		t.Fatal("expected non-nil harness")
	}
}

// TestValidate_MultipleDefaults confirms two substrate harnesses both
// marked default is rejected.
func TestValidate_MultipleDefaults(t *testing.T) {
	c := validConfig()
	c.Registry.Substrate = append(c.Registry.Substrate, SubstrateHarnessConfig{
		ID: "custom2", Namespace: "team-ns", Template: "custom-template", Default: true,
	})
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "multiple harnesses marked as default") {
		t.Fatalf("Validate() = %v, want multiple defaults error", err)
	}
}

func TestLoadFromFile_Version(t *testing.T) {
	data := `
version: "1.2.3"
eventlog:
  sqlite:
    filename: "test.db"
`
	var cfg Config
	if err := yaml.Unmarshal([]byte(data), &cfg); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if cfg.Version != "1.2.3" {
		t.Errorf("cfg.Version = %q, want %q", cfg.Version, "1.2.3")
	}
}

func TestLoadFromBytes(t *testing.T) {
	cfg, err := LoadFromBytes([]byte(`
version: v1alpha
`))
	if err != nil {
		t.Fatalf("LoadFromBytes: %v", err)
	}
	if cfg.Version != "v1alpha" {
		t.Errorf("Version = %q, want %q", cfg.Version, "v1alpha")
	}
	// setDefaults must run (same as LoadFromFile).
	if got, want := cfg.EventLog.SQLiteConfig.Filename, "eventlog/log.sqlite"; got != want {
		t.Errorf("EventLog.SQLiteConfig.Filename = %q, want default %q", got, want)
	}
}

// TestLoadFromBytes_Invalid returns an error on malformed YAML.
func TestLoadFromBytes_Invalid(t *testing.T) {
	if _, err := LoadFromBytes([]byte("registry: [unterminated")); err == nil {
		t.Fatal("LoadFromBytes(invalid): got nil error, want error")
	}
}
