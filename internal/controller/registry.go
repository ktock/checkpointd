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

package controller

import (
	"errors"
	"fmt"
	"sync"

	"github.com/ktock/checkpointd/internal/harness"
)

// ErrHarnessNotFound means id names no registered harness.
var ErrHarnessNotFound = errors.New("harness not found")

// Registry manages a collection of harnesses.
type Registry struct {
	mu             sync.RWMutex
	harnesses      map[string]harness.Harness
	defaultHarness string
}

// NewRegistry creates a new harness registry.
func NewRegistry() *Registry {
	return &Registry{
		harnesses: make(map[string]harness.Harness),
	}
}

// RegisterHarness registers a harness under the given id.
func (r *Registry) RegisterHarness(id string, h harness.Harness) error {
	if err := validateID(id); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.harnesses[id]; ok {
		return fmt.Errorf("harness %q already registered", id)
	}
	r.harnesses[id] = h
	return nil
}

// Harness retrieves a harness by id.
func (r *Registry) Harness(id string) (harness.Harness, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.harnesses[id]
	if !ok {
		return nil, fmt.Errorf("agent %q not found (its ActorTemplate may not exist, or may have been removed): %w", id, ErrHarnessNotFound)
	}
	return h, nil
}

// Replace swaps this registry's entire harness set.
func (r *Registry) Replace(harnesses map[string]harness.Harness, defaultID string) error {
	for id := range harnesses {
		if err := validateID(id); err != nil {
			return err
		}
	}
	if defaultID != "" {
		if _, ok := harnesses[defaultID]; !ok {
			return fmt.Errorf("default harness %q not found in the replacement set", defaultID)
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.harnesses = harnesses
	r.defaultHarness = defaultID
	return nil
}

// SetDefaultHarness marks a registered harness id as the default.
func (r *Registry) SetDefaultHarness(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.harnesses[id]; !ok {
		return fmt.Errorf("harness %q not found", id)
	}
	r.defaultHarness = id
	return nil
}

// Close releases resources held by the registry.
func (r *Registry) Close() error {
	return nil
}
