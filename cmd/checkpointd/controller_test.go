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
	"strings"
	"testing"

	config "github.com/ktock/checkpointd/internal/config/checkpointd"
	"github.com/ktock/checkpointd/internal/harness"
	"github.com/ktock/checkpointd/internal/harness/substrate"
	"github.com/ktock/checkpointd/proto"
)

// TestNewController_DuplicateSubstrateIDFails is the regression test for
// registry.substrate having two entries claiming the same id in checkpointd's
// own static config.
func TestNewController_DuplicateSubstrateIDFails(t *testing.T) {
	cfg := &config.Config{
		Registry: config.RegistryConfig{
			Substrate: []config.SubstrateHarnessConfig{
				{ID: "dup-agent", Namespace: "ns", Template: "template-a"},
				{ID: "dup-agent", Namespace: "ns", Template: "template-b"},
			},
		},
	}
	_, _, err := newController(context.Background(), cfg, substrate.ControlAPIOptions{})
	if err == nil {
		t.Fatal("newController err = nil, want an error for a duplicated registry.substrate id")
	}
	if !strings.Contains(err.Error(), "dup-agent") {
		t.Errorf("newController err = %q, want it to name the duplicated id %q", err, "dup-agent")
	}
}

// noOutputExecution completes its turn without ever calling OnMessage --
// standing in for an agent whose Execute() yields nothing at all, for
// TestRawExec_NoOutputErrors.
type noOutputExecution struct{}

func (noOutputExecution) ID() string { return "no-output" }
func (noOutputExecution) Queue(ctx context.Context, steps ...*proto.Step) error {
	return nil
}
func (noOutputExecution) Run(ctx context.Context, handler harness.Handler) error {
	return handler.OnComplete(ctx, "no-output")
}
func (noOutputExecution) Checkpoint(ctx context.Context) error { return nil }
func (noOutputExecution) Close(ctx context.Context) error      { return nil }

type noOutputHarness struct{}

func (noOutputHarness) Start(ctx context.Context, conversationID string, config []byte) (harness.Execution, error) {
	return noOutputExecution{}, nil
}

// TestRawExec_NoOutputErrors confirms rawExec fails loudly when a turn
// completes successfully but produces no output text at all, instead of
// silently returning "" for its own caller (execA2A) to then fail on with a
// confusing "not an A2A message" error further downstream.
func TestRawExec_NoOutputErrors(t *testing.T) {
	c, _, _ := newTestServerController(t, map[string]harness.Harness{"a": noOutputHarness{}})
	ctx := context.Background()

	_, err := rawExec(ctx, c, &proto.CreateInteractionEvent{
		AgentId:        "a",
		ConversationId: "conv-1",
		Inputs:         []*proto.Step{userStep("hi")},
	})
	if err == nil {
		t.Fatal("rawExec with an agent that produced no output: got nil error, want one")
	}
	if !strings.Contains(err.Error(), "no output") {
		t.Errorf("err = %q, want it to mention the agent producing no output", err)
	}
}
