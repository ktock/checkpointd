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

// Package controller implements the single-writer orchestrator that coordinates
// agentic loops, manages executions, and communicates with local and remote agents.
package controller

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/ktock/checkpointd/internal/controller/eventlog"
	"github.com/ktock/checkpointd/internal/harness"
	"github.com/ktock/checkpointd/proto"
	"google.golang.org/protobuf/encoding/protojson"
	protolib "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

type ExecHandler func(resp *proto.CreateInteractionResponse) error

// Controller is the main controller that coordinates all components.
// It acts as a single-writer system for managing agentic loops.
type Controller struct {
	registry *Registry
	eventLog eventlog.EventLog
}

// Config configures the controller.
type Config struct {
	Registry        *Registry
	EventLogBuilder eventlog.EventLogBuilder
}

// New creates a new controller instance.
func New(ctx context.Context, cfg Config) (*Controller, error) {
	if cfg.Registry == nil {
		return nil, fmt.Errorf("registry is required")
	}
	if cfg.EventLogBuilder == nil {
		return nil, fmt.Errorf("event log builder is required")
	}
	eventLog, err := cfg.EventLogBuilder()
	if err != nil {
		return nil, fmt.Errorf("failed to create event log: %w", err)
	}

	return &Controller{
		registry: cfg.Registry,
		eventLog: eventLog,
	}, nil
}

// Exec executes a new agentic loop execution or resumes an existing one.
// If id is empty, a UUID will be generated.
// If the execution already exists, it will be resumed with optional new inputs.
func (d *Controller) Exec(ctx context.Context, req *proto.CreateInteractionEvent, handler ExecHandler) error {
	if req.ConversationId == "" {
		return fmt.Errorf("conversation_id is required")
	}
	conversationID := req.ConversationId

	// TODO(jbd): Resume an incomplete execution if there exists one.
	// TODO(jbd): Enable bringing a remote harness that implements HarnessService.
	// TODO(anj): We need to consolidate agents and harness registration.
	// Adding harness registration support temporarily.
	l := newLogger(d.eventLog, conversationID, req.AgentId, req.SessionId)
	state, storedHarnessID, err := l.ResumptionState(ctx)
	if err != nil {
		return fmt.Errorf("failed to check resumption state: %w", err)
	}

	// On resume, use the conversation's recorded harness. Using a different harness
	// for the same conversation is not allowed.
	if req.AgentId != "" && storedHarnessID != "" && req.AgentId != storedHarnessID {
		return fmt.Errorf("resumption not allowed: harness ID changed from %s to %s", storedHarnessID, req.AgentId)
	}
	harnessID := req.AgentId
	// Use the conversations's stored harness if no harness is specified.
	if harnessID == "" {
		harnessID = storedHarnessID
	}
	// For new conversations, use the default harness if no harness is specified.
	if harnessID == "" {
		harnessID = d.registry.defaultHarness
	}
	l.harnessID = harnessID

	h, err := d.registry.Harness(harnessID)
	if err != nil {
		return fmt.Errorf("failed to get harness %q: %w", harnessID, err)
	}

	if state == proto.State_STATE_PENDING {
		// Do not assume the agent remember the previous input. If the agent
		// resumes from the previously committed checkpoint, it uses this
		// input to continue the process. If the agent was successfully
		// checkpointed in the previous attempt (by checkpointd crashed before
		// committingthe completion event), the agent must deduplicate this input
		// and return the exact previous output. Our runtime harness ensure this
		// deduplication.
		pending, err := l.pendingInputs(ctx)
		if err != nil {
			return fmt.Errorf("failed to load pending inputs: %w", err)
		}
		if len(pending) == 0 {
			return fmt.Errorf("conversation %q is PENDING but no logged input was found to resume it with", conversationID)
		}
		exec, err := h.Start(ctx, conversationID, req.AgentConfig)
		if err != nil {
			return fmt.Errorf("failed to start harness session: %w", err)
		}
		defer exec.Close(ctx)

		if err := exec.Queue(ctx, pending...); err != nil {
			return fmt.Errorf("failed to queue pending inputs: %w", err)
		}
		return d.runTurn(ctx, exec, l, handler)
	}

	if len(req.Inputs) == 0 {
		return fmt.Errorf("conversation %q has no pending turn: Exec requires Inputs to start a new turn", conversationID)
	}

	exec, err := h.Start(ctx, conversationID, req.AgentConfig)
	if err != nil {
		return fmt.Errorf("failed to start harness session: %w", err)
	}
	defer exec.Close(ctx)

	if err := exec.Queue(ctx, req.Inputs...); err != nil {
		return fmt.Errorf("failed to queue inputs: %w", err)
	}
	// Log inputs before running harness
	if _, err := l.LogInputs(ctx, req.Inputs, req.AgentConfig); err != nil {
		return fmt.Errorf("failed to log inputs: %w", err)
	}
	return d.runTurn(ctx, exec, l, handler)
}

// runTurn drives exec through one turn and only durably commits its output
// to the event log -- the source of truth -- after the harness has actually
// checkpointed it. The required order is: 1) resume (h.Start, by the
// caller), 2/3) send and receive (exec.Run), 4) checkpoint (exec.Checkpoint),
// 5) log (this method, last). 4 and 5 must never be reversed: if the log
// recorded a turn that was never actually checkpointed, a future resume
// would restart the harness from a stale checkpoint while the log claims the
// turn already happened, silently diverging from the log it's supposed to
// be the truth for. So on a Checkpoint error, runTurn returns without
// touching the log at all -- the turn is left uncommitted (the log still
// only has the PENDING marker LogInputs wrote before Run), which is safe to
// retry on the next resume.
func (d *Controller) runTurn(ctx context.Context, exec harness.Execution, l *logger, handler ExecHandler) error {
	hh := &harnessHandler{logger: l, execHandler: handler}

	if err := exec.Run(ctx, hh); err != nil {
		return fmt.Errorf("harness execution failed: %w", err)
	}

	if err := exec.Checkpoint(ctx); err != nil {
		return fmt.Errorf("checkpoint failed: %w", err)
	}

	return hh.commit(ctx)
}

type harnessHandler struct {
	logger      *logger
	execHandler ExecHandler

	buffered  []*proto.Step
	completed bool
}

func (a *harnessHandler) OnMessage(ctx context.Context, execID string, step *proto.Step) error {
	a.buffered = append(a.buffered, step)

	if a.execHandler == nil {
		return nil
	}
	return a.execHandler(&proto.CreateInteractionResponse{
		Outputs: []*proto.Step{step},
	})
}

func (a *harnessHandler) OnComplete(ctx context.Context, execID string) error {
	a.completed = true
	return nil
}

// commit durably appends the turn buffered by OnMessage/OnComplete to the
// event log, and must only be called after the harness has successfully
// checkpointed the turn.
func (a *harnessHandler) commit(ctx context.Context) error {
	already, err := a.logger.pendingOutputs(ctx)
	if err != nil {
		return fmt.Errorf("failed to load already-logged outputs: %w", err)
	}
	if len(already) > len(a.buffered) {
		return fmt.Errorf("harness's redone output has %d steps, fewer than the %d already durably logged for this turn: not a valid idempotent redo", len(a.buffered), len(already))
	}
	for i, prior := range already {
		if !protolib.Equal(prior, a.buffered[i]) {
			return fmt.Errorf("harness's redone output step %d does not match what was already durably logged for this turn: not a valid idempotent redo", i)
		}
	}
	for _, step := range a.buffered[len(already):] {
		// TODO(anj): The harness should send the full input sent to get this particular response.
		logStep, err := a.logger.LogOutputs(ctx, []*proto.Step{step}, proto.State_STATE_PENDING)
		if err != nil {
			return fmt.Errorf("failed to log output: %w", err)
		}
		step.Index = logStep
	}
	if !a.completed {
		return fmt.Errorf("harness Run returned without ever calling OnComplete for this turn")
	}
	if _, err := a.logger.LogOutputs(ctx, nil, proto.State_STATE_COMPLETED); err != nil {
		return fmt.Errorf("failed to log completion: %w", err)
	}
	return nil
}

// Registry returns the agent registry.
func (d *Controller) Registry() *Registry {
	return d.registry
}

// Close gracefully shuts down the controller.
func (d *Controller) Close() error {
	if err := d.eventLog.Close(); err != nil {
		return fmt.Errorf("failed to close event log: %w", err)
	}
	if err := d.registry.Close(); err != nil {
		return fmt.Errorf("failed to close registry: %w", err)
	}
	return nil
}

type logger struct {
	conversationID string
	interactionID  string
	el             eventlog.EventLog
	harnessID      string
	sessionID      string
}

func newLogger(
	el eventlog.EventLog,
	conversationID string,
	harnessID string,
	sessionID string) *logger {
	return &logger{
		el:             el,
		conversationID: conversationID,
		harnessID:      harnessID,
		sessionID:      sessionID,
	}
}

// ResumptionState returns the conversation's current state and the harness it used.
func (l *logger) ResumptionState(ctx context.Context) (proto.State, string, error) {
	events, err := l.el.Events(ctx, l.conversationID)
	if err != nil {
		return proto.State_STATE_UNSPECIFIED, "", err
	}

	var state proto.State
	var harnessID string
	for _, ev := range events {
		if harnessID == "" && ev.AgentId != "" {
			harnessID = ev.AgentId
		}
		if ev.State != proto.State_STATE_UNSPECIFIED {
			state = ev.State
		}
	}
	return state, harnessID, nil
}

// pendingInputs returns the steps LogInputs recorded for the most recent
// PENDING marker.
func (l *logger) pendingInputs(ctx context.Context) ([]*proto.Step, error) {
	events, err := l.el.Events(ctx, l.conversationID)
	if err != nil {
		return nil, err
	}
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].State == proto.State_STATE_PENDING && events[i].AgentId != "" {
			return events[i].Steps, nil
		}
	}
	return nil, nil
}

// pendingOutputs returns the steps an earlier, interrupted attempt at
// committing the current still-PENDING turn already durably logged.
func (l *logger) pendingOutputs(ctx context.Context) ([]*proto.Step, error) {
	events, err := l.el.Events(ctx, l.conversationID)
	if err != nil {
		return nil, err
	}
	var reversed []*proto.Step
	for i := len(events) - 1; i >= 0; i-- {
		ev := events[i]
		if ev.State != proto.State_STATE_PENDING || ev.AgentId != "" {
			// Either the prior turn's own COMPLETED marker (nothing of
			// this turn's own output logged yet) or this turn's own
			// LogInputs marker (everything collected so far is this
			// turn's own already-logged output) -- either way, done.
			break
		}
		reversed = append(reversed, ev.Steps...)
	}
	steps := make([]*proto.Step, len(reversed))
	for i, s := range reversed {
		steps[len(reversed)-1-i] = s
	}
	return steps, nil
}

func (l *logger) LogInputs(ctx context.Context, steps []*proto.Step, agentConfig []byte) (int64, error) {
	// Parse the agent config into a human-readable struct for logging.
	var cfg *structpb.Struct
	if len(agentConfig) > 0 {
		cfg = &structpb.Struct{}
		if err := protojson.Unmarshal(agentConfig, cfg); err != nil {
			slog.WarnContext(ctx, "Failed to parse agent config for logging",
				slog.String("conversation_id", l.conversationID),
				slog.Any("error", err),
			)
			cfg = nil
		}
	}
	ev := &proto.StepEvent{
		ConversationId: l.conversationID,
		InteractionId:  l.interactionID,
		AgentId:        l.harnessID,
		AgentConfig:    cfg,
		Steps:          steps,
		State:          proto.State_STATE_PENDING,
		SessionId:      l.sessionID,
	}
	return l.el.Append(ctx, ev)
}

func (l *logger) LogOutputs(ctx context.Context, steps []*proto.Step, state proto.State) (int64, error) {
	ev := &proto.StepEvent{
		ConversationId: l.conversationID,
		InteractionId:  l.interactionID,
		Steps:          steps,
		State:          state,
		SessionId:      l.sessionID,
	}
	return l.el.Append(ctx, ev)
}
