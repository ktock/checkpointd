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
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/ktock/checkpointd/internal/controller/eventlog"
	"github.com/ktock/checkpointd/internal/controller/eventlog/eventlogtest"
	"github.com/ktock/checkpointd/internal/harness"
	"github.com/ktock/checkpointd/internal/harness/harnesstest"
	"github.com/ktock/checkpointd/proto"
)

type fakeHarness struct{}

func (f *fakeHarness) Start(ctx context.Context, conversationID string, config []byte) (harness.Execution, error) {
	return &fakeExecution{id: "fake-exec-id"}, nil
}

type fakeExecution struct {
	id     string
	queued []*proto.Step
}

func (f *fakeExecution) ID() string {
	return f.id
}

func (f *fakeExecution) Queue(ctx context.Context, steps ...*proto.Step) error {
	f.queued = append(f.queued, steps...)
	return nil
}

func (f *fakeExecution) Run(ctx context.Context, handler harness.Handler) error {
	step := &proto.Step{
		Type: &proto.Step_Content{
			Content: &proto.ContentStep{
				Role: "assistant",
				Content: []*proto.Content{
					{
						Type: &proto.Content_Text{
							Text: &proto.TextContent{
								Text: "Hello world",
							},
						},
					},
				},
			},
		},
	}
	if err := handler.OnMessage(ctx, f.id, step); err != nil {
		return err
	}
	return handler.OnComplete(ctx, f.id)
}

func (f *fakeExecution) Checkpoint(ctx context.Context) error {
	return nil
}

func (f *fakeExecution) Close(ctx context.Context) error {
	return nil
}

func TestController2_ExecHelloWorld(t *testing.T) {
	ctx := context.Background()
	cid := "test-conversation-id"

	log := &eventlogtest.MemoryEventLog{}
	reg := NewRegistry()
	if err := reg.RegisterHarness("default-harness", &fakeHarness{}); err != nil {
		t.Fatal(err)
	}
	if err := reg.SetDefaultHarness("default-harness"); err != nil {
		t.Fatal(err)
	}

	c, err := New(ctx, Config{
		Registry: reg,
		EventLogBuilder: func() (eventlog.EventLog, error) {
			return log, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	var outputs []*proto.Step
	handler := ExecHandler(func(resp *proto.CreateInteractionResponse) error {
		outputs = append(outputs, resp.Outputs...)
		return nil
	})

	err = c.Exec(ctx, &proto.CreateInteractionEvent{
		ConversationId: cid,
		Inputs:         []*proto.Step{harnesstest.UserStep("Trigger prompt")},
	}, handler)
	if err != nil {
		t.Fatalf("Controller2.Exec failed: %v", err)
	}

	if len(outputs) != 1 {
		t.Fatalf("expected exactly 1 output message, got %d", len(outputs))
	}

	gotText := outputs[0].GetContent().Content[0].GetText().GetText()
	if gotText != "Hello world" {
		t.Errorf("expected 'Hello world' output text response, got %q", gotText)
	}

	// Verify that events were logged correctly in Conversation Log
	events, err := log.Events(ctx, cid)
	if err != nil {
		t.Fatalf("failed to retrieve logged events: %v", err)
	}

	if len(events) != 3 {
		t.Fatalf("expected 3 logged events, got %d", len(events))
	}

	// 1. First event should be inputs
	if len(events[0].Steps) != 1 {
		t.Errorf("expected 1 step in first event, got %d", len(events[0].Steps))
	} else {
		gotInputText := events[0].Steps[0].GetContent().Content[0].GetText().GetText()
		if gotInputText != "Trigger prompt" {
			t.Errorf("expected 'Trigger prompt' in logged input, got %q", gotInputText)
		}
	}
	if events[0].State != proto.State_STATE_PENDING {
		t.Errorf("expected first event state to be PENDING, got %v", events[0].State)
	}

	// 2. Second event should be output
	if len(events[1].Steps) != 1 {
		t.Errorf("expected 1 step in second event, got %d", len(events[1].Steps))
	} else {
		gotOutputText := events[1].Steps[0].GetContent().Content[0].GetText().GetText()
		if gotOutputText != "Hello world" {
			t.Errorf("expected 'Hello world' in logged output, got %q", gotOutputText)
		}
	}
	if events[1].State != proto.State_STATE_PENDING {
		t.Errorf("expected second event state to be PENDING, got %v", events[1].State)
	}

	// 3. Third event should be completion
	if len(events[2].Steps) != 0 {
		t.Errorf("expected 0 steps in third event, got %d", len(events[2].Steps))
	}
	if events[2].State != proto.State_STATE_COMPLETED {
		t.Errorf("expected third event state to be COMPLETED, got %v", events[2].State)
	}

}

func TestController2_ExecWithAgentID(t *testing.T) {
	ctx := context.Background()
	cid := "test-conversation-id"

	log := &eventlogtest.MemoryEventLog{}
	reg := NewRegistry()
	if err := reg.RegisterHarness("my-agent", &fakeHarness{}); err != nil {
		t.Fatal(err)
	}

	c, err := New(ctx, Config{
		Registry: reg,
		EventLogBuilder: func() (eventlog.EventLog, error) {
			return log, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	var outputs []*proto.Step
	handler := ExecHandler(func(resp *proto.CreateInteractionResponse) error {
		outputs = append(outputs, resp.Outputs...)
		return nil
	})

	err = c.Exec(ctx, &proto.CreateInteractionEvent{
		ConversationId: cid,
		AgentId:        "my-agent",
		Inputs:         []*proto.Step{harnesstest.UserStep("Trigger prompt")},
	}, handler)
	if err != nil {
		t.Fatalf("Controller2.Exec failed: %v", err)
	}

	if len(outputs) != 1 {
		t.Fatalf("expected exactly 1 output message, got %d", len(outputs))
	}

	gotText := outputs[0].GetContent().Content[0].GetText().GetText()
	if gotText != "Hello world" {
		t.Errorf("expected 'Hello world' output text response, got %q", gotText)
	}
}

func TestController2_ExecHarnessNotFound(t *testing.T) {
	ctx := context.Background()
	cid := "test-conversation-id"

	log := &eventlogtest.MemoryEventLog{}
	reg := NewRegistry() // Empty registry, will force error for any requested agent

	c, err := New(ctx, Config{
		Registry: reg,
		EventLogBuilder: func() (eventlog.EventLog, error) {
			return log, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	handler := ExecHandler(func(resp *proto.CreateInteractionResponse) error {
		return nil
	})

	err = c.Exec(ctx, &proto.CreateInteractionEvent{
		ConversationId: cid,
		AgentId:        "antigravity",
		Inputs:         []*proto.Step{harnesstest.UserStep("Trigger prompt")},
	}, handler)
	if err == nil {
		t.Fatal("expected error requesting unregistered agent, got nil")
	}
	// cmd/checkpointd/relay.go's retry backoff distinguishes "unknown agent"
	// from any other failure by checking this; confirm Exec's own error
	// wrapping still lets it unwrap all the way through.
	if !errors.Is(err, ErrHarnessNotFound) {
		t.Errorf("Exec error = %v, want it to wrap ErrHarnessNotFound", err)
	}
}

type testHarness struct {
	startCalls int
	startFunc  func(ctx context.Context, conversationID string) (harness.Execution, error)
}

func (c *testHarness) Start(ctx context.Context, conversationID string, config []byte) (harness.Execution, error) {
	c.startCalls++
	return c.startFunc(ctx, conversationID)
}

type testExecution struct {
	id              string
	queueCalls      int
	runCalls        int
	checkpointCalls int
	closeCalls      int
	queued          []*proto.Step
	runFunc         func(ctx context.Context, execID string, handler harness.Handler) error
	// checkpointFunc, if set, is called by Checkpoint in place of returning nil.
	checkpointFunc func(ctx context.Context) error
}

func (c *testExecution) ID() string {
	return c.id
}

func (c *testExecution) Queue(ctx context.Context, steps ...*proto.Step) error {
	c.queueCalls++
	c.queued = append(c.queued, steps...)
	return nil
}

func (c *testExecution) Run(ctx context.Context, handler harness.Handler) error {
	c.runCalls++
	if c.runFunc != nil {
		return c.runFunc(ctx, c.id, handler)
	}
	// Matches every real harness.Execution's own contract: Run must call
	// OnComplete before returning nil.
	return handler.OnComplete(ctx, c.id)
}

func (c *testExecution) Checkpoint(ctx context.Context) error {
	c.checkpointCalls++
	if c.checkpointFunc != nil {
		return c.checkpointFunc(ctx)
	}
	return nil
}

func (c *testExecution) Close(ctx context.Context) error {
	c.closeCalls++
	return nil
}

func TestController2_ExecResumptionFlow(t *testing.T) {
	// Subtest 1: New Execution with Inputs
	t.Run("NewExecutionWithInputs", func(t *testing.T) {
		ctx := context.Background()
		cid := "new-conv"

		log := &eventlogtest.MemoryEventLog{}
		reg := NewRegistry()

		var exec *testExecution
		h := &testHarness{
			startFunc: func(ctx context.Context, conversationID string) (harness.Execution, error) {
				exec = &testExecution{
					id: "exec-new",
					runFunc: func(ctx context.Context, execID string, handler harness.Handler) error {
						return handler.OnComplete(ctx, execID)
					},
				}
				return exec, nil
			},
		}
		if err := reg.RegisterHarness("test-agent", h); err != nil {
			t.Fatal(err)
		}

		c, err := New(ctx, Config{
			Registry:        reg,
			EventLogBuilder: func() (eventlog.EventLog, error) { return log, nil },
		})
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		err = c.Exec(ctx, &proto.CreateInteractionEvent{
			ConversationId: cid,
			AgentId:        "test-agent",
			Inputs:         []*proto.Step{harnesstest.UserStep("Hello")},
		}, func(resp *proto.CreateInteractionResponse) error { return nil })
		if err != nil {
			t.Fatal(err)
		}

		if h.startCalls != 1 {
			t.Errorf("expected 1 Start call, got %d", h.startCalls)
		}
		if exec.queueCalls != 1 {
			t.Errorf("expected 1 Queue call, got %d", exec.queueCalls)
		}
		if exec.runCalls != 1 {
			t.Errorf("expected 1 Run call, got %d", exec.runCalls)
		}
	})

	// Subtest 2: Pending Execution with NO New Inputs
	t.Run("PendingExecutionWithoutNewInputs", func(t *testing.T) {
		ctx := context.Background()
		cid := "pending-no-inputs"

		log := &eventlogtest.MemoryEventLog{}
		// Seed the event log with a pending event
		_, err := log.Append(ctx, &proto.StepEvent{
			ConversationId: cid,
			AgentId:        "test-agent",
			State:          proto.State_STATE_PENDING,
			Steps:          []*proto.Step{harnesstest.UserStep("Initial")},
		})
		if err != nil {
			t.Fatal(err)
		}

		reg := NewRegistry()

		var exec *testExecution
		h := &testHarness{
			startFunc: func(ctx context.Context, conversationID string) (harness.Execution, error) {
				exec = &testExecution{
					id: "exec-pending",
					runFunc: func(ctx context.Context, execID string, handler harness.Handler) error {
						return handler.OnComplete(ctx, execID)
					},
				}
				return exec, nil
			},
		}
		if err := reg.RegisterHarness("test-agent", h); err != nil {
			t.Fatal(err)
		}

		c, err := New(ctx, Config{
			Registry:        reg,
			EventLogBuilder: func() (eventlog.EventLog, error) { return log, nil },
		})
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		err = c.Exec(ctx, &proto.CreateInteractionEvent{
			ConversationId: cid,
			AgentId:        "test-agent",
			Inputs:         nil, // NO new inputs
		}, func(resp *proto.CreateInteractionResponse) error { return nil })
		if err != nil {
			t.Fatal(err)
		}

		if h.startCalls != 1 {
			t.Errorf("expected 1 Start call, got %d", h.startCalls)
		}
		// The pending turn's own logged inputs ("Initial") are queued and
		// resent -- resuming a pending turn is never a silent no-op on the
		// harness side, even though the caller sent nothing new.
		if exec.queueCalls != 1 {
			t.Errorf("expected 1 Queue call, got %d", exec.queueCalls)
		}
		if got := stepText(t, exec.queued); got != "Initial" {
			t.Errorf("expected queued step text %q, got %q", "Initial", got)
		}
		if exec.runCalls != 1 {
			t.Errorf("expected 1 Run call, got %d", exec.runCalls)
		}
	})

	// Subtest 3: Pending Execution WITH New Inputs -- the new inputs must be
	// ignored in favor of the pending turn's own logged inputs, and the
	// pending turn must be resumed exactly once, not run twice.
	t.Run("PendingExecutionWithNewInputs", func(t *testing.T) {
		ctx := context.Background()
		cid := "pending-with-inputs"

		log := &eventlogtest.MemoryEventLog{}
		// Seed the event log with a pending event
		_, err := log.Append(ctx, &proto.StepEvent{
			ConversationId: cid,
			AgentId:        "test-agent",
			State:          proto.State_STATE_PENDING,
			Steps:          []*proto.Step{harnesstest.UserStep("Initial")},
		})
		if err != nil {
			t.Fatal(err)
		}

		reg := NewRegistry()

		var execs []*testExecution
		h := &testHarness{
			startFunc: func(ctx context.Context, conversationID string) (harness.Execution, error) {
				exec := &testExecution{
					id: fmt.Sprintf("exec-%d", len(execs)+1),
					runFunc: func(ctx context.Context, execID string, handler harness.Handler) error {
						return handler.OnComplete(ctx, execID)
					},
				}
				execs = append(execs, exec)
				return exec, nil
			},
		}
		if err := reg.RegisterHarness("test-agent", h); err != nil {
			t.Fatal(err)
		}

		c, err := New(ctx, Config{
			Registry:        reg,
			EventLogBuilder: func() (eventlog.EventLog, error) { return log, nil },
		})
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		err = c.Exec(ctx, &proto.CreateInteractionEvent{
			ConversationId: cid,
			AgentId:        "test-agent",
			Inputs:         []*proto.Step{harnesstest.UserStep("New input")},
		}, func(resp *proto.CreateInteractionResponse) error { return nil })
		if err != nil {
			t.Fatal(err)
		}

		// Only the pending turn runs -- "New input" is never queued as a
		// second, separate turn (that would run the pending turn's own
		// input, which is what a retry actually resends, twice under a
		// different guise).
		if h.startCalls != 1 {
			t.Errorf("expected 1 Start call, got %d", h.startCalls)
		}
		if len(execs) != 1 {
			t.Fatalf("expected 1 execution session, got %d", len(execs))
		}
		if execs[0].queueCalls != 1 {
			t.Errorf("expected 1 Queue call, got %d", execs[0].queueCalls)
		}
		if got := stepText(t, execs[0].queued); got != "Initial" {
			t.Errorf("expected queued step text %q, got %q", "Initial", got)
		}
		if execs[0].runCalls != 1 {
			t.Errorf("expected 1 Run call, got %d", execs[0].runCalls)
		}
	})

	// Subtest 4: no pending turn and no new Inputs -- there is nothing to
	// resume and nothing to start, so Exec must reject the call instead of
	// silently replaying the conversation's last response.
	t.Run("CompletedExecutionWithoutNewInputs", func(t *testing.T) {
		ctx := context.Background()
		cid := "completed-no-inputs"

		log := &eventlogtest.MemoryEventLog{}
		// Seed the event log with a completed turn.
		_, err := log.Append(ctx, &proto.StepEvent{
			ConversationId: cid,
			AgentId:        "test-agent",
			State:          proto.State_STATE_COMPLETED,
		})
		if err != nil {
			t.Fatal(err)
		}

		reg := NewRegistry()
		h := &testHarness{
			startFunc: func(ctx context.Context, conversationID string) (harness.Execution, error) {
				t.Fatal("Start must not be called when there is no pending turn and no new Inputs")
				return nil, nil
			},
		}
		if err := reg.RegisterHarness("test-agent", h); err != nil {
			t.Fatal(err)
		}

		c, err := New(ctx, Config{
			Registry:        reg,
			EventLogBuilder: func() (eventlog.EventLog, error) { return log, nil },
		})
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		err = c.Exec(ctx, &proto.CreateInteractionEvent{
			ConversationId: cid,
			AgentId:        "test-agent",
			Inputs:         nil,
		}, func(resp *proto.CreateInteractionResponse) error { return nil })
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
		if h.startCalls != 0 {
			t.Errorf("expected 0 Start calls, got %d", h.startCalls)
		}
	})
}

// stepText extracts the text of the first content step in steps, failing
// the test if steps has none.
func stepText(t *testing.T, steps []*proto.Step) string {
	t.Helper()
	if len(steps) == 0 {
		t.Fatal("expected at least one queued step, got none")
	}
	return steps[0].GetContent().GetContent()[0].GetText().GetText()
}

// A conversation started on one harness must resume on that harness when the
// harness id is empty, not fall back to the registry default.
func TestExec_ResumeEmptyHarnessUsesStored(t *testing.T) {
	ctx := context.Background()
	cid := "resume-empty"
	log := &eventlogtest.MemoryEventLog{}
	reg := NewRegistry()

	def := &testHarness{startFunc: func(context.Context, string) (harness.Execution, error) {
		return &testExecution{}, nil
	}}
	stored := &testHarness{startFunc: func(context.Context, string) (harness.Execution, error) {
		return &testExecution{}, nil
	}}
	if err := reg.RegisterHarness("harness-a", def); err != nil {
		t.Fatal(err)
	}
	if err := reg.RegisterHarness("harness-b", stored); err != nil {
		t.Fatal(err)
	}
	if err := reg.SetDefaultHarness("harness-a"); err != nil {
		t.Fatal(err)
	}

	c, err := New(ctx, Config{
		Registry:        reg,
		EventLogBuilder: func() (eventlog.EventLog, error) { return log, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	noop := ExecHandler(func(*proto.CreateInteractionResponse) error { return nil })

	// Turn 1: explicitly run the NON-default harness.
	if err := c.Exec(ctx, &proto.CreateInteractionEvent{
		ConversationId: cid,
		AgentId:        "harness-b",
		Inputs:         []*proto.Step{harnesstest.UserStep("hi")},
	}, noop); err != nil {
		t.Fatalf("turn 1: %v", err)
	}

	// Turn 2: resume WITHOUT a harness id. Must reuse harness-b, not the default.
	before := stored.startCalls
	if err := c.Exec(ctx, &proto.CreateInteractionEvent{
		ConversationId: cid,
		Inputs:         []*proto.Step{harnesstest.UserStep("more")},
	}, noop); err != nil {
		t.Fatalf("turn 2 (resume, empty harness): %v", err)
	}
	if stored.startCalls <= before {
		t.Errorf("stored harness not used on resume (startCalls stayed %d)", stored.startCalls)
	}
	if def.startCalls != 0 {
		t.Errorf("default harness used on resume (startCalls=%d), want the stored harness", def.startCalls)
	}
}

// Verifies that an explicit harness change during resume is rejected.
func TestExec_ResumeExplicitDifferentHarnessRejected(t *testing.T) {
	ctx := context.Background()
	cid := "resume-mismatch"
	log := &eventlogtest.MemoryEventLog{}
	reg := NewRegistry()
	if err := reg.RegisterHarness("harness-a", &testHarness{startFunc: func(context.Context, string) (harness.Execution, error) {
		return &testExecution{}, nil
	}}); err != nil {
		t.Fatal(err)
	}
	if err := reg.RegisterHarness("harness-b", &testHarness{startFunc: func(context.Context, string) (harness.Execution, error) {
		return &testExecution{}, nil
	}}); err != nil {
		t.Fatal(err)
	}

	c, err := New(ctx, Config{
		Registry:        reg,
		EventLogBuilder: func() (eventlog.EventLog, error) { return log, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	noop := ExecHandler(func(*proto.CreateInteractionResponse) error { return nil })

	if err := c.Exec(ctx, &proto.CreateInteractionEvent{
		ConversationId: cid,
		AgentId:        "harness-a",
		Inputs:         []*proto.Step{harnesstest.UserStep("hi")},
	}, noop); err != nil {
		t.Fatalf("turn 1: %v", err)
	}
	err = c.Exec(ctx, &proto.CreateInteractionEvent{
		ConversationId: cid,
		AgentId:        "harness-b",
		Inputs:         []*proto.Step{harnesstest.UserStep("more")},
	}, noop)
	if err == nil || !strings.Contains(err.Error(), "harness ID changed from harness-a to harness-b") {
		t.Fatalf("resume with a different harness: got %v, want error 'harness ID changed from harness-a to harness-b'", err)
	}
}

// Verifies that a new conversation started without a harness id records the
// default harness's canonical id, not "".
func TestExec_NewConversationLogsCanonicalDefault(t *testing.T) {
	ctx := context.Background()
	cid := "canonical-log"
	log := &eventlogtest.MemoryEventLog{}
	reg := NewRegistry()
	if err := reg.RegisterHarness("harness-a", &testHarness{startFunc: func(context.Context, string) (harness.Execution, error) {
		return &testExecution{}, nil
	}}); err != nil {
		t.Fatal(err)
	}
	if err := reg.SetDefaultHarness("harness-a"); err != nil {
		t.Fatal(err)
	}

	c, err := New(ctx, Config{
		Registry:        reg,
		EventLogBuilder: func() (eventlog.EventLog, error) { return log, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.Exec(ctx, &proto.CreateInteractionEvent{
		ConversationId: cid,
		Inputs:         []*proto.Step{harnesstest.UserStep("hi")},
	}, ExecHandler(func(*proto.CreateInteractionResponse) error { return nil })); err != nil {
		t.Fatalf("exec: %v", err)
	}
	_, stored, err := newLogger(log, cid, "", "").ResumptionState(ctx)
	if err != nil {
		t.Fatalf("ResumptionState: %v", err)
	}
	if stored != "harness-a" {
		t.Errorf("logged harness id = %q, want canonical %q (not empty)", stored, "harness-a")
	}
}

// TestExec_CheckpointHappensBeforeLogCommit confirms runTurn calls
// exec.Checkpoint after Run's response arrives but before that response is
// appended to the event log.
func TestExec_CheckpointHappensBeforeLogCommit(t *testing.T) {
	ctx := context.Background()
	cid := "checkpoint-order"

	log := &eventlogtest.MemoryEventLog{}
	reg := NewRegistry()

	var outputLoggedBeforeCheckpoint bool
	h := &testHarness{
		startFunc: func(ctx context.Context, conversationID string) (harness.Execution, error) {
			exec := &testExecution{id: "exec-order"}
			exec.runFunc = func(ctx context.Context, execID string, handler harness.Handler) error {
				if err := handler.OnMessage(ctx, execID, harnesstest.AssistantStep("world")); err != nil {
					return err
				}
				return handler.OnComplete(ctx, execID)
			}
			exec.checkpointFunc = func(ctx context.Context) error {
				// Only the PENDING input event (logged by LogInputs before
				// Run) should exist at this point; the response must not be
				// committed until after Checkpoint returns.
				events, err := log.Events(ctx, cid)
				if err != nil {
					t.Fatalf("Events: %v", err)
				}
				if len(events) != 1 {
					outputLoggedBeforeCheckpoint = true
				}
				return nil
			}
			return exec, nil
		},
	}
	if err := reg.RegisterHarness("test-agent", h); err != nil {
		t.Fatal(err)
	}

	c, err := New(ctx, Config{
		Registry:        reg,
		EventLogBuilder: func() (eventlog.EventLog, error) { return log, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.Exec(ctx, &proto.CreateInteractionEvent{
		ConversationId: cid,
		AgentId:        "test-agent",
		Inputs:         []*proto.Step{harnesstest.UserStep("hi")},
	}, ExecHandler(func(*proto.CreateInteractionResponse) error { return nil })); err != nil {
		t.Fatalf("exec: %v", err)
	}

	if outputLoggedBeforeCheckpoint {
		t.Error("the response was already in the event log when Checkpoint ran; checkpoint must happen before the log commit")
	}

	// Sanity check: the response really was committed once Checkpoint returned.
	events, err := log.Events(ctx, cid)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("expected 3 logged events (input, output, completion) after a successful checkpoint, got %d", len(events))
	}
	completion := events[2]
	if completion.State != proto.State_STATE_COMPLETED {
		t.Fatalf("expected the third event to be the completion marker, got state %v", completion.State)
	}
}

// TestExec_CheckpointFailureLeavesLogUncommitted confirms a failed
// Checkpoint leaves the response out of the event log, reports the error,
// and leaves the conversation PENDING so a retry is safe.
func TestExec_CheckpointFailureLeavesLogUncommitted(t *testing.T) {
	ctx := context.Background()
	cid := "checkpoint-failure"

	log := &eventlogtest.MemoryEventLog{}
	reg := NewRegistry()

	checkpointErr := fmt.Errorf("suspend actor: unavailable")
	var relayed []*proto.Step
	h := &testHarness{
		startFunc: func(ctx context.Context, conversationID string) (harness.Execution, error) {
			exec := &testExecution{id: "exec-fail"}
			exec.runFunc = func(ctx context.Context, execID string, handler harness.Handler) error {
				if err := handler.OnMessage(ctx, execID, harnesstest.AssistantStep("world")); err != nil {
					return err
				}
				return handler.OnComplete(ctx, execID)
			}
			exec.checkpointFunc = func(ctx context.Context) error {
				return checkpointErr
			}
			return exec, nil
		},
	}
	if err := reg.RegisterHarness("test-agent", h); err != nil {
		t.Fatal(err)
	}

	c, err := New(ctx, Config{
		Registry:        reg,
		EventLogBuilder: func() (eventlog.EventLog, error) { return log, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	err = c.Exec(ctx, &proto.CreateInteractionEvent{
		ConversationId: cid,
		AgentId:        "test-agent",
		Inputs:         []*proto.Step{harnesstest.UserStep("hi")},
	}, ExecHandler(func(resp *proto.CreateInteractionResponse) error {
		relayed = append(relayed, resp.Outputs...)
		return nil
	}))
	if err == nil {
		t.Fatal("expected an error when Checkpoint fails, got nil")
	}
	if !strings.Contains(err.Error(), "checkpoint") {
		t.Errorf("error = %q, want it to mention the checkpoint failure", err.Error())
	}

	// The response was still relayed live (for real-time display); that is
	// independent of the durability contract on the log.
	if len(relayed) != 1 {
		t.Fatalf("expected the response to still be relayed live, got %d steps", len(relayed))
	}

	// But the log must NOT record the response or completion: only the
	// PENDING marker LogInputs wrote before Run.
	events, err := log.Events(ctx, cid)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected only the input event to be logged after a checkpoint failure, got %d events", len(events))
	}
	for _, step := range events[0].Steps {
		if step.GetContent().GetRole() == "assistant" {
			t.Error("the unCheckpointed response must not appear in the log")
		}
	}

	// The conversation must still look PENDING/resumable, not completed --
	// so a future Start/Run retries this turn instead of silently treating
	// it as done.
	state, _, err := newLogger(log, cid, "", "").ResumptionState(ctx)
	if err != nil {
		t.Fatalf("ResumptionState: %v", err)
	}
	if state != proto.State_STATE_PENDING {
		t.Errorf("state = %v, want STATE_PENDING so the turn is retried on resume", state)
	}
}

// TestExec_RunWithoutOnCompleteFails confirms a buggy harness.Execution.Run
// that returns nil without ever calling handler.OnComplete.
func TestExec_RunWithoutOnCompleteFails(t *testing.T) {
	ctx := context.Background()
	cid := "run-without-oncomplete"

	log := &eventlogtest.MemoryEventLog{}
	reg := NewRegistry()

	h := &testHarness{
		startFunc: func(ctx context.Context, conversationID string) (harness.Execution, error) {
			exec := &testExecution{id: "exec-buggy"}
			exec.runFunc = func(ctx context.Context, execID string, handler harness.Handler) error {
				// Reports output but never calls OnComplete, then still
				// reports success -- exactly the contract violation this
				// test guards against.
				return handler.OnMessage(ctx, execID, harnesstest.AssistantStep("world"))
			}
			return exec, nil
		},
	}
	if err := reg.RegisterHarness("test-agent", h); err != nil {
		t.Fatal(err)
	}

	c, err := New(ctx, Config{
		Registry:        reg,
		EventLogBuilder: func() (eventlog.EventLog, error) { return log, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	err = c.Exec(ctx, &proto.CreateInteractionEvent{
		ConversationId: cid,
		AgentId:        "test-agent",
		Inputs:         []*proto.Step{harnesstest.UserStep("hi")},
	}, ExecHandler(func(*proto.CreateInteractionResponse) error { return nil }))
	if err == nil {
		t.Fatal("expected an error when Run returns without calling OnComplete, got nil")
	}
	if !strings.Contains(err.Error(), "OnComplete") {
		t.Errorf("error = %q, want it to mention OnComplete", err.Error())
	}
}

// TestExec_ResumeAfterPartialOutputCommitUsesOriginalInput confirms a crash
// between commit's own per-step LogOutputs calls and the turn's terminal
// COMPLETED marker resumes with the turn's own *original* input, not one
// of its own already-logged (but not yet fully committed) response steps
// -- both are logged as PENDING, and only LogInputs's own marker (the one
// that stamps AgentId) is the real input.
func TestExec_ResumeAfterPartialOutputCommitUsesOriginalInput(t *testing.T) {
	ctx := context.Background()
	cid := "partial-commit"

	log := &eventlogtest.MemoryEventLog{}
	// Simulate a turn that got as far as logging its own input and one of
	// its own buffered output steps (via commit's own per-step
	// LogOutputs), then crashed before the terminal COMPLETED marker.
	if _, err := log.Append(ctx, &proto.StepEvent{
		ConversationId: cid,
		AgentId:        "test-agent",
		Steps:          []*proto.Step{harnesstest.UserStep("original-input")},
		State:          proto.State_STATE_PENDING,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := log.Append(ctx, &proto.StepEvent{
		ConversationId: cid,
		Steps:          []*proto.Step{harnesstest.AssistantStep("partial-output")},
		State:          proto.State_STATE_PENDING,
	}); err != nil {
		t.Fatal(err)
	}

	reg := NewRegistry()
	var queuedTexts []string
	h := &testHarness{
		startFunc: func(ctx context.Context, conversationID string) (harness.Execution, error) {
			exec := &testExecution{id: "exec-resume"}
			exec.runFunc = func(ctx context.Context, execID string, handler harness.Handler) error {
				for _, s := range exec.queued {
					queuedTexts = append(queuedTexts, stepText(t, []*proto.Step{s}))
				}
				// A real idempotent harness redoes the exact same output
				// the crashed attempt already logged, then the rest.
				if err := handler.OnMessage(ctx, execID, harnesstest.AssistantStep("partial-output")); err != nil {
					return err
				}
				return handler.OnComplete(ctx, execID)
			}
			return exec, nil
		},
	}
	if err := reg.RegisterHarness("test-agent", h); err != nil {
		t.Fatal(err)
	}

	c, err := New(ctx, Config{
		Registry:        reg,
		EventLogBuilder: func() (eventlog.EventLog, error) { return log, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.Exec(ctx, &proto.CreateInteractionEvent{
		ConversationId: cid,
		AgentId:        "test-agent",
	}, ExecHandler(func(*proto.CreateInteractionResponse) error { return nil })); err != nil {
		t.Fatalf("Exec: %v", err)
	}

	if len(queuedTexts) != 1 || queuedTexts[0] != "original-input" {
		t.Fatalf("queued input = %v, want [%q] (the turn's own original input, not its already-logged output)", queuedTexts, "original-input")
	}
}

// TestExec_ResumeAfterPartialOutputCommitContinuesLogging confirms commit
// doesn't re-log output steps an earlier, interrupted attempt already
// durably wrote -- it only logs the remainder, then the terminal COMPLETED
// marker -- once the redone harness run reproduces that already-logged
// prefix exactly.
func TestExec_ResumeAfterPartialOutputCommitContinuesLogging(t *testing.T) {
	ctx := context.Background()
	cid := "partial-commit-continue"

	log := &eventlogtest.MemoryEventLog{}
	if _, err := log.Append(ctx, &proto.StepEvent{
		ConversationId: cid,
		AgentId:        "test-agent",
		Steps:          []*proto.Step{harnesstest.UserStep("hi")},
		State:          proto.State_STATE_PENDING,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := log.Append(ctx, &proto.StepEvent{
		ConversationId: cid,
		Steps:          []*proto.Step{harnesstest.AssistantStep("step-1")},
		State:          proto.State_STATE_PENDING,
	}); err != nil {
		t.Fatal(err)
	}

	reg := NewRegistry()
	h := &testHarness{
		startFunc: func(ctx context.Context, conversationID string) (harness.Execution, error) {
			exec := &testExecution{id: "exec-resume"}
			exec.runFunc = func(ctx context.Context, execID string, handler harness.Handler) error {
				if err := handler.OnMessage(ctx, execID, harnesstest.AssistantStep("step-1")); err != nil {
					return err
				}
				if err := handler.OnMessage(ctx, execID, harnesstest.AssistantStep("step-2")); err != nil {
					return err
				}
				return handler.OnComplete(ctx, execID)
			}
			return exec, nil
		},
	}
	if err := reg.RegisterHarness("test-agent", h); err != nil {
		t.Fatal(err)
	}

	c, err := New(ctx, Config{
		Registry:        reg,
		EventLogBuilder: func() (eventlog.EventLog, error) { return log, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.Exec(ctx, &proto.CreateInteractionEvent{
		ConversationId: cid,
		AgentId:        "test-agent",
	}, ExecHandler(func(*proto.CreateInteractionResponse) error { return nil })); err != nil {
		t.Fatalf("Exec: %v", err)
	}

	events, err := log.Events(ctx, cid)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	// input, step-1 (from before the crash), step-2 (newly logged),
	// completion -- step-1 must not be duplicated.
	if len(events) != 4 {
		t.Fatalf("expected 4 logged events, got %d: %+v", len(events), events)
	}
	if got := stepText(t, events[1].Steps); got != "step-1" {
		t.Errorf("events[1] text = %q, want %q", got, "step-1")
	}
	if got := stepText(t, events[2].Steps); got != "step-2" {
		t.Errorf("events[2] text = %q, want %q", got, "step-2")
	}
	if events[3].State != proto.State_STATE_COMPLETED {
		t.Errorf("events[3].State = %v, want STATE_COMPLETED", events[3].State)
	}
}

// TestExec_ResumeAfterPartialOutputCommitMismatchErrors confirms commit
// fails loudly if a redone harness run doesn't reproduce output an earlier,
// interrupted attempt already durably logged -- the harness/agent contract
// requires a resent turn to be handled idempotently, so a mismatch means a
// bug worth surfacing, not silently overwriting the log with the new,
// conflicting content.
func TestExec_ResumeAfterPartialOutputCommitMismatchErrors(t *testing.T) {
	ctx := context.Background()
	cid := "partial-commit-mismatch"

	log := &eventlogtest.MemoryEventLog{}
	if _, err := log.Append(ctx, &proto.StepEvent{
		ConversationId: cid,
		AgentId:        "test-agent",
		Steps:          []*proto.Step{harnesstest.UserStep("hi")},
		State:          proto.State_STATE_PENDING,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := log.Append(ctx, &proto.StepEvent{
		ConversationId: cid,
		Steps:          []*proto.Step{harnesstest.AssistantStep("original-output")},
		State:          proto.State_STATE_PENDING,
	}); err != nil {
		t.Fatal(err)
	}

	reg := NewRegistry()
	h := &testHarness{
		startFunc: func(ctx context.Context, conversationID string) (harness.Execution, error) {
			exec := &testExecution{id: "exec-nondeterministic"}
			exec.runFunc = func(ctx context.Context, execID string, handler harness.Handler) error {
				// A non-idempotent (buggy) harness: different output for
				// the exact same resent input.
				if err := handler.OnMessage(ctx, execID, harnesstest.AssistantStep("different-output")); err != nil {
					return err
				}
				return handler.OnComplete(ctx, execID)
			}
			return exec, nil
		},
	}
	if err := reg.RegisterHarness("test-agent", h); err != nil {
		t.Fatal(err)
	}

	c, err := New(ctx, Config{
		Registry:        reg,
		EventLogBuilder: func() (eventlog.EventLog, error) { return log, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	err = c.Exec(ctx, &proto.CreateInteractionEvent{
		ConversationId: cid,
		AgentId:        "test-agent",
	}, ExecHandler(func(*proto.CreateInteractionResponse) error { return nil }))
	if err == nil {
		t.Fatal("expected an error when the redone output doesn't match what was already logged, got nil")
	}
	if !strings.Contains(err.Error(), "idempotent") {
		t.Errorf("error = %q, want it to mention the idempotent-redo mismatch", err.Error())
	}

	// The log must be left exactly as it was -- no duplicate/conflicting
	// entries, and still resumable (PENDING).
	events, err := log.Events(ctx, cid)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected the log to be untouched (2 events), got %d: %+v", len(events), events)
	}
}

// TestExec_PendingWithNoLoggedInputErrors confirms Exec fails loudly if
// ResumptionState reports PENDING but pendingInputs finds no logged input
// to resume with -- an event log inconsistency, since a PENDING turn is
// always logged by LogInputs with at least one input step (Controller.Exec's
// own new-turn path already rejects empty Inputs before ever calling
// LogInputs) -- rather than silently starting the harness with zero input.
func TestExec_PendingWithNoLoggedInputErrors(t *testing.T) {
	ctx := context.Background()
	cid := "pending-no-input"

	log := &eventlogtest.MemoryEventLog{}
	// A PENDING event exists (so ResumptionState reports PENDING), but
	// it's not a genuine LogInputs marker (no AgentId), so pendingInputs
	// correctly finds nothing -- standing in for an inconsistent log.
	if _, err := log.Append(ctx, &proto.StepEvent{
		ConversationId: cid,
		Steps:          []*proto.Step{harnesstest.AssistantStep("orphaned-output")},
		State:          proto.State_STATE_PENDING,
	}); err != nil {
		t.Fatal(err)
	}

	reg := NewRegistry()
	var startCalls int
	h := &testHarness{
		startFunc: func(ctx context.Context, conversationID string) (harness.Execution, error) {
			startCalls++
			return &testExecution{id: "exec-unreachable"}, nil
		},
	}
	if err := reg.RegisterHarness("test-agent", h); err != nil {
		t.Fatal(err)
	}

	c, err := New(ctx, Config{
		Registry:        reg,
		EventLogBuilder: func() (eventlog.EventLog, error) { return log, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	err = c.Exec(ctx, &proto.CreateInteractionEvent{
		ConversationId: cid,
		AgentId:        "test-agent",
	}, ExecHandler(func(*proto.CreateInteractionResponse) error { return nil }))
	if err == nil {
		t.Fatal("expected an error when PENDING but no logged input was found, got nil")
	}
	if !strings.Contains(err.Error(), "PENDING") {
		t.Errorf("error = %q, want it to mention the PENDING/no-input inconsistency", err.Error())
	}
	if startCalls != 0 {
		t.Errorf("harness Start called %d times, want 0 (must fail before ever starting the harness)", startCalls)
	}
}
