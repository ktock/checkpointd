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
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/ktock/checkpointd/internal/controller"
	"github.com/ktock/checkpointd/internal/controller/eventlog"
	"github.com/ktock/checkpointd/internal/harness"
	"github.com/ktock/checkpointd/internal/hop"
	"github.com/ktock/checkpointd/proto"
)

// maxActorIDBytes is Kubernetes' resource name length limit.
const maxActorIDBytes = 63

// fakeHarness implements harness.Harness by calling respond on whatever was
// queued for the turn, parsed as an A2A message (nil if it doesn't parse).
// Each Start call gets a fresh fakeExecution, so any state that must survive
// across turns/attempts belongs on respond, not the execution. Every
// conversationID Start is called with is recorded, in order, so tests can
// assert on exactly which actor id a run used.
type fakeHarness struct {
	respond func(in *hop.Envelope) (*hop.Envelope, error)

	mu     sync.Mutex
	starts []string
}

func (h *fakeHarness) Start(ctx context.Context, conversationID string, config []byte) (harness.Execution, error) {
	h.mu.Lock()
	h.starts = append(h.starts, conversationID)
	h.mu.Unlock()
	return &fakeExecution{harness: h, id: "exec-" + conversationID}, nil
}

type fakeExecution struct {
	harness *fakeHarness
	id      string
	pending []*proto.Step
}

func (e *fakeExecution) ID() string { return e.id }

func (e *fakeExecution) Queue(ctx context.Context, steps ...*proto.Step) error {
	e.pending = append(e.pending, steps...)
	return nil
}

func (e *fakeExecution) Run(ctx context.Context, handler harness.Handler) error {
	in, _ := hop.Parse(latestText(e.pending))
	out, err := e.harness.respond(in)
	if err != nil {
		return err
	}
	// Mirrors harness/runtime.go's Connect, which centrally stamps every
	// real reply with the delivered turn's own StepID (see its own doc
	// comment) -- individual respond closures below build out from scratch
	// and never set this themselves, matching how a real
	// a2asrv.AgentExecutor never touches StepID either.
	if in != nil && out != nil {
		out.StepID = in.StepID
	}
	b, err := json.Marshal(out)
	if err != nil {
		return err
	}
	if err := handler.OnMessage(ctx, e.id, textStep(string(b))); err != nil {
		return err
	}
	return handler.OnComplete(ctx, e.id)
}

func (e *fakeExecution) Checkpoint(ctx context.Context) error { return nil }
func (e *fakeExecution) Close(ctx context.Context) error      { return nil }

// fakeDeleterHarness additionally implements actorDeleter and
// crashTagDeleter, recording every actorID it was asked to delete the actor
// (or crash-recovery tag) for, for cleanupActors tests.
type fakeDeleterHarness struct {
	*fakeHarness

	mu           sync.Mutex
	deleted      []string
	tagsDeleted  []string
	deleteTagErr error // returned from DeleteCrashRecoveryTag when non-nil, for every actorID
}

func (h *fakeDeleterHarness) DeleteActor(ctx context.Context, actorID string) error {
	h.mu.Lock()
	h.deleted = append(h.deleted, actorID)
	h.mu.Unlock()
	return nil
}

func (h *fakeDeleterHarness) DeleteCrashRecoveryTag(ctx context.Context, actorID string) error {
	h.mu.Lock()
	h.tagsDeleted = append(h.tagsDeleted, actorID)
	h.mu.Unlock()
	return h.deleteTagErr
}

// latestText returns the text of the *last* Content_Text step, not the
// first: execA2A appends a turn's new input after any forwarded history
// (see historySteps), so the first match would return stale history instead
// of the turn's actual new input on any turn after the first.
func latestText(steps []*proto.Step) string {
	var text string
	for _, s := range steps {
		c := s.GetContent()
		if c == nil {
			continue
		}
		for _, ct := range c.Content {
			if t := ct.GetText(); t != nil {
				text = t.Text
			}
		}
	}
	return text
}

func textStep(text string) *proto.Step {
	return &proto.Step{
		Type: &proto.Step_Content{
			Content: &proto.ContentStep{
				Role: "assistant",
				Content: []*proto.Content{{
					Type: &proto.Content_Text{Text: &proto.TextContent{Text: text}},
				}},
			},
		},
	}
}

func TestNewestText_TakesLastNotFirstMatch(t *testing.T) {
	steps := []*proto.Step{textStep("old"), textStep("new")}
	if got := newestText(steps); got != "new" {
		t.Errorf("newestText = %q, want %q", got, "new")
	}
	// stepText takes the opposite (first match): actor-log events can
	// bundle forwarded history ahead of the real new message, so
	// newestText can't reuse it.
	if got := stepText(steps); got != "old" {
		t.Errorf("stepText = %q, want %q", got, "old")
	}
}

// envNew returns a fresh Envelope carrying a plain-text Message as its Data.
// Its TaskID is deliberately left unset, marking it a new, independent task
// rather than a continuation -- use envReply to continue an existing one.
func envNew(role a2a.MessageRole, from, to, data string) *hop.Envelope {
	return &hop.Envelope{From: from, To: to, Data: hop.Data{Message: a2a.NewMessage(role, a2a.NewTextPart(data)), Type: hop.DataTypeMessage}}
}

// envReply returns a fresh Envelope replying to in, carrying in's TaskID
// forward so a chain of replies stays part of the same task. in == nil is
// treated the same as envNew (nothing to carry forward).
func envReply(in *hop.Envelope, role a2a.MessageRole, to, data string) *hop.Envelope {
	env := envNew(role, "", to, data)
	env.Reply = true
	if in == nil {
		return env
	}
	// Task-shaped in has no Message, so its real TaskID lives on the Task.
	switch {
	case in.Data.Task != nil:
		env.Data.Message.TaskID = in.Data.Task.ID
	case in.Data.Message != nil:
		env.Data.Message.TaskID = in.Data.Message.TaskID
	}
	return env
}

// inMessage returns in's turn content: the bare Message a hop carries, or
// its Task's Status.Message when in is Task-shaped instead. Message and
// Task are mutually exclusive, so this is a plain either/or read, never a
// merge of two fields that could disagree.
func inMessage(in *hop.Envelope) *a2a.Message {
	if in == nil {
		return nil
	}
	if in.Data.Task != nil {
		return in.Data.Task.Status.Message
	}
	return in.Data.Message
}

// textOf returns msg's first Part's text, or "" if msg has none.
func textOf(msg *a2a.Message) string {
	if msg == nil || len(msg.Parts) == 0 {
		return ""
	}
	return msg.Parts[0].Text()
}

// replyFrom is envReply plus the "from" stamp a real harness adds, which
// fakeHarness must supply itself since it implements harness.Harness
// directly, below the SDK layer that normally sets it.
func replyFrom(in *hop.Envelope, from, to, data string) *hop.Envelope {
	reply := envReply(in, a2a.MessageRoleAgent, to, data)
	reply.From = from
	return reply
}

// taskReply is replyFrom's Task-shaped counterpart, for an executor that
// yields a Task directly instead of a bare Message: state as
// Task.Status.State, data as Task.Status.Message's text. Task.ID reuses
// the TaskID replyFrom already propagated, minting a fresh one only for a
// genuinely new task's first reply.
func taskReply(in *hop.Envelope, from, to string, state a2a.TaskState, data string) *hop.Envelope {
	reply := replyFrom(in, from, to, data)
	id := reply.Data.Message.TaskID
	if id == "" {
		id = a2a.NewTaskID()
	}
	reply.Data.Task = &a2a.Task{ID: id, Status: a2a.TaskStatus{State: state, Message: reply.Data.Message}}
	reply.Data.Message = nil
	reply.Data.Type = hop.DataTypeTask
	return reply
}

// testRelay creates a brand-new store-backed session (newServerSession, the
// same construction serverhandler.go's startTask uses) and drives it to
// completion via runRelayLoop, returning the terminal reply's text and the
// resulting *bookkeeping for tests that need to inspect it afterward (e.g.
// bk.sessionID).
func testRelay(ctx context.Context, t *testing.T, c *controller.Controller, el eventlog.EventLog, store *sqlTaskStore, agent, bootstrapData string) (string, *bookkeeping, error) {
	t.Helper()
	bootstrap := envNew(a2a.MessageRoleUser, checkpointdIdentity, agent, bootstrapData)
	bk, err := newServerSession(ctx, store, el, agent, bootstrap)
	if err != nil {
		return "", nil, err
	}
	res, err := runRelayLoop(ctx, c, el, bk, agent, bootstrap, func(*hop.Envelope) {})
	if err != nil {
		return "", bk, err
	}
	return textOf(inMessage(res)), bk, nil
}

// flakyHarness fails its first `failures` turns (regardless of which
// conversation), then delegates to respond -- for exercising retryExec's
// retry-on-transient-failure path. Failures are counted on the harness, not
// the execution, since (matching a real harness) Start hands back a fresh
// execution on every Controller.Exec call/retry.
type flakyHarness struct {
	mu       sync.Mutex
	failures int
	respond  func(in *hop.Envelope) (*hop.Envelope, error)
}

func (h *flakyHarness) Start(ctx context.Context, conversationID string, config []byte) (harness.Execution, error) {
	return &flakyExecution{harness: h, id: "exec-" + conversationID}, nil
}

type flakyExecution struct {
	harness *flakyHarness
	id      string
	pending []*proto.Step
}

func (e *flakyExecution) ID() string { return e.id }

func (e *flakyExecution) Queue(ctx context.Context, steps ...*proto.Step) error {
	e.pending = append(e.pending, steps...)
	return nil
}

func (e *flakyExecution) Run(ctx context.Context, handler harness.Handler) error {
	e.harness.mu.Lock()
	if e.harness.failures > 0 {
		e.harness.failures--
		e.harness.mu.Unlock()
		return errors.New("simulated transient failure")
	}
	e.harness.mu.Unlock()

	in, _ := hop.Parse(latestText(e.pending))
	out, err := e.harness.respond(in)
	if err != nil {
		return err
	}
	// Mirrors harness/runtime.go's Connect -- see fakeExecution.Run's
	// identical stamp for why.
	if in != nil && out != nil {
		out.StepID = in.StepID
	}
	b, err := json.Marshal(out)
	if err != nil {
		return err
	}
	if err := handler.OnMessage(ctx, e.id, textStep(string(b))); err != nil {
		return err
	}
	return handler.OnComplete(ctx, e.id)
}

func (e *flakyExecution) Checkpoint(ctx context.Context) error { return nil }
func (e *flakyExecution) Close(ctx context.Context) error      { return nil }

func TestRelay_FreshBootstrapAndRelay(t *testing.T) {
	a := &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		return envNew(a2a.MessageRoleUser, "a", "b", "from-a:"+textOf(inMessage(in))), nil
	}}
	b := &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		return envNew(a2a.MessageRoleUser, "b", "", "from-b:"+textOf(inMessage(in))), nil
	}}
	c, el, store := newTestServerController(t, map[string]harness.Harness{"a": a, "b": b})
	defer c.Close()

	got, _, err := testRelay(context.Background(), t, c, el, store, "a", "seed")
	if err != nil {
		t.Fatalf("testRelay: %v", err)
	}
	if want := "from-b:from-a:seed"; got != want {
		t.Errorf("relay result = %q, want %q", got, want)
	}
}

// TestRelay_StateSurvivesRelayHop confirms driveRelayLoop forwards a hop's
// full Data (Message/State/Artifacts) to the next agent, not just its text --
// a receiving agent that depends on reading a relayed State would retry
// forever if it silently went missing. "a" stamps InputRequired on its
// reply to "b"; "b" echoes back whatever state it actually observed.
func TestRelay_StateSurvivesRelayHop(t *testing.T) {
	a := &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		env := envNew(a2a.MessageRoleUser, "a", "b", "")
		env.Data.Message = nil
		env.Data.Task = &a2a.Task{
			ID: a2a.NewTaskID(),
			Status: a2a.TaskStatus{
				State:   a2a.TaskStateInputRequired,
				Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart(textOf(inMessage(in)))),
			},
		}
		env.Data.Type = hop.DataTypeTask
		return env, nil
	}}
	b := &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		// in.Data.Task.Status.State directly, not a generic "effective
		// state" helper: in.Data.Task == nil unambiguously means no Task
		// claim was ever made, i.e. TaskStateUnspecified.
		var state a2a.TaskState
		if in.Data.Task != nil {
			state = in.Data.Task.Status.State
		}
		env := envNew(a2a.MessageRoleUser, "b", "", "")
		env.Data.Message = nil
		env.Data.Task = &a2a.Task{
			ID: a2a.NewTaskID(),
			Status: a2a.TaskStatus{
				State:   a2a.TaskStateCompleted,
				Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("state-seen:"+string(state))),
			},
		}
		env.Data.Type = hop.DataTypeTask
		return env, nil
	}}
	c, el, store := newTestServerController(t, map[string]harness.Harness{"a": a, "b": b})
	defer c.Close()

	got, _, err := testRelay(context.Background(), t, c, el, store, "a", "seed")
	if err != nil {
		t.Fatalf("testRelay: %v", err)
	}
	if want := "state-seen:TASK_STATE_INPUT_REQUIRED"; got != want {
		t.Errorf("relay result = %q, want %q -- State didn't survive the relay hop from a to b", got, want)
	}
}

// TestRelay_DataPartSurvivesRelayHop confirms driveRelayLoop forwards the
// sender's Message object verbatim, Parts included, rather than collapsing
// it to text -- a non-text DataPart reply (e.g.
// script/test-k8s/agents/long_wait's {"status":"pending"} convention)
// must survive the hop unchanged.
func TestRelay_DataPartSurvivesRelayHop(t *testing.T) {
	a := &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		reply := envNew(a2a.MessageRoleAgent, "a", "b", "")
		reply.Data.Message.Parts = a2a.ContentParts{a2a.NewDataPart(map[string]any{"status": "pending", "id": textOf(inMessage(in))})}
		return reply, nil
	}}
	b := &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		// Reports a mismatch as ordinary (wrong) reply text rather than an
		// error: an error here would make retryExec retry indefinitely (by
		// design -- see its own doc comment), hanging this test instead of
		// failing it if the bug this guards against ever recurs.
		data, ok := inMessage(in).Parts[0].Data().(map[string]any)
		if !ok {
			return envNew(a2a.MessageRoleUser, "b", "", fmt.Sprintf("unexpected part content: %v", inMessage(in).Parts[0])), nil
		}
		return envNew(a2a.MessageRoleUser, "b", "", fmt.Sprintf("status=%v id=%v", data["status"], data["id"])), nil
	}}
	c, el, store := newTestServerController(t, map[string]harness.Harness{"a": a, "b": b})
	defer c.Close()

	got, _, err := testRelay(context.Background(), t, c, el, store, "a", "seed")
	if err != nil {
		t.Fatalf("testRelay: %v", err)
	}
	if want := "status=pending id=seed"; got != want {
		t.Errorf("relay result = %q, want %q -- the DataPart didn't survive the relay hop from a to b", got, want)
	}
}

func TestRelay_SelfLoop(t *testing.T) {
	var calls int
	a := &fakeDeleterHarness{fakeHarness: &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		calls++
		// envReply, not envNew: it propagates the input's TaskID forward,
		// keeping every self-loop hop keyed to the same actor.
		// MessageRoleAgent matches what a real harness reply uses --
		// visitedActorsFromHistory only counts those as hops to clean up.
		if calls < 3 {
			reply := envReply(in, a2a.MessageRoleAgent, "a", textOf(inMessage(in)))
			reply.From = "a"
			return reply, nil
		}
		reply := envReply(in, a2a.MessageRoleAgent, "", "final:"+textOf(inMessage(in)))
		reply.From = "a"
		return reply, nil
	}}}
	c, el, store := newTestServerController(t, map[string]harness.Harness{"a": a})
	defer c.Close()

	got, _, err := testRelay(context.Background(), t, c, el, store, "a", "seed")
	if err != nil {
		t.Fatalf("testRelay: %v", err)
	}
	if want := "final:seed"; got != want {
		t.Errorf("relay result = %q, want %q", got, want)
	}
	if calls != 3 {
		t.Errorf("harness called %d times, want 3", calls)
	}
	// "a" was visited 3 times (a self-loop), but is still exactly one
	// distinct actor for this run -- cleanup must delete it exactly once.
	if len(a.deleted) != 1 {
		t.Errorf("DeleteActor called %d times for a self-looped agent, want 1", len(a.deleted))
	}
}

// TestRelay_EventsBySessionID_RevisitedAgentNotDuplicated confirms
// EventsBySessionID returns each event exactly once, correctly interleaved
// via session_step, even when an agent (here, "a") is revisited within the
// same session. For this a->b->a scenario -- 3 hops, 2 events each (an
// input + a reply) -- that's exactly 6 events, not a double-counted 10.
// (Two adjacent entries can legitimately repeat the same text, since a
// relay forwards its predecessor's reply verbatim as the next agent's
// input -- asserting on count and order here, not unique text.)
func TestRelay_EventsBySessionID_RevisitedAgentNotDuplicated(t *testing.T) {
	var callsA int
	a := &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		callsA++
		if callsA == 1 {
			return replyFrom(in, "a", "b", "from-a1:"+textOf(inMessage(in))), nil
		}
		return replyFrom(in, "a", "", "from-a2:"+textOf(inMessage(in))), nil
	}}
	b := &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		return replyFrom(in, "b", "a", "from-b:"+textOf(inMessage(in))), nil
	}}
	c, el, store := newTestServerController(t, map[string]harness.Harness{"a": a, "b": b})
	defer c.Close()

	ctx := context.Background()
	_, bk, err := testRelay(ctx, t, c, el, store, "a", "seed")
	if err != nil {
		t.Fatalf("testRelay: %v", err)
	}

	events, err := el.EventsBySessionID(ctx, bk.sessionID)
	if err != nil {
		t.Fatalf("EventsBySessionID: %v", err)
	}

	// One message per event via newestText -- the same convention
	// taskstore.go's GetTask uses, since a turn's own PENDING input event
	// legitimately bundles forwarded prior-turn text (see historySteps)
	// ahead of that turn's actual new message; taking the last is what
	// isolates it, not a workaround for this test.
	var texts []string
	for _, ev := range events {
		msg, ok := hop.Parse(newestText(ev.Steps))
		if !ok {
			continue
		}
		texts = append(texts, textOf(inMessage(msg)))
	}
	// 2 events per hop (a PENDING input + a COMPLETED reply) x 3 hops = 6,
	// not 10 -- the exact signature of the bug this guards against.
	want := []string{
		"seed", "from-a1:seed", // hop 1: a's input, a's reply
		"from-a1:seed", "from-b:from-a1:seed", // hop 2: b's input (a's reply, forwarded), b's reply
		"from-b:from-a1:seed", "from-a2:from-b:from-a1:seed", // hop 3: a's input (b's reply, forwarded), a's reply
	}
	if len(texts) != len(want) {
		t.Fatalf("EventsBySessionID returned %d events %v, want %d %v (agent a's conversation must not be double-counted for being visited twice)", len(texts), texts, len(want), want)
	}
	for i := range want {
		if texts[i] != want[i] {
			t.Errorf("EventsBySessionID texts = %v, want %v", texts, want)
			break
		}
	}
}

func TestRetryExec_RecoversFromTransientFailure(t *testing.T) {
	h := &flakyHarness{
		failures: 1,
		respond: func(in *hop.Envelope) (*hop.Envelope, error) {
			return envNew(a2a.MessageRoleUser, "a", "", "ok:"+textOf(inMessage(in))), nil
		},
	}
	c, el, store := newTestServerController(t, map[string]harness.Harness{"a": h})
	defer c.Close()

	got, _, err := testRelay(context.Background(), t, c, el, store, "a", "seed")
	if err != nil {
		t.Fatalf("testRelay: %v", err)
	}
	if want := "ok:seed"; got != want {
		t.Errorf("relay result = %q, want %q", got, want)
	}
}

func TestActorConv_DeterministicAndIsolatedPerInstance(t *testing.T) {
	if actorConv("inst1", "agent") != actorConv("inst1", "agent") {
		t.Error("actorConv is not deterministic for the same (session, agent)")
	}
	if actorConv("inst1", "a") == actorConv("inst1", "b") {
		t.Error("actorConv collided for two different agents under the same session")
	}
	if actorConv("inst1", "agent") == actorConv("inst2", "agent") {
		t.Error("actorConv collided for the same agent under two different sessions")
	}
}

// TestActorConv_BoundedRegardlessOfInputLength confirms actorConv's result
// never exceeds Kubernetes' 63-byte resource name limit.
func TestActorConv_BoundedRegardlessOfInputLength(t *testing.T) {
	actorID := actorConv(strings.Repeat("s", 200), strings.Repeat("a", 200))
	if len(actorID) > maxActorIDBytes {
		t.Errorf("actorConv(long session, long agent) = %q is %d bytes, want <= %d", actorID, len(actorID), maxActorIDBytes)
	}
}

// TestActorConv_PrefixIsReadableAgentName confirms actorConv keeps agent's
// own first actorIDPrefixBytes bytes verbatim at the front of its result.
func TestActorConv_PrefixIsReadableAgentName(t *testing.T) {
	if got, want := actorConv("sess", "short"), "short-"; !strings.HasPrefix(got, want) {
		t.Errorf("actorConv(short agent) = %q, want prefix %q", got, want)
	}
	long := "a-very-long-agent-name-that-exceeds-ten-bytes"
	if got, want := actorConv("sess", long), long[:actorIDPrefixBytes]+"-"; !strings.HasPrefix(got, want) {
		t.Errorf("actorConv(long agent) = %q, want prefix %q", got, want)
	}
}

// TestRelay_TwoIndependentRunsDoNotShareAnActor is the regression test for
// the bug this design fixes: two independent checkpointd tasks (two separate
// event logs/stores, e.g. two separate checkpointd-server deployments) that
// both need to talk to "the same" target agent id must never resolve to the
// same underlying Substrate actor.
func TestRelay_TwoIndependentRunsDoNotShareAnActor(t *testing.T) {
	a := &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		return envNew(a2a.MessageRoleUser, "a", "", "done:"+textOf(inMessage(in))), nil
	}}

	c1, el1, store1 := newTestServerController(t, map[string]harness.Harness{"a": a})
	defer c1.Close()
	if _, _, err := testRelay(context.Background(), t, c1, el1, store1, "a", "seed1"); err != nil {
		t.Fatalf("testRelay (run 1): %v", err)
	}

	c2, el2, store2 := newTestServerController(t, map[string]harness.Harness{"a": a})
	defer c2.Close()
	if _, _, err := testRelay(context.Background(), t, c2, el2, store2, "a", "seed2"); err != nil {
		t.Fatalf("testRelay (run 2): %v", err)
	}

	if len(a.starts) != 2 {
		t.Fatalf("harness Start called %d times, want 2", len(a.starts))
	}
	if a.starts[0] == a.starts[1] {
		t.Errorf("two independent checkpointd runs both started the same actor %q for agent %q -- this is exactly the collision the fix prevents", a.starts[0], "a")
	}
}

func TestRelay_CleansUpVisitedActorsOnCompletion(t *testing.T) {
	// MessageRoleAgent, not MessageRoleUser: see TestRelay_SelfLoop for why.
	a := &fakeDeleterHarness{fakeHarness: &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		return envNew(a2a.MessageRoleAgent, "a", "b", "from-a:"+textOf(inMessage(in))), nil
	}}}
	b := &fakeDeleterHarness{fakeHarness: &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		return envNew(a2a.MessageRoleAgent, "b", "", "from-b:"+textOf(inMessage(in))), nil
	}}}
	c, el, store := newTestServerController(t, map[string]harness.Harness{"a": a, "b": b})
	defer c.Close()

	if _, _, err := testRelay(context.Background(), t, c, el, store, "a", "seed"); err != nil {
		t.Fatalf("testRelay: %v", err)
	}

	if len(a.deleted) != 1 {
		t.Errorf("agent a: DeleteActor called %d times, want 1", len(a.deleted))
	}
	if len(b.deleted) != 1 {
		t.Errorf("agent b: DeleteActor called %d times, want 1", len(b.deleted))
	}
	if len(a.deleted) == 1 && len(a.starts) == 1 && a.deleted[0] != a.starts[0] {
		t.Errorf("agent a: deleted actor %q, want the same id it was started with (%q)", a.deleted[0], a.starts[0])
	}

	// cleanupActors also makes a best-effort attempt to delete each visited
	// actor's crash-recovery snapshot tag, unconditionally -- a missing tag
	// is already treated as success.
	if len(a.tagsDeleted) != 1 {
		t.Errorf("agent a: DeleteCrashRecoveryTag called %d times, want 1", len(a.tagsDeleted))
	}
	if len(b.tagsDeleted) != 1 {
		t.Errorf("agent b: DeleteCrashRecoveryTag called %d times, want 1", len(b.tagsDeleted))
	}
	if len(a.tagsDeleted) == 1 && len(a.deleted) == 1 && a.tagsDeleted[0] != a.deleted[0] {
		t.Errorf("agent a: deleted crash-recovery tag for actor %q, want the same actor id DeleteActor was called with (%q)", a.tagsDeleted[0], a.deleted[0])
	}
}

// TestRelay_CleanupContinuesAfterCrashTagDeleteFails confirms
// cleanupActors treats a failed DeleteCrashRecoveryTag the same
// best-effort way it already treats a failed DeleteActor: logged and
// skipped, never aborting cleanup for the other actors a task visited.
func TestRelay_CleanupContinuesAfterCrashTagDeleteFails(t *testing.T) {
	a := &fakeDeleterHarness{deleteTagErr: errors.New("boom"), fakeHarness: &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		return envNew(a2a.MessageRoleAgent, "a", "b", "from-a:"+textOf(inMessage(in))), nil
	}}}
	b := &fakeDeleterHarness{fakeHarness: &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		return envNew(a2a.MessageRoleAgent, "b", "", "from-b:"+textOf(inMessage(in))), nil
	}}}
	c, el, store := newTestServerController(t, map[string]harness.Harness{"a": a, "b": b})
	defer c.Close()

	if _, _, err := testRelay(context.Background(), t, c, el, store, "a", "seed"); err != nil {
		t.Fatalf("testRelay: %v", err)
	}

	// a's own actor delete and b's whole cleanup (actor + tag) still ran,
	// despite a's tag delete failing.
	if len(a.deleted) != 1 {
		t.Errorf("agent a: DeleteActor called %d times, want 1 (a failed tag delete must not skip the actor delete)", len(a.deleted))
	}
	if len(b.deleted) != 1 {
		t.Errorf("agent b: DeleteActor called %d times, want 1 (agent a's failure must not abort cleanup for agent b)", len(b.deleted))
	}
	if len(b.tagsDeleted) != 1 {
		t.Errorf("agent b: DeleteCrashRecoveryTag called %d times, want 1", len(b.tagsDeleted))
	}
}

// TestRetryBackoff_NotFoundUsesSlowCurve confirms an unresolvable-agent
// error (wrapped the same way Controller.Exec actually wraps it:
// controller.ErrHarnessNotFound through two levels of fmt.Errorf's %w) gets
// the Kubernetes-style slow curve: 10s, 20s, 40s, ..., capped at 5m -- and
// stays capped, never growing past it. Pure, no real sleeping.
func TestRetryBackoff_NotFoundUsesSlowCurve(t *testing.T) {
	err := fmt.Errorf("failed to get harness %q: %w", "missing", fmt.Errorf("agent %q not found: %w", "missing", controller.ErrHarnessNotFound))
	want := []time.Duration{
		10 * time.Second,
		20 * time.Second,
		40 * time.Second,
		80 * time.Second,
		160 * time.Second,
		5 * time.Minute, // 320s would exceed the cap
		5 * time.Minute, // stays capped
	}
	var b retryBackoff
	for i, w := range want {
		if got := b.next(err); got != w {
			t.Errorf("attempt %d: delay = %s, want %s", i+1, got, w)
		}
	}
}

// TestRetryBackoff_ConnFailUsesFastCurve confirms any other error gets the
// standard fast curve: 1s, 2s, 4s, ..., capped at 30s. Pure, no real
// sleeping.
func TestRetryBackoff_ConnFailUsesFastCurve(t *testing.T) {
	err := errors.New("connection refused")
	want := []time.Duration{
		1 * time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
		30 * time.Second, // 32s would exceed the cap
		30 * time.Second, // stays capped
	}
	var b retryBackoff
	for i, w := range want {
		if got := b.next(err); got != w {
			t.Errorf("attempt %d: delay = %s, want %s", i+1, got, w)
		}
	}
}

// TestRetryBackoff_ClassesAreIndependent confirms the two curves track
// separately: a run of connFail failures doesn't advance the notFound
// curve, and switching to notFound doesn't inherit or reset connFail's own
// progress -- covering the case where a hop legitimately alternates between
// the two (e.g. under --discover, the target agent briefly disappears from
// the ActorTemplate list, then reappears but its actor is slow to start).
func TestRetryBackoff_ClassesAreIndependent(t *testing.T) {
	notFoundErr := fmt.Errorf("not found: %w", controller.ErrHarnessNotFound)
	connFailErr := errors.New("connection refused")

	var b retryBackoff
	if got, want := b.next(connFailErr), 1*time.Second; got != want {
		t.Fatalf("connFail attempt 1: delay = %s, want %s", got, want)
	}
	if got, want := b.next(connFailErr), 2*time.Second; got != want {
		t.Fatalf("connFail attempt 2: delay = %s, want %s", got, want)
	}
	// Switching to notFound starts at its own initial delay, unaffected by
	// connFail's progress so far.
	if got, want := b.next(notFoundErr), 10*time.Second; got != want {
		t.Fatalf("notFound attempt 1 (after 2 connFail attempts): delay = %s, want %s", got, want)
	}
	// Switching back to connFail resumes exactly where it left off (3rd
	// attempt: 4s), not reset by the intervening notFound call.
	if got, want := b.next(connFailErr), 4*time.Second; got != want {
		t.Fatalf("connFail attempt 3 (after 1 notFound attempt): delay = %s, want %s", got, want)
	}
}

// TestEstablishRelayState_ResumesTerminalReplyLoggedButNotCompleted confirms
// establishRelayState checks lastStep.State, not just isTerminalReply(last):
// a single-hop conversation whose agent already produced its own terminal
// reply, but where checkpointd crashed between commit's own per-step
// LogOutputs call and the turn's terminal COMPLETED marker, must retry that
// agent's own turn (using lastStep's own empty AgentId to know it's
// last.From's turn, not last.To's -- there is no last.To for a terminal
// reply) rather than wrongly treating the not-yet-durable reply as done.
func TestEstablishRelayState_ResumesTerminalReplyLoggedButNotCompleted(t *testing.T) {
	// canonicalReply is what "a"'s harness deterministically returns for
	// bootstrap's own StepID -- standing in for a real harness's own
	// exactly-once redelivery guarantee (once Checkpoint has durably
	// persisted a turn, redelivering its own StepID always returns the
	// exact same cached output, even across a process restart -- see
	// harness/runtime.go's own lastInputID/lastOutput). fakeHarness has no
	// such cache itself, and a2a.NewMessage mints a fresh random ID on
	// every call, so this test builds the reply's Message once, with a
	// fixed ID, and returns a copy of it deterministically instead, to
	// correctly simulate that guarantee holding.
	canonicalReply := &a2a.Message{ID: "reply-1", Role: a2a.MessageRoleAgent, Parts: a2a.ContentParts{a2a.NewTextPart("done")}}
	a := &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		msg := *canonicalReply
		return &hop.Envelope{From: "a", To: "", StepID: in.StepID, Data: hop.Data{Message: &msg, Type: hop.DataTypeMessage}}, nil
	}}
	c, el, store := newTestServerController(t, map[string]harness.Harness{"a": a})
	defer c.Close()
	ctx := context.Background()

	bk := &bookkeeping{el: el, store: store, sessionID: "sess-1", ownerAgent: "a"}
	actorID := actorConv(bk.sessionID, "a")

	bootstrap := envNew(a2a.MessageRoleUser, checkpointdIdentity, "a", "seed")
	bootstrap.StepID = "step-1"
	bIn, err := json.Marshal(bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	// "a"'s own actor received bootstrap as input (LogInputs, AgentId set).
	if _, err := el.Append(ctx, &proto.StepEvent{
		ConversationId: actorID,
		SessionId:      bk.sessionID,
		AgentId:        "a",
		Steps:          []*proto.Step{userStep(string(bIn))},
		State:          proto.State_STATE_PENDING,
	}); err != nil {
		t.Fatal(err)
	}

	replyMsg := *canonicalReply
	reply := &hop.Envelope{From: "a", To: "", StepID: bootstrap.StepID, Data: hop.Data{Message: &replyMsg, Type: hop.DataTypeMessage}}
	bOut, err := json.Marshal(reply)
	if err != nil {
		t.Fatal(err)
	}
	// "a"'s own reply got as far as one of commit's own per-step outputs
	// (PENDING, no AgentId) -- but the crash happened before the terminal
	// COMPLETED marker.
	if _, err := el.Append(ctx, &proto.StepEvent{
		ConversationId: actorID,
		SessionId:      bk.sessionID,
		Steps:          []*proto.Step{textStep(string(bOut))},
		State:          proto.State_STATE_PENDING,
	}); err != nil {
		t.Fatal(err)
	}

	res, err := establishRelayState(ctx, c, el, bk, "a", bootstrap)
	if err != nil {
		t.Fatalf("establishRelayState: %v", err)
	}
	if got, want := textOf(inMessage(res)), "done"; got != want {
		t.Errorf("res text = %q, want %q", got, want)
	}
	if !isTerminalReply(res) {
		t.Errorf("res.To = %q, want terminal (empty)", res.To)
	}

	// The turn must now be durably COMPLETED in the event log -- not left
	// exactly as it was, which would mean establishRelayState wrongly
	// treated the not-yet-durable reply as already done.
	events, err := el.Events(ctx, actorID)
	if err != nil {
		t.Fatal(err)
	}
	last := events[len(events)-1]
	if last.State != proto.State_STATE_COMPLETED {
		t.Errorf("last event state = %v, want STATE_COMPLETED", last.State)
	}

	// bk.lastHop -- what continueRelayLoop's own "is this session paused
	// for external continuation" check (and anything else scoped by
	// EventsBySessionID) actually reads -- must see this same turn's real
	// reply and completion, not fall back to an earlier, stale
	// session-tagged event because establishRelayState's own retryExec
	// call forgot to set SessionId on the events its retried turn logs.
	lastHopEnv, lastHopStep, err := bk.lastHop(ctx)
	if err != nil {
		t.Fatalf("lastHop: %v", err)
	}
	if lastHopEnv == nil {
		t.Fatal("lastHop returned nil -- the retried turn's own events are invisible to this session-scoped lookup")
	}
	if !isTerminalReply(lastHopEnv) {
		t.Errorf("lastHop's own last.To = %q, want terminal (empty)", lastHopEnv.To)
	}
	if lastHopStep.State != proto.State_STATE_COMPLETED {
		t.Errorf("lastHop's own lastStep.State = %v, want STATE_COMPLETED", lastHopStep.State)
	}
}
