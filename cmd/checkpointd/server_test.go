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
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	config "github.com/ktock/checkpointd/internal/config/checkpointd"
	"github.com/ktock/checkpointd/internal/controller"
	"github.com/ktock/checkpointd/internal/controller/eventlog"
	"github.com/ktock/checkpointd/internal/harness"
	"github.com/ktock/checkpointd/internal/hop"
	"github.com/ktock/checkpointd/proto"
)

// newTestServerController builds a Controller, EventLog, and sqlTaskStore
// backed by a real temp-file SQLite database, since sqlTaskStore needs a
// real *sql.DB to share. Callers own closing c, which closes el in turn.
func newTestServerController(t *testing.T, harnesses map[string]harness.Harness) (*controller.Controller, eventlog.EventLog, *sqlTaskStore) {
	t.Helper()
	el, err := eventlog.OpenSQLiteEventLog(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("OpenSQLiteEventLog: %v", err)
	}
	db, dialect, ok := eventlog.SQLDB(el)
	if !ok {
		t.Fatalf("eventlog.SQLDB: not a SQL-backed EventLog")
	}
	store, err := newSQLTaskStore(db, dialect, el)
	if err != nil {
		t.Fatalf("newSQLTaskStore: %v", err)
	}
	reg := controller.NewRegistry()
	for id, h := range harnesses {
		if err := reg.RegisterHarness(id, h); err != nil {
			t.Fatalf("RegisterHarness(%q): %v", id, err)
		}
	}
	c, err := controller.New(context.Background(), controller.Config{
		Registry:        reg,
		EventLogBuilder: func() (eventlog.EventLog, error) { return el, nil },
	})
	if err != nil {
		t.Fatalf("controller.New: %v", err)
	}
	return c, el, store
}

// TestNewServerSession_ShortSessionID confirms bk.sessionID stays short
// enough for a real session id.
func TestNewServerSession_ShortSessionID(t *testing.T) {
	_, el, store := newTestServerController(t, nil)
	ctx := context.Background()
	msg := envNew(a2a.MessageRoleUser, "checkpointd", "a", "seed")
	msg.Data.Message.ContextID = "ctx-1"

	bk, err := newServerSession(ctx, store, el, "a", msg)
	if err != nil {
		t.Fatalf("newServerSession: %v", err)
	}
	if bk.taskID != "" {
		t.Errorf("bk.taskID = %q, want \"\" (no Task-shaped claim has been made yet)", bk.taskID)
	}
	longestKnownAgent := "crash-test-selfpanic"
	actorID := actorConv(bk.sessionID, longestKnownAgent)
	if len(actorID) > maxActorIDBytes {
		t.Errorf("actor id %q for agent %q is %d bytes, over the %d-byte limit", actorID, longestKnownAgent, len(actorID), maxActorIDBytes)
	}
}

func TestLoadServerSession_NotFound(t *testing.T) {
	_, el, store := newTestServerController(t, nil)
	_, _, ok, err := loadServerSession(context.Background(), store, el, "never-created")
	if err != nil {
		t.Fatalf("loadServerSession: %v", err)
	}
	if ok {
		t.Error("loadServerSession for an id never created: ok = true, want false")
	}
}

// TestLoadServerSession_RecoversBootstrapBeforeFirstHop confirms a session
// crashed before its first hop was ever recorded still has its bootstrap
// message recoverable, since CreateSession writes both in one INSERT.
func TestLoadServerSession_RecoversBootstrapBeforeFirstHop(t *testing.T) {
	_, el, store := newTestServerController(t, nil)
	ctx := context.Background()
	msg := envNew(a2a.MessageRoleUser, "checkpointd", "a", "seed-data")
	msg.Data.Message.ContextID = "ctx-1"

	created, err := newServerSession(ctx, store, el, "a", msg)
	if err != nil {
		t.Fatalf("newServerSession: %v", err)
	}

	bk, bootstrap, ok, err := loadServerSession(ctx, store, el, created.sessionID)
	if err != nil {
		t.Fatalf("loadServerSession: %v", err)
	}
	if !ok {
		t.Fatal("loadServerSession: ok = false, want true")
	}
	last, _, err := bk.lastHop(ctx)
	if err != nil {
		t.Fatalf("bk.lastHop: %v", err)
	}
	if last != nil {
		t.Errorf("lastHop = %+v, want nil (nothing logged yet)", last)
	}
	if bootstrap == nil {
		t.Fatal("bootstrap = nil, want the original message recovered")
	}
	if got, want := textOf(bootstrap.Data.Message), "seed-data"; got != want {
		t.Errorf("recovered bootstrap text = %q, want %q", got, want)
	}
}

// TestRunRelayLoop_RecoversHopRecordedButNeverAttempted confirms a crash
// after a hop's target agent is decided but before that agent's own
// execA2A call ever ran doesn't leave the task stuck: "a" replies, handing
// off to "b", but "b"'s conversation is left untouched, and runRelayLoop
// must reconstruct and resend "a"'s reply to "b" rather than erroring.
func TestRunRelayLoop_RecoversHopRecordedButNeverAttempted(t *testing.T) {
	a := &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		return envNew(a2a.MessageRoleUser, "a", "b", "from-a:"+textOf(inMessage(in))), nil
	}}
	b := &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		return replyFrom(in, "b", "", "done:"+textOf(inMessage(in))), nil
	}}
	c, el, store := newTestServerController(t, map[string]harness.Harness{"a": a, "b": b})
	defer c.Close()
	ctx := context.Background()

	bootstrap := envNew(a2a.MessageRoleUser, checkpointdIdentity, "a", "seed")
	bk, err := newServerSession(ctx, store, el, "a", bootstrap)
	if err != nil {
		t.Fatalf("newServerSession: %v", err)
	}
	// Drive "a"'s turn directly (not via the full relay loop, which would
	// immediately go on to attempt "b" too) and record its result, leaving
	// "b" untouched -- the exact crash window this test targets.
	aActorID := actorConv(bk.sessionID, "a")
	aReply, err := execA2A(ctx, c, el, "a", aActorID, bk.sessionID, bootstrap)
	if err != nil {
		t.Fatalf("execA2A(a): %v", err)
	}
	if err := bk.recordTaskStatus(ctx, aReply, "a"); err != nil {
		t.Fatalf("recordTaskStatus: %v", err)
	}

	// Confirm the constructed state: "b" is next, but nothing is logged for
	// "b" yet.
	last, lastStep, err := bk.lastHop(ctx)
	if err != nil {
		t.Fatalf("bk.lastHop: %v", err)
	}
	if last == nil || last.To != "b" {
		t.Fatalf("bk.lastHop() = %+v, want an envelope with To \"b\"", last)
	}
	if lastStep.State == proto.State_STATE_PENDING {
		t.Fatalf("lastHop's own lastStep.State = %v, want not PENDING (b's own hop was never attempted)", lastStep.State)
	}

	res, err := runRelayLoop(ctx, c, el, bk, "a", bootstrap, func(*hop.Envelope) {})
	if err != nil {
		t.Fatalf("runRelayLoop: %v, want it to recover by resending the never-attempted hop to b", err)
	}
	if got, want := textOf(inMessage(res)), "done:from-a:seed"; got != want {
		t.Errorf("result text = %q, want %q", got, want)
	}
}

// pollTask retries GetTask until it returns state or a bounded timeout
// elapses. tenant is the sessionID GetTask requires for scoping, the same
// value SendMessage's result carries in Metadata[checkpointdTaskMetaTenant].
func pollTask(t *testing.T, h *serverRequestHandler, id a2a.TaskID, tenant string, want a2a.TaskState) *a2a.Task {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var last *a2a.Task
	for time.Now().Before(deadline) {
		task, err := h.GetTask(context.Background(), &a2a.GetTaskRequest{ID: id, Tenant: tenant})
		if err != nil {
			t.Fatalf("GetTask: %v", err)
		}
		last = task
		if task.Status.State == want {
			return task
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("GetTask never reached state %q, last seen: %+v", want, last)
	return nil
}

// pollSessionState polls store.GetSession(id) until its state matches want
// or a deadline passes. Session termination happens after SendMessage's
// caller has already been unblocked, so a racing caller must poll for it,
// the same as pollTask does for the task-status write.
func pollSessionState(t *testing.T, store *sqlTaskStore, id, want string) *sessionRow {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var last *sessionRow
	for time.Now().Before(deadline) {
		row, err := store.GetSession(context.Background(), id)
		if err != nil {
			t.Fatalf("GetSession: %v", err)
		}
		last = row
		if row.state == want {
			return row
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("session %s never reached state %q, last seen: %+v", id, want, last)
	return nil
}

// pollTaskIDForSession polls store.TaskIDForSession(sessionID) until it
// finds one or a deadline passes, for a test whose session's TaskID is
// only assigned lazily (see bookkeeping.recordTaskStatus) and isn't known
// up front.
func pollTaskIDForSession(t *testing.T, store *sqlTaskStore, sessionID string) a2a.TaskID {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		taskID, ok, err := store.TaskIDForSession(context.Background(), sessionID)
		if err != nil {
			t.Fatalf("TaskIDForSession: %v", err)
		}
		if ok {
			return a2a.TaskID(taskID)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("session %s never got a task id", sessionID)
	return ""
}

// onlySessionID returns the id of store's one and only checkpointd_sessions
// row, for a message-only session that has no checkpointd_tasks row to
// find it through. Fails the test if there isn't exactly one.
func onlySessionID(t *testing.T, store *sqlTaskStore) string {
	t.Helper()
	rows, err := store.db.QueryContext(context.Background(), "SELECT id FROM checkpointd_sessions")
	if err != nil {
		t.Fatalf("querying checkpointd_sessions: %v", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scanning checkpointd_sessions: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterating checkpointd_sessions: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("checkpointd_sessions has %d row(s), want exactly 1: %v", len(ids), ids)
	}
	return ids[0]
}

// TestServerRequestHandler_SendMessage_ThenGetTask_ReachesCompleted confirms
// SendMessage blocks and returns the task's real final state directly for
// an agent that finishes quickly, not an immediate "submitted" placeholder.
func TestServerRequestHandler_SendMessage_ThenGetTask_ReachesCompleted(t *testing.T) {
	a := &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		reply := taskReply(in, "a", "", a2a.TaskStateCompleted, "done:"+textOf(inMessage(in)))
		return reply, nil
	}}
	c, el, store := newTestServerController(t, map[string]harness.Harness{"a": a})
	defer c.Close()

	h := &serverRequestHandler{agent: "a", c: c, el: el, store: store, registry: newTaskRegistry()}
	req := &a2a.SendMessageRequest{Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("seed"))}
	result, err := h.SendMessage(context.Background(), req)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	task, ok := result.(*a2a.Task)
	if !ok {
		t.Fatalf("SendMessage result is %T, want *a2a.Task", result)
	}
	if task.Status.State != a2a.TaskStateCompleted {
		t.Errorf("Status.State = %q, want %q", task.Status.State, a2a.TaskStateCompleted)
	}
	// History holds only the client's original message: the current status
	// message never also gets duplicated as History's own last entry.
	if len(task.History) != 1 {
		t.Fatalf("len(History) = %d, want 1", len(task.History))
	}
	if got, want := textOf(task.History[0]), "seed"; got != want {
		t.Errorf("first history entry = %q, want %q", got, want)
	}
	if got, want := textOf(task.Status.Message), "done:seed"; got != want {
		t.Errorf("final reply text = %q, want %q", got, want)
	}

	// GetTask afterward must agree.
	sessionID, _ := task.Metadata[checkpointdTaskMetaTenant].(string)
	final := pollTask(t, h, task.ID, sessionID, a2a.TaskStateCompleted)
	if got, want := textOf(final.Status.Message), "done:seed"; got != want {
		t.Errorf("GetTask final reply text = %q, want %q", got, want)
	}
}

// TestServerRequestHandler_GetTask_NoTenantResolvesUnambiguously confirms a
// tenant-agnostic client (the A2A spec never requires more than task_id)
// can still read back a task it just created, as long as (TaskID, agent)
// is currently unambiguous.
func TestServerRequestHandler_GetTask_NoTenantResolvesUnambiguously(t *testing.T) {
	a := &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		reply := taskReply(in, "a", "", a2a.TaskStateCompleted, "done:"+textOf(inMessage(in)))
		return reply, nil
	}}
	c, el, store := newTestServerController(t, map[string]harness.Harness{"a": a})
	defer c.Close()

	h := &serverRequestHandler{agent: "a", c: c, el: el, store: store, registry: newTaskRegistry()}
	req := &a2a.SendMessageRequest{Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("seed"))}
	result, err := h.SendMessage(context.Background(), req)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	task, ok := result.(*a2a.Task)
	if !ok {
		t.Fatalf("SendMessage result is %T, want *a2a.Task", result)
	}

	got, err := h.GetTask(context.Background(), &a2a.GetTaskRequest{ID: task.ID})
	if err != nil {
		t.Fatalf("GetTask with no tenant: %v", err)
	}
	if got.Status.State != a2a.TaskStateCompleted {
		t.Errorf("GetTask with no tenant: Status.State = %q, want %q", got.Status.State, a2a.TaskStateCompleted)
	}
}

// TestServerRequestHandler_SendMessage_MessageOnly confirms a single,
// unstamped, terminal reply (the agent never claimed any task state) is
// returned by SendMessage as the bare *a2a.Message it is, with no task row
// ever created for it -- ListTasks and GetTask have nothing to report.
func TestServerRequestHandler_SendMessage_MessageOnly(t *testing.T) {
	a := &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		// reply.Data.State is deliberately left unset.
		return replyFrom(in, "a", "", "Direct message response"), nil
	}}
	c, el, store := newTestServerController(t, map[string]harness.Harness{"a": a})
	defer c.Close()

	h := &serverRequestHandler{agent: "a", c: c, el: el, store: store, registry: newTaskRegistry()}
	req := &a2a.SendMessageRequest{Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("seed"))}
	result, err := h.SendMessage(context.Background(), req)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	msg, ok := result.(*a2a.Message)
	if !ok {
		t.Fatalf("SendMessage result is %T, want *a2a.Message", result)
	}
	if got, want := textOf(msg), "Direct message response"; got != want {
		t.Errorf("message text = %q, want %q", got, want)
	}

	listed, err := h.ListTasks(context.Background(), &a2a.ListTasksRequest{})
	if err != nil {
		t.Fatalf("ListTasks: %v", err)
	}
	if len(listed.Tasks) != 0 {
		t.Errorf("ListTasks = %+v, want no tasks (a message-only reply never creates one)", listed.Tasks)
	}
}

// TestServerRequestHandler_SendMessage_TerminatesSessionOnCompletion
// confirms a session's checkpointd_sessions row is created RUNNING when
// SendMessage starts a new task and reaches TERMINATED once it completes.
func TestServerRequestHandler_SendMessage_TerminatesSessionOnCompletion(t *testing.T) {
	a := &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		reply := taskReply(in, "a", "", a2a.TaskStateCompleted, "done:"+textOf(inMessage(in)))
		return reply, nil
	}}
	c, el, store := newTestServerController(t, map[string]harness.Harness{"a": a})
	defer c.Close()

	h := &serverRequestHandler{agent: "a", c: c, el: el, store: store, registry: newTaskRegistry()}
	result, err := h.SendMessage(context.Background(), &a2a.SendMessageRequest{
		Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("seed")),
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	task, ok := result.(*a2a.Task)
	if !ok {
		t.Fatalf("SendMessage result is %T, want *a2a.Task", result)
	}
	sessionID, _ := task.Metadata[checkpointdTaskMetaTenant].(string)
	if sessionID == "" {
		t.Fatal("task.Metadata carries no session id, can't look up its session")
	}

	pollSessionState(t, store, sessionID, sessionStateTerminated)
}

// TestServerRequestHandler_SendMessage_TerminatesSessionOnMessageOnlyReply
// confirms a purely message-only relay (the agent never produces a real
// a2a.Task) still gets a session row, and that row still ends up
// TERMINATED.
func TestServerRequestHandler_SendMessage_TerminatesSessionOnMessageOnlyReply(t *testing.T) {
	a := &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		return replyFrom(in, "a", "", "done:"+textOf(inMessage(in))), nil
	}}
	c, el, store := newTestServerController(t, map[string]harness.Harness{"a": a})
	defer c.Close()

	h := &serverRequestHandler{agent: "a", c: c, el: el, store: store, registry: newTaskRegistry()}
	result, err := h.SendMessage(context.Background(), &a2a.SendMessageRequest{
		Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("seed")),
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if _, ok := result.(*a2a.Message); !ok {
		t.Fatalf("SendMessage result is %T, want *a2a.Message (message-only reply)", result)
	}

	// No task row exists for a message-only reply, so read the session id
	// directly off checkpointd_sessions instead.
	pollSessionState(t, store, onlySessionID(t, store), sessionStateTerminated)
}

// TestServerRequestHandler_SendMessage_WaitsForFullChainByDefault confirms
// SendMessage blocks past an intermediate self-continuation hop (Working)
// and returns the eventual terminal state instead, when the caller never
// sets ReturnImmediately.
func TestServerRequestHandler_SendMessage_WaitsForFullChainByDefault(t *testing.T) {
	var calls int
	a := &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		calls++
		if calls == 1 {
			return taskReply(in, "a", "a", a2a.TaskStateWorking, "still working"), nil
		}
		return taskReply(in, "a", "", a2a.TaskStateCompleted, "done"), nil
	}}
	c, el, store := newTestServerController(t, map[string]harness.Harness{"a": a})
	defer c.Close()

	h := &serverRequestHandler{agent: "a", c: c, el: el, store: store, registry: newTaskRegistry()}
	req := &a2a.SendMessageRequest{
		Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("seed")),
	}
	result, err := h.SendMessage(context.Background(), req)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	task, ok := result.(*a2a.Task)
	if !ok {
		t.Fatalf("SendMessage result is %T, want *a2a.Task", result)
	}
	if task.Status.State != a2a.TaskStateCompleted {
		t.Errorf("Status.State = %q, want %q (should wait for the whole chain when ReturnImmediately is unset)", task.Status.State, a2a.TaskStateCompleted)
	}
}

// TestServerRequestHandler_SendMessage_ReturnsAfterFirstSelfContinuationHop
// confirms that with ReturnImmediately set, SendMessage unblocks as soon as
// the first self-continuation hop (Working, addressed back to itself)
// lands, without waiting for the whole chain to finish.
func TestServerRequestHandler_SendMessage_ReturnsAfterFirstSelfContinuationHop(t *testing.T) {
	unblockSecondHop := make(chan struct{})
	var calls int
	a := &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		calls++
		if calls == 1 {
			return taskReply(in, "a", "a", a2a.TaskStateWorking, "still working"), nil
		}
		<-unblockSecondHop
		return taskReply(in, "a", "", a2a.TaskStateCompleted, "done"), nil
	}}
	c, el, store := newTestServerController(t, map[string]harness.Harness{"a": a})
	defer c.Close()

	h := &serverRequestHandler{agent: "a", c: c, el: el, store: store, registry: newTaskRegistry()}
	req := &a2a.SendMessageRequest{
		Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("seed")),
		Config:  &a2a.SendMessageConfig{ReturnImmediately: true},
	}
	result, err := h.SendMessage(context.Background(), req)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	task, ok := result.(*a2a.Task)
	if !ok {
		t.Fatalf("SendMessage result is %T, want *a2a.Task", result)
	}
	if task.Status.State != a2a.TaskStateWorking {
		t.Errorf("Status.State = %q, want %q (should reflect the first hop, not wait for completion)", task.Status.State, a2a.TaskStateWorking)
	}

	close(unblockSecondHop)
	sessionID, _ := task.Metadata[checkpointdTaskMetaTenant].(string)
	final := pollTask(t, h, task.ID, sessionID, a2a.TaskStateCompleted)
	// "done" is the current status message; "still working" (now
	// superseded) ends up in History instead.
	if got, want := textOf(final.Status.Message), "done"; got != want {
		t.Errorf("final reply text = %q, want %q", got, want)
	}
}

// TestServerRequestHandler_SendMessage_HandoffNotInHistory confirms
// assembleTask keeps a private sub-call "a" makes to another agent -- the
// same shape harness.WithTransport's own nested calls produce -- out of
// the external caller's own History/Status entirely. The external caller
// only ever addressed "a"; it never had a card for "b" and shouldn't see
// "b"'s own reply text as if it were part of that conversation.
func TestServerRequestHandler_SendMessage_HandoffNotInHistory(t *testing.T) {
	var aCalls int
	a := &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		aCalls++
		if aCalls == 1 {
			return replyFrom(in, "a", "b", "ask-b:"+textOf(inMessage(in))), nil
		}
		return taskReply(in, "a", "", a2a.TaskStateCompleted, "done:"+textOf(inMessage(in))), nil
	}}
	b := &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		return replyFrom(in, "b", "a", "b-answer"), nil
	}}
	c, el, store := newTestServerController(t, map[string]harness.Harness{"a": a, "b": b})
	defer c.Close()

	h := &serverRequestHandler{agent: "a", c: c, el: el, store: store, registry: newTaskRegistry()}
	req := &a2a.SendMessageRequest{Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("seed"))}
	result, err := h.SendMessage(context.Background(), req)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	task, ok := result.(*a2a.Task)
	if !ok {
		t.Fatalf("SendMessage result is %T, want *a2a.Task", result)
	}
	if task.Status.State != a2a.TaskStateCompleted {
		t.Errorf("Status.State = %q, want %q", task.Status.State, a2a.TaskStateCompleted)
	}
	// Just the seed message: "a"'s own private consultation of "b" ("a"'s
	// own hand-off message and "b"'s own reply, neither ever addressed to
	// the external caller) never becomes part of this History.
	if len(task.History) != 1 {
		t.Fatalf("History = %+v (len %d), want 1 entry", task.History, len(task.History))
	}
	if got, want := textOf(task.History[0]), "seed"; got != want {
		t.Errorf("History[0] = %q, want %q", got, want)
	}
	if got, want := textOf(task.Status.Message), "done:b-answer"; got != want {
		t.Errorf("final reply text = %q, want %q", got, want)
	}
}

// TestServerRequestHandler_SendMessage_Continuation confirms a2a's standard
// multi-turn pattern: SendMessage reaches InputRequired and genuinely
// pauses, then a follow-up SendMessage with the same TaskID delivers new
// input and drives the task to completion.
func TestServerRequestHandler_SendMessage_Continuation(t *testing.T) {
	var calls int
	a := &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		calls++
		if textOf(inMessage(in)) == "please continue" {
			// Task-shaped, not a bare Message: "a" already claimed a Task on
			// its first turn (below), and a real agent (via NewAgentHarness's
			// own "established" check) can never revert to a bare Message
			// once it has -- see harness/agent.go.
			return taskReply(in, "a", "", a2a.TaskStateCompleted, "done:"+textOf(inMessage(in))), nil
		}
		// hop.To == "" (not self): a genuine pause hands control back to
		// checkpointd's external caller, unlike a self-addressed hop that
		// loops internally.
		reply := taskReply(in, "a", "", a2a.TaskStateInputRequired, "need more input")
		return reply, nil
	}}
	c, el, store := newTestServerController(t, map[string]harness.Harness{"a": a})
	defer c.Close()

	h := &serverRequestHandler{agent: "a", c: c, el: el, store: store, registry: newTaskRegistry()}
	req := &a2a.SendMessageRequest{Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("seed"))}
	result, err := h.SendMessage(context.Background(), req)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	paused := result.(*a2a.Task)
	taskID := paused.ID
	sessionID, _ := paused.Metadata[checkpointdTaskMetaTenant].(string)
	// A fast agent returns the real, already-paused state directly.
	if paused.Status.State != a2a.TaskStateInputRequired {
		t.Fatalf("initial Status.State = %q, want %q", paused.Status.State, a2a.TaskStateInputRequired)
	}
	// Just the seed message: the status message isn't duplicated into History.
	if len(paused.History) != 1 {
		t.Fatalf("len(History) after first turn = %d, want 1", len(paused.History))
	}

	// Confirm it's genuinely paused: no active goroutine, so a second
	// continuation attempt right now must not collide with anything.
	followUp := &a2a.SendMessageRequest{Message: &a2a.Message{
		ID:     "follow-up-1",
		Role:   a2a.MessageRoleUser,
		Parts:  a2a.ContentParts{a2a.NewTextPart("please continue")},
		TaskID: taskID,
	}, Tenant: sessionID}
	result2, err := h.SendMessage(context.Background(), followUp)
	if err != nil {
		t.Fatalf("SendMessage (continuation): %v", err)
	}
	final := result2.(*a2a.Task)
	if final.Status.State != a2a.TaskStateCompleted {
		t.Errorf("continuation's Status.State = %q, want %q", final.Status.State, a2a.TaskStateCompleted)
	}
	// seed, "need more input", and "please continue" -- each superseded in
	// turn; "done:please continue" is the current status message, not
	// History's last entry.
	if len(final.History) != 3 {
		t.Fatalf("len(History) after continuation = %d, want 3", len(final.History))
	}
	if got, want := textOf(final.Status.Message), "done:please continue"; got != want {
		t.Errorf("final reply text = %q, want %q", got, want)
	}
	if calls != 2 {
		t.Errorf("harness called %d times, want 2 (one per turn)", calls)
	}
}

// TestServerRequestHandler_GetTask_MessageAfterTaskSkippedNotErrored
// confirms assembleTask skips (rather than errors on) an agent's own reply
// reverting to a bare Message after that same agent already claimed a Task
// earlier in this task's history -- a scenario harness/agent.go's own
// "established" check already makes unreachable for a real agent
// (fakeHarness bypasses it here to simulate what a corrupted event log
// would otherwise surface): the corrupted entry is simply ignored rather
// than either failing the whole call or being folded into the task's own
// Status.Message as if it were legitimate.
func TestServerRequestHandler_GetTask_MessageAfterTaskSkippedNotErrored(t *testing.T) {
	a := &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		if textOf(inMessage(in)) == "please continue" {
			return replyFrom(in, "a", "", "done:"+textOf(inMessage(in))), nil
		}
		return taskReply(in, "a", "", a2a.TaskStateInputRequired, "need more input"), nil
	}}
	c, el, store := newTestServerController(t, map[string]harness.Harness{"a": a})
	defer c.Close()

	h := &serverRequestHandler{agent: "a", c: c, el: el, store: store, registry: newTaskRegistry()}
	req := &a2a.SendMessageRequest{Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("seed"))}
	result, err := h.SendMessage(context.Background(), req)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	paused := result.(*a2a.Task)
	sessionID, _ := paused.Metadata[checkpointdTaskMetaTenant].(string)

	followUp := &a2a.SendMessageRequest{Message: &a2a.Message{
		ID:     "follow-up-1",
		Role:   a2a.MessageRoleUser,
		Parts:  a2a.ContentParts{a2a.NewTextPart("please continue")},
		TaskID: paused.ID,
	}, Tenant: sessionID}
	if _, err := h.SendMessage(context.Background(), followUp); err != nil {
		t.Fatalf("SendMessage with a reply reverting to a bare Message after claiming a Task: %v, want it skipped rather than erroring", err)
	}

	if _, err := h.GetTask(context.Background(), &a2a.GetTaskRequest{ID: paused.ID, Tenant: sessionID}); err != nil {
		t.Fatalf("GetTask after a corrupted reply was skipped: %v", err)
	}
}

// TestServerRequestHandler_GetTask_OtherAgentSelfContinuationExcluded
// confirms assembleTask's own self-continuation/terminal-reply
// classification is scoped to the task's own owner agent: a different
// agent's own self-continuation sharing the same session (e.g. a private
// sub-call target running its own internal multi-turn loop) must never
// leak into this task's own externally-visible History.
func TestServerRequestHandler_GetTask_OtherAgentSelfContinuationExcluded(t *testing.T) {
	c, el, store := newTestServerController(t, nil)
	defer c.Close()
	ctx := context.Background()

	bootstrap := envNew(a2a.MessageRoleUser, checkpointdIdentity, "a", "seed")
	bk, err := newServerSession(ctx, store, el, "a", bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	aActorID := actorConv(bk.sessionID, "a")

	appendEvent := func(actorID string, env *hop.Envelope) {
		t.Helper()
		b, err := json.Marshal(env)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := el.Append(ctx, &proto.StepEvent{ConversationId: actorID, SessionId: bk.sessionID, Steps: []*proto.Step{textStep(string(b))}}); err != nil {
			t.Fatal(err)
		}
	}

	// "a" self-continues once, establishing its own task.
	waiting := taskReply(bootstrap, "a", "a", a2a.TaskStateInputRequired, "a waiting")
	appendEvent(aActorID, waiting)
	if err := bk.recordTaskStatus(ctx, waiting, "a"); err != nil {
		t.Fatal(err)
	}
	taskID := a2a.TaskID(bk.taskID)

	// "b" -- some other agent sharing this same session (e.g. a private
	// sub-call "a" made) -- has its own, completely unrelated
	// self-continuation. Must never leak into "a"'s own assembled task.
	bActorID := actorConv(bk.sessionID, "b")
	bSelf := taskReply(nil, "b", "b", a2a.TaskStateWorking, "b's own internal progress")
	appendEvent(bActorID, bSelf)

	// "a" completes.
	final := taskReply(waiting, "a", "", a2a.TaskStateCompleted, "a is done")
	appendEvent(aActorID, final)
	if err := bk.recordTaskStatus(ctx, final, "a"); err != nil {
		t.Fatal(err)
	}

	h := &serverRequestHandler{agent: "a", c: c, el: el, store: store, registry: newTaskRegistry()}
	task, err := h.GetTask(ctx, &a2a.GetTaskRequest{ID: taskID, Tenant: bk.sessionID})
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if got, want := textOf(task.Status.Message), "a is done"; got != want {
		t.Errorf("Status.Message = %q, want %q", got, want)
	}
	for _, m := range task.History {
		if textOf(m) == "b's own internal progress" {
			t.Errorf("History = %+v, must not include another agent's own self-continuation text", task.History)
		}
	}
}

// TestServerRequestHandler_SendMessage_InfersContextIDOnContinuation
// (regression test for CORE-MULTI-005) confirms a follow-up continuation
// that omits ContextID gets the task's existing, agent-assigned ContextID
// back, not an empty string. fakeHarness sets ContextID explicitly on
// "a"'s first reply to simulate what a2a-go's own canonical constructors
// do for a real agent's first Task-shaped claim.
func TestServerRequestHandler_SendMessage_InfersContextIDOnContinuation(t *testing.T) {
	const generatedContextID = "agent-generated-ctx-1"
	a := &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		if textOf(inMessage(in)) == "please continue" {
			reply := taskReply(in, "a", "", a2a.TaskStateCompleted, "done:"+textOf(inMessage(in)))
			reply.Data.Task.ContextID = generatedContextID
			return reply, nil
		}
		reply := taskReply(in, "a", "", a2a.TaskStateInputRequired, "need more input")
		reply.Data.Task.ContextID = generatedContextID
		return reply, nil
	}}
	c, el, store := newTestServerController(t, map[string]harness.Harness{"a": a})
	defer c.Close()

	h := &serverRequestHandler{agent: "a", c: c, el: el, store: store, registry: newTaskRegistry()}
	req := &a2a.SendMessageRequest{Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("seed"))}
	result, err := h.SendMessage(context.Background(), req)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	paused := result.(*a2a.Task)
	if paused.ContextID == "" {
		t.Fatalf("initial Task.ContextID is empty, want a generated id")
	}

	// The follow-up omits ContextID, matching a real client that only knows
	// the TaskID, but still passes Tenant back (a separate concern).
	sessionID, _ := paused.Metadata[checkpointdTaskMetaTenant].(string)
	followUp := &a2a.SendMessageRequest{Message: &a2a.Message{
		ID:     "follow-up-1",
		Role:   a2a.MessageRoleUser,
		Parts:  a2a.ContentParts{a2a.NewTextPart("please continue")},
		TaskID: paused.ID,
	}, Tenant: sessionID}
	result2, err := h.SendMessage(context.Background(), followUp)
	if err != nil {
		t.Fatalf("SendMessage (continuation): %v", err)
	}
	final := result2.(*a2a.Task)
	if final.ContextID != paused.ContextID {
		t.Errorf("continuation's Task.ContextID = %q, want the inferred %q", final.ContextID, paused.ContextID)
	}
}

// TestServerRequestHandler_SendMessage_RejectsConcurrentContinuation
// confirms mutual exclusion: a continuation attempt while a goroutine is
// still actively driving the same task is rejected, not raced. The
// harness's first turn reports Working to get a real taskID, and the relay
// loop's own automatic second turn provides the in-flight goroutine this
// test races a continuation against.
func TestServerRequestHandler_SendMessage_RejectsConcurrentContinuation(t *testing.T) {
	entered := make(chan string, 1)
	release := make(chan struct{})
	var calls int
	a := &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		calls++
		if calls == 1 {
			reply := taskReply(in, "a", "a", a2a.TaskStateWorking, "still working")
			return reply, nil
		}
		entered <- textOf(inMessage(in))
		<-release
		reply := taskReply(in, "a", "", a2a.TaskStateCompleted, "done:"+textOf(inMessage(in))) // a real Task: this test is about mutual exclusion, not isMessageOnlyTask
		return reply, nil
	}}
	c, el, store := newTestServerController(t, map[string]harness.Harness{"a": a})
	defer c.Close()

	h := &serverRequestHandler{agent: "a", c: c, el: el, store: store, registry: newTaskRegistry()}
	req := &a2a.SendMessageRequest{
		Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("seed")),
		// ReturnImmediately: this test needs the session's background
		// goroutine to still be actively relaying (the second turn, blocked
		// on <-release below) once SendMessage itself has returned.
		Config: &a2a.SendMessageConfig{ReturnImmediately: true},
	}
	res, err := h.SendMessage(context.Background(), req)
	if err != nil {
		t.Fatalf("SendMessage (first turn): %v", err)
	}
	task, ok := res.(*a2a.Task)
	if !ok {
		t.Fatalf("SendMessage result is %T, want *a2a.Task", res)
	}
	taskID := task.ID
	sessionID, _ := task.Metadata[checkpointdTaskMetaTenant].(string)

	select {
	case text := <-entered:
		if text != "still working" {
			t.Fatalf("second turn entered with text %q, want %q", text, "still working")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("harness's automatic second turn was never entered")
	}

	// The session's background goroutine is now blocked inside the harness
	// call.
	followUp := &a2a.SendMessageRequest{Message: &a2a.Message{
		ID:     "follow-up-1",
		Role:   a2a.MessageRoleUser,
		Parts:  a2a.ContentParts{a2a.NewTextPart("too soon")},
		TaskID: taskID,
	}, Tenant: sessionID}
	if _, err := h.SendMessage(context.Background(), followUp); err == nil {
		t.Error("SendMessage against an actively-relaying task: got nil error, want a rejection")
	}

	close(release)
	pollTask(t, h, taskID, sessionID, a2a.TaskStateCompleted)
}

func TestServerRequestHandler_ListTasks_ScopedToOwnAgent(t *testing.T) {
	a := &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		reply := taskReply(in, "a", "", a2a.TaskStateCompleted, "done-a") // a real Task: this test is about ListTasks scoping, not isMessageOnlyTask
		return reply, nil
	}}
	b := &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		reply := taskReply(in, "b", "", a2a.TaskStateCompleted, "done-b")
		return reply, nil
	}}
	c, el, store := newTestServerController(t, map[string]harness.Harness{"a": a, "b": b})
	defer c.Close()

	registry := newTaskRegistry()
	hA := &serverRequestHandler{agent: "a", c: c, el: el, store: store, registry: registry}
	hB := &serverRequestHandler{agent: "b", c: c, el: el, store: store, registry: registry}

	resA, err := hA.SendMessage(context.Background(), &a2a.SendMessageRequest{Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("seed"))})
	if err != nil {
		t.Fatalf("SendMessage (a): %v", err)
	}
	taskA := resA.(*a2a.Task)
	sessionA, _ := taskA.Metadata[checkpointdTaskMetaTenant].(string)
	pollTask(t, hA, taskA.ID, sessionA, a2a.TaskStateCompleted)

	if _, err := hB.SendMessage(context.Background(), &a2a.SendMessageRequest{Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("seed"))}); err != nil {
		t.Fatalf("SendMessage (b): %v", err)
	}

	listA, err := hA.ListTasks(context.Background(), &a2a.ListTasksRequest{})
	if err != nil {
		t.Fatalf("ListTasks (a): %v", err)
	}
	if len(listA.Tasks) != 1 {
		t.Fatalf("agent a's ListTasks returned %d tasks, want 1 (its own only)", len(listA.Tasks))
	}
	if listA.Tasks[0].ID != taskA.ID {
		t.Errorf("agent a's ListTasks returned task %q, want its own %q", listA.Tasks[0].ID, taskA.ID)
	}
}

func TestServerRequestHandler_GetTask_UnknownIDNotFound(t *testing.T) {
	_, _, store := newTestServerController(t, nil)
	h := &serverRequestHandler{agent: "a", store: store}
	if _, err := h.GetTask(context.Background(), &a2a.GetTaskRequest{ID: "bogus"}); err == nil {
		t.Error("GetTask for an unknown id: got nil error, want a2a.ErrTaskNotFound")
	}
}

func TestServerRequestHandler_CancelTask_UnknownIDNotFound(t *testing.T) {
	_, el, store := newTestServerController(t, nil)
	h := &serverRequestHandler{agent: "a", el: el, store: store, registry: newTaskRegistry()}
	if _, err := h.CancelTask(context.Background(), &a2a.CancelTaskRequest{ID: "unknown-task"}); !errors.Is(err, a2a.ErrTaskNotFound) {
		t.Errorf("CancelTask for an unknown task: err = %v, want a2a.ErrTaskNotFound", err)
	}
}

// TestServerRequestHandler_CancelTask_PausedTaskSucceeds (CORE-CANCEL-001)
// confirms a paused task (e.g. InputRequired, no goroutine driving it) is
// directly marked canceled, not met with a blanket ErrTaskNotCancelable.
func TestServerRequestHandler_CancelTask_PausedTaskSucceeds(t *testing.T) {
	c, el, store := newTestServerController(t, nil)
	defer c.Close()
	ctx := context.Background()
	bootstrap := envNew(a2a.MessageRoleUser, checkpointdIdentity, "a", "seed")
	bootstrap.Data.Message.ContextID = "ctx-1"
	bk, err := newServerSession(ctx, store, el, "a", bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	actorID := actorConv(bk.sessionID, "a")
	// Simulate a paused (InputRequired) state: a logged, non-terminal reply
	// with no goroutine active.
	reply := taskReply(bootstrap, "a", "a", a2a.TaskStateInputRequired, "waiting")
	b, err := json.Marshal(reply)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := el.Append(ctx, &proto.StepEvent{ConversationId: actorID, SessionId: bk.sessionID, Steps: []*proto.Step{textStep(string(b))}}); err != nil {
		t.Fatal(err)
	}
	// This Task-shaped reply is what lazily creates bk's checkpointd_tasks
	// row; bk.taskID is populated by this call, not chosen by the test.
	if err := bk.recordTaskStatus(ctx, reply, "a"); err != nil {
		t.Fatal(err)
	}
	taskID := a2a.TaskID(bk.taskID)

	h := &serverRequestHandler{agent: "a", c: c, el: el, store: store, registry: newTaskRegistry()}
	task, err := h.CancelTask(ctx, &a2a.CancelTaskRequest{ID: taskID, Tenant: bk.sessionID})
	if err != nil {
		t.Fatalf("CancelTask for a paused task: %v", err)
	}
	if task.Status.State != a2a.TaskStateCanceled {
		t.Errorf("Status.State = %q, want %q", task.Status.State, a2a.TaskStateCanceled)
	}

	// A second cancel, now that the task is terminal, must be rejected.
	if _, err := h.CancelTask(ctx, &a2a.CancelTaskRequest{ID: taskID, Tenant: bk.sessionID}); !errors.Is(err, a2a.ErrTaskNotCancelable) {
		t.Errorf("CancelTask on an already-canceled task: err = %v, want a2a.ErrTaskNotCancelable", err)
	}

	// finishTermination runs asynchronously, so poll for the session to
	// actually reach TERMINATED rather than just checking Canceled on the task.
	pollSessionState(t, store, bk.sessionID, sessionStateTerminated)
}

// TestServerRequestHandler_CancelTask_ActiveTaskStopsRelayNotAgent confirms
// cancelling behavior: canceling a task with an actively-driving goroutine
// stops checkpointd's own relay loop and marks the task canceled, but doesn't
// interrupt the agent's own in-flight logic (nothing calls AgentExecutor.Cancel)
// -- its eventual result must be discarded, not clobber the recorded cancellation.
func TestServerRequestHandler_CancelTask_ActiveTaskStopsRelayNotAgent(t *testing.T) {
	started := make(chan struct{})
	proceed := make(chan struct{})
	returned := make(chan struct{})
	var calls int
	a := &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		calls++
		if calls == 1 {
			reply := taskReply(in, "a", "a", a2a.TaskStateWorking, "still working")
			return reply, nil
		}
		close(started)
		<-proceed
		reply := taskReply(in, "a", "", a2a.TaskStateCompleted, "done")
		close(returned)
		return reply, nil
	}}
	c, el, store := newTestServerController(t, map[string]harness.Harness{"a": a})
	defer c.Close()
	ctx := context.Background()

	registry := newTaskRegistry()
	h := &serverRequestHandler{agent: "a", c: c, el: el, store: store, registry: registry}

	res, err := h.SendMessage(ctx, &a2a.SendMessageRequest{
		Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("seed")),
		// ReturnImmediately: this test needs the session's background
		// goroutine to still be actively relaying (the second turn, blocked
		// on <-proceed below) once SendMessage itself has returned, so
		// there's something for CancelTask to interrupt.
		Config: &a2a.SendMessageConfig{ReturnImmediately: true},
	})
	if err != nil {
		t.Fatalf("SendMessage (first turn): %v", err)
	}
	task, ok := res.(*a2a.Task)
	if !ok {
		t.Fatalf("SendMessage result is %T, want *a2a.Task", res)
	}
	id := task.ID
	sessionID, _ := task.Metadata[checkpointdTaskMetaTenant].(string)
	if sessionID == "" {
		t.Fatal("task.Metadata carries no session id, can't check the registry")
	}

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("agent's second-turn respond was never called")
	}
	if !registry.isActive(sessionID) {
		t.Fatal("registry.isActive = false while the agent's respond call is still blocked, want true")
	}

	type cancelResult struct {
		task *a2a.Task
		err  error
	}
	cancelDone := make(chan cancelResult, 1)
	go func() {
		task, err := h.CancelTask(ctx, &a2a.CancelTaskRequest{ID: id, Tenant: sessionID})
		cancelDone <- cancelResult{task, err}
	}()

	// Give cancelAndWait a moment to actually start waiting on the goroutine
	// before unblocking respond, so this exercises the real wait path
	// rather than a lucky race that skips it.
	time.Sleep(20 * time.Millisecond)
	close(proceed)

	var result cancelResult
	select {
	case result = <-cancelDone:
	case <-time.After(2 * time.Second):
		t.Fatal("CancelTask did not return")
	}
	if result.err != nil {
		t.Fatalf("CancelTask on an actively-running task: %v", result.err)
	}
	if result.task.Status.State != a2a.TaskStateCanceled {
		t.Errorf("Status.State = %q, want %q", result.task.Status.State, a2a.TaskStateCanceled)
	}
	// finishTermination's own async cleanup briefly still holds the
	// registry entry, so poll for TERMINATED rather than checking
	// registry.isActive immediately -- a stronger assertion that isn't
	// racy against that window.
	pollSessionState(t, store, sessionID, sessionStateTerminated)

	select {
	case <-returned:
	default:
		t.Error("respond never returned -- want it to have run to completion, uninterrupted by CancelTask (see the caveat this test documents)")
	}

	stored, err := store.Get(ctx, id, "a", sessionID)
	if err != nil {
		t.Fatalf("store.Get: %v", err)
	}
	if stored.Task.Status.State != a2a.TaskStateCanceled {
		t.Errorf("final stored state = %q, want %q (the agent's late, discarded completion must not overwrite the cancellation)", stored.Task.Status.State, a2a.TaskStateCanceled)
	}
}

// TestResumeServerSessions_SkipsCompletedResumesInFlight confirms
// resumeServerSessions re-drives a session that never got a chance to run
// but doesn't re-touch one that already completed.
func TestResumeServerSessions_SkipsCompletedResumesInFlight(t *testing.T) {
	var completedCalls, inFlightCalls int
	completed := &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		completedCalls++
		reply := taskReply(in, "a", "", a2a.TaskStateCompleted, "done") // a real Task: this test is about resume behavior, not isMessageOnlyTask
		return reply, nil
	}}
	inFlight := &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		inFlightCalls++
		reply := taskReply(in, "b", "", a2a.TaskStateCompleted, "done")
		return reply, nil
	}}
	c, el, store := newTestServerController(t, map[string]harness.Harness{"a": completed, "b": inFlight})
	defer c.Close()
	ctx := context.Background()

	registry := newTaskRegistry()

	// A task already driven to completion via direct SendMessage + poll.
	hA := &serverRequestHandler{agent: "a", c: c, el: el, store: store, registry: registry}
	resA, err := hA.SendMessage(ctx, &a2a.SendMessageRequest{Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("seed"))})
	if err != nil {
		t.Fatalf("SendMessage (a): %v", err)
	}
	taskA := resA.(*a2a.Task)
	sessionA, _ := taskA.Metadata[checkpointdTaskMetaTenant].(string)
	pollTask(t, hA, taskA.ID, sessionA, a2a.TaskStateCompleted)
	if completedCalls != 1 {
		t.Fatalf("completedCalls = %d before resume, want 1", completedCalls)
	}

	// A session recorded in the store whose goroutine never ran, simulating
	// a crash between SendMessage returning and the goroutine being
	// scheduled. No checkpointd_tasks row exists yet, so resumeServerSessions
	// must find it via checkpointd_sessions.
	bootstrap := envNew(a2a.MessageRoleUser, "checkpointd", "b", "seed")
	bootstrap.Data.Message.ContextID = "ctx-in-flight"
	inFlightSession, err := newServerSession(ctx, store, el, "b", bootstrap)
	if err != nil {
		t.Fatal(err)
	}

	if err := resumeServerSessions(ctx, c, el, store, registry); err != nil {
		t.Fatalf("resumeServerSessions: %v", err)
	}

	hB := &serverRequestHandler{agent: "b", c: c, el: el, store: store, registry: registry}
	inFlightTaskID := pollTaskIDForSession(t, store, inFlightSession.sessionID)
	pollTask(t, hB, inFlightTaskID, inFlightSession.sessionID, a2a.TaskStateCompleted)
	if inFlightCalls != 1 {
		t.Errorf("inFlightCalls = %d, want 1 (resumeServerSessions should have driven it)", inFlightCalls)
	}

	// Give an incorrect re-drive a moment to happen before asserting it didn't.
	time.Sleep(50 * time.Millisecond)
	if completedCalls != 1 {
		t.Errorf("completedCalls = %d after resume, want still 1 (an already-completed task should not be re-driven)", completedCalls)
	}
}

// TestResumeServerSessions_TerminatingSessionFinishesTerminationWithoutRelaying
// confirms a session that crashed after being durably marked TERMINATING,
// but before finishTermination ran, resumes straight into finishing that
// teardown -- it must never re-enter the relay loop and re-contact the
// agent.
func TestResumeServerSessions_TerminatingSessionFinishesTerminationWithoutRelaying(t *testing.T) {
	var calls int
	a := &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		calls++
		return taskReply(in, "a", "a", a2a.TaskStateInputRequired, "waiting"), nil
	}}
	c, el, store := newTestServerController(t, map[string]harness.Harness{"a": a})
	defer c.Close()
	ctx := context.Background()

	// A real actor/hop recorded, no goroutine currently active.
	bootstrap := envNew(a2a.MessageRoleUser, checkpointdIdentity, "a", "seed")
	bk, err := newServerSession(ctx, store, el, "a", bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	actorID := actorConv(bk.sessionID, "a")
	reply := taskReply(bootstrap, "a", "a", a2a.TaskStateInputRequired, "waiting")
	b, err := json.Marshal(reply)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := el.Append(ctx, &proto.StepEvent{ConversationId: actorID, SessionId: bk.sessionID, Steps: []*proto.Step{textStep(string(b))}}); err != nil {
		t.Fatal(err)
	}
	if err := bk.recordTaskStatus(ctx, reply, "a"); err != nil {
		t.Fatal(err)
	}
	taskID := a2a.TaskID(bk.taskID)

	stored, err := store.Get(ctx, taskID, "a", bk.sessionID)
	if err != nil {
		t.Fatal(err)
	}

	// Simulate the crash window directly: CancelTaskAndSession's own
	// transaction commits (task Canceled, session TERMINATING), but
	// finishTermination never runs.
	if err := store.CancelTaskAndSession(ctx, taskID, "a", bk.sessionID, stored.Version); err != nil {
		t.Fatal(err)
	}
	pollSessionState(t, store, bk.sessionID, sessionStateTerminating)

	registry := newTaskRegistry()
	if err := resumeServerSessions(ctx, c, el, store, registry); err != nil {
		t.Fatalf("resumeServerSessions: %v", err)
	}

	pollSessionState(t, store, bk.sessionID, sessionStateTerminated)
	if calls != 0 {
		t.Errorf("harness respond called %d times during resume, want 0 (a TERMINATING session must never be relayed, only torn down)", calls)
	}
	stored, err = store.Get(ctx, taskID, "a", bk.sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Task.Status.State != a2a.TaskStateCanceled {
		t.Errorf("final task state = %q, want %q (unchanged by resume)", stored.Task.Status.State, a2a.TaskStateCanceled)
	}
}

func TestBuildAgentCard_ComputesInfraFieldsNotFromConfig(t *testing.T) {
	content := &config.AgentCardConfig{Name: "Workflow", Description: "desc"}
	card := buildAgentCard(content, "workflow", "http://checkpointd.example.svc:80/agents/workflow/")

	if card.Capabilities.Streaming || card.Capabilities.PushNotifications {
		t.Errorf("Capabilities = %+v, want both false regardless of content", card.Capabilities)
	}
	if len(card.SupportedInterfaces) != 2 {
		t.Fatalf("len(SupportedInterfaces) = %d, want 2 (checkpointd-internal + real JSON-RPC)", len(card.SupportedInterfaces))
	}
	if got, want := card.SupportedInterfaces[0].URL, hop.CheckpointdURLPrefix+"workflow"; got != want {
		t.Errorf("SupportedInterfaces[0].URL = %q, want %q (the checkpointd-internal one, ahead of the real one)", got, want)
	}
	if got, want := card.SupportedInterfaces[1].URL, "http://checkpointd.example.svc:80/agents/workflow/"; got != want {
		t.Errorf("SupportedInterfaces[1].URL = %q, want %q", got, want)
	}
	if card.Name != "Workflow" || card.Description != "desc" {
		t.Errorf("content fields not carried through: Name=%q Description=%q", card.Name, card.Description)
	}
}

// TestBuildAgentCard_TopLevelSlicesNeverNil (CARD-STRUCT-001) confirms
// DefaultInputModes, DefaultOutputModes, and Skills never marshal as JSON
// null, which the AgentCard schema rejects: none has `omitempty`, so unset
// config would otherwise leave them nil.
func TestBuildAgentCard_TopLevelSlicesNeverNil(t *testing.T) {
	content := &config.AgentCardConfig{Name: "Workflow", Description: "desc"} // modes/skills deliberately unset
	card := buildAgentCard(content, "workflow", "http://checkpointd.example.svc:80/agents/workflow/")

	if card.DefaultInputModes == nil {
		t.Error("DefaultInputModes is nil, want a non-nil (possibly empty) slice")
	}
	if card.DefaultOutputModes == nil {
		t.Error("DefaultOutputModes is nil, want a non-nil (possibly empty) slice")
	}
	if card.Skills == nil {
		t.Error("Skills is nil, want a non-nil (possibly empty) slice")
	}

	b, err := json.Marshal(card)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	for _, field := range []string{"defaultInputModes", "defaultOutputModes", "skills"} {
		if strings.Contains(string(b), `"`+field+`":null`) {
			t.Errorf("marshaled card contains %q, want an empty array instead: %s", `"`+field+`":null`, b)
		}
	}
}

// TestBuildAgentCard_SkillTagsNeverNil (CARD-STRUCT-001) confirms Tags
// never marshals as JSON null, which the AgentCard schema rejects: Tags has
// no `omitempty`, so an unset nil slice would otherwise do exactly that.
func TestBuildAgentCard_SkillTagsNeverNil(t *testing.T) {
	content := &config.AgentCardConfig{
		Name:   "Workflow",
		Skills: []config.AgentCardSkillConfig{{ID: "s1", Name: "Skill 1"}}, // Tags deliberately unset
	}
	card := buildAgentCard(content, "workflow", "http://checkpointd.example.svc:80/agents/workflow/")

	if len(card.Skills) != 1 {
		t.Fatalf("len(Skills) = %d, want 1", len(card.Skills))
	}
	if card.Skills[0].Tags == nil {
		t.Error("Skills[0].Tags is nil, want a non-nil (possibly empty) slice")
	}

	b, err := json.Marshal(card)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	if strings.Contains(string(b), `"tags":null`) {
		t.Errorf("marshaled card contains \"tags\":null, want an empty array instead: %s", b)
	}
}
