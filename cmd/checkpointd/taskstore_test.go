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
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv/taskstore"
	"github.com/ktock/checkpointd/internal/controller/eventlog"
	"github.com/ktock/checkpointd/proto"
)

// newTestTaskStore returns a sqlTaskStore backed by a fresh, temp-file
// SQLite database sharing its connection with a real eventlog.EventLog,
// the same way production wiring does.
func newTestTaskStore(t *testing.T) (*sqlTaskStore, eventlog.EventLog) {
	t.Helper()
	el, err := eventlog.OpenSQLiteEventLog(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("OpenSQLiteEventLog: %v", err)
	}
	t.Cleanup(func() { el.Close() })
	db, dialect, ok := eventlog.SQLDB(el)
	if !ok {
		t.Fatalf("eventlog.SQLDB: not a SQL-backed EventLog")
	}
	ts, err := newSQLTaskStore(db, dialect, el)
	if err != nil {
		t.Fatalf("newSQLTaskStore: %v", err)
	}
	return ts, el
}

func TestSQLTaskStore_CreateGetUpdate(t *testing.T) {
	ts, el := newTestTaskStore(t)
	ctx := context.Background()

	task := &a2a.Task{
		ID:        "task-1",
		ContextID: "ctx-1",
		Status:    a2a.TaskStatus{State: a2a.TaskStateWorking},
	}
	version, err := ts.Create(ctx, task, "agent-a", "session-1")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if version != 1 {
		t.Errorf("Create version = %d, want 1", version)
	}

	// A real client-originated message (Role: "user", From: checkpointdIdentity,
	// matching what userStep/continueRelayLoop actually log) for this task's
	// session, so Get's history assembly has something to find.
	if _, err := el.Append(ctx, &proto.StepEvent{
		ConversationId: "actor-1",
		SessionId:      "session-1",
		Steps:          []*proto.Step{userStep(`{"from":"","to":"agent-a","data":{"message":{"messageId":"m1","role":"ROLE_USER","parts":[{"text":"hi"}]}}}`)},
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	stored, err := ts.Get(ctx, "task-1", "agent-a", "session-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if stored.Version != 1 {
		t.Errorf("Get version = %d, want 1", stored.Version)
	}
	if stored.Task.Status.State != a2a.TaskStateWorking {
		t.Errorf("Get Status.State = %q, want %q", stored.Task.Status.State, a2a.TaskStateWorking)
	}
	if stored.Task.ContextID != "ctx-1" {
		t.Errorf("Get ContextID = %q, want %q", stored.Task.ContextID, "ctx-1")
	}
	// The caller's own message goes straight into History, matching
	// a2asrv's own loadExecutionContext (which appends a caller's new
	// message there immediately, never through Status.Message).
	// Status.Message stays nil: no Task-shaped reply from "agent-a" itself
	// was ever logged here, only its row's own Create/Update fields.
	if len(stored.Task.History) != 1 {
		t.Fatalf("Get History = %+v, want 1 entry", stored.Task.History)
	}
	if got, want := textOf(stored.Task.History[0]), "hi"; got != want {
		t.Errorf("Get History[0] = %q, want %q", got, want)
	}
	if stored.Task.Status.Message != nil {
		t.Errorf("Get Status.Message = %+v, want nil (no Task-shaped reply from agent-a was ever logged)", stored.Task.Status.Message)
	}

	updated := &a2a.Task{ID: "task-1", Status: a2a.TaskStatus{State: a2a.TaskStateCompleted}}
	newVersion, err := ts.Update(ctx, &taskstore.UpdateRequest{Task: updated, PrevVersion: version}, "agent-a", "session-1")
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if newVersion != 2 {
		t.Errorf("Update version = %d, want 2", newVersion)
	}

	stored, err = ts.Get(ctx, "task-1", "agent-a", "session-1")
	if err != nil {
		t.Fatalf("Get after update: %v", err)
	}
	if stored.Task.Status.State != a2a.TaskStateCompleted {
		t.Errorf("Get after update Status.State = %q, want %q", stored.Task.Status.State, a2a.TaskStateCompleted)
	}
	if stored.Version != 2 {
		t.Errorf("Get after update Version = %d, want 2", stored.Version)
	}
}

func TestSQLTaskStore_Get_UnknownTaskNotFound(t *testing.T) {
	ts, _ := newTestTaskStore(t)
	if _, err := ts.Get(context.Background(), "nonexistent", "agent-a", "session-1"); !errors.Is(err, a2a.ErrTaskNotFound) {
		t.Errorf("Get(nonexistent) error = %v, want a2a.ErrTaskNotFound", err)
	}
}

func TestSQLTaskStore_Update_UnknownTaskNotFound(t *testing.T) {
	ts, _ := newTestTaskStore(t)
	_, err := ts.Update(context.Background(), &taskstore.UpdateRequest{
		Task:        &a2a.Task{ID: "nonexistent", Status: a2a.TaskStatus{State: a2a.TaskStateCompleted}},
		PrevVersion: 1,
	}, "", "")
	if !errors.Is(err, a2a.ErrTaskNotFound) {
		t.Errorf("Update(nonexistent) error = %v, want a2a.ErrTaskNotFound", err)
	}
}

// TestSQLTaskStore_Update_ConcurrentModificationConflict confirms Update
// rejects a stale PrevVersion with taskstore.ErrConcurrentModification,
// rather than silently overwriting a write it never saw.
func TestSQLTaskStore_Update_ConcurrentModificationConflict(t *testing.T) {
	ts, _ := newTestTaskStore(t)
	ctx := context.Background()

	task := &a2a.Task{ID: "task-1", ContextID: "ctx-1", Status: a2a.TaskStatus{State: a2a.TaskStateWorking}}
	version, err := ts.Create(ctx, task, "", "")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// First update succeeds and advances the version.
	if _, err := ts.Update(ctx, &taskstore.UpdateRequest{
		Task:        &a2a.Task{ID: "task-1", Status: a2a.TaskStatus{State: a2a.TaskStateInputRequired}},
		PrevVersion: version,
	}, "", ""); err != nil {
		t.Fatalf("first Update: %v", err)
	}

	// A second update using the same, now-stale PrevVersion must fail.
	_, err = ts.Update(ctx, &taskstore.UpdateRequest{
		Task:        &a2a.Task{ID: "task-1", Status: a2a.TaskStatus{State: a2a.TaskStateCompleted}},
		PrevVersion: version,
	}, "", "")
	if !errors.Is(err, taskstore.ErrConcurrentModification) {
		t.Errorf("second Update (stale version) error = %v, want taskstore.ErrConcurrentModification", err)
	}

	// The task's actual state must reflect only the first update.
	stored, err := ts.Get(ctx, "task-1", "", "")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if stored.Task.Status.State != a2a.TaskStateInputRequired {
		t.Errorf("Status.State after rejected update = %q, want %q (the first update's, not the second's)", stored.Task.Status.State, a2a.TaskStateInputRequired)
	}
}

// TestSQLTaskStore_CancelTaskAndSession confirms the task's status and its
// session's state change together in one atomic commit, and a stale
// version rejects the whole attempt rather than applying just one write.
func TestSQLTaskStore_CancelTaskAndSession(t *testing.T) {
	ts, _ := newTestTaskStore(t)
	ctx := context.Background()

	bootstrap := envNew(a2a.MessageRoleUser, checkpointdIdentity, "agent-a", "seed")
	if err := ts.CreateSession(ctx, "session-1", "agent-a", "pod-1", "uid-1", bootstrap); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	task := &a2a.Task{
		ID:        "task-1",
		ContextID: "ctx-1",
		Status:    a2a.TaskStatus{State: a2a.TaskStateWorking},
	}
	version, err := ts.Create(ctx, task, "agent-a", "session-1")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := ts.CancelTaskAndSession(ctx, "task-1", "agent-a", "session-1", version); err != nil {
		t.Fatalf("CancelTaskAndSession: %v", err)
	}

	stored, err := ts.Get(ctx, "task-1", "agent-a", "session-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if stored.Task.Status.State != a2a.TaskStateCanceled {
		t.Errorf("task Status.State = %q, want %q", stored.Task.Status.State, a2a.TaskStateCanceled)
	}
	row, err := ts.GetSession(ctx, "session-1")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if row.state != sessionStateTerminating {
		t.Errorf("session state = %q, want %q", row.state, sessionStateTerminating)
	}

	// A second call using the same, now-stale version must fail, leaving
	// both rows exactly as the first call left them.
	if err := ts.CancelTaskAndSession(ctx, "task-1", "agent-a", "session-1", version); !errors.Is(err, taskstore.ErrConcurrentModification) {
		t.Errorf("second CancelTaskAndSession (stale version) error = %v, want taskstore.ErrConcurrentModification", err)
	}
	row, err = ts.GetSession(ctx, "session-1")
	if err != nil {
		t.Fatalf("GetSession after rejected second call: %v", err)
	}
	if row.state != sessionStateTerminating {
		t.Errorf("session state after rejected second call = %q, want still %q", row.state, sessionStateTerminating)
	}
}

func TestSQLTaskStore_Create_DuplicateTaskIDRejected(t *testing.T) {
	ts, _ := newTestTaskStore(t)
	ctx := context.Background()
	task := &a2a.Task{ID: "task-1", ContextID: "ctx-1", Status: a2a.TaskStatus{State: a2a.TaskStateWorking}}
	if _, err := ts.Create(ctx, task, "", ""); err != nil {
		t.Fatalf("first Create: %v", err)
	}
	if _, err := ts.Create(ctx, task, "", ""); !errors.Is(err, taskstore.ErrTaskAlreadyExists) {
		t.Errorf("second Create(same id) error = %v, want taskstore.ErrTaskAlreadyExists", err)
	}
}

// TestSQLTaskStore_SameTaskIDDifferentAgentOrSessionDoesNotCollide confirms
// two different agents, or the same agent across two sessions, can claim
// the same TaskID string without colliding: Get must return the right row
// for the right (agent, session) scoping.
func TestSQLTaskStore_SameTaskIDDifferentAgentOrSessionDoesNotCollide(t *testing.T) {
	ts, _ := newTestTaskStore(t)
	ctx := context.Background()

	const sharedID = a2a.TaskID("task-1")
	create := func(owner, session string, state a2a.TaskState) {
		t.Helper()
		if _, err := ts.Create(ctx, &a2a.Task{
			ID: sharedID, ContextID: "ctx-" + owner, Status: a2a.TaskStatus{State: state},
		}, owner, session); err != nil {
			t.Fatalf("Create(%s/%s): %v", owner, session, err)
		}
	}
	// Same TaskID, claimed by two different agents...
	create("agent-a", "session-1", a2a.TaskStateWorking)
	create("agent-b", "session-2", a2a.TaskStateInputRequired)
	// ...and again by the same agent across two different sessions.
	create("agent-a", "session-3", a2a.TaskStateCompleted)

	for _, tc := range []struct {
		owner, session string
		want           a2a.TaskState
	}{
		{"agent-a", "session-1", a2a.TaskStateWorking},
		{"agent-b", "session-2", a2a.TaskStateInputRequired},
		{"agent-a", "session-3", a2a.TaskStateCompleted},
	} {
		stored, err := ts.Get(ctx, sharedID, tc.owner, tc.session)
		if err != nil {
			t.Fatalf("Get(%s, %s/%s): %v", sharedID, tc.owner, tc.session, err)
		}
		if stored.Task.Status.State != tc.want {
			t.Errorf("Get(%s, %s/%s).Status.State = %q, want %q", sharedID, tc.owner, tc.session, stored.Task.Status.State, tc.want)
		}
		if stored.Task.ContextID != "ctx-"+tc.owner {
			t.Errorf("Get(%s, %s/%s).ContextID = %q, want %q", sharedID, tc.owner, tc.session, stored.Task.ContextID, "ctx-"+tc.owner)
		}
	}

	// The wrong scoping for a real TaskID must not accidentally resolve to
	// someone else's row.
	if _, err := ts.Get(ctx, sharedID, "agent-a", "session-2"); !errors.Is(err, a2a.ErrTaskNotFound) {
		t.Errorf("Get with mismatched scoping error = %v, want a2a.ErrTaskNotFound", err)
	}

	// A tenant-agnostic caller (no sessionID) still resolves fine as long
	// as (taskID, ownerAgent) is unambiguous: agent-b only claimed sharedID once.
	stored, err := ts.Get(ctx, sharedID, "agent-b", "")
	if err != nil {
		t.Fatalf("Get(%s, agent-b, <no tenant>): %v", sharedID, err)
	}
	if stored.Task.Status.State != a2a.TaskStateInputRequired {
		t.Errorf("Get(%s, agent-b, <no tenant>).Status.State = %q, want %q", sharedID, stored.Task.Status.State, a2a.TaskStateInputRequired)
	}

	// But agent-a claimed sharedID twice (session-1 and session-3) -- with
	// no tenant to disambiguate, this must report not-found, not silently
	// pick either one.
	if _, err := ts.Get(ctx, sharedID, "agent-a", ""); !errors.Is(err, a2a.ErrTaskNotFound) {
		t.Errorf("Get(%s, agent-a, <no tenant>) error = %v, want a2a.ErrTaskNotFound (genuinely ambiguous)", sharedID, err)
	}
}

// TestSQLTaskStore_ListForAgent_FiltersByAgentAndStatus confirms
// ListForAgent scopes to one agent's own tasks, and that req.Status
// filters correctly.
func TestSQLTaskStore_ListForAgent_FiltersByAgentAndStatus(t *testing.T) {
	ts, _ := newTestTaskStore(t)
	ctx := context.Background()

	create := func(id, owner string, state a2a.TaskState) {
		t.Helper()
		if _, err := ts.Create(ctx, &a2a.Task{
			ID: a2a.TaskID(id), ContextID: "ctx", Status: a2a.TaskStatus{State: state},
		}, owner, ""); err != nil {
			t.Fatalf("Create(%s): %v", id, err)
		}
	}
	create("a-working", "agent-a", a2a.TaskStateWorking)
	create("a-completed", "agent-a", a2a.TaskStateCompleted)
	create("b-working", "agent-b", a2a.TaskStateWorking)

	resp, err := ts.ListForAgent(ctx, "agent-a", &a2a.ListTasksRequest{})
	if err != nil {
		t.Fatalf("ListForAgent(agent-a, {}): %v", err)
	}
	if len(resp.Tasks) != 2 {
		t.Fatalf("ListForAgent(agent-a, {}) = %d tasks, want 2", len(resp.Tasks))
	}

	resp, err = ts.ListForAgent(ctx, "agent-a", &a2a.ListTasksRequest{Status: a2a.TaskStateCompleted})
	if err != nil {
		t.Fatalf("ListForAgent(agent-a, completed): %v", err)
	}
	if len(resp.Tasks) != 1 || resp.Tasks[0].ID != "a-completed" {
		t.Fatalf("ListForAgent(agent-a, completed) = %+v, want exactly [a-completed]", resp.Tasks)
	}

	resp, err = ts.ListForAgent(ctx, "agent-b", &a2a.ListTasksRequest{})
	if err != nil {
		t.Fatalf("ListForAgent(agent-b, {}): %v", err)
	}
	if len(resp.Tasks) != 1 || resp.Tasks[0].ID != "b-working" {
		t.Fatalf("ListForAgent(agent-b, {}) = %+v, want exactly [b-working]", resp.Tasks)
	}

	// No agent scoping must see everything.
	all, err := ts.list(ctx, "", &a2a.ListTasksRequest{})
	if err != nil {
		t.Fatalf("list({}): %v", err)
	}
	if len(all.Tasks) != 3 || all.TotalSize != 3 {
		t.Fatalf("list({}) = %d tasks (TotalSize %d), want 3", len(all.Tasks), all.TotalSize)
	}
}

// TestSQLTaskStore_List_PaginationRoundTrips confirms paging through
// NextPageToken visits every task exactly once, in a stable order, and
// terminates with an empty token.
func TestSQLTaskStore_List_PaginationRoundTrips(t *testing.T) {
	ts, _ := newTestTaskStore(t)
	ctx := context.Background()

	const n = 5
	for i := range n {
		id := fmt.Sprintf("task-%d", i)
		if _, err := ts.Create(ctx, &a2a.Task{
			ID: a2a.TaskID(id), ContextID: "ctx", Status: a2a.TaskStatus{State: a2a.TaskStateWorking},
		}, "agent-a", ""); err != nil {
			t.Fatalf("Create(%s): %v", id, err)
		}
	}

	seen := map[a2a.TaskID]bool{}
	pageToken := ""
	pages := 0
	for {
		resp, err := ts.ListForAgent(ctx, "agent-a", &a2a.ListTasksRequest{PageSize: 2, PageToken: pageToken})
		if err != nil {
			t.Fatalf("ListForAgent (page %d): %v", pages, err)
		}
		if resp.TotalSize != n {
			t.Errorf("page %d: TotalSize = %d, want %d", pages, resp.TotalSize, n)
		}
		for _, task := range resp.Tasks {
			if seen[task.ID] {
				t.Fatalf("task %s returned more than once across pages", task.ID)
			}
			seen[task.ID] = true
		}
		pages++
		if resp.NextPageToken == "" {
			break
		}
		pageToken = resp.NextPageToken
		if pages > n {
			t.Fatalf("pagination did not terminate after %d pages", pages)
		}
	}
	if len(seen) != n {
		t.Errorf("saw %d distinct tasks across %d pages, want %d", len(seen), pages, n)
	}
}
