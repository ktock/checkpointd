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
	"database/sql"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/ktock/checkpointd/internal/controller/eventlog"
	"github.com/ktock/checkpointd/internal/harness"
	"github.com/ktock/checkpointd/internal/hop"
)

// countConversationLogRows returns how many conversation_log rows carry
// sessionID -- a direct, table-level check independent of what GetTask
// reports. Takes the session id, not the task id, since conversation_log is
// keyed by session_id and a sweep may have already deleted the row a task
// id would otherwise resolve through.
func countConversationLogRows(t *testing.T, db *sql.DB, sessionID string) int {
	t.Helper()
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM conversation_log WHERE session_id = $1", sessionID).Scan(&n); err != nil {
		t.Fatalf("counting conversation_log rows for session %s: %v", sessionID, err)
	}
	return n
}

func checkpointdTaskRowExists(t *testing.T, db *sql.DB, taskID string) bool {
	t.Helper()
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM checkpointd_tasks WHERE task_id = $1", taskID).Scan(&n); err != nil {
		t.Fatalf("counting checkpointd_tasks rows for %s: %v", taskID, err)
	}
	return n > 0
}

// checkpointdSessionRowExists reports whether sessionID's checkpointd_sessions
// row -- which carries that session's bootstrap message directly -- still
// exists. A sweep leaving it behind would leak that message forever.
func checkpointdSessionRowExists(t *testing.T, db *sql.DB, sessionID string) bool {
	t.Helper()
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM checkpointd_sessions WHERE id = $1", sessionID).Scan(&n); err != nil {
		t.Fatalf("counting checkpointd_sessions rows for %s: %v", sessionID, err)
	}
	return n > 0
}

// backdateLastUpdated rewrites sessionID's last_updated column directly, so
// tests can simulate an old session without actually sleeping.
func backdateLastUpdated(t *testing.T, db *sql.DB, sessionID string, when time.Time) {
	t.Helper()
	if _, err := db.Exec("UPDATE checkpointd_sessions SET last_updated = $1 WHERE id = $2", when.UnixNano(), sessionID); err != nil {
		t.Fatalf("backdating last_updated for session %s: %v", sessionID, err)
	}
}

// TestSweepExpiredSessions_DeletesOnlyExpiredTerminatedSessions confirms a
// sweep deletes exactly the sessions both TERMINATED and older than its
// cutoff -- an old-but-RUNNING session and a TERMINATED-but-recent session
// must both survive with their history fully intact.
func TestSweepExpiredSessions_DeletesOnlyExpiredTerminatedSessions(t *testing.T) {
	a := &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		text := textOf(inMessage(in))
		if text == "seed-paused" {
			reply := taskReply(in, "a", "", a2a.TaskStateInputRequired, "waiting")
			return reply, nil
		}
		reply := taskReply(in, "a", "", a2a.TaskStateCompleted, "done:"+text)
		return reply, nil
	}}
	c, el, store := newTestServerController(t, map[string]harness.Harness{"a": a})
	defer c.Close()
	db, _, ok := eventlog.SQLDB(el)
	if !ok {
		t.Fatal("eventlog.SQLDB: not a SQL-backed EventLog")
	}

	registry := newTaskRegistry()
	h := &serverRequestHandler{agent: "a", c: c, el: el, store: store, registry: registry}

	send := func(text string) (a2a.TaskID, string) {
		t.Helper()
		res, err := h.SendMessage(context.Background(), &a2a.SendMessageRequest{
			Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart(text)),
		})
		if err != nil {
			t.Fatalf("SendMessage(%q): %v", text, err)
		}
		task := res.(*a2a.Task)
		sessionID, _ := task.Metadata[checkpointdTaskMetaTenant].(string)
		return task.ID, sessionID
	}

	oldExpiredID, oldExpiredSessionID := send("seed-old-expired")
	pollTask(t, h, oldExpiredID, oldExpiredSessionID, a2a.TaskStateCompleted)

	recentID, recentSessionID := send("seed-recent")
	pollTask(t, h, recentID, recentSessionID, a2a.TaskStateCompleted)

	oldPausedID, oldPausedSessionID := send("seed-paused")
	pollTask(t, h, oldPausedID, oldPausedSessionID, a2a.TaskStateInputRequired)

	// Both "old" sessions are backdated 10 days; the cutoff below is 5 days.
	// oldPausedSessionID stays ineligible regardless of age, since
	// InputRequired keeps its session RUNNING, not TERMINATED.
	now := time.Now()
	backdateLastUpdated(t, db, oldExpiredSessionID, now.Add(-10*24*time.Hour))
	backdateLastUpdated(t, db, oldPausedSessionID, now.Add(-10*24*time.Hour))

	deleted, err := sweepExpiredSessions(context.Background(), db, registry, now.Add(-5*24*time.Hour))
	if err != nil {
		t.Fatalf("sweepExpiredSessions: %v", err)
	}
	if deleted != 1 {
		t.Errorf("sweepExpiredSessions deleted %d session(s), want 1", deleted)
	}

	// The expired, terminal task is fully gone from all three tables.
	if checkpointdTaskRowExists(t, db, string(oldExpiredID)) {
		t.Error("checkpointd_tasks row for the expired task still exists")
	}
	if n := countConversationLogRows(t, db, oldExpiredSessionID); n != 0 {
		t.Errorf("conversation_log rows for the expired task = %d, want 0", n)
	}
	if checkpointdSessionRowExists(t, db, oldExpiredSessionID) {
		t.Error("checkpointd_sessions row for the expired task still exists (its own bootstrap message just leaked)")
	}
	if _, err := h.GetTask(context.Background(), &a2a.GetTaskRequest{ID: oldExpiredID, Tenant: oldExpiredSessionID}); err == nil {
		t.Error("GetTask for the expired, swept task: got nil error, want a2a.ErrTaskNotFound")
	}

	// The recent (not-yet-expired) terminal task survives, history intact.
	if !checkpointdTaskRowExists(t, db, string(recentID)) {
		t.Error("checkpointd_tasks row for the recent task was deleted, want it kept")
	}
	if n := countConversationLogRows(t, db, recentSessionID); n == 0 {
		t.Error("conversation_log rows for the recent task were deleted, want them kept")
	}
	recentTask, err := h.GetTask(context.Background(), &a2a.GetTaskRequest{ID: recentID, Tenant: recentSessionID})
	if err != nil {
		t.Fatalf("GetTask for the recent task: %v", err)
	}
	// Status.Message, not History: a single-turn task's current status
	// message is never duplicated as History's last entry too.
	if got, want := textOf(recentTask.Status.Message), "done:seed-recent"; got != want {
		t.Errorf("recent task's final reply = %q, want %q (history survived intact)", got, want)
	}

	// The old-but-never-terminal task also survives, regardless of age.
	if !checkpointdTaskRowExists(t, db, string(oldPausedID)) {
		t.Error("checkpointd_tasks row for the old-but-paused task was deleted, want it kept (never terminal)")
	}
	if n := countConversationLogRows(t, db, oldPausedSessionID); n == 0 {
		t.Error("conversation_log rows for the old-but-paused task were deleted, want them kept")
	}
}

// TestSweepExpiredSessions_SkipsActiveSession confirms a candidate row that
// matches the sweep query but is still marked active in the in-memory
// registry (taskRegistry.isActive) is skipped rather than deleted -- a
// defense-in-depth check that shouldn't normally trigger.
func TestSweepExpiredSessions_SkipsActiveSession(t *testing.T) {
	a := &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		reply := taskReply(in, "a", "", a2a.TaskStateCompleted, "done:"+textOf(inMessage(in)))
		return reply, nil
	}}
	c, el, store := newTestServerController(t, map[string]harness.Harness{"a": a})
	defer c.Close()
	db, _, ok := eventlog.SQLDB(el)
	if !ok {
		t.Fatal("eventlog.SQLDB: not a SQL-backed EventLog")
	}

	registry := newTaskRegistry()
	h := &serverRequestHandler{agent: "a", c: c, el: el, store: store, registry: registry}

	res, err := h.SendMessage(context.Background(), &a2a.SendMessageRequest{
		Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("seed")),
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	task := res.(*a2a.Task)
	taskID := task.ID
	sessionID, _ := task.Metadata[checkpointdTaskMetaTenant].(string)
	pollTask(t, h, taskID, sessionID, a2a.TaskStateCompleted)

	now := time.Now()
	backdateLastUpdated(t, db, sessionID, now.Add(-10*24*time.Hour))

	// Artificially mark it active, standing in for the "shouldn't happen"
	// case this check guards against. registry.acquire races the relay
	// goroutine's own release, which can still be pending briefly after
	// GetTask already observes Completed -- poll instead of asserting on
	// the first attempt, since that's what made this test flake.
	var release func()
	for deadline := time.Now().Add(2 * time.Second); ; {
		var acquired bool
		if _, release, acquired = registry.acquire(context.Background(), sessionID); acquired {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("registry.acquire: session still active 2s after its task completed")
		}
		time.Sleep(5 * time.Millisecond)
	}
	defer release()

	deleted, err := sweepExpiredSessions(context.Background(), db, registry, now.Add(-5*24*time.Hour))
	if err != nil {
		t.Fatalf("sweepExpiredSessions: %v", err)
	}
	if deleted != 0 {
		t.Errorf("sweepExpiredSessions deleted %d session(s), want 0 (session is marked active)", deleted)
	}
	if !checkpointdTaskRowExists(t, db, string(taskID)) {
		t.Error("checkpointd_tasks row for the active session's task was deleted, want it kept")
	}
}
