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

package eventlog

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/ktock/checkpointd/proto"
)

// testEventLog runs the EventLog contract against a backend. newLog returns a
// fresh, empty event log for each subtest, using the subtest's *testing.T for
// temp dirs and cleanup.
//
// Subtests derive their IDs from t.Name() (which includes the backend and
// subtest name) and clear them with DeleteAll before and after running, so they
// are safe against the shared Postgres database and harmless against SQLite's
// per-test file.
func testEventLog(t *testing.T, newLog func(t *testing.T) EventLog) {
	t.Run("AppendAndEvents", func(t *testing.T) {
		ctx := context.Background()
		log := newLog(t)

		conv := t.Name() + "-conv-1"
		task1 := t.Name() + "-task-1"
		task2 := t.Name() + "-task-2"

		// 1. Conversation log.
		cev1 := &proto.StepEvent{ConversationId: conv, InteractionId: task1}
		cev2 := &proto.StepEvent{ConversationId: conv, InteractionId: task2}
		s1, err := log.Append(ctx, cev1)
		if err != nil {
			t.Fatalf("failed to append cev1: %v", err)
		}
		s2, err := log.Append(ctx, cev2)
		if err != nil {
			t.Fatalf("failed to append cev2: %v", err)
		}

		cEvents, err := log.Events(ctx, conv)
		if err != nil {
			t.Fatalf("failed to read conversation events: %v", err)
		}
		if len(cEvents) != 2 {
			t.Fatalf("expected 2 conversation events, got %d", len(cEvents))
		}
		if s1 != 1 || s2 != 2 {
			t.Errorf("conversation events out of order: %d, %d", s1, s2)
		}
		if cEvents[0].InteractionId != task1 || cEvents[1].InteractionId != task2 {
			t.Errorf("conversation events mismatch: %q, %q", cEvents[0].InteractionId, cEvents[1].InteractionId)
		}

	})

	t.Run("Empty", func(t *testing.T) {
		ctx := context.Background()
		log := newLog(t)

		events, err := log.Events(ctx, t.Name()+"-none")
		if err != nil {
			t.Fatalf("failed to read events: %v", err)
		}
		if len(events) != 0 {
			t.Fatalf("expected 0 events, got %d", len(events))
		}
	})

	// EventsBySessionID confirms one task's full history comes back
	// correctly ordered even though it spans multiple conversation_ids, and
	// that other tasks' events (or events with no task at all) never leak in.
	t.Run("EventsBySessionID", func(t *testing.T) {
		ctx := context.Background()
		log := newLog(t)

		task := t.Name() + "-task"
		otherTask := t.Name() + "-other-task"
		convA := t.Name() + "-conv-a"
		convB := t.Name() + "-conv-b"

		mustAppend := func(conversationID, taskID, marker string) {
			t.Helper()
			if _, err := log.Append(ctx, &proto.StepEvent{ConversationId: conversationID, SessionId: taskID, InteractionId: marker}); err != nil {
				t.Fatalf("append(%q, %q, %q): %v", conversationID, taskID, marker, err)
			}
		}

		// Interleaved on purpose: convA's two turns, convB's one turn in
		// between, a same-conversationID/different-task event, and a
		// no-task-at-all event -- none of which EventsBySessionID(task)
		// should ever return except the four genuinely belonging to task.
		mustAppend(convA, task, "a1")
		mustAppend(convB, task, "b1")
		mustAppend(convA, otherTask, "a-other-task")
		mustAppend(convA, "", "a-no-task")
		mustAppend(convA, task, "a2")
		mustAppend(convB, task, "b2")

		events, err := log.EventsBySessionID(ctx, task)
		if err != nil {
			t.Fatalf("EventsBySessionID: %v", err)
		}
		var gotMarkers []string
		for _, ev := range events {
			gotMarkers = append(gotMarkers, ev.InteractionId)
		}
		want := []string{"a1", "b1", "a2", "b2"}
		if len(gotMarkers) != len(want) {
			t.Fatalf("EventsBySessionID markers = %v, want %v", gotMarkers, want)
		}
		for i := range want {
			if gotMarkers[i] != want[i] {
				t.Errorf("EventsBySessionID markers = %v, want %v", gotMarkers, want)
				break
			}
		}

		// A task_id nobody ever appended with, and the empty task_id
		// itself, both come back empty rather than erroring or matching
		// unrelated rows.
		if events, err := log.EventsBySessionID(ctx, t.Name()+"-nonexistent-task"); err != nil || len(events) != 0 {
			t.Errorf("EventsBySessionID(unknown task) = %v, %v, want empty, nil", events, err)
		}
		if events, err := log.EventsBySessionID(ctx, ""); err != nil || len(events) != 0 {
			t.Errorf(`EventsBySessionID("") = %v, %v, want empty, nil (must never match no-task-id events)`, events, err)
		}
	})

	// ConcurrentTasksDoNotRaceTaskStep appends to many distinct task_ids
	// concurrently and confirms each task's own step sequence comes out
	// dense and gap-free, never colliding with another task's counter.
	t.Run("ConcurrentTasksDoNotRaceTaskStep", func(t *testing.T) {
		ctx := context.Background()
		log := newLog(t)

		const numTasks = 8
		const eventsPerTask = 5
		var wg sync.WaitGroup
		errs := make(chan error, numTasks)
		for i := range numTasks {
			task := fmt.Sprintf("%s-task-%d", t.Name(), i)
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range eventsPerTask {
					if _, err := log.Append(ctx, &proto.StepEvent{ConversationId: task, SessionId: task}); err != nil {
						errs <- err
						return
					}
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatalf("concurrent append failed: %v", err)
		}

		for i := range numTasks {
			task := fmt.Sprintf("%s-task-%d", t.Name(), i)
			events, err := log.EventsBySessionID(ctx, task)
			if err != nil {
				t.Fatalf("EventsBySessionID(%s): %v", task, err)
			}
			if len(events) != eventsPerTask {
				t.Errorf("task %s: got %d events, want %d", task, len(events), eventsPerTask)
			}
		}
	})

	// AutoStep exercises the step==0 auto-assignment path: appends with Step unset
	// receive sequential numbers starting at 1.
	t.Run("AutoStep", func(t *testing.T) {
		ctx := context.Background()
		log := newLog(t)

		conv := t.Name() + "-conv"

		const n = 3
		for i := int64(1); i <= n; i++ {
			step, err := log.Append(ctx, &proto.StepEvent{ConversationId: conv, InteractionId: "t"})
			if err != nil {
				t.Fatalf("auto-step append failed: %v", err)
			}
			if step != i {
				t.Errorf("expected step %d, got %d", i, step)
			}
		}

		events, err := log.Events(ctx, conv)
		if err != nil {
			t.Fatalf("failed to read events: %v", err)
		}
		if len(events) != n {
			t.Fatalf("expected %d events, got %d", n, len(events))
		}
	})
}

// TestSQLiteEventLog runs the EventLog contract against the SQLite backend.
func TestSQLiteEventLog(t *testing.T) {
	testEventLog(t, func(t *testing.T) EventLog {
		t.Helper()
		log, err := OpenSQLiteEventLog(filepath.Join(t.TempDir(), "test.db"))
		if err != nil {
			t.Fatalf("failed to open sqlite event log: %v", err)
		}
		t.Cleanup(func() { log.Close() })
		return log
	})
}

// TestPostgresEventLog runs the EventLog contract against the Postgres backend
// described by PG_TEST_DSN, skipping when that variable is not set.
func TestPostgresEventLog(t *testing.T) {
	dsn := os.Getenv("PG_TEST_DSN")
	if dsn == "" {
		t.Skip("PG_TEST_DSN not set; skipping Postgres event log tests")
	}
	testEventLog(t, func(t *testing.T) EventLog {
		t.Helper()
		log, err := OpenPostgresEventLog(dsn)
		if err != nil {
			t.Fatalf("failed to open postgres event log: %v", err)
		}
		t.Cleanup(func() { log.Close() })
		return log
	})
}

// TestSQLiteEventLog_CreatesParentDirectory is SQLite-specific: OpenSQLiteEventLog
// must create the database file's parent directory if it does not exist.
func TestSQLiteEventLog_CreatesParentDirectory(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "newdir", "test.db")

	log, err := OpenSQLiteEventLog(dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite event log and create directory: %v", err)
	}
	defer log.Close()

	if _, err := os.Stat(filepath.Dir(dbPath)); os.IsNotExist(err) {
		t.Fatalf("expected parent directory to be created, but it does not exist")
	}
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Fatalf("expected database file to be created, but it does not exist")
	}
}
