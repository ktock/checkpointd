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
	"errors"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/ktock/checkpointd/internal/hop"
)

// hopMessage returns d's turn content: the bare Message a hop carries, or
// its Task's Status.Message when d is Task-shaped instead.
func hopMessage(d hop.Data) *a2a.Message {
	if d.Task != nil {
		return d.Task.Status.Message
	}
	return d.Message
}

func TestSQLTaskStore_CreateGetTerminateSession(t *testing.T) {
	ts, _ := newTestTaskStore(t)
	ctx := context.Background()

	bootstrap := envNew(a2a.MessageRoleUser, checkpointdIdentity, "agent-a", "seed")
	if err := ts.CreateSession(ctx, "session-1", "agent-a", bootstrap); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	row, err := ts.GetSession(ctx, "session-1")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if row.state != sessionStateRunning {
		t.Errorf("GetSession state = %q, want %q", row.state, sessionStateRunning)
	}
	// CreateSession/GetSession must round-trip the bootstrap message itself,
	// not just the session's bookkeeping fields.
	if got := textOf(hopMessage(row.bootstrap.Data)); got != "seed" {
		t.Errorf("GetSession bootstrap text = %q, want %q", got, "seed")
	}
	if row.ownerAgent != "agent-a" {
		t.Errorf("GetSession ownerAgent = %q, want %q", row.ownerAgent, "agent-a")
	}

	if err := ts.TerminateSession(ctx, "session-1"); err != nil {
		t.Fatalf("TerminateSession: %v", err)
	}
	row, err = ts.GetSession(ctx, "session-1")
	if err != nil {
		t.Fatalf("GetSession after terminate: %v", err)
	}
	if row.state != sessionStateTerminated {
		t.Errorf("GetSession state after terminate = %q, want %q", row.state, sessionStateTerminated)
	}

	// Idempotent: terminating an already-terminated session is a no-op, not
	// an error, so a resumed relay loop reaching its end again after a
	// crash doesn't fail on it.
	if err := ts.TerminateSession(ctx, "session-1"); err != nil {
		t.Errorf("TerminateSession (again): %v, want nil", err)
	}
}

// TestSQLTaskStore_MarkTerminating confirms a RUNNING session transitions
// to TERMINATING, and a stale MarkTerminating call can never move an
// already-TERMINATED session backward.
func TestSQLTaskStore_MarkTerminating(t *testing.T) {
	ts, _ := newTestTaskStore(t)
	ctx := context.Background()

	bootstrap := envNew(a2a.MessageRoleUser, checkpointdIdentity, "agent-a", "seed")
	if err := ts.CreateSession(ctx, "session-1", "agent-a", bootstrap); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if err := ts.MarkTerminating(ctx, "session-1"); err != nil {
		t.Fatalf("MarkTerminating: %v", err)
	}
	row, err := ts.GetSession(ctx, "session-1")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if row.state != sessionStateTerminating {
		t.Errorf("GetSession state = %q, want %q", row.state, sessionStateTerminating)
	}

	if err := ts.TerminateSession(ctx, "session-1"); err != nil {
		t.Fatalf("TerminateSession: %v", err)
	}

	// A late/duplicate MarkTerminating must stay a no-op here, not move the
	// session back out of TERMINATED.
	if err := ts.MarkTerminating(ctx, "session-1"); err != nil {
		t.Fatalf("MarkTerminating (after terminate): %v, want nil (no-op)", err)
	}
	row, err = ts.GetSession(ctx, "session-1")
	if err != nil {
		t.Fatalf("GetSession after late MarkTerminating: %v", err)
	}
	if row.state != sessionStateTerminated {
		t.Errorf("GetSession state after late MarkTerminating = %q, want still %q", row.state, sessionStateTerminated)
	}
}

func TestSQLTaskStore_GetSession_NotFound(t *testing.T) {
	ts, _ := newTestTaskStore(t)
	if _, err := ts.GetSession(context.Background(), "never-created"); !errors.Is(err, sql.ErrNoRows) {
		t.Errorf("GetSession for an id never created: err = %v, want sql.ErrNoRows", err)
	}
}

// TestSQLTaskStore_CreateSession_DuplicateIDFails confirms id collisions
// are caught structurally, via the primary key.
func TestSQLTaskStore_CreateSession_DuplicateIDFails(t *testing.T) {
	ts, _ := newTestTaskStore(t)
	ctx := context.Background()
	bootstrap := envNew(a2a.MessageRoleUser, checkpointdIdentity, "agent-a", "seed")
	if err := ts.CreateSession(ctx, "dup", "agent-a", bootstrap); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	err := ts.CreateSession(ctx, "dup", "agent-b", bootstrap)
	if err == nil {
		t.Fatal("CreateSession with a duplicate id = nil error, want one")
	}
	if !errors.Is(err, errDuplicateSessionID) {
		t.Errorf("CreateSession with a duplicate id: err = %v, want it to wrap errDuplicateSessionID", err)
	}
}

// TestSQLTaskStore_CreateSession_CanceledContextIsNotDuplicate confirms a
// non-collision failure.
func TestSQLTaskStore_CreateSession_CanceledContextIsNotDuplicate(t *testing.T) {
	ts, _ := newTestTaskStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	bootstrap := envNew(a2a.MessageRoleUser, checkpointdIdentity, "agent-a", "seed")
	err := ts.CreateSession(ctx, "canceled", "agent-a", bootstrap)
	if err == nil {
		t.Fatal("CreateSession with an already-canceled context = nil error, want one")
	}
	if errors.Is(err, errDuplicateSessionID) {
		t.Errorf("CreateSession with an already-canceled context misclassified as errDuplicateSessionID: %v", err)
	}
}
