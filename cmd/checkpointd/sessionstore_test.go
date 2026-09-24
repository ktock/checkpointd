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
	"sync"
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
	if err := ts.CreateSession(ctx, "session-1", "agent-a", "pod-1", "uid-1", bootstrap); err != nil {
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
	if row.ownerPod != "pod-1" {
		t.Errorf("GetSession ownerPod = %q, want %q", row.ownerPod, "pod-1")
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
	if err := ts.CreateSession(ctx, "session-1", "agent-a", "pod-1", "uid-1", bootstrap); err != nil {
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
	if err := ts.CreateSession(ctx, "dup", "agent-a", "pod-1", "uid-1", bootstrap); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	err := ts.CreateSession(ctx, "dup", "agent-b", "pod-1", "uid-1", bootstrap)
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
	err := ts.CreateSession(ctx, "canceled", "agent-a", "pod-1", "uid-1", bootstrap)
	if err == nil {
		t.Fatal("CreateSession with an already-canceled context = nil error, want one")
	}
	if errors.Is(err, errDuplicateSessionID) {
		t.Errorf("CreateSession with an already-canceled context misclassified as errDuplicateSessionID: %v", err)
	}
}

// TestSQLTaskStore_ClaimSession_SucceedsWhenUnowned confirms a session
// created with no owner (or already released back to "") can be claimed.
func TestSQLTaskStore_ClaimSession_SucceedsWhenUnowned(t *testing.T) {
	ts, _ := newTestTaskStore(t)
	ctx := context.Background()
	bootstrap := envNew(a2a.MessageRoleUser, checkpointdIdentity, "agent-a", "seed")
	if err := ts.CreateSession(ctx, "session-1", "agent-a", "", "", bootstrap); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	claimed, err := ts.ClaimSession(ctx, "session-1", "pod-1", "uid-1")
	if err != nil {
		t.Fatalf("ClaimSession: %v", err)
	}
	if !claimed {
		t.Fatal("ClaimSession on an unowned session = false, want true")
	}
	row, err := ts.GetSession(ctx, "session-1")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if row.ownerPod != "pod-1" {
		t.Errorf("GetSession ownerPod after claim = %q, want %q", row.ownerPod, "pod-1")
	}
}

// TestSQLTaskStore_ClaimSession_FailsWhenAlreadyOwned confirms the CAS
// refuses to hand off a session that's already owned, by anyone -- including
// the same instance trying again -- without mutating the row.
func TestSQLTaskStore_ClaimSession_FailsWhenAlreadyOwned(t *testing.T) {
	ts, _ := newTestTaskStore(t)
	ctx := context.Background()
	bootstrap := envNew(a2a.MessageRoleUser, checkpointdIdentity, "agent-a", "seed")
	if err := ts.CreateSession(ctx, "session-1", "agent-a", "pod-1", "uid-1", bootstrap); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	claimed, err := ts.ClaimSession(ctx, "session-1", "pod-2", "uid-2")
	if err != nil {
		t.Fatalf("ClaimSession: %v", err)
	}
	if claimed {
		t.Fatal("ClaimSession on an already-owned session = true, want false")
	}
	row, err := ts.GetSession(ctx, "session-1")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if row.ownerPod != "pod-1" {
		t.Errorf("GetSession ownerPod after a failed claim = %q, want unchanged %q", row.ownerPod, "pod-1")
	}
}

// TestSQLTaskStore_ReleaseSession_ClearsOwner confirms ReleaseSession clears
// owner_pod/owner_uid unconditionally, regardless of current state.
func TestSQLTaskStore_ReleaseSession_ClearsOwner(t *testing.T) {
	ts, _ := newTestTaskStore(t)
	ctx := context.Background()
	bootstrap := envNew(a2a.MessageRoleUser, checkpointdIdentity, "agent-a", "seed")
	if err := ts.CreateSession(ctx, "session-1", "agent-a", "pod-1", "uid-1", bootstrap); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if err := ts.ReleaseSession(ctx, "session-1"); err != nil {
		t.Fatalf("ReleaseSession: %v", err)
	}
	row, err := ts.GetSession(ctx, "session-1")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if row.ownerPod != "" {
		t.Errorf("GetSession ownerPod after release = %q, want empty", row.ownerPod)
	}
	// A second, later claim by a different instance must now succeed.
	claimed, err := ts.ClaimSession(ctx, "session-1", "pod-2", "uid-2")
	if err != nil {
		t.Fatalf("ClaimSession after release: %v", err)
	}
	if !claimed {
		t.Fatal("ClaimSession after release = false, want true")
	}
}

// TestSQLTaskStore_CountOwnedRunningSessions confirms the count only
// includes RUNNING sessions currently owned by the given instance.
func TestSQLTaskStore_CountOwnedRunningSessions(t *testing.T) {
	ts, _ := newTestTaskStore(t)
	ctx := context.Background()
	bootstrap := envNew(a2a.MessageRoleUser, checkpointdIdentity, "agent-a", "seed")
	if err := ts.CreateSession(ctx, "owned-running", "agent-a", "pod-1", "uid-1", bootstrap); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if err := ts.CreateSession(ctx, "owned-terminating", "agent-a", "pod-1", "uid-1", bootstrap); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if err := ts.MarkTerminating(ctx, "owned-terminating"); err != nil {
		t.Fatalf("MarkTerminating: %v", err)
	}
	if err := ts.CreateSession(ctx, "other-instance", "agent-a", "pod-2", "uid-2", bootstrap); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	n, err := ts.CountOwnedRunningSessions(ctx, "pod-1")
	if err != nil {
		t.Fatalf("CountOwnedRunningSessions: %v", err)
	}
	if n != 1 {
		t.Errorf("CountOwnedRunningSessions(instance-1) = %d, want 1 (only owned-running is RUNNING and owned by instance-1)", n)
	}
}

// TestSQLTaskStore_ClaimOrphanedSession_SucceedsWhenUIDMatches confirms the
// salvage/restamp CAS succeeds and rewrites both owner fields when the
// currently recorded owner_uid still matches what the caller expected.
func TestSQLTaskStore_ClaimOrphanedSession_SucceedsWhenUIDMatches(t *testing.T) {
	ts, _ := newTestTaskStore(t)
	ctx := context.Background()
	bootstrap := envNew(a2a.MessageRoleUser, checkpointdIdentity, "agent-a", "seed")
	if err := ts.CreateSession(ctx, "session-1", "agent-a", "pod-a", "uid-old", bootstrap); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	claimed, err := ts.ClaimOrphanedSession(ctx, "session-1", "pod-b", "uid-new", "uid-old")
	if err != nil {
		t.Fatalf("ClaimOrphanedSession: %v", err)
	}
	if !claimed {
		t.Fatal("ClaimOrphanedSession with a matching expected uid = false, want true")
	}
	row, err := ts.GetSession(ctx, "session-1")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if row.ownerPod != "pod-b" || row.ownerUID != "uid-new" {
		t.Errorf("GetSession after claim = (%q, %q), want (%q, %q)", row.ownerPod, row.ownerUID, "pod-b", "uid-new")
	}
}

// TestSQLTaskStore_ClaimOrphanedSession_FailsWhenUIDStale confirms the CAS
// refuses to hand off a session whose currently recorded owner_uid no
// longer matches what the caller expected, leaving the row untouched.
func TestSQLTaskStore_ClaimOrphanedSession_FailsWhenUIDStale(t *testing.T) {
	ts, _ := newTestTaskStore(t)
	ctx := context.Background()
	bootstrap := envNew(a2a.MessageRoleUser, checkpointdIdentity, "agent-a", "seed")
	if err := ts.CreateSession(ctx, "session-1", "agent-a", "pod-a", "uid-old", bootstrap); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	claimed, err := ts.ClaimOrphanedSession(ctx, "session-1", "pod-b", "uid-new", "wrong-expected-uid")
	if err != nil {
		t.Fatalf("ClaimOrphanedSession: %v", err)
	}
	if claimed {
		t.Fatal("ClaimOrphanedSession with a stale expected uid = true, want false")
	}
	row, err := ts.GetSession(ctx, "session-1")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if row.ownerPod != "pod-a" || row.ownerUID != "uid-old" {
		t.Errorf("GetSession after a failed claim = (%q, %q), want unchanged (%q, %q)", row.ownerPod, row.ownerUID, "pod-a", "uid-old")
	}
}

// TestSQLTaskStore_ClaimOrphanedSession_OnlyOneOfTwoRacingClaimsSucceeds is
// the concurrency guarantee the whole salvage design leans on: two
// instances racing to claim the same apparently-orphaned session must never
// both succeed.
func TestSQLTaskStore_ClaimOrphanedSession_OnlyOneOfTwoRacingClaimsSucceeds(t *testing.T) {
	ts, _ := newTestTaskStore(t)
	ctx := context.Background()
	bootstrap := envNew(a2a.MessageRoleUser, checkpointdIdentity, "agent-a", "seed")
	if err := ts.CreateSession(ctx, "session-1", "agent-a", "pod-a", "uid-old", bootstrap); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	results := make(chan bool, 2)
	var wg sync.WaitGroup
	for _, cand := range []struct{ pod, uid string }{{"pod-b", "uid-b"}, {"pod-c", "uid-c"}} {
		wg.Add(1)
		go func(pod, uid string) {
			defer wg.Done()
			claimed, err := ts.ClaimOrphanedSession(ctx, "session-1", pod, uid, "uid-old")
			if err != nil {
				t.Errorf("ClaimOrphanedSession(%s, %s): %v", pod, uid, err)
				return
			}
			results <- claimed
		}(cand.pod, cand.uid)
	}
	wg.Wait()
	close(results)

	successes := 0
	for r := range results {
		if r {
			successes++
		}
	}
	if successes != 1 {
		t.Errorf("racing ClaimOrphanedSession calls: %d succeeded, want exactly 1", successes)
	}

	row, err := ts.GetSession(ctx, "session-1")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if row.ownerPod != "pod-b" && row.ownerPod != "pod-c" {
		t.Errorf("GetSession after racing claims: ownerPod = %q, want either pod-b or pod-c (whichever won)", row.ownerPod)
	}
}

// TestSQLTaskStore_ListDistinctActiveOwners_ExcludesUnownedAndTerminal
// confirms only non-terminal, currently-owned sessions' (owner_pod,
// owner_uid) pairs are returned -- unowned (released) and TERMINATED rows
// must never contribute one, regardless of which instance is running the
// sweep.
func TestSQLTaskStore_ListDistinctActiveOwners_ExcludesUnownedAndTerminal(t *testing.T) {
	ts, _ := newTestTaskStore(t)
	ctx := context.Background()
	bootstrap := envNew(a2a.MessageRoleUser, checkpointdIdentity, "agent-a", "seed")

	if err := ts.CreateSession(ctx, "owned-running", "agent-a", "pod-a", "uid-a", bootstrap); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if err := ts.CreateSession(ctx, "owned-terminating", "agent-a", "pod-b", "uid-b", bootstrap); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if err := ts.MarkTerminating(ctx, "owned-terminating"); err != nil {
		t.Fatalf("MarkTerminating: %v", err)
	}
	if err := ts.CreateSession(ctx, "released", "agent-a", "pod-c", "uid-c", bootstrap); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if err := ts.ReleaseSession(ctx, "released"); err != nil {
		t.Fatalf("ReleaseSession: %v", err)
	}
	if err := ts.CreateSession(ctx, "terminated", "agent-a", "pod-d", "uid-d", bootstrap); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if err := ts.TerminateSession(ctx, "terminated"); err != nil {
		t.Fatalf("TerminateSession: %v", err)
	}
	// A second session sharing pod-a's exact (pod, uid), so the DISTINCT
	// itself is exercised, not just the state/owner_pod filter.
	if err := ts.CreateSession(ctx, "owned-running-2", "agent-a", "pod-a", "uid-a", bootstrap); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	// A third session sharing pod-a's name but a *different* uid, so the
	// pairing (not just the pod name) is exercised.
	if err := ts.CreateSession(ctx, "owned-running-3", "agent-a", "pod-a", "uid-a2", bootstrap); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	got, err := ts.ListDistinctActiveOwners(ctx)
	if err != nil {
		t.Fatalf("ListDistinctActiveOwners: %v", err)
	}
	gotSet := make(map[ownerIncarnation]bool, len(got))
	for _, o := range got {
		gotSet[o] = true
	}
	want := map[ownerIncarnation]bool{
		{pod: "pod-a", uid: "uid-a"}:  true,
		{pod: "pod-a", uid: "uid-a2"}: true,
		{pod: "pod-b", uid: "uid-b"}:  true,
	}
	if len(gotSet) != len(want) {
		t.Errorf("ListDistinctActiveOwners = %v, want exactly %v", got, want)
	}
	for o := range want {
		if !gotSet[o] {
			t.Errorf("ListDistinctActiveOwners missing expected owner %+v", o)
		}
	}
	for o := range gotSet {
		if !want[o] {
			t.Errorf("ListDistinctActiveOwners unexpectedly returned %+v (should be excluded: released or terminated)", o)
		}
	}
}

// TestSQLTaskStore_ListSessionsOwnedByPodUID_ScopesToExactUID confirms
// sessions currently owned by the same pod name but a *different* uid are
// excluded -- this is what lets the salvage sweep re-select a specific dead
// incarnation's own sessions without also picking up a same-named
// replacement's already-legitimate claim.
func TestSQLTaskStore_ListSessionsOwnedByPodUID_ScopesToExactUID(t *testing.T) {
	ts, _ := newTestTaskStore(t)
	ctx := context.Background()
	bootstrap := envNew(a2a.MessageRoleUser, checkpointdIdentity, "agent-a", "seed")

	if err := ts.CreateSession(ctx, "old-incarnation", "agent-a", "pod-a", "old-uid", bootstrap); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if err := ts.CreateSession(ctx, "new-incarnation", "agent-a", "pod-a", "new-uid", bootstrap); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	sessions, next, err := ts.ListSessionsOwnedByPodUID(ctx, "", "pod-a", "old-uid")
	if err != nil {
		t.Fatalf("ListSessionsOwnedByPodUID: %v", err)
	}
	if next != "" {
		t.Errorf("ListSessionsOwnedByPodUID nextPageToken = %q, want empty", next)
	}
	if len(sessions) != 1 || sessions[0].id != "old-incarnation" {
		t.Errorf("ListSessionsOwnedByPodUID(pod-a, old-uid) = %v, want exactly [old-incarnation]", sessions)
	}
}
