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
	"sync"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/ktock/checkpointd/internal/harness"
	"github.com/ktock/checkpointd/internal/hop"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// fakePodExistenceChecker is podExistenceChecker's test double. alive is
// keyed by "podName/uid" -> true for a live (non-Failed, exact-uid) pod,
// absent means not alive. errs, when set for a pod name, makes PodExists
// return that error instead -- for testing the fail-closed path. forbid,
// when set for a "podName/uid" key, fails the test if PodExists is ever
// called for it, so a test can prove sweepOrphanedSessions skipped it
// locally (e.g. its own incarnation) without ever reaching the Pod lookup.
// before, when set, runs immediately before PodExists returns, letting a
// test inject a concurrent write (e.g. a same-named replacement's own
// self-recovery claim) between the liveness check and the caller's later
// re-selection of that owner's sessions.
type fakePodExistenceChecker struct {
	mu     sync.Mutex
	alive  map[string]bool
	errs   map[string]error
	forbid map[string]bool
	before func(namespace, podName, uid string)
	t      *testing.T
}

func (f *fakePodExistenceChecker) PodExists(ctx context.Context, namespace, podName, uid string) (bool, error) {
	key := podName + "/" + uid
	f.mu.Lock()
	forbidden := f.forbid != nil && f.forbid[key]
	f.mu.Unlock()
	if forbidden {
		f.t.Errorf("PodExists unexpectedly called for %q -- caller should have skipped it locally", key)
	}
	if err, ok := f.errs[podName]; ok {
		return false, err
	}
	if f.before != nil {
		f.before(namespace, podName, uid)
	}
	return f.alive[key], nil
}

// TestClientsetPodExistenceChecker_PodExists confirms the real,
// client-go-backed checker's outcomes: a genuinely Running pod under the
// exact uid is (true, nil); a missing pod, a Failed-phase pod, and a pod
// that exists under a *different* uid (already replaced) are all (false,
// nil) -- equally safe to salvage without waiting on Kubernetes GC or on
// the old uid's Pod object ever disappearing.
func TestClientsetPodExistenceChecker_PodExists(t *testing.T) {
	alive := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "alive-pod", Namespace: "ns", UID: "live-uid"},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
	failed := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "failed-pod", Namespace: "ns", UID: "dead-uid"},
		Status:     corev1.PodStatus{Phase: corev1.PodFailed},
	}
	cs := fake.NewClientset(alive, failed)
	checker := &clientsetPodExistenceChecker{cs: cs}
	ctx := context.Background()

	tests := []struct {
		name, podName, uid string
		want               bool
	}{
		{"alive, matching uid", "alive-pod", "live-uid", true},
		{"alive, but a different (stale) uid", "alive-pod", "old-uid", false},
		{"never existed", "no-such-pod", "any-uid", false},
		{"Failed, matching uid", "failed-pod", "dead-uid", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := checker.PodExists(ctx, "ns", tt.podName, tt.uid)
			if err != nil {
				t.Fatalf("PodExists: %v", err)
			}
			if got != tt.want {
				t.Errorf("PodExists(%q, %q) = %v, want %v", tt.podName, tt.uid, got, tt.want)
			}
		})
	}
}

// TestSweepOrphanedSessions_ClaimsDeadOwner confirms a session owned by an
// incarnation that's no longer alive is claimed and resumed.
func TestSweepOrphanedSessions_ClaimsDeadOwner(t *testing.T) {
	a := &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		return taskReply(in, "a", "", a2a.TaskStateCompleted, "done"), nil
	}}
	c, el, store := newTestServerController(t, map[string]harness.Harness{"a": a})
	ctx := context.Background()
	registry := newTestRegistry(t)

	bootstrap := envNew(a2a.MessageRoleUser, "checkpointd", "a", "seed")
	bk, err := newServerSession(ctx, store, el, "a", "checkpointd-server-1", "old-uid", bootstrap)
	if err != nil {
		t.Fatalf("newServerSession: %v", err)
	}

	checker := &fakePodExistenceChecker{alive: map[string]bool{}, t: t}
	claimed, err := sweepOrphanedSessions(ctx, c, el, store, registry, checker, "ns", "checkpointd-server-0", "this-uid")
	if err != nil {
		t.Fatalf("sweepOrphanedSessions: %v", err)
	}
	if claimed != 1 {
		t.Fatalf("sweepOrphanedSessions claimed = %d, want 1", claimed)
	}
	row := pollOwnerPod(t, store, bk.sessionID, "checkpointd-server-0")
	if row.ownerUID != "this-uid" {
		t.Errorf("after salvage, ownerUID = %q, want %q", row.ownerUID, "this-uid")
	}
	pollTaskState(t, store, bk.sessionID, a2a.TaskStateCompleted)
}

// TestSweepOrphanedSessions_SkipsAliveOwner confirms a session owned by an
// incarnation that's currently alive is left untouched.
func TestSweepOrphanedSessions_SkipsAliveOwner(t *testing.T) {
	a := &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		t.Fatal("agent must not be invoked -- the session's owner pod is still alive")
		return nil, nil
	}}
	c, el, store := newTestServerController(t, map[string]harness.Harness{"a": a})
	ctx := context.Background()
	registry := newTestRegistry(t)

	bootstrap := envNew(a2a.MessageRoleUser, "checkpointd", "a", "seed")
	bk, err := newServerSession(ctx, store, el, "a", "checkpointd-server-1", "live-uid", bootstrap)
	if err != nil {
		t.Fatalf("newServerSession: %v", err)
	}

	checker := &fakePodExistenceChecker{alive: map[string]bool{"checkpointd-server-1/live-uid": true}, t: t}
	claimed, err := sweepOrphanedSessions(ctx, c, el, store, registry, checker, "ns", "checkpointd-server-0", "this-uid")
	if err != nil {
		t.Fatalf("sweepOrphanedSessions: %v", err)
	}
	if claimed != 0 {
		t.Fatalf("sweepOrphanedSessions claimed = %d, want 0", claimed)
	}
	row, err := store.GetSession(ctx, bk.sessionID)
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if row.ownerPod != "checkpointd-server-1" || row.ownerUID != "live-uid" {
		t.Errorf("GetSession after sweep = (%q, %q), want unchanged (%q, %q)", row.ownerPod, row.ownerUID, "checkpointd-server-1", "live-uid")
	}
}

// TestSweepOrphanedSessions_FailsClosedOnCheckerError confirms a PodExists
// error (ambiguous/transient) must never be treated as "not alive" -- the
// correctness-critical case this whole design depends on.
func TestSweepOrphanedSessions_FailsClosedOnCheckerError(t *testing.T) {
	a := &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		t.Fatal("agent must not be invoked -- a checker error must never be treated as an orphan")
		return nil, nil
	}}
	c, el, store := newTestServerController(t, map[string]harness.Harness{"a": a})
	ctx := context.Background()
	registry := newTestRegistry(t)

	bootstrap := envNew(a2a.MessageRoleUser, "checkpointd", "a", "seed")
	bk, err := newServerSession(ctx, store, el, "a", "checkpointd-server-1", "flaky-uid", bootstrap)
	if err != nil {
		t.Fatalf("newServerSession: %v", err)
	}

	checker := &fakePodExistenceChecker{
		alive: map[string]bool{},
		errs:  map[string]error{"checkpointd-server-1": errors.New("timeout talking to the Kubernetes API")},
		t:     t,
	}
	claimed, err := sweepOrphanedSessions(ctx, c, el, store, registry, checker, "ns", "checkpointd-server-0", "this-uid")
	if err != nil {
		t.Fatalf("sweepOrphanedSessions: %v", err)
	}
	if claimed != 0 {
		t.Fatalf("sweepOrphanedSessions claimed = %d, want 0 (checker error must be skipped, not treated as orphaned)", claimed)
	}
	row, err := store.GetSession(ctx, bk.sessionID)
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if row.ownerPod != "checkpointd-server-1" || row.ownerUID != "flaky-uid" {
		t.Errorf("GetSession after a checker error = (%q, %q), want untouched (%q, %q)", row.ownerPod, row.ownerUID, "checkpointd-server-1", "flaky-uid")
	}
}

// TestSweepOrphanedSessions_SkipsOwnIncarnation confirms a session owned by
// (thisPod, thisUID) itself is recognized locally and never even triggers a
// Pod lookup for it.
func TestSweepOrphanedSessions_SkipsOwnIncarnation(t *testing.T) {
	a := &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		t.Fatal("agent must not be invoked -- the session is already actively owned, not orphaned")
		return nil, nil
	}}
	c, el, store := newTestServerController(t, map[string]harness.Harness{"a": a})
	ctx := context.Background()
	registry := newTestRegistry(t)

	bootstrap := envNew(a2a.MessageRoleUser, "checkpointd", "a", "seed")
	if _, err := newServerSession(ctx, store, el, "a", "checkpointd-server-0", "this-uid", bootstrap); err != nil {
		t.Fatalf("newServerSession: %v", err)
	}

	checker := &fakePodExistenceChecker{
		alive:  map[string]bool{},
		forbid: map[string]bool{"checkpointd-server-0/this-uid": true},
		t:      t,
	}
	claimed, err := sweepOrphanedSessions(ctx, c, el, store, registry, checker, "ns", "checkpointd-server-0", "this-uid")
	if err != nil {
		t.Fatalf("sweepOrphanedSessions: %v", err)
	}
	if claimed != 0 {
		t.Fatalf("sweepOrphanedSessions claimed = %d, want 0 (already its own session)", claimed)
	}
}

// TestSweepOrphanedSessions_DoesNotHijackReplacementIncarnation is the
// regression test for the race a same-named replacement pod can create: the
// sweep confirms (podName, oldUID) is dead, but before it gets around to
// re-selecting that incarnation's own sessions, a fresh, live replacement
// under the same podName (but a new uid) races in and legitimately claims
// the very session the sweep was about to examine (this is exactly what a
// scaled-in-then-scaled-back-out StatefulSet ordinal, or any other
// same-name pod replacement, can do concurrently with a sweep). The sweep
// must come away empty-handed, leaving the replacement's own fresh claim
// alone -- not steal it merely because the podName briefly looked stale.
func TestSweepOrphanedSessions_DoesNotHijackReplacementIncarnation(t *testing.T) {
	a := &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		return taskReply(in, "a", "", a2a.TaskStateCompleted, "done"), nil
	}}
	c, el, store := newTestServerController(t, map[string]harness.Harness{"a": a})
	ctx := context.Background()
	registry := newTestRegistry(t)

	bootstrap := envNew(a2a.MessageRoleUser, "checkpointd", "a", "seed")
	bk, err := newServerSession(ctx, store, el, "a", "checkpointd-server-1", "old-uid", bootstrap)
	if err != nil {
		t.Fatalf("newServerSession: %v", err)
	}

	checker := &fakePodExistenceChecker{
		alive: map[string]bool{},
		// Simulates a fresh checkpointd-server-1 incarnation winning its own
		// self-recovery claim (see resumeServerSessions) in the window
		// between this sweep confirming old-uid is dead and it re-selecting
		// old-uid's own sessions.
		before: func(namespace, podName, uid string) {
			if podName != "checkpointd-server-1" || uid != "old-uid" {
				return
			}
			ok, err := store.ClaimOrphanedSession(ctx, bk.sessionID, "checkpointd-server-1", "new-uid", "old-uid")
			if err != nil {
				t.Fatalf("simulated replacement claim: %v", err)
			}
			if !ok {
				t.Fatalf("simulated replacement claim didn't affect any row")
			}
		},
		t: t,
	}
	claimed, err := sweepOrphanedSessions(ctx, c, el, store, registry, checker, "ns", "checkpointd-server-0", "this-uid")
	if err != nil {
		t.Fatalf("sweepOrphanedSessions: %v", err)
	}
	if claimed != 0 {
		t.Fatalf("sweepOrphanedSessions claimed = %d, want 0 -- must not hijack the replacement's own fresh claim", claimed)
	}
	row, err := store.GetSession(ctx, bk.sessionID)
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if row.ownerPod != "checkpointd-server-1" || row.ownerUID != "new-uid" {
		t.Errorf("GetSession after sweep = (%q, %q), want left with the replacement's own claim (%q, %q)", row.ownerPod, row.ownerUID, "checkpointd-server-1", "new-uid")
	}
}

// TestSweepOrphanedSessions_RacingSweepsOnlyOneClaims confirms two
// concurrent sweeps (simulating two instances) against the same orphaned
// session only let one of them claim and resume it.
func TestSweepOrphanedSessions_RacingSweepsOnlyOneClaims(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	a := &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		mu.Lock()
		calls++
		mu.Unlock()
		return taskReply(in, "a", "", a2a.TaskStateCompleted, "done"), nil
	}}
	c, el, store := newTestServerController(t, map[string]harness.Harness{"a": a})
	ctx := context.Background()

	bootstrap := envNew(a2a.MessageRoleUser, "checkpointd", "a", "seed")
	bk, err := newServerSession(ctx, store, el, "a", "checkpointd-server-1", "dead-uid", bootstrap)
	if err != nil {
		t.Fatalf("newServerSession: %v", err)
	}

	checker := &fakePodExistenceChecker{alive: map[string]bool{}, t: t}
	registry1 := newTestRegistry(t)
	registry2 := newTestRegistry(t)

	var wg sync.WaitGroup
	results := make(chan int, 2)
	for _, r := range []*taskRegistry{registry1, registry2} {
		wg.Add(1)
		go func(reg *taskRegistry) {
			defer wg.Done()
			n, err := sweepOrphanedSessions(ctx, c, el, store, reg, checker, "ns", "checkpointd-server-0", "uid-x")
			if err != nil {
				t.Errorf("sweepOrphanedSessions: %v", err)
				return
			}
			results <- n
		}(r)
	}
	wg.Wait()
	close(results)

	total := 0
	for n := range results {
		total += n
	}
	if total != 1 {
		t.Errorf("racing sweeps claimed a total of %d sessions, want exactly 1", total)
	}
	pollTaskState(t, store, bk.sessionID, a2a.TaskStateCompleted)
}

// TestSweepOrphanedSessions_PanicsOnRegistryConflict confirms
// resumeClaimedSession's panic on a registry conflict (see
// TestResumeClaimedSession_PanicsOnRegistryConflict) propagates straight
// out of sweepOrphanedSessions too, rather than being caught and turned
// into a revert: by the time the conflict is discovered,
// sweepOrphanedSessions's own claim has already durably succeeded, and
// reverting it back to the prior (dead) owner while whatever already holds
// the registry entry keeps running would risk a genuine double-drive, not
// prevent one.
func TestSweepOrphanedSessions_PanicsOnRegistryConflict(t *testing.T) {
	a := &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		t.Fatal("agent must not be invoked -- the registry conflict must prevent driving, not somehow still drive it")
		return nil, nil
	}}
	c, el, store := newTestServerController(t, map[string]harness.Harness{"a": a})
	ctx := context.Background()
	registry := newTestRegistry(t)

	bootstrap := envNew(a2a.MessageRoleUser, "checkpointd", "a", "seed")
	bk, err := newServerSession(ctx, store, el, "a", "checkpointd-server-1", "dead-uid", bootstrap)
	if err != nil {
		t.Fatalf("newServerSession: %v", err)
	}

	// Pre-occupy the local registry entry to force resumeClaimedSession's
	// own acquire to fail, simulating the "somehow already has a local
	// entry" case its own doc comment says should never normally happen.
	_, preRelease, ok := registry.acquire(ctx, bk.sessionID)
	if !ok {
		t.Fatal("pre-acquiring the registry entry failed unexpectedly")
	}
	defer preRelease()

	checker := &fakePodExistenceChecker{alive: map[string]bool{}, t: t}
	func() {
		defer func() {
			if recover() == nil {
				t.Error("sweepOrphanedSessions returned normally on a registry conflict, want a panic")
			}
		}()
		sweepOrphanedSessions(ctx, c, el, store, registry, checker, "ns", "checkpointd-server-0", "this-uid") //nolint:errcheck
		t.Error("unreachable: sweepOrphanedSessions should have panicked")
	}()

	row, err := store.GetSession(ctx, bk.sessionID)
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if row.ownerPod != "checkpointd-server-0" || row.ownerUID != "this-uid" {
		t.Errorf("GetSession after a panicked resume = (%q, %q), want left claimed by (%q, %q) -- the panic must skip past sweepOrphanedSessions' own revert-on-error, not trigger it", row.ownerPod, row.ownerUID, "checkpointd-server-0", "this-uid")
	}
}

// pollTaskState polls sessionID's own task until it reaches want or a
// deadline passes, using the session's already-known bootstrap agent to
// resolve its task id -- reuses pollTaskIDForSession/pollTask via a plain
// GetSession-based store poll, since a salvage-resumed session has no
// serverRequestHandler of its own driving assertions.
func pollTaskState(t *testing.T, store *sqlTaskStore, sessionID string, want a2a.TaskState) {
	t.Helper()
	taskID := pollTaskIDForSession(t, store, sessionID)
	pollSessionState(t, store, sessionID, sessionStateTerminated)
	stored, err := store.Get(context.Background(), taskID, "a", sessionID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if stored.Task.Status.State != want {
		t.Errorf("task %s state = %q, want %q", taskID, stored.Task.Status.State, want)
	}
}
