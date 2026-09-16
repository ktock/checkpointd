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
	"testing"
	"time"
)

// TestTaskRegistry_AcquireWait_SucceedsImmediatelyWhenFree confirms
// acquireWait behaves exactly like acquire when id isn't busy.
func TestTaskRegistry_AcquireWait_SucceedsImmediatelyWhenFree(t *testing.T) {
	r := newTaskRegistry()
	_, release, ok := r.acquireWait(context.Background(), "x")
	if !ok {
		t.Fatal("acquireWait on a free id = false, want true")
	}
	release()
}

// TestTaskRegistry_AcquireWait_WaitsForBusyIDToFree is the regression test
// for the CancelTask race this was added to fix: cancelAndWait's own wait is
// bounded (cancelWaitTimeout) and can give up while the relay loop it asked
// to stop is still running -- a plain acquire right after that would
// wrongly fail as if that were unexpected. acquireWait must instead keep
// waiting past that point, however long the original holder actually takes
// to release, and then succeed.
func TestTaskRegistry_AcquireWait_WaitsForBusyIDToFree(t *testing.T) {
	r := newTaskRegistry()
	_, firstRelease, ok := r.acquire(context.Background(), "x")
	if !ok {
		t.Fatal("acquire on a free id = false, want true")
	}

	done := make(chan struct{})
	go func() {
		_, release, ok := r.acquireWait(context.Background(), "x")
		if !ok {
			t.Error("acquireWait = false, want true (context.Background() never ends)")
		} else {
			release()
		}
		close(done)
	}()

	// acquireWait must still be blocked while the first holder hasn't
	// released -- simulates cancelAndWait having already timed out while
	// the relay loop it cancelled is still finishing up.
	select {
	case <-done:
		t.Fatal("acquireWait returned before the busy id was released, want it still blocked")
	case <-time.After(50 * time.Millisecond):
	}

	firstRelease()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("acquireWait never returned after the busy id was released")
	}

	if r.isActive("x") {
		t.Error("registry still reports id active after acquireWait's own release, want it freed")
	}
}

// TestTaskRegistry_AcquireWait_AbortsOnContextDone confirms a caller with
// its own deadline (unlike CancelTask's context.Background() cleanup
// goroutine) can still give up waiting.
func TestTaskRegistry_AcquireWait_AbortsOnContextDone(t *testing.T) {
	r := newTaskRegistry()
	_, _, ok := r.acquire(context.Background(), "x")
	if !ok {
		t.Fatal("acquire on a free id = false, want true")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, _, ok = r.acquireWait(ctx, "x")
	if ok {
		t.Fatal("acquireWait with a context that expired while still busy = true, want false")
	}
}
