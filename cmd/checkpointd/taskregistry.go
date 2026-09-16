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
	"sync"
	"time"
)

// taskRegistry is `checkpointd`'s process-wide, in-memory record of
// which tasks currently have a goroutine actively driving their relay loop.
type taskRegistry struct {
	mu     sync.Mutex
	active map[string]*activeTask
}

type activeTask struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func newTaskRegistry() *taskRegistry {
	return &taskRegistry{active: make(map[string]*activeTask)}
}

// acquire claims id for a new goroutine about to drive its relay loop.
func (r *taskRegistry) acquire(ctx context.Context, id string) (taskCtx context.Context, release func(), ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, busy := r.active[id]; busy {
		return nil, nil, false
	}
	cctx, cancel := context.WithCancel(ctx)
	entry := &activeTask{cancel: cancel, done: make(chan struct{})}
	r.active[id] = entry
	var releaseOnce sync.Once
	release = func() {
		releaseOnce.Do(func() {
			r.mu.Lock()
			if r.active[id] == entry {
				delete(r.active, id)
			}
			r.mu.Unlock()
			close(entry.done)
		})
	}
	return cctx, release, true
}

// isActive reports whether id currently has a goroutine actively driving
// its relay loop.
func (r *taskRegistry) isActive(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, busy := r.active[id]
	return busy
}

// acquireWait is like acquire, but if id is currently busy it waits for
// release.
func (r *taskRegistry) acquireWait(ctx context.Context, id string) (taskCtx context.Context, release func(), ok bool) {
	for {
		taskCtx, release, ok := r.acquire(ctx, id)
		if ok {
			return taskCtx, release, true
		}
		r.mu.Lock()
		entry, busy := r.active[id]
		r.mu.Unlock()
		if !busy {
			continue // freed between the failed acquire above and this check
		}
		select {
		case <-entry.done:
		case <-ctx.Done():
			return nil, nil, false
		}
	}
}

// cancelAndWait cancels id's actively-driving goroutine and waits up to timeout
// for it to confirm it has actually stopped.
func (r *taskRegistry) cancelAndWait(ctx context.Context, id string, timeout time.Duration) bool {
	r.mu.Lock()
	entry, busy := r.active[id]
	r.mu.Unlock()
	if !busy {
		return false
	}
	entry.cancel()
	select {
	case <-entry.done:
		return true
	case <-time.After(timeout):
		return false
	case <-ctx.Done():
		return false
	}
}
