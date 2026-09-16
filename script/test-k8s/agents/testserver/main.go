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

// Command testserver is a minimal, in-memory notification/counter server used as a generic helper by the test suites under script/.
package main

import (
	"context"
	"flag"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"
)

var addr = flag.String("addr", "0.0.0.0:8080", "address to listen on")

func main() {
	flag.Parse()
	b := newBroker()
	mux := http.NewServeMux()
	mux.HandleFunc("POST /notify/{id}", b.handleNotify)
	mux.HandleFunc("GET /wait/{id}", b.handleWait)
	mux.HandleFunc("POST /incr/{id}", b.handleIncr)
	mux.HandleFunc("GET /count/{id}", b.handleCount)
	log.Printf("testserver listening on %s", *addr)
	log.Fatal(http.ListenAndServe(*addr, mux))
}

// broker holds one notification per id, its waiter channels, and a separate set of increment counters.
type broker struct {
	mu            sync.Mutex
	notifications map[string]string
	waiters       map[string][]chan string
	counters      map[string]int64
}

func newBroker() *broker {
	return &broker{
		notifications: make(map[string]string),
		waiters:       make(map[string][]chan string),
		counters:      make(map[string]int64),
	}
}

func (b *broker) handleNotify(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	b.notify(id, string(body))
	w.WriteHeader(http.StatusNoContent)
}

func (b *broker) notify(id, payload string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.notifications[id]; exists {
		return // first notification for id wins; a retry is a no-op.
	}
	b.notifications[id] = payload
	for _, ch := range b.waiters[id] {
		ch <- payload
	}
	delete(b.waiters, id)
}

// handleWait blocks until id is notified, ctx is canceled, or (only when
// timeout is explicitly given a positive value) timeout elapses. timeout=0
// (the default, when the query param is absent, zero, or negative) means:
// block until notified, with no timeout at all.
func (b *broker) handleWait(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var timeout time.Duration
	if s := r.URL.Query().Get("timeout"); s != "" {
		if secs, err := strconv.Atoi(s); err == nil && secs > 0 {
			timeout = time.Duration(secs) * time.Second
		}
	}
	payload, ok := b.wait(r.Context(), id, timeout)
	if !ok {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	w.Write([]byte(payload))
}

// wait returns the payload for id, blocking until it is notified or ctx is
// canceled. If timeout is positive, it also gives up (returning false) once
// timeout elapses; timeout<=0 means wait forever, so the only way to unblock
// is an actual notification (or ctx being canceled).
func (b *broker) wait(ctx context.Context, id string, timeout time.Duration) (string, bool) {
	b.mu.Lock()
	if payload, ok := b.notifications[id]; ok {
		b.mu.Unlock()
		return payload, true
	}
	ch := make(chan string, 1)
	b.waiters[id] = append(b.waiters[id], ch)
	b.mu.Unlock()
	defer b.removeWaiter(id, ch)

	if timeout <= 0 {
		select {
		case payload := <-ch:
			return payload, true
		case <-ctx.Done():
			return "", false
		}
	}
	select {
	case payload := <-ch:
		return payload, true
	case <-time.After(timeout):
		return "", false
	case <-ctx.Done():
		return "", false
	}
}

// removeWaiter unregisters ch so a caller that gave up doesn't leak it.
func (b *broker) removeWaiter(id string, ch chan string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	ws := b.waiters[id]
	for i, w := range ws {
		if w == ch {
			b.waiters[id] = append(ws[:i], ws[i+1:]...)
			return
		}
	}
}

func (b *broker) handleIncr(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	n := b.incr(id)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Write([]byte(strconv.FormatInt(n, 10)))
}

func (b *broker) incr(id string) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.counters[id]++
	return b.counters[id]
}

func (b *broker) handleCount(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	b.mu.Lock()
	n := b.counters[id]
	b.mu.Unlock()
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Write([]byte(strconv.FormatInt(n, 10)))
}
