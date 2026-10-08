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
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestClaimCrash_FirstThenAlready(t *testing.T) {
	seen := map[string]bool{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		turn := r.URL.Query().Get("turn")
		if seen[turn] {
			w.WriteHeader(http.StatusConflict)
			return
		}
		seen[turn] = true
	}))
	defer srv.Close()
	addr := srv.Listener.Addr().String()

	first, err := claimCrash(context.Background(), addr, "turn/1 with spaces")
	if err != nil || !first {
		t.Fatalf("first claim = (%v, %v), want (true, nil)", first, err)
	}
	first, err = claimCrash(context.Background(), addr, "turn/1 with spaces")
	if err != nil || first {
		t.Fatalf("second claim = (%v, %v), want (false, nil)", first, err)
	}
}

func TestClaimCrash_UnexpectedStatusIsNotRetried(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	if _, err := claimCrash(context.Background(), srv.Listener.Addr().String(), "t"); err == nil {
		t.Fatal("want an error for a 500 answer")
	}
}

func TestClaimCrash_RetriesUntilTheLedgerIsReachable(t *testing.T) {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	addr := srv.Listener.Addr().String()
	srv.Listener.Close()

	go func() {
		time.Sleep(1500 * time.Millisecond)
		srv.Listener = mustListen(t, addr)
		srv.Start()
	}()
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	first, err := claimCrash(ctx, addr, "t")
	if err != nil || !first {
		t.Fatalf("claim after the ledger came up = (%v, %v), want (true, nil)", first, err)
	}
}

func TestClaimCrash_GivesUpWhenContextEnds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, err := claimCrash(ctx, "127.0.0.1:1", "t"); err == nil {
		t.Fatal("want an error when the ledger never answers")
	}
}

func mustListen(t *testing.T, addr string) net.Listener {
	t.Helper()
	l, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	return l
}
