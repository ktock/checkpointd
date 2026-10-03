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
	"net/http"
	"net/http/httptest"
	"testing"
)

func post(t *testing.T, srv *httptest.Server, path string) int {
	t.Helper()
	resp, err := http.Post(srv.URL+path, "text/plain", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

func TestCrashGrantedOncePerTurn(t *testing.T) {
	srv := httptest.NewServer(newHandler())
	defer srv.Close()

	if got := post(t, srv, "/crash?turn=a"); got != http.StatusOK {
		t.Fatalf("first crash for turn a = %d, want 200", got)
	}
	if got := post(t, srv, "/crash?turn=a"); got != http.StatusConflict {
		t.Fatalf("second crash for turn a = %d, want 409", got)
	}
	if got := post(t, srv, "/crash?turn=b"); got != http.StatusOK {
		t.Fatalf("first crash for turn b = %d, want 200 (turns are independent)", got)
	}
}

func TestCrashRequiresTurn(t *testing.T) {
	srv := httptest.NewServer(newHandler())
	defer srv.Close()

	if got := post(t, srv, "/crash"); got != http.StatusBadRequest {
		t.Fatalf("crash without a turn = %d, want 400", got)
	}
}

func TestHealth(t *testing.T) {
	srv := httptest.NewServer(newHandler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health = %d, want 200", resp.StatusCode)
	}
}
