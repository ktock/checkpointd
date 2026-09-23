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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/a2aproject/a2a-go/v2/a2aclient/agentcard"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	config "github.com/ktock/checkpointd/internal/config/checkpointd"
	"github.com/ktock/checkpointd/internal/controller/eventlog"
	"github.com/ktock/checkpointd/internal/harness"
	"github.com/ktock/checkpointd/internal/hop"
)

// TestServer_RealClientOverJSONRPC is the one test in this package that
// goes through actual JSON-RPC marshaling on both ends, using an unmodified
// a2aclient the way a real external caller would: resolve the AgentCard,
// build a client, SendMessage, then poll GetTask.
func TestServer_RealClientOverJSONRPC(t *testing.T) {
	a := &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		reply := taskReply(in, "a", "", a2a.TaskStateCompleted, "done:"+textOf(inMessage(in)))
		return reply, nil
	}}
	c, el, store := newTestServerController(t, map[string]harness.Harness{"a": a})

	cards := newAgentCardStore(map[string]*config.AgentCardConfig{
		"a": {Name: "Agent A", Description: "test agent"},
	})

	// The Listener is already bound before Start, so the mux can be built
	// with the correct base URL for each agent's AgentCard from the start.
	ts := httptest.NewUnstartedServer(nil)
	baseURL := "http://" + ts.Listener.Addr().String()
	ts.Config = &http.Server{Handler: buildServerMux(cards, baseURL, c, el, store, newTestRegistry(t), "pod-1", "uid-1", nil)}
	ts.Start()
	defer ts.Close()

	ctx := context.Background()
	// Non-root path: the resolver fetches exactly this URL as the literal
	// card location, rather than auto-appending .well-known/agent-card.json.
	card, err := agentcard.DefaultResolver.Resolve(ctx, baseURL+"/agents/a"+a2asrv.WellKnownAgentCardPath)
	if err != nil {
		t.Fatalf("resolving AgentCard: %v", err)
	}
	if card.Name != "Agent A" {
		t.Errorf("resolved AgentCard.Name = %q, want %q", card.Name, "Agent A")
	}
	if card.Capabilities.Streaming || card.Capabilities.PushNotifications {
		t.Errorf("resolved AgentCard.Capabilities = %+v, want both false", card.Capabilities)
	}

	client, err := a2aclient.NewFromCard(ctx, card)
	if err != nil {
		t.Fatalf("a2aclient.NewFromCard: %v", err)
	}

	result, err := client.SendMessage(ctx, &a2a.SendMessageRequest{
		Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("seed")),
	})
	if err != nil {
		t.Fatalf("client.SendMessage: %v", err)
	}
	task, ok := result.(*a2a.Task)
	if !ok {
		t.Fatalf("SendMessage result is %T, want *a2a.Task", result)
	}
	// SendMessage blocks and returns the real, final state directly for an
	// agent this fast, over real JSON-RPC too.
	if task.Status.State != a2a.TaskStateCompleted {
		t.Fatalf("Status.State = %q, want %q", task.Status.State, a2a.TaskStateCompleted)
	}
	// Just the seed message: the status message isn't duplicated into History.
	if len(task.History) != 1 {
		t.Fatalf("len(History) = %d, want 1", len(task.History))
	}
	if got, want := textOf(task.Status.Message), "done:seed"; got != want {
		t.Errorf("final reply text = %q, want %q", got, want)
	}

	// GetTask afterward must agree; Tenant echoes back the sessionID
	// SendMessage stamped into Metadata.
	sessionID, _ := task.Metadata[checkpointdTaskMetaTenant].(string)
	final, err := client.GetTask(ctx, &a2a.GetTaskRequest{ID: task.ID, Tenant: sessionID})
	if err != nil {
		t.Fatalf("client.GetTask: %v", err)
	}
	if final.Status.State != a2a.TaskStateCompleted {
		t.Fatalf("GetTask Status.State = %q, want %q", final.Status.State, a2a.TaskStateCompleted)
	}

	list, err := client.ListTasks(ctx, &a2a.ListTasksRequest{})
	if err != nil {
		t.Fatalf("client.ListTasks: %v", err)
	}
	if len(list.Tasks) != 1 || list.Tasks[0].ID != task.ID {
		t.Errorf("ListTasks over real JSON-RPC = %+v, want exactly the one task %q", list.Tasks, task.ID)
	}
}

// TestServer_ReadyEndpoint confirms /ready -- the readinessProbe target in
// script/test-k8s's own manifests -- reflects
// cards.ready() rather than any one specific agent's own presence, and
// that a sync which finds zero agents still counts as ready (checkpointd
// can be deployed before its agents exist).
func TestServer_ReadyEndpoint(t *testing.T) {
	c, el, store := newTestServerController(t, nil)

	cards := newAgentCardStore(nil)
	ts := httptest.NewServer(buildServerMux(cards, "http://ignored", c, el, store, newTestRegistry(t), "pod-1", "uid-1", nil))
	defer ts.Close()

	get := func() int {
		t.Helper()
		resp, err := http.Get(ts.URL + "/ready")
		if err != nil {
			t.Fatalf("GET /ready: %v", err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}

	if got := get(); got != http.StatusServiceUnavailable {
		t.Errorf("GET /ready before any sync = %d, want %d", got, http.StatusServiceUnavailable)
	}

	cards.store(map[string]*config.AgentCardConfig{})
	if got := get(); got != http.StatusOK {
		t.Errorf("GET /ready after a sync that found no agents = %d, want %d", got, http.StatusOK)
	}

	cards.store(map[string]*config.AgentCardConfig{"a": {Name: "Agent A"}})
	if got := get(); got != http.StatusOK {
		t.Errorf("GET /ready with an agent known = %d, want %d", got, http.StatusOK)
	}
}

// TestServer_HealthzEndpoint confirms /healthz -- checkpointd's
// livenessProbe target -- reflects real database reachability: 200 while
// the DB is up, 503 once it's closed out from under the store, simulating
// the wedged-connection case a livenessProbe is meant to catch.
func TestServer_HealthzEndpoint(t *testing.T) {
	c, el, store := newTestServerController(t, nil)

	cards := newAgentCardStore(nil)
	ts := httptest.NewServer(buildServerMux(cards, "http://ignored", c, el, store, newTestRegistry(t), "pod-1", "uid-1", nil))
	defer ts.Close()

	get := func() int {
		t.Helper()
		resp, err := http.Get(ts.URL + "/healthz")
		if err != nil {
			t.Fatalf("GET /healthz: %v", err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}

	if got := get(); got != http.StatusOK {
		t.Errorf("GET /healthz with the DB reachable = %d, want %d", got, http.StatusOK)
	}

	db, _, ok := eventlog.SQLDB(el)
	if !ok {
		t.Fatal("eventlog.SQLDB: not a SQL-backed EventLog")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("closing the DB out from under the store: %v", err)
	}
	if got := get(); got != http.StatusServiceUnavailable {
		t.Errorf("GET /healthz with the DB closed = %d, want %d", got, http.StatusServiceUnavailable)
	}
}

// TestServer_GetTask_AcceptsSnakeCaseHistoryLength (CORE-HIST-002) confirms
// checkTransportPreconditions rewrites a GetTask params.history_length
// (snake_case, what the A2A TCK's own client sends) to the spec-mandated
// historyLength, over a real POST.
func TestServer_GetTask_AcceptsSnakeCaseHistoryLength(t *testing.T) {
	a := &fakeHarness{respond: func(in *hop.Envelope) (*hop.Envelope, error) {
		reply := taskReply(in, "a", "", a2a.TaskStateCompleted, "done:"+textOf(inMessage(in)))
		return reply, nil
	}}
	c, el, store := newTestServerController(t, map[string]harness.Harness{"a": a})

	cards := newAgentCardStore(map[string]*config.AgentCardConfig{
		"a": {Name: "Agent A", Description: "test agent"},
	})
	ts := httptest.NewUnstartedServer(nil)
	baseURL := "http://" + ts.Listener.Addr().String()
	ts.Config = &http.Server{Handler: checkTransportPreconditions(buildServerMux(cards, baseURL, c, el, store, newTestRegistry(t), "pod-1", "uid-1", nil))}
	ts.Start()
	defer ts.Close()

	ctx := context.Background()
	card, err := agentcard.DefaultResolver.Resolve(ctx, baseURL+"/agents/a"+a2asrv.WellKnownAgentCardPath)
	if err != nil {
		t.Fatalf("resolving AgentCard: %v", err)
	}
	client, err := a2aclient.NewFromCard(ctx, card)
	if err != nil {
		t.Fatalf("a2aclient.NewFromCard: %v", err)
	}
	result, err := client.SendMessage(ctx, &a2a.SendMessageRequest{
		Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("seed")),
	})
	if err != nil {
		t.Fatalf("client.SendMessage: %v", err)
	}
	task, ok := result.(*a2a.Task)
	if !ok {
		t.Fatalf("SendMessage result is %T, want *a2a.Task", result)
	}
	// Just the seed message: the status message isn't duplicated into History.
	if len(task.History) != 1 {
		t.Fatalf("len(History) = %d, want 1", len(task.History))
	}

	// Raw POST bypassing a2aclient (which always sends the spec-correct
	// historyLength), matching what the TCK's own client sends on the wire.
	sessionID, _ := task.Metadata[checkpointdTaskMetaTenant].(string)
	body := `{"jsonrpc":"2.0","id":1,"method":"GetTask","params":{"id":"` + string(task.ID) + `","tenant":"` + sessionID + `","history_length":1}}`
	req, _ := http.NewRequest(http.MethodPost, baseURL+"/agents/a/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST GetTask with history_length: %v", err)
	}
	defer resp.Body.Close()
	var parsed struct {
		Error *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
		Result *struct {
			History []*a2a.Message `json:"history"`
		} `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	if parsed.Error != nil {
		t.Fatalf("GetTask with history_length=1 returned an error: %+v", parsed.Error)
	}
	if parsed.Result == nil || len(parsed.Result.History) != 1 {
		t.Fatalf("GetTask with history_length=1 returned History = %+v, want exactly 1 entry", parsed.Result)
	}
}

// TestCheckTransportPreconditions_Version confirms an unsupported
// A2A-Version header is rejected with -32009, since a2a-go's own JSON-RPC
// binding never validates that header itself.
func TestCheckTransportPreconditions_Version(t *testing.T) {
	cards := newAgentCardStore(map[string]*config.AgentCardConfig{
		"a": {Name: "Agent A"},
	})
	_, el, store := newTestServerController(t, nil)
	ts := httptest.NewUnstartedServer(nil)
	baseURL := "http://" + ts.Listener.Addr().String()
	mux := buildServerMux(cards, baseURL, nil, el, store, newTestRegistry(t), "pod-1", "uid-1", nil)
	ts.Config = &http.Server{Handler: checkTransportPreconditions(mux)}
	ts.Start()
	defer ts.Close()

	body := `{"jsonrpc":"2.0","id":1,"method":"GetTask","params":{"id":"bogus"}}`

	// A present, unsupported version must be rejected with a real JSON-RPC
	// VersionNotSupportedError, not passed through.
	req, _ := http.NewRequest(http.MethodPost, baseURL+"/agents/a/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(string(a2a.SvcParamVersion), "99.0")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST with A2A-Version: 99.0: %v", err)
	}
	defer resp.Body.Close()
	var parsed struct {
		Error *struct {
			Code int `json:"code"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	if parsed.Error == nil || parsed.Error.Code != -32009 {
		t.Errorf("response for A2A-Version: 99.0 = %+v, want error code -32009", parsed)
	}

	// A missing version header must still pass through untouched (matches
	// the TCK's own "empty version treated as default" requirement).
	req2, _ := http.NewRequest(http.MethodPost, baseURL+"/agents/a/", strings.NewReader(body))
	req2.Header.Set("Content-Type", "application/json")
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("POST with no A2A-Version: %v", err)
	}
	defer resp2.Body.Close()
	var parsed2 struct {
		Error *struct {
			Code int `json:"code"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp2.Body).Decode(&parsed2); err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	if parsed2.Error != nil && parsed2.Error.Code == -32009 {
		t.Errorf("a missing A2A-Version header was rejected, want it treated as the default")
	}
}

// TestCheckTransportPreconditions_ContentType confirms a wrong Content-Type
// is rejected at the HTTP level (415) rather than reaching the JSON-RPC
// handler, which would otherwise fail it with a generic ParseError instead
// of the spec-correct ContentTypeNotSupportedError.
func TestCheckTransportPreconditions_ContentType(t *testing.T) {
	cards := newAgentCardStore(map[string]*config.AgentCardConfig{
		"a": {Name: "Agent A"},
	})
	_, el, store := newTestServerController(t, nil)
	ts := httptest.NewUnstartedServer(nil)
	baseURL := "http://" + ts.Listener.Addr().String()
	mux := buildServerMux(cards, baseURL, nil, el, store, newTestRegistry(t), "pod-1", "uid-1", nil)
	ts.Config = &http.Server{Handler: checkTransportPreconditions(mux)}
	ts.Start()
	defer ts.Close()

	resp, err := http.Post(baseURL+"/agents/a/", "text/plain", strings.NewReader("not json"))
	if err != nil {
		t.Fatalf("POST with Content-Type: text/plain: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnsupportedMediaType {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusUnsupportedMediaType)
	}
}

// TestCheckTransportPreconditions_RejectsStreaming (JSONRPC-SSE-002,
// CORE-CAP-002) confirms a streaming call gets a plain JSON-RPC error
// before a2a-go's own handler is reached, rather than committing to SSE
// framing the TCK's raw JSON-RPC client can't parse.
func TestCheckTransportPreconditions_RejectsStreaming(t *testing.T) {
	cards := newAgentCardStore(map[string]*config.AgentCardConfig{
		"a": {Name: "Agent A"},
	})
	_, el, store := newTestServerController(t, nil)
	ts := httptest.NewUnstartedServer(nil)
	baseURL := "http://" + ts.Listener.Addr().String()
	mux := buildServerMux(cards, baseURL, nil, el, store, newTestRegistry(t), "pod-1", "uid-1", nil)
	ts.Config = &http.Server{Handler: checkTransportPreconditions(mux)}
	ts.Start()
	defer ts.Close()

	for _, method := range []string{"SendStreamingMessage", "SubscribeToTask"} {
		body := `{"jsonrpc":"2.0","id":1,"method":"` + method + `","params":{}}`
		resp, err := http.Post(baseURL+"/agents/a/", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("POST %s: %v", method, err)
		}
		defer resp.Body.Close()
		if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
			t.Errorf("%s: Content-Type = %q, want application/json (not SSE)", method, ct)
		}
		var parsed struct {
			Error *struct {
				Code int `json:"code"`
			} `json:"error"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
			t.Fatalf("%s: decoding response: %v", method, err)
		}
		if parsed.Error == nil || parsed.Error.Code != -32004 {
			t.Errorf("%s: response = %+v, want error code -32004", method, parsed)
		}
	}
}
