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
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/a2aproject/a2a-go/v2/a2a"
)

// checkTransportPreconditions wraps next with a few request-level checks
// that needs to pass TCK.
func checkTransportPreconditions(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// A present but unsupported A2A-Version header gets a real,
		// spec-shaped VersionNotSupportedError; a missing/empty header just
		// means "use the default".
		if v := r.Header.Get(string(a2a.SvcParamVersion)); v != "" && v != string(a2a.Version) {
			writeJSONRPCError(w, nil, -32009, a2a.ErrVersionNotSupported.Error())
			return
		}
		if r.Method == http.MethodPost {
			// Reject a non-JSON Content-Type at the HTTP level (415) instead
			// of letting the JSON-RPC handler fail with a generic ParseError;
			// the TCK's own test for this tolerates an HTTP-level rejection.
			if ct := r.Header.Get("Content-Type"); ct != "" && !strings.HasPrefix(ct, "application/json") {
				w.WriteHeader(http.StatusUnsupportedMediaType)
				return
			}
			var handled bool
			r, handled = rewriteOrRejectJSONRPCBody(w, r)
			if handled {
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// rewriteOrRejectJSONRPCBody inspects a POST request's JSON-RPC body and
// either rewrites it, rejects it directly (handled=true), or leaves it
// untouched, depending on its "method".
func rewriteOrRejectJSONRPCBody(w http.ResponseWriter, r *http.Request) (*http.Request, bool) {
	if r.Body == nil {
		return r, false
	}
	body, err := io.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		r.Body = io.NopCloser(bytes.NewReader(nil))
		return r, false
	}

	var whole map[string]any
	if err := json.Unmarshal(body, &whole); err != nil {
		r.Body = io.NopCloser(bytes.NewReader(body))
		return r, false
	}
	method, _ := whole["method"].(string)

	// checkpointd never advertises streaming, so reject these directly with
	// a plain JSON-RPC error instead of letting a2a-go's handler commit to
	// SSE framing first, which the TCK's own tests for this can't parse.
	if method == "SendStreamingMessage" || method == "SubscribeToTask" {
		writeJSONRPCError(w, whole["id"], -32004, a2a.ErrUnsupportedOperation.Error())
		return r, true
	}

	// The TCK's own JSON-RPC client sends history_length (snake_case)
	// instead of the spec's historyLength, so alias it when present.
	if method == "GetTask" {
		if params, ok := whole["params"].(map[string]any); ok {
			snake, hasSnake := params["history_length"]
			_, hasCamel := params["historyLength"]
			if hasSnake && !hasCamel {
				params["historyLength"] = snake
				delete(params, "history_length")
			}
		}
	}

	rewritten, err := json.Marshal(whole)
	if err != nil {
		rewritten = body
	}
	r.Body = io.NopCloser(bytes.NewReader(rewritten))
	return r, false
}

// writeJSONRPCError writes a JSON-RPC 2.0 error response, echoing id back
// (nil if the caller never had one to echo).
func writeJSONRPCError(w http.ResponseWriter, id any, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK) // JSON-RPC errors are still HTTP 200, per a2a-go's own convention.
	_ = json.NewEncoder(w).Encode(struct {
		JSONRPC string `json:"jsonrpc"`
		ID      any    `json:"id"`
		Error   struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}{
		JSONRPC: "2.0",
		ID:      id,
		Error: struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		}{Code: code, Message: message},
	})
}
