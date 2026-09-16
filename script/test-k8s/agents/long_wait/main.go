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

// Command long_wait is the "long-wait" agent for the script/test-k8s long-poll/long-wait durability demo.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"iter"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/ktock/checkpointd/harness"
)

var (
	harnessAddr    = flag.String("harness-addr", "127.0.0.1:50060", "address of this agent's HarnessService, i.e. this actor's own worker port")
	testserverAddr = flag.String("testserver-addr", "127.0.0.1:8080", "address of the script/test-k8s/agents/testserver server")
	pollTimeout    = flag.Duration("poll-timeout", 10*time.Second, "how long each turn's single long-poll against testserver waits before replying 'not yet'")
)

func main() {
	flag.Parse()
	h := harness.NewAgentHarness(longWaitAgent(*testserverAddr, *pollTimeout))
	if err := harness.Serve(*harnessAddr, h); err != nil {
		log.Fatal(err)
	}
}

// longWaitAgent does one bounded poll against testserverAddr per turn, replying with a {"status": ...} DataPart.
func longWaitAgent(testserverAddr string, pollTimeout time.Duration) a2asrv.AgentExecutorFunc {
	return func(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			id := text(execCtx.Message)

			payload, notified, err := pollTestServer(ctx, testserverAddr, id, pollTimeout)
			if err != nil {
				yield(nil, fmt.Errorf("polling testserver for %q: %w", id, err))
				return
			}

			data := map[string]any{"status": "pending"}
			if notified {
				data = map[string]any{"status": "notified", "payload": payload}
			}
			yield(a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewDataPart(data)), nil)
		}
	}
}

// text returns the text of msg's first part, or "" if there is none.
func text(msg *a2a.Message) string {
	if msg == nil || len(msg.Parts) == 0 {
		return ""
	}
	return msg.Parts[0].Text()
}

// pollTestServer does one bounded long-poll GET /wait/{id} against testserver.
func pollTestServer(ctx context.Context, addr, id string, timeout time.Duration) (string, bool, error) {
	u := fmt.Sprintf("http://%s/wait/%s?timeout=%d", addr, url.PathEscape(id), int(timeout.Seconds()))
	// A little slack over the server's own timeout so a slow response isn't mistaken for a client-side one.
	ctx, cancel := context.WithTimeout(ctx, timeout+2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return "", false, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", false, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusNoContent:
		return "", false, nil
	case http.StatusOK:
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return "", false, err
		}
		return string(body), true, nil
	default:
		return "", false, fmt.Errorf("testserver: unexpected status %s", resp.Status)
	}
}
