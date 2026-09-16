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

// Command long_poll is the "long-poll" agent for the script/test-k8s
// long-poll/long-wait durability demo: it polls long-wait
// (script/test-k8s/agents/long_wait) in a retry loop, then reports the
// result, driven entirely by durable `ax exec` turns.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"iter"
	"log"
	"net/http"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/a2aproject/a2a-go/v2/a2aclient/agentcard"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/ktock/checkpointd/harness"
)

var (
	harnessAddr     = flag.String("harness-addr", "127.0.0.1:50060", "address of this agent's HarnessService, i.e. this actor's own worker port")
	checkpointdAddr = flag.String("checkpointd-addr", "", "address of checkpointd's own A2A endpoint (e.g. checkpointd-server.<namespace>.svc:80) -- used to resolve long-wait's real AgentCard via a real HTTP fetch")
	testserverAddr  = flag.String("testserver-addr", "127.0.0.1:8080", "address of the script/test-k8s/agents/testserver server, used to record this workflow's own entry count")
)

func main() {
	flag.Parse()
	if *checkpointdAddr == "" {
		log.Fatal("--checkpointd-addr is required (used to resolve long-wait's real AgentCard -- see resolveCard)")
	}
	h := harness.NewAgentHarness(a2asrv.AgentExecutorFunc(longPollWorkflow))
	if err := harness.Serve(*harnessAddr, h); err != nil {
		log.Fatal(err)
	}
}

// longPollWorkflow reads an id, then polls long-wait until it's notified, yielding its payload as the completed result.
func longPollWorkflow(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		info := execCtx.TaskInfo()
		// Reports Working first, then continues in this same call.
		if !yield(&a2a.Task{
			ID:        info.TaskID,
			ContextID: info.ContextID,
			Status: a2a.TaskStatus{
				State:   a2a.TaskStateWorking,
				Message: execCtx.Message,
			},
		}, nil) {
			return
		}

		id := text(execCtx.Message)
		// A healthy run reaches this line exactly once, no matter how many suspend/resume cycles happen first.
		if err := incrTestServer(ctx, *testserverAddr, id+"-entered"); err != nil {
			yield(nil, fmt.Errorf("recording entry: %w", err))
			return
		}

		waitCard, err := resolveCard(ctx, "long-wait")
		if err != nil {
			yield(nil, err)
			return
		}
		waitClient, err := a2aclient.NewFromCard(ctx, waitCard,
			a2aclient.WithDefaultsDisabled(), harness.WithTransport(execCtx))
		if err != nil {
			yield(nil, err)
			return
		}
		var payload string
		// This loop, not long-wait itself, owns the retry count.
		for {
			res, err := waitClient.SendMessage(ctx, &a2a.SendMessageRequest{
				Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart(id)),
			})
			if err != nil {
				yield(nil, err)
				return
			}
			msg, ok := res.(*a2a.Message)
			if !ok || len(msg.Parts) == 0 {
				yield(nil, fmt.Errorf("unexpected reply from long-wait: %v", res))
				return
			}
			// long-wait reports pending/notified as DataPart content, not a2a.TaskState.
			status, ok := msg.Parts[0].Data().(map[string]any)
			if !ok {
				yield(nil, fmt.Errorf("unexpected reply content from long-wait: %v", msg.Parts[0]))
				return
			}
			if status["status"] == "notified" {
				payload, _ = status["payload"].(string)
				break
			}
		}

		// A single whole Task, not a bare update+Message pair.
		yield(&a2a.Task{
			ID:        info.TaskID,
			ContextID: info.ContextID,
			Status: a2a.TaskStatus{
				State:   a2a.TaskStateCompleted,
				Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart(payload)),
			},
		}, nil)
	}
}

// text returns the text of msg's first part, or "" if there is none.
func text(msg *a2a.Message) string {
	if msg == nil || len(msg.Parts) == 0 {
		return ""
	}
	return msg.Parts[0].Text()
}

// incrTestServer POSTs to testserver's counter endpoint for id, incrementing it by one, and is deliberately not idempotent.
func incrTestServer(ctx context.Context, addr, id string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("http://%s/incr/%s", addr, id), nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("testserver: unexpected status %s", resp.Status)
	}
	return nil
}

// resolveCard resolves id's AgentCard from checkpointd's /agents/{id}/ endpoint via a real fetch.
func resolveCard(ctx context.Context, id string) (*a2a.AgentCard, error) {
	card, err := agentcard.DefaultResolver.Resolve(ctx, "http://"+*checkpointdAddr+"/agents/"+id+"/.well-known/agent-card.json")
	if err != nil {
		return nil, fmt.Errorf("resolving %s's AgentCard from %s: %w", id, *checkpointdAddr, err)
	}
	return card, nil
}
