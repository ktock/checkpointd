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

// Command crash_test_agent is "crash-test-agent" for script/test-k8s/test.sh's crash-recovery tests, calling --target --calls times in sequence so a mid-sequence crash can be injected and recovery verified.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"iter"
	"log"
	"net/http"
	"strings"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/a2aproject/a2a-go/v2/a2aclient/agentcard"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/ktock/checkpointd/harness"
)

var (
	harnessAddr     = flag.String("harness-addr", "127.0.0.1:50060", "address of this agent's HarnessService, i.e. this actor's own worker port")
	target          = flag.String("target", "echo-agent", "ax id of the downstream agent to call --calls times in sequence")
	calls           = flag.Int("calls", 3, "how many sequential calls to make to --target")
	crashBeforeCall = flag.Int("crash-before-call", 2, "signal readiness (and, with --self-panic, crash) immediately before making the call numbered this (1-indexed)")
	selfPanic       = flag.Bool("self-panic", false, "panic this process right after signaling readiness, once per bootstrap input (see this file's own doc comment)")
	testserverAddr  = flag.String("testserver-addr", "127.0.0.1:8080", "address of the script/test-k8s/agents/testserver server")
	checkpointdAddr = flag.String("checkpointd-addr", "", "address of checkpointd's own A2A endpoint (e.g. checkpointd-server.<namespace>.svc:80), required -- used to resolve --target's and long-wait's real AgentCards via a real HTTP fetch")
)

func main() {
	flag.Parse()
	if *checkpointdAddr == "" {
		log.Fatal("--checkpointd-addr is required (used to resolve --target's and long-wait's real AgentCards -- see resolveCard)")
	}
	h := harness.NewAgentHarness(a2asrv.AgentExecutorFunc(crashTestWorkflow))
	if err := harness.Serve(*harnessAddr, h); err != nil {
		log.Fatal(err)
	}
}

func crashTestWorkflow(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		// Reports Working first, then everything else runs in this same invocation without returning.
		info := execCtx.TaskInfo()
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

		targetCard, err := resolveCard(ctx, *target)
		if err != nil {
			yield(nil, err)
			return
		}
		client, err := a2aclient.NewFromCard(ctx, targetCard, a2aclient.WithDefaultsDisabled(), harness.WithTransport(execCtx))
		if err != nil {
			yield(nil, err)
			return
		}

		var replies []string
		for i := 1; i <= *calls; i++ {
			if i == *crashBeforeCall {
				// Signals readiness right before the call the test wants to crash around.
				if err := notifyTestServer(ctx, *testserverAddr, id+"-ready", "ready"); err != nil {
					yield(nil, fmt.Errorf("signaling readiness: %w", err))
					return
				}
				if *selfPanic {
					// Panics once per bootstrap input, skipping if a "crashed" marker for it already exists.
					already, err := checkNotified(ctx, *testserverAddr, id+"-crashed")
					if err != nil {
						yield(nil, fmt.Errorf("checking prior crash: %w", err))
						return
					}
					if !already {
						if err := notifyTestServer(ctx, *testserverAddr, id+"-crashed", "crashed"); err != nil {
							yield(nil, fmt.Errorf("recording crash: %w", err))
							return
						}
						panic("crash_test_agent: simulated self-crash for durability test")
					}
				} else {
					// Polls long-wait in a retry loop, each retry its own checkpoint boundary rather than one long in-process block.
					waitCard, err := resolveCard(ctx, "long-wait")
					if err != nil {
						yield(nil, fmt.Errorf("resolving long-wait: %w", err))
						return
					}
					waitClient, err := a2aclient.NewFromCard(ctx, waitCard, a2aclient.WithDefaultsDisabled(), harness.WithTransport(execCtx))
					if err != nil {
						yield(nil, fmt.Errorf("connecting to long-wait: %w", err))
						return
					}
					for {
						res, err := waitClient.SendMessage(ctx, &a2a.SendMessageRequest{
							Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart(id+"-go")),
						})
						if err != nil {
							yield(nil, fmt.Errorf("waiting for go signal: %w", err))
							return
						}
						// long-wait reports pending/notified as DataPart content, not a2a.TaskState.
						msg, ok := res.(*a2a.Message)
						if !ok || len(msg.Parts) == 0 {
							yield(nil, fmt.Errorf("unexpected reply from long-wait: %v", res))
							return
						}
						status, ok := msg.Parts[0].Data().(map[string]any)
						if !ok {
							yield(nil, fmt.Errorf("unexpected reply content from long-wait: %v", msg.Parts[0]))
							return
						}
						if status["status"] == "notified" {
							break
						}
					}
				}
			}
			resp, err := sendText(ctx, client, fmt.Sprintf("call-%d", i))
			if err != nil {
				yield(nil, err)
				return
			}
			replies = append(replies, text(resp))
		}

		// A single whole Task with Status.Message set, not a bare update event.
		yield(&a2a.Task{
			ID:        info.TaskID,
			ContextID: info.ContextID,
			Status: a2a.TaskStatus{
				State:   a2a.TaskStateCompleted,
				Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart(strings.Join(replies, ","))),
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

// notifyTestServer POSTs payload to testserver under id and is idempotent, so it is safe to call again on a resumed invocation.
func notifyTestServer(ctx context.Context, addr, id, payload string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("http://%s/notify/%s", addr, id), strings.NewReader(payload))
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("testserver: unexpected status %s", resp.Status)
	}
	return nil
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

// checkNotified reports whether id has already been notified in testserver, via a short-timeout wait.
func checkNotified(ctx context.Context, addr, id string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://%s/wait/%s?timeout=1", addr, id), nil)
	if err != nil {
		return false, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()
	return resp.StatusCode == http.StatusOK, nil
}

// resolveCard resolves id's AgentCard from checkpointd's /agents/{id}/ endpoint via a real fetch.
func resolveCard(ctx context.Context, id string) (*a2a.AgentCard, error) {
	card, err := agentcard.DefaultResolver.Resolve(ctx, "http://"+*checkpointdAddr+"/agents/"+id+"/.well-known/agent-card.json")
	if err != nil {
		return nil, fmt.Errorf("resolving %s's AgentCard from %s: %w", id, *checkpointdAddr, err)
	}
	return card, nil
}

// sendText sends data through client and unwraps the reply into a plain *a2a.Message.
func sendText(ctx context.Context, client *a2aclient.Client, data string) (*a2a.Message, error) {
	result, err := client.SendMessage(ctx, &a2a.SendMessageRequest{
		Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart(data)),
	})
	if err != nil {
		return nil, err
	}
	switch v := result.(type) {
	case *a2a.Message:
		return v, nil
	case *a2a.Task:
		return v.Status.Message, nil
	default:
		return nil, fmt.Errorf("unexpected reply: %T", result)
	}
}
