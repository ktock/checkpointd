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

// Command crash_recovery_agent is "crash-recovery" for script/test-k8s/test.sh's
// 02-crash-recovery-from-last-message subtest: a minimal, single-hop version
// of crash-test-agent. It calls echo-agent exactly once, then blocks on a
// plain (non-agent-to-agent) testserver wait -- the test force-deletes this
// actor's own worker pod during that wait, to confirm recovery resumes past
// the echo-agent reply it already has rather than replaying this workflow
// (and its call to echo-agent) from the beginning.
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
	testserverAddr  = flag.String("testserver-addr", "127.0.0.1:8080", "address of the script/test-k8s/agents/testserver server")
	checkpointdAddr = flag.String("checkpointd-addr", "", "address of checkpointd's own A2A endpoint (e.g. checkpointd-server.<namespace>.svc:80), required -- used to resolve echo-agent's real AgentCard via a real HTTP fetch")
)

func main() {
	flag.Parse()
	if *checkpointdAddr == "" {
		log.Fatal("--checkpointd-addr is required (used to resolve echo-agent's real AgentCard -- see resolveCard)")
	}
	h := harness.NewAgentHarness(a2asrv.AgentExecutorFunc(crashRecoveryWorkflow))
	if err := harness.Serve(*harnessAddr, h); err != nil {
		log.Fatal(err)
	}
}

func crashRecoveryWorkflow(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
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

		echoCard, err := resolveCard(ctx, "echo-agent")
		if err != nil {
			yield(nil, err)
			return
		}
		client, err := a2aclient.NewFromCard(ctx, echoCard, a2aclient.WithDefaultsDisabled(), harness.WithTransport(execCtx))
		if err != nil {
			yield(nil, err)
			return
		}
		// checkpointd checkpoints and suspends this agent right here, on the
		// send, and resumes it directly from this reply once echo-agent
		// answers.
		reply, err := sendText(ctx, client, id)
		if err != nil {
			yield(nil, err)
			return
		}
		// Placed directly after the checkpoint above, to empirically verify
		// README.md's claim that checkpointd resumes this agent "directly
		// from the checkpoint": if the worker crashes later in this same
		// turn, recovery must replay this ordinary (non-a2a, so not itself
		// logged) line again rather than restoring exact pre-crash process
		// state -- so a healthy crash-recovered run reaches this line
		// twice, unlike the entrypoint counter above, which must stay at 1.
		if err := incrTestServer(ctx, *testserverAddr, id+"-resumed"); err != nil {
			yield(nil, fmt.Errorf("recording resume: %w", err))
			return
		}

		// Signals readiness, then blocks on a plain HTTP wait that is not an
		// agent-to-agent call. The test force-deletes this actor's own
		// worker pod once signaled, so the crash lands after the agent has
		// already resumed with echo-agent's reply above.
		if err := notifyTestServer(ctx, *testserverAddr, id+"-ready", "ready"); err != nil {
			yield(nil, fmt.Errorf("signaling readiness: %w", err))
			return
		}
		if err := waitTestServer(ctx, *testserverAddr, id+"-go"); err != nil {
			yield(nil, fmt.Errorf("waiting for go signal: %w", err))
			return
		}

		// A single whole Task with Status.Message set, not a bare update event.
		yield(&a2a.Task{
			ID:        info.TaskID,
			ContextID: info.ContextID,
			Status: a2a.TaskStatus{
				State:   a2a.TaskStateCompleted,
				Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart(text(reply))),
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

// waitTestServer blocks on testserver's GET /wait/{id} with no timeout at
// all -- it only unblocks once id is actually notified (or ctx is
// canceled), deliberately never on a mere elapsed-time threshold, so this
// test can't pass by racing a timeout against a real recovery.
func waitTestServer(ctx context.Context, addr, id string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://%s/wait/%s", addr, id), nil)
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
		return fmt.Errorf("testserver: wait for %s failed (status %s)", id, resp.Status)
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
