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

// Command echo_agent is the plain echo agent for script/test-k8s, a real Agent Substrate actor registered as "echo-agent".
package main

import (
	"context"
	"flag"
	"fmt"
	"iter"
	"log"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/ktock/checkpointd/harness"
)

var (
	harnessAddr = flag.String("harness-addr", "127.0.0.1:50060", "address of this agent's HarnessService, i.e. this actor's own worker port")
	agentID     = flag.String("agent-id", "echo-agent", "this agent's own ax id (registry.substrate[].id/CHECKPOINTD_AGENT_ID) -- this process's own deployment already knows it, so it's passed in directly rather than asked back from checkpointd on every turn")
)

func main() {
	flag.Parse()
	h := harness.NewAgentHarness(echoAgent(*agentID))
	if err := harness.Serve(*harnessAddr, h); err != nil {
		log.Fatal(err)
	}
}

// echoAgent replies with Data set to "<agentID> received <input>", as a single whole Task, never a bare update event.
func echoAgent(agentID string) a2asrv.AgentExecutorFunc {
	return func(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			data := text(execCtx.Message)
			out := fmt.Sprintf("%s received %s", agentID, data)
			yield(&a2a.Task{
				Status: a2a.TaskStatus{
					State:   a2a.TaskStateCompleted,
					Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart(out)),
				},
			}, nil)
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
