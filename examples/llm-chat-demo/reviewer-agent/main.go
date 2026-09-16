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

// Command reviewer-agent gives a second opinion on chat-agent's answer,
// using the same stateless llama.cpp completion server.
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
	harnessAddr = flag.String("harness-addr", "127.0.0.1:50060", "address of this agent's HarnessService")
	llamaAddr   = flag.String("llama-addr", "127.0.0.1:8080", "address of the llama.cpp completion server")
)

func main() {
	flag.Parse()
	h := harness.NewAgentHarness(a2asrv.AgentExecutorFunc(reviewerAgent))
	if err := harness.Serve(*harnessAddr, h); err != nil {
		log.Fatal(err)
	}
}

// conversation and history mirror chat-agent's own package vars of the same
// name.
var (
	conversation []string
	history      []turn
)

func reviewerAgent(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		in := text(execCtx.Message)
		messages := buildMessages("You are a thoughtful reviewer. Give a brief, one- or two-sentence second opinion on the assistant's answer.", history, in)
		out, err := complete(ctx, *llamaAddr, messages)
		if err != nil {
			yield(nil, fmt.Errorf("llama completion: %w", err))
			return
		}
		history = append(history, turn{User: in, Assistant: out})
		conversation = append(conversation, in+"\nReviewer: "+out)
		yield(&a2a.Task{
			Status: a2a.TaskStatus{
				State:   a2a.TaskStateCompleted,
				Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart(out)),
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
