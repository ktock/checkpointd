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

// Command chat-agent answers the user via a llama.cpp completion server, asks
// reviewer-agent for a second opinion, and replies with both plus the running log.
package main

import (
	"context"
	"flag"
	"fmt"
	"iter"
	"log"
	"strings"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/a2aproject/a2a-go/v2/a2aclient/agentcard"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/ktock/checkpointd/harness"
)

var (
	harnessAddr     = flag.String("harness-addr", "127.0.0.1:50060", "address of this agent's HarnessService")
	llamaAddr       = flag.String("llama-addr", "127.0.0.1:8080", "address of the llama.cpp completion server")
	checkpointdAddr = flag.String("checkpointd-addr", "", "address of checkpointd's own A2A endpoint, required to resolve reviewer-agent's AgentCard")
	reviewerAgentID = flag.String("reviewer-agent-id", "reviewer-agent", "the ID reviewer-agent is discovered under")
)

func main() {
	flag.Parse()
	h := harness.NewAgentHarness(a2asrv.AgentExecutorFunc(chatAgent))
	if err := harness.Serve(*harnessAddr, h); err != nil {
		log.Fatal(err)
	}
}

// conversation is this actor's running history shown to the user; history is the
// same turns shaped for buildMessages.
var (
	conversation []string
	history      []turn
)

func chatAgent(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		in := text(execCtx.Message)

		messages := buildMessages("You are a helpful, concise assistant. Keep answers to one short sentence.", history, in)
		myAnswer, err := complete(ctx, *llamaAddr, messages)
		if err != nil {
			yield(nil, fmt.Errorf("llama completion: %w", err))
			return
		}

		reviewerAnswer, err := askReviewer(ctx, execCtx, in, myAnswer)
		if err != nil {
			yield(nil, err)
			return
		}

		history = append(history, turn{User: in, Assistant: myAnswer})
		conversation = append(conversation, fmt.Sprintf("User: %s\nAssistant: %s\nReviewer: %s", in, myAnswer, reviewerAnswer))
		reply := fmt.Sprintf("Assistant: %s\nReviewer: %s\n\n--- conversation log from process memory ---\n%s",
			myAnswer, reviewerAnswer, strings.Join(conversation, "\n------------------------\n"))
		yield(&a2a.Task{
			Status: a2a.TaskStatus{
				State:   a2a.TaskStateInputRequired,
				Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart(reply)),
			},
		}, nil)
	}
}

// askReviewer sends reviewer-agent the user's input plus this agent's answer,
// durably via SendMessage.
func askReviewer(ctx context.Context, execCtx *a2asrv.ExecutorContext, in, myAnswer string) (string, error) {
	if *checkpointdAddr == "" {
		return "", fmt.Errorf("--checkpointd-addr is required (used to resolve %s's AgentCard)", *reviewerAgentID)
	}
	card, err := agentcard.DefaultResolver.Resolve(ctx, "http://"+*checkpointdAddr+"/agents/"+*reviewerAgentID+"/.well-known/agent-card.json")
	if err != nil {
		return "", fmt.Errorf("resolving %s's AgentCard from %s: %w", *reviewerAgentID, *checkpointdAddr, err)
	}
	client, err := a2aclient.NewFromCard(ctx, card, a2aclient.WithDefaultsDisabled(), harness.WithTransport(execCtx))
	if err != nil {
		return "", fmt.Errorf("connecting to %s: %w", *reviewerAgentID, err)
	}
	ask := fmt.Sprintf("User asked: %s\nAssistant's answer: %s\nWhat's your take?", in, myAnswer)
	res, err := client.SendMessage(ctx, &a2a.SendMessageRequest{
		Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart(ask)),
	})
	if err != nil {
		return "", fmt.Errorf("SendMessage to %s: %w", *reviewerAgentID, err)
	}
	task, ok := res.(*a2a.Task)
	if !ok || task.Status.Message == nil || len(task.Status.Message.Parts) == 0 {
		return "", fmt.Errorf("%s returned an unexpected reply shape: %T", *reviewerAgentID, res)
	}
	return task.Status.Message.Parts[0].Text(), nil
}

// text returns the text of msg's first part, or "" if there is none.
func text(msg *a2a.Message) string {
	if msg == nil || len(msg.Parts) == 0 {
		return ""
	}
	return msg.Parts[0].Text()
}
