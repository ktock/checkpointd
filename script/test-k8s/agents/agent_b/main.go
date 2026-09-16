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

// Command agent_b (registered as "agent-b") is a four-turn state machine going Working, InputRequired twice, then Completed, driven by both agent_a and a direct external A2A client.
package main

import (
	"context"
	"flag"
	"fmt"
	"iter"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/ktock/checkpointd/harness"
)

var (
	harnessAddr = flag.String("harness-addr", "127.0.0.1:50060", "address of this agent's HarnessService, i.e. this actor's own worker port")
)

func main() {
	flag.Parse()
	h := harness.NewAgentHarness(a2asrv.AgentExecutorFunc(agentB))
	if err := harness.Serve(*harnessAddr, h); err != nil {
		panic(err)
	}
}

func agentB(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		if execCtx.StoredTask == nil {
			// Turns 1 and 2 both run in this same call, but turn 2 still returns since InputRequired is a real interruption.
			info := execCtx.TaskInfo()
			if !yield(&a2a.Task{
				ID:        info.TaskID,
				ContextID: info.ContextID,
				Status:    a2a.TaskStatus{State: a2a.TaskStateWorking},
				Metadata:  map[string]any{metadataOriginKey: "turn1"},
			}, nil) {
				return
			}
			// This is the first artifact chunk; turn 3 appends a second chunk under the same ID.
			if !yield(&a2a.TaskArtifactUpdateEvent{
				TaskID:    info.TaskID,
				ContextID: info.ContextID,
				Artifact: &a2a.Artifact{
					ID:    notesArtifactID,
					Parts: a2a.ContentParts{a2a.NewTextPart("turn1-")},
				},
			}, nil) {
				return
			}
			yield(&a2a.Task{
				ID:        info.TaskID,
				ContextID: info.ContextID,
				Status: a2a.TaskStatus{
					State:   a2a.TaskStateInputRequired,
					Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("agent-b: please provide input")),
				},
			}, nil)
			return
		}

		received := text(execCtx.Message)
		if received == firstAnswer {
			// Turn 3 answers the first prompt, appends a second artifact chunk and metadata key, and asks a second question.
			printReceived(received)
			if !yield(a2a.NewArtifactUpdateEvent(execCtx, notesArtifactID, a2a.NewTextPart("turn3-")), nil) {
				return
			}
			statusUpdate := a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateInputRequired,
				a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("agent-b: please provide a second answer")))
			statusUpdate.Metadata = map[string]any{metadataTurn3Key: "seen"}
			yield(statusUpdate, nil)
			return
		}

		// Turn 4 answers the second prompt and completes, touching neither the artifact nor Metadata.
		printReceived(received)
		yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateCompleted,
			a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewDataPart(map[string]any{
				"data": fmt.Sprintf("B received: %s", received),
			}))), nil)
	}
}

// firstAnswer is the caller's answer to agent-b's first InputRequired prompt.
const firstAnswer = "hello"

// notesArtifactID, metadataOriginKey, and metadataTurn3Key are the fixed identifiers for this state machine's cross-turn Artifacts and Metadata.
const (
	notesArtifactID   a2a.ArtifactID = "b-notes"
	metadataOriginKey                = "agent-b/origin"
	metadataTurn3Key                 = "agent-b/turn3"
)

// printReceived writes to this process's own stdout, which test.sh's subtests check via `kubectl logs`.
func printReceived(received string) {
	fmt.Printf("agent-b: received input: %s\n", received)
}

// text returns the text of msg's first part, or "" if there is none.
func text(msg *a2a.Message) string {
	if msg == nil || len(msg.Parts) == 0 {
		return ""
	}
	return msg.Parts[0].Text()
}
