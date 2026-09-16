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

// Command tck_agent runs the A2A TCK against checkpointd server mode, routing
// on well-known messageId prefixes like the TCK's own reference SUT does (
// https://github.com/a2aproject/a2a-tck/blob/5996b79f9cefa6fc390980e383e358a66fb9e49e/sut/a2a-python/sut_agent.py)
package main

import (
	"context"
	"flag"
	"iter"
	"log"
	"strings"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/ktock/checkpointd/harness"
)

var (
	harnessAddr = flag.String("harness-addr", "127.0.0.1:50060", "address of this agent's HarnessService, i.e. this actor's own worker port")
)

func main() {
	flag.Parse()
	h := harness.NewAgentHarness(a2asrv.AgentExecutorFunc(tckAgent))
	if err := harness.Serve(*harnessAddr, h); err != nil {
		log.Fatal(err)
	}
}

// tckAgent routes purely on execCtx.Message.ID's prefix, matching the TCK reference SUT's own dispatch.
func tckAgent(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		msg := execCtx.Message

		switch {
		case strings.HasPrefix(msg.ID, "tck-input-required"):
			// InputRequired is non-terminal, so this pauses and hands control back to the external caller.
			info := execCtx.TaskInfo()
			yield(&a2a.Task{
				ID:        info.TaskID,
				ContextID: info.ContextID,
				Status: a2a.TaskStatus{
					State:   a2a.TaskStateInputRequired,
					Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("input required")),
				},
			}, nil)

		case strings.HasPrefix(msg.ID, "tck-reject-task"):
			// An ordinary business rejection is a terminal Task state, not a returned error.
			info := execCtx.TaskInfo()
			yield(&a2a.Task{ID: info.TaskID, ContextID: info.ContextID, Status: a2a.TaskStatus{State: a2a.TaskStateRejected}}, nil)

		case strings.HasPrefix(msg.ID, "tck-message-response"):
			// No TaskStatusUpdateEvent here, so SendMessage returns this bare Message directly.
			reply := a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("Direct message response"))
			yield(reply, nil)

		case strings.HasPrefix(msg.ID, "tck-artifact-text"):
			yieldArtifactCompletion(execCtx, yield, a2a.NewTextPart("Generated text content"))

		case strings.HasPrefix(msg.ID, "tck-artifact-file-url"):
			part := a2a.NewFileURLPart("https://example.com/output.txt", "text/plain")
			part.Filename = "output.txt"
			yieldArtifactCompletion(execCtx, yield, part)

		case strings.HasPrefix(msg.ID, "tck-artifact-file"):
			part := a2a.NewRawPart([]byte("tck"))
			part.Filename = "output.txt"
			part.MediaType = "text/plain"
			yieldArtifactCompletion(execCtx, yield, part)

		case strings.HasPrefix(msg.ID, "tck-artifact-data"):
			data := map[string]any{"key": "value", "count": 42}
			yieldArtifactCompletion(execCtx, yield, a2a.NewDataPart(data))

		case strings.HasPrefix(msg.ID, "tck-complete-task"):
			fallthrough
		default:
			// Every other prefix completes with an echo reply, as a single whole Task.
			info := execCtx.TaskInfo()
			yield(&a2a.Task{
				ID:        info.TaskID,
				ContextID: info.ContextID,
				Status: a2a.TaskStatus{
					State:   a2a.TaskStateCompleted,
					Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("tck_agent handled "+msg.ID)),
				},
			}, nil)
		}
	}
}

// yieldArtifactCompletion yields a Completed reply carrying one artifact with one part, as a single whole Task.
func yieldArtifactCompletion(execCtx *a2asrv.ExecutorContext, yield func(a2a.Event, error) bool, part *a2a.Part) {
	info := execCtx.TaskInfo()
	yield(&a2a.Task{
		ID:        info.TaskID,
		ContextID: info.ContextID,
		Status: a2a.TaskStatus{
			State:   a2a.TaskStateCompleted,
			Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("artifact produced")),
		},
		Artifacts: []*a2a.Artifact{{ID: a2a.NewArtifactID(), Parts: a2a.ContentParts{part}}},
	}, nil)
}
