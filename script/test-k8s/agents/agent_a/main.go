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

// Command agent_a (registered as "agent-a") drives agent_b through three sequential SendMessage calls, asserting on each reply's real Task shape.
package main

import (
	"context"
	"flag"
	"fmt"
	"iter"
	"log"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/a2aproject/a2a-go/v2/a2aclient/agentcard"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/ktock/checkpointd/harness"
)

var (
	harnessAddr     = flag.String("harness-addr", "127.0.0.1:50060", "address of this agent's HarnessService, i.e. this actor's own worker port")
	checkpointdAddr = flag.String("checkpointd-addr", "", "address of checkpointd's own A2A endpoint (e.g. checkpointd-server.<namespace>.svc:80), required -- used to resolve agent-b's real AgentCard via a real HTTP fetch")
)

func main() {
	flag.Parse()
	if *checkpointdAddr == "" {
		log.Fatal("--checkpointd-addr is required (used to resolve agent-b's real AgentCard -- see resolveCard)")
	}
	h := harness.NewAgentHarness(a2asrv.AgentExecutorFunc(agentA))
	if err := harness.Serve(*harnessAddr, h); err != nil {
		log.Fatal(err)
	}
}

func agentA(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		// Reports Working before doing anything else, without returning.
		info := execCtx.TaskInfo()
		if !yield(&a2a.Task{
			ID:        info.TaskID,
			ContextID: info.ContextID,
			Status:    a2a.TaskStatus{State: a2a.TaskStateWorking},
		}, nil) {
			return
		}

		result := driveAgentB(ctx, execCtx)
		// A single whole Task, never a bare update+Message pair.
		yield(&a2a.Task{
			ID:        info.TaskID,
			ContextID: info.ContextID,
			Status: a2a.TaskStatus{
				State:   a2a.TaskStateCompleted,
				Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart(result)),
			},
		}, nil)
	}
}

// driveAgentB runs the three-call sequence against agent-b and returns a human-readable PASS/FAIL string.
func driveAgentB(ctx context.Context, execCtx *a2asrv.ExecutorContext) string {
	card, err := resolveCard(ctx, "agent-b")
	if err != nil {
		return "FAIL: resolving agent-b's AgentCard: " + err.Error()
	}
	client, err := a2aclient.NewFromCard(ctx, card, a2aclient.WithDefaultsDisabled(), harness.WithTransport(execCtx))
	if err != nil {
		return "FAIL: connecting to agent-b: " + err.Error()
	}

	res1, err := client.SendMessage(ctx, &a2a.SendMessageRequest{
		Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("start")),
	})
	if err != nil {
		return "FAIL: first SendMessage to agent-b: " + err.Error()
	}
	task1, ok := res1.(*a2a.Task)
	if !ok {
		return fmt.Sprintf("FAIL: first reply from agent-b is %T, want *a2a.Task (agent-a must not wake on agent-b's own non-interrupting Working state)", res1)
	}
	if task1.Status.State != a2a.TaskStateInputRequired {
		return fmt.Sprintf("FAIL: first reply from agent-b has State=%s, want TASK_STATE_INPUT_REQUIRED", task1.Status.State)
	}
	// Turn 1's artifact chunk and metadata key must already be visible on
	// this very first reply.
	if err := checkArtifact(task1, "turn1-"); err != nil {
		return "FAIL: first reply from agent-b: " + err.Error()
	}
	if err := checkMetadata(task1, wantOriginKey, "turn1"); err != nil {
		return "FAIL: first reply from agent-b: " + err.Error()
	}

	res2, err := client.SendMessage(ctx, &a2a.SendMessageRequest{
		Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("hello")),
	})
	if err != nil {
		return "FAIL: second SendMessage to agent-b: " + err.Error()
	}
	task2, ok := res2.(*a2a.Task)
	if !ok {
		return fmt.Sprintf("FAIL: second reply from agent-b is %T, want *a2a.Task", res2)
	}
	if task2.Status.State != a2a.TaskStateInputRequired {
		return fmt.Sprintf("FAIL: second reply from agent-b has State=%s, want TASK_STATE_INPUT_REQUIRED", task2.Status.State)
	}
	// Turn 3 appends a second chunk and metadata key, rather than replacing turn 1's.
	if err := checkArtifact(task2, "turn1-turn3-"); err != nil {
		return "FAIL: second reply from agent-b: " + err.Error()
	}
	if err := checkMetadata(task2, wantOriginKey, "turn1"); err != nil {
		return "FAIL: second reply from agent-b: " + err.Error()
	}
	if err := checkMetadata(task2, wantTurn3Key, "seen"); err != nil {
		return "FAIL: second reply from agent-b: " + err.Error()
	}

	res3, err := client.SendMessage(ctx, &a2a.SendMessageRequest{
		Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("world")),
	})
	if err != nil {
		return "FAIL: third SendMessage to agent-b: " + err.Error()
	}
	// agent-b's turn 4 yields only a TaskStatusUpdateEvent, so this checks the aggregated reply still has Status.Message intact.
	task3, ok := res3.(*a2a.Task)
	if !ok {
		return fmt.Sprintf("FAIL: third reply from agent-b is %T, want *a2a.Task (a trailing TaskStatusUpdateEvent with no separate Message must still wrap into a real Task)", res3)
	}
	if task3.Status.State != a2a.TaskStateCompleted {
		return fmt.Sprintf("FAIL: third reply from agent-b has State=%s, want TASK_STATE_COMPLETED", task3.Status.State)
	}
	if task3.Status.Message == nil || len(task3.Status.Message.Parts) == 0 {
		return fmt.Sprintf("FAIL: third reply from agent-b has no Status.Message (the trailing update's own message was lost)")
	}
	data, ok := task3.Status.Message.Parts[0].Data().(map[string]any)
	if !ok {
		return fmt.Sprintf("FAIL: third reply from agent-b's own Status.Message part is not a DataPart: %v", task3.Status.Message.Parts[0])
	}
	got, _ := data["data"].(string)
	const want = "B received: world"
	if got != want {
		return fmt.Sprintf("FAIL: agent-b's own final data = %q, want %q", got, want)
	}
	// Turn 4 touches neither the artifact nor metadata; both must carry forward unchanged.
	if err := checkArtifact(task3, "turn1-turn3-"); err != nil {
		return "FAIL: third reply from agent-b: " + err.Error()
	}
	if err := checkMetadata(task3, wantOriginKey, "turn1"); err != nil {
		return "FAIL: third reply from agent-b: " + err.Error()
	}
	if err := checkMetadata(task3, wantTurn3Key, "seen"); err != nil {
		return "FAIL: third reply from agent-b: " + err.Error()
	}

	return "PASS: agent-b correctly reported InputRequired twice before waking us, then completed with: " + got +
		" (Artifacts and Metadata both correctly preserved across all three turns)"
}

// Mirrors agent_b's own constants, kept as independent literals since they're separate packages.
const (
	notesArtifactID a2a.ArtifactID = "b-notes"
	wantOriginKey                  = "agent-b/origin"
	wantTurn3Key                   = "agent-b/turn3"
)

// checkArtifact confirms task carries exactly one artifact, under
// notesArtifactID, whose Parts join into wantText.
func checkArtifact(task *a2a.Task, wantText string) error {
	if len(task.Artifacts) != 1 {
		return fmt.Errorf("Artifacts has %d entries, want 1 (id %q)", len(task.Artifacts), notesArtifactID)
	}
	art := task.Artifacts[0]
	if art.ID != notesArtifactID {
		return fmt.Errorf("Artifacts[0].ID = %q, want %q", art.ID, notesArtifactID)
	}
	var got string
	for _, p := range art.Parts {
		got += p.Text()
	}
	if got != wantText {
		return fmt.Errorf("artifact %q text = %q, want %q (turn 1's and turn 3's own chunks must merge into one artifact, not replace or duplicate each other)", notesArtifactID, got, wantText)
	}
	return nil
}

// checkMetadata confirms task.Metadata[key] == want.
func checkMetadata(task *a2a.Task, key, want string) error {
	got, _ := task.Metadata[key].(string)
	if got != want {
		return fmt.Errorf("Metadata[%q] = %q, want %q", key, got, want)
	}
	return nil
}

// resolveCard resolves id's AgentCard from checkpointd's
// /agents/{id}/ endpoint via a real, unmodified a2aclient/agentcard fetch.
func resolveCard(ctx context.Context, id string) (*a2a.AgentCard, error) {
	card, err := agentcard.DefaultResolver.Resolve(ctx, "http://"+*checkpointdAddr+"/agents/"+id+"/.well-known/agent-card.json")
	if err != nil {
		return nil, fmt.Errorf("resolving %s's AgentCard from %s: %w", id, *checkpointdAddr, err)
	}
	return card, nil
}
