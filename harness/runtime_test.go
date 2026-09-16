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

package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/google/uuid"
	"github.com/ktock/checkpointd/internal/hop"
	"github.com/ktock/checkpointd/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// testCardFor returns a minimal *a2a.AgentCard for target, carrying only a
// checkpointd: interface -- enough to exercise WithTransport's routing
// without a real card fetch.
func testCardFor(target string) *a2a.AgentCard {
	return &a2a.AgentCard{
		Name:                target,
		SupportedInterfaces: []*a2a.AgentInterface{a2a.NewAgentInterface(hop.CheckpointdURLPrefix+target, hop.CheckpointdTransportProtocol)},
	}
}

// newClient builds a client bound to target the same way real agent code
// does: a2aclient.NewFromCard plus WithTransport.
func newClient(execCtx *a2asrv.ExecutorContext, target string) (*a2aclient.Client, error) {
	return a2aclient.NewFromCard(context.Background(), testCardFor(target),
		a2aclient.WithDefaultsDisabled(), WithTransport(execCtx))
}

// startTestHarness starts h on a real, local gRPC listener and returns a
// client dialed to it, exercising the actual proto.HarnessService wire
// protocol rather than a hand-rolled fake.
func startTestHarness(t *testing.T, h proto.HarnessServiceServer) proto.HarnessServiceClient {
	t.Helper()
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer()
	proto.RegisterHarnessServiceServer(s, h)
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return proto.NewHarnessServiceClient(conn)
}

// envNew returns a fresh Envelope carrying a plain-text Message, with a
// freshly minted StepID. A caller simulating a genuine redelivery reuses
// the same *hop.Envelope value across two sendTurn calls instead of calling
// this again.
func envNew(role a2a.MessageRole, from, to, data string) *hop.Envelope {
	return &hop.Envelope{From: from, To: to, StepID: uuid.NewString(), Data: hop.Data{Message: a2a.NewMessage(role, a2a.NewTextPart(data)), Type: hop.DataTypeMessage}}
}

// envReply returns a fresh Envelope replying to in, carrying in's own
// Data.Message.TaskID forward. in == nil behaves the same as envNew.
func envReply(in *hop.Envelope, role a2a.MessageRole, to, data string) *hop.Envelope {
	env := envNew(role, "", to, data)
	env.Reply = true
	if in != nil && in.Data.Message != nil {
		env.Data.Message.TaskID = in.Data.Message.TaskID
	}
	return env
}

// text returns the text of msg's first part, or "" if there is none.
func text(msg *a2a.Message) string {
	if msg == nil || len(msg.Parts) == 0 {
		return ""
	}
	return msg.Parts[0].Text()
}

// taskState returns d.Task.Status.State, or a2a.TaskStateUnspecified if d
// carries no Task claim at all -- not a merge with anything on Message,
// since Message has no State of its own to consider.
func taskState(d hop.Data) a2a.TaskState {
	if d.Task == nil {
		return a2a.TaskStateUnspecified
	}
	return d.Task.Status.State
}

// msgStep wraps env as a single HarnessStart step: one turn's input, or one
// prior turn's logged history entry.
func msgStep(t *testing.T, env *hop.Envelope) *proto.Step {
	t.Helper()
	b, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	return &proto.Step{
		Type: &proto.Step_Content{
			Content: &proto.ContentStep{
				Role: "user",
				Content: []*proto.Content{{
					Type: &proto.Content_Text{Text: &proto.TextContent{Text: string(b)}},
				}},
			},
		},
	}
}

// turnResult is what sendTurn reports back: either a decoded reply
// envelope, or the error the Connect() RPC itself returned.
type turnResult struct {
	msg *hop.Envelope
	err error
}

// sendTurn drives one full HarnessService.Connect round trip against
// client: opens a fresh stream, sends msgs as that turn's history plus
// current input, and returns the decoded reply.
func sendTurn(t *testing.T, ctx context.Context, client proto.HarnessServiceClient, conversationID, agentID string, msgs ...*hop.Envelope) turnResult {
	t.Helper()
	stream, err := client.Connect(ctx)
	if err != nil {
		return turnResult{err: err}
	}
	steps := make([]*proto.Step, len(msgs))
	for i, m := range msgs {
		steps[i] = msgStep(t, m)
	}
	if err := stream.Send(&proto.HarnessRequest{
		ConversationId: conversationID,
		AgentId:        agentID,
		Type:           &proto.HarnessRequest_Start{Start: &proto.HarnessStart{Steps: steps}},
	}); err != nil {
		return turnResult{err: err}
	}
	if err := stream.CloseSend(); err != nil {
		return turnResult{err: err}
	}

	var reply *hop.Envelope
	for {
		resp, err := stream.Recv()
		if err != nil {
			return turnResult{err: err}
		}
		switch v := resp.Type.(type) {
		case *proto.HarnessResponse_Outputs:
			for _, step := range v.Outputs.GetSteps() {
				text := firstText([]*proto.Step{step})
				if text == "" {
					continue
				}
				var env hop.Envelope
				if err := json.Unmarshal([]byte(text), &env); err != nil {
					t.Fatalf("unmarshal output: %v", err)
				}
				reply = &env
			}
		case *proto.HarnessResponse_End:
			return turnResult{msg: reply}
		}
	}
}

// echoExecutor immediately replies with data unaddressed (hop.To == "") --
// the simplest possible AgentExecutor: one turn in, one reply out, never
// touching a client.
func echoExecutor(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		yield(a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("echo: "+text(execCtx.Message))), nil)
	}
}

func TestAgentHarness_SingleTurn_NoClientUse(t *testing.T) {
	client := startTestHarness(t, NewAgentHarness(a2asrv.AgentExecutorFunc(echoExecutor)))
	ctx := context.Background()

	bootstrap := envNew(a2a.MessageRoleUser, "caller", "echo-agent", "hello")
	res := sendTurn(t, ctx, client, "conv-1", "echo-agent", bootstrap)
	if res.err != nil {
		t.Fatalf("sendTurn: %v", res.err)
	}
	if got, want := text(res.msg.Data.Message), "echo: hello"; got != want {
		t.Errorf("reply text = %q, want %q", got, want)
	}
	if got, want := res.msg.From, "echo-agent"; got != want {
		t.Errorf("reply From = %q, want %q (must be stamped regardless of what the executor itself set)", got, want)
	}
	if got, want := res.msg.To, "caller"; got != want {
		t.Errorf("reply To = %q, want %q (runInvocation defaults an unaddressed reply back to whoever sent the current message)", got, want)
	}
}

// TestAgentHarness_MissingStepIDRejected confirms Connect rejects a current
// envelope whose own StepID is empty, since every real production caller
// mints one unconditionally.
func TestAgentHarness_MissingStepIDRejected(t *testing.T) {
	client := startTestHarness(t, NewAgentHarness(a2asrv.AgentExecutorFunc(echoExecutor)))
	ctx := context.Background()

	bootstrap := envNew(a2a.MessageRoleUser, "caller", "echo-agent", "hello")
	bootstrap.StepID = ""
	res := sendTurn(t, ctx, client, "conv-missing-stepid", "echo-agent", bootstrap)
	if res.err == nil {
		t.Fatal("sendTurn succeeded, want an error (StepID must be set)")
	}
	if got, want := status.Code(res.err), codes.InvalidArgument; got != want {
		t.Errorf("error code = %v, want %v (err: %v)", got, want, res.err)
	}
}

// TestAgentHarness_EmptyFromBootstrapIsAcceptedAndDefaultsReplyToEmpty
// confirms a bootstrap envelope with an explicit, empty From (checkpointd's
// own reserved identity) round-trips correctly, and that runInvocation's
// default routing echoes it back as an equally empty To.
func TestAgentHarness_EmptyFromBootstrapIsAcceptedAndDefaultsReplyToEmpty(t *testing.T) {
	client := startTestHarness(t, NewAgentHarness(a2asrv.AgentExecutorFunc(echoExecutor)))
	ctx := context.Background()

	bootstrap := envNew(a2a.MessageRoleUser, "", "echo-agent", "hello")
	res := sendTurn(t, ctx, client, "conv-1", "echo-agent", bootstrap)
	if res.err != nil {
		t.Fatalf("sendTurn: %v", res.err)
	}
	if got, want := text(res.msg.Data.Message), "echo: hello"; got != want {
		t.Errorf("reply text = %q, want %q", got, want)
	}
	if got, want := res.msg.To, ""; got != want {
		t.Errorf("reply To = %q, want %q (defaults back to the bootstrap's own empty From)", got, want)
	}
}

// callAndWaitExecutor calls "other-agent" and returns whatever it gets
// back, prefixed -- a single-turn AgentExecutor blocking mid-invocation on
// a real client.SendMessage call.
func callAndWaitExecutor(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		client, err := newClient(execCtx, "other-agent")
		if err != nil {
			yield(nil, err)
			return
		}
		result, err := client.SendMessage(ctx, &a2a.SendMessageRequest{
			Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("ping")),
		})
		if err != nil {
			yield(nil, err)
			return
		}
		otherReply, ok := result.(*a2a.Message)
		if !ok {
			yield(nil, errors.New("unexpected reply type"))
			return
		}
		reply := a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("got: "+text(otherReply)))
		yield(reply, nil)
	}
}

func TestAgentHarness_CallAndWait(t *testing.T) {
	client := startTestHarness(t, NewAgentHarness(a2asrv.AgentExecutorFunc(callAndWaitExecutor)))
	ctx := context.Background()

	// Turn 1: bootstraps the invocation, which immediately calls
	// SendMessage("ping") on "other-agent" and blocks -- that outbound
	// message is what this turn's own response carries.
	bootstrap := envNew(a2a.MessageRoleUser, "caller", "caller-agent", "start")
	res1 := sendTurn(t, ctx, client, "conv-1", "caller-agent", bootstrap)
	if res1.err != nil {
		t.Fatalf("turn 1: %v", res1.err)
	}
	if got, want := res1.msg.To, "other-agent"; got != want {
		t.Fatalf("turn 1 reply To = %q, want %q (the outbound SendMessage call)", got, want)
	}
	if got, want := text(res1.msg.Data.Message), "ping"; got != want {
		t.Fatalf("turn 1 reply text = %q, want %q", got, want)
	}

	// Turn 2: simulate "other-agent"'s own reply, matched by its correlation
	// token -- built by hand here since this test drives "other-agent"
	// directly, not through a second real harness.
	otherReply := envReply(res1.msg, a2a.MessageRoleAgent, "caller-agent", "pong")
	otherReply.From = "other-agent" // envReply leaves From empty
	otherReply.Correlation = res1.msg.Correlation
	res2 := sendTurn(t, ctx, client, "conv-1", "caller-agent", otherReply)
	if res2.err != nil {
		t.Fatalf("turn 2: %v", res2.err)
	}
	if got, want := text(res2.msg.Data.Message), "got: pong"; got != want {
		t.Errorf("turn 2 (final) reply text = %q, want %q", got, want)
	}
}

// TestAgentHarness_BareUpdateAsFirstEventIsRejected confirms
// NewAgentHarness rejects a *a2a.TaskStatusUpdateEvent or
// *a2a.TaskArtifactUpdateEvent as an executor's first-ever event: a Task
// must be established first via a whole *a2a.Task.
func TestAgentHarness_BareUpdateAsFirstEventIsRejected(t *testing.T) {
	client := startTestHarness(t, NewAgentHarness(a2asrv.AgentExecutorFunc(func(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateCompleted, nil), nil)
		}
	})))
	ctx := context.Background()
	bootstrap := envNew(a2a.MessageRoleUser, "caller", "bad-first-event", "hi")
	res := sendTurn(t, ctx, client, "conv-bad-first-event", "bad-first-event", bootstrap)
	if res.err == nil {
		t.Fatal("sendTurn succeeded, want an error (a bare update can't be an executor's first event)")
	}
}

// TestAgentHarness_MessageAfterTaskEstablishedIsRejected confirms
// NewAgentHarness rejects a bare *a2a.Message yielded after a Task has
// already been established.
func TestAgentHarness_MessageAfterTaskEstablishedIsRejected(t *testing.T) {
	client := startTestHarness(t, NewAgentHarness(a2asrv.AgentExecutorFunc(func(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			if !yield(&a2a.Task{Status: a2a.TaskStatus{State: a2a.TaskStateCompleted}}, nil) {
				return
			}
			yield(a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("pong")), nil)
		}
	})))
	ctx := context.Background()
	bootstrap := envNew(a2a.MessageRoleUser, "caller", "trailing-message", "hi")
	res := sendTurn(t, ctx, client, "conv-trailing-message", "trailing-message", bootstrap)
	if res.err == nil {
		t.Fatal("sendTurn succeeded, want an error (a Message can't follow an established Task)")
	}
}

// TestAgentHarness_NestedCallSeesCalleeTaskState confirms a nested
// client.SendMessage() call sees the callee's full *a2a.Task (State,
// Status.Message, Artifacts) once it interrupts or finishes, matching how a
// real A2A server's blocking send handler behaves.
func TestAgentHarness_NestedCallSeesCalleeTaskState(t *testing.T) {
	client := startTestHarness(t, NewAgentHarness(a2asrv.AgentExecutorFunc(func(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			c, err := newClient(execCtx, "other-agent")
			if err != nil {
				yield(nil, err)
				return
			}
			res, err := c.SendMessage(ctx, &a2a.SendMessageRequest{
				Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("ping")),
			})
			if err != nil {
				yield(nil, err)
				return
			}
			task, ok := res.(*a2a.Task)
			if !ok {
				yield(nil, fmt.Errorf("SendMessage result is %T, want *a2a.Task", res))
				return
			}
			if task.Status.State != a2a.TaskStateInputRequired {
				yield(nil, fmt.Errorf("SendMessage result Status.State = %q, want %q", task.Status.State, a2a.TaskStateInputRequired))
				return
			}
			yield(a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("saw: "+text(task.Status.Message))), nil)
		}
	})))
	ctx := context.Background()

	bootstrap := envNew(a2a.MessageRoleUser, "caller", "caller-agent", "start")
	res1 := sendTurn(t, ctx, client, "conv-1", "caller-agent", bootstrap)
	if res1.err != nil {
		t.Fatalf("turn 1: %v", res1.err)
	}

	// "other-agent"'s own reply, built by hand as a Task-shaped claim
	// (State + Status.Message, not a separate flat Data.Message, since a
	// bare Message can't coexist with an established Task).
	otherReply := envReply(res1.msg, a2a.MessageRoleAgent, "caller-agent", "")
	otherReply.From = "other-agent"
	otherReply.Correlation = res1.msg.Correlation
	otherReply.Data.Message = nil
	otherReply.Data.Task = &a2a.Task{Status: a2a.TaskStatus{
		State:   a2a.TaskStateInputRequired,
		Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("please provide more info")),
	}}
	otherReply.Data.Type = hop.DataTypeTask
	res2 := sendTurn(t, ctx, client, "conv-1", "caller-agent", otherReply)
	if res2.err != nil {
		t.Fatalf("turn 2 (final): %v", res2.err)
	}
	if got, want := text(res2.msg.Data.Message), "saw: please provide more info"; got != want {
		t.Errorf("turn 2 (final) reply text = %q, want %q", got, want)
	}
}

// TestAgentHarness_MessageAfterTaskInHistoryRejected confirms Connect
// rejects a redelivered history where this same actor's own earlier reply
// reverted to a bare Message after it had already claimed a Task.
func TestAgentHarness_MessageAfterTaskInHistoryRejected(t *testing.T) {
	client := startTestHarness(t, NewAgentHarness(a2asrv.AgentExecutorFunc(func(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			yield(a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("unreachable")), nil)
		}
	})))
	ctx := context.Background()

	bootstrap := envNew(a2a.MessageRoleUser, "caller", "self-agent", "start")

	// This actor's own logged self-continuation, from some earlier turn.
	selfContinuation := envReply(bootstrap, a2a.MessageRoleAgent, "self-agent", "")
	selfContinuation.From = "self-agent"
	selfContinuation.Data.Message = nil
	selfContinuation.Data.Task = &a2a.Task{Status: a2a.TaskStatus{State: a2a.TaskStateWorking}}
	selfContinuation.Data.Type = hop.DataTypeTask

	// Impossible via a real agent (NewAgentHarness's own "established"
	// check would reject it) -- this same actor's own reply reverting to a
	// bare Message after that Task was already claimed.
	corrupted := envReply(selfContinuation, a2a.MessageRoleAgent, "self-agent", "corrupted")
	corrupted.From = "self-agent"

	current := envNew(a2a.MessageRoleUser, "caller", "self-agent", "resume")
	res := sendTurn(t, ctx, client, "conv-corrupt", "self-agent", selfContinuation, corrupted, current)
	if res.err == nil {
		t.Fatal("Connect with history reverting to a bare Message after a Task claim: got nil error, want one")
	}
}

// TestAgentHarness_NonOwnReplyNeverContaminatesStatusMessage confirms
// Connect's own history reconstruction only ever sets
// StoredTask.Status.Message from this actor's own claim -- never from an
// input delivered to it (here, a different agent's own reply routed back
// in, e.g. a private sub-call's response) -- matching a2asrv's own
// loadExecutionContext, which appends such content straight into History,
// never through Status.Message.
func TestAgentHarness_NonOwnReplyNeverContaminatesStatusMessage(t *testing.T) {
	client := startTestHarness(t, NewAgentHarness(a2asrv.AgentExecutorFunc(func(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			var priorStatus string
			if execCtx.StoredTask != nil {
				priorStatus = text(execCtx.StoredTask.Status.Message)
			}
			// Task-shaped, not a bare Message: this actor already claimed a
			// Task (below), and a real agent can never revert to a Message
			// once it has (see agent.go's own "established" check).
			info := execCtx.TaskInfo()
			yield(&a2a.Task{
				ID:        info.TaskID,
				ContextID: info.ContextID,
				Status: a2a.TaskStatus{
					State:   a2a.TaskStateCompleted,
					Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("prior-status-message-was:"+priorStatus)),
				},
			}, nil)
		}
	})))
	ctx := context.Background()

	bootstrap := envNew(a2a.MessageRoleUser, "caller", "self-agent", "start")

	// This actor's own logged self-continuation -- its own real claim.
	selfContinuation := envReply(bootstrap, a2a.MessageRoleAgent, "self-agent", "")
	selfContinuation.From = "self-agent"
	selfContinuation.Data.Message = nil
	selfContinuation.Data.Task = &a2a.Task{Status: a2a.TaskStatus{
		State:   a2a.TaskStateWorking,
		Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("self-agent's own claim")),
	}}
	selfContinuation.Data.Type = hop.DataTypeTask

	// "b"'s own reply, routed back to self-agent as an input (e.g. a
	// private sub-call's response) -- must never contaminate self-agent's
	// own Status.Message.
	bReply := envNew(a2a.MessageRoleAgent, "b", "self-agent", "b's own reply content")

	current := envNew(a2a.MessageRoleUser, "caller", "self-agent", "resume")
	res := sendTurn(t, ctx, client, "conv-1", "self-agent", selfContinuation, bReply, current)
	if res.err != nil {
		t.Fatalf("sendTurn: %v", res.err)
	}
	if got, want := text(res.msg.Data.Task.Status.Message), "prior-status-message-was:self-agent's own claim"; got != want {
		t.Errorf("reply text = %q, want %q (StoredTask.Status.Message must reflect self-agent's own claim, not b's reply)", got, want)
	}
}

// TestAgentHarness_SubCallPlumbingExcludedFromHistory confirms Connect's own
// history reconstruction excludes this actor's own outbound sub-call
// requests and the other agent's own replies completing them (internal
// plumbing, already delivered directly to the waiting invocation -- see
// deliverOnce) from StoredTask.History, keeping only genuinely fresh input
// delivered to this actor (the caller's own bootstrap/continuation, or
// another agent's own fresh call into this one).
func TestAgentHarness_SubCallPlumbingExcludedFromHistory(t *testing.T) {
	client := startTestHarness(t, NewAgentHarness(a2asrv.AgentExecutorFunc(func(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			var texts []string
			if execCtx.StoredTask != nil {
				for _, m := range execCtx.StoredTask.History {
					texts = append(texts, text(m))
				}
			}
			info := execCtx.TaskInfo()
			yield(&a2a.Task{
				ID:        info.TaskID,
				ContextID: info.ContextID,
				Status: a2a.TaskStatus{
					State:   a2a.TaskStateCompleted,
					Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("history-was:"+strings.Join(texts, ","))),
				},
			}, nil)
		}
	})))
	ctx := context.Background()

	bootstrap := envNew(a2a.MessageRoleUser, "caller", "self-agent", "hello")

	selfContinuation := envReply(bootstrap, a2a.MessageRoleAgent, "self-agent", "")
	selfContinuation.From = "self-agent"
	selfContinuation.Data.Message = nil
	selfContinuation.Data.Task = &a2a.Task{Status: a2a.TaskStatus{State: a2a.TaskStateWorking}}
	selfContinuation.Data.Type = hop.DataTypeTask

	// This actor's own outbound sub-call request -- internal plumbing.
	outboundCall := envNew(a2a.MessageRoleUser, "self-agent", "b", "please help")

	// "b"'s own reply completing that sub-call -- also internal plumbing.
	bReply := envReply(outboundCall, a2a.MessageRoleAgent, "self-agent", "ok")
	bReply.From = "b"

	current := envNew(a2a.MessageRoleUser, "caller", "self-agent", "resume")
	res := sendTurn(t, ctx, client, "conv-1", "self-agent", bootstrap, selfContinuation, outboundCall, bReply, current)
	if res.err != nil {
		t.Fatalf("sendTurn: %v", res.err)
	}
	if got, want := text(res.msg.Data.Task.Status.Message), "history-was:hello"; got != want {
		t.Errorf("reply text = %q, want %q (sub-call plumbing must be excluded from History)", got, want)
	}
}

// TestAgentHarness_RedeliveredReplyThatFailsInvocationReturnsSameErrorNotOrphaned
// confirms that redelivering a reply whose resumed invocation failed (a
// bare update as its first-ever yield) returns that same cached error
// again, not the generic "no invocation waiting" error a second delivery
// against an already-consumed waiting entry would otherwise produce.
func TestAgentHarness_RedeliveredReplyThatFailsInvocationReturnsSameErrorNotOrphaned(t *testing.T) {
	client := startTestHarness(t, NewAgentHarness(a2asrv.AgentExecutorFunc(func(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			c, err := newClient(execCtx, "other-agent")
			if err != nil {
				yield(nil, err)
				return
			}
			if _, err := c.SendMessage(ctx, &a2a.SendMessageRequest{
				Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("ping")),
			}); err != nil {
				yield(nil, err)
				return
			}
			// Deliberately invalid: this invocation's own first-ever yield
			// is a bare update event -- the nested SendMessage call above
			// establishes nothing of *this* invocation's own claim.
			yield(&a2a.TaskArtifactUpdateEvent{
				Artifact: &a2a.Artifact{ID: "x", Parts: a2a.ContentParts{a2a.NewTextPart("y")}},
			}, nil)
		}
	})))
	ctx := context.Background()

	bootstrap := envNew(a2a.MessageRoleUser, "caller", "caller-agent", "start")
	res1 := sendTurn(t, ctx, client, "conv-orphan", "caller-agent", bootstrap)
	if res1.err != nil {
		t.Fatalf("turn 1: %v", res1.err)
	}

	otherReply := envReply(res1.msg, a2a.MessageRoleAgent, "caller-agent", "")
	otherReply.From = "other-agent"
	otherReply.Correlation = res1.msg.Correlation
	otherReply.Data.Message = nil
	otherReply.Data.Task = &a2a.Task{Status: a2a.TaskStatus{State: a2a.TaskStateInputRequired}}
	otherReply.Data.Type = hop.DataTypeTask

	res2 := sendTurn(t, ctx, client, "conv-orphan", "caller-agent", otherReply)
	if res2.err == nil {
		t.Fatal("turn 2: sendTurn succeeded, want an error (caller-agent's own first yield is an invalid bare update)")
	}

	// Redeliver the exact same reply (same StepID).
	res3 := sendTurn(t, ctx, client, "conv-orphan", "caller-agent", otherReply)
	if res3.err == nil {
		t.Fatal("turn 3 (redelivery): sendTurn succeeded, want an error")
	}
	if got, want := res3.err.Error(), res2.err.Error(); got != want {
		t.Errorf("turn 3 error = %q, want the identical cached error %q", got, want)
	}
	if strings.Contains(res3.err.Error(), "no invocation waiting") {
		t.Errorf("turn 3 error = %q -- the redelivery was orphaned (waiting entry already consumed by turn 2) instead of replaying turn 2's own cached error", res3.err.Error())
	}
}

// TestAgentHarness_NestedCallSeesBareMessageReply confirms a callee that
// never claims a Task -- just a bare *a2a.Message reply, the shape
// long_wait and tck_agent's "tck-message-response" case both use --
// surfaces to a nested caller as a bare *a2a.Message too, never wrapped
// into a synthetic Task the callee never claimed.
func TestAgentHarness_NestedCallSeesBareMessageReply(t *testing.T) {
	client := startTestHarness(t, NewAgentHarness(a2asrv.AgentExecutorFunc(func(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			c, err := newClient(execCtx, "other-agent")
			if err != nil {
				yield(nil, err)
				return
			}
			res, err := c.SendMessage(ctx, &a2a.SendMessageRequest{
				Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("ping")),
			})
			if err != nil {
				yield(nil, err)
				return
			}
			msg, ok := res.(*a2a.Message)
			if !ok {
				yield(nil, fmt.Errorf("SendMessage result is %T, want *a2a.Message", res))
				return
			}
			yield(a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("saw: "+text(msg))), nil)
		}
	})))
	ctx := context.Background()

	bootstrap := envNew(a2a.MessageRoleUser, "caller", "caller-agent", "start")
	res1 := sendTurn(t, ctx, client, "conv-1", "caller-agent", bootstrap)
	if res1.err != nil {
		t.Fatalf("turn 1: %v", res1.err)
	}

	// "other-agent"'s own reply: a bare Message, no Task ever claimed.
	otherReply := envReply(res1.msg, a2a.MessageRoleAgent, "caller-agent", "pong")
	otherReply.From = "other-agent"
	otherReply.Correlation = res1.msg.Correlation
	otherReply.Data.Type = hop.DataTypeMessage
	res2 := sendTurn(t, ctx, client, "conv-1", "caller-agent", otherReply)
	if res2.err != nil {
		t.Fatalf("turn 2 (final): %v", res2.err)
	}
	if got, want := text(res2.msg.Data.Message), "saw: pong"; got != want {
		t.Errorf("turn 2 (final) reply text = %q, want %q", got, want)
	}
}

// TestAgentHarness_TrailingStatusUpdateCarriesOwnMessage confirms a
// *a2a.TaskStatusUpdateEvent yielded last still carries its own
// Status.Message through aggregation into a real *a2a.Task. Unlike the
// nested-call tests above, "other-agent" here is a second, real
// NewAgentHarness instance so the actual aggregation code runs, not a
// hand-built envelope.
func TestAgentHarness_TrailingStatusUpdateCarriesOwnMessage(t *testing.T) {
	ctx := context.Background()

	clientCaller := startTestHarness(t, NewAgentHarness(a2asrv.AgentExecutorFunc(func(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			c, err := newClient(execCtx, "other-agent")
			if err != nil {
				yield(nil, err)
				return
			}
			res, err := c.SendMessage(ctx, &a2a.SendMessageRequest{
				Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("ping")),
			})
			if err != nil {
				yield(nil, err)
				return
			}
			task, ok := res.(*a2a.Task)
			if !ok {
				yield(nil, fmt.Errorf("SendMessage result is %T, want *a2a.Task", res))
				return
			}
			if task.Status.State != a2a.TaskStateCompleted {
				yield(nil, fmt.Errorf("SendMessage result Status.State = %q, want %q", task.Status.State, a2a.TaskStateCompleted))
				return
			}
			yield(a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("saw: "+text(task.Status.Message))), nil)
		}
	})))

	clientOther := startTestHarness(t, NewAgentHarness(a2asrv.AgentExecutorFunc(func(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			// Establish the Task first, then yield only the trailing
			// update below -- what this test actually exercises.
			if !yield(&a2a.Task{}, nil) {
				return
			}
			yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateCompleted,
				a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("pong"))), nil)
		}
	})))

	bootstrap := envNew(a2a.MessageRoleUser, testExternalIdentity, "caller-agent", "start")
	res1 := sendTurn(t, ctx, clientCaller, "conv-1", "caller-agent", bootstrap)
	if res1.err != nil {
		t.Fatalf("caller turn 1: %v", res1.err)
	}

	// Driven directly, not through fakeRelay, so its reply can be relayed
	// back into clientCaller's pending nested wait with the right Correlation.
	bootstrapOther := envNew(a2a.MessageRoleUser, "caller-agent", "other-agent", "ping")
	otherRes := sendTurn(t, ctx, clientOther, "conv-other", "other-agent", bootstrapOther)
	if otherRes.err != nil {
		t.Fatalf("other-agent turn: %v", otherRes.err)
	}

	// Deliver other-agent's real, aggregation-produced reply back to
	// caller-agent's pending nested call -- only Correlation is overwritten
	// to match; Data is otherRes.msg's genuine aggregation output.
	otherReply := otherRes.msg
	otherReply.From = "other-agent"
	otherReply.Correlation = res1.msg.Correlation
	res2 := sendTurn(t, ctx, clientCaller, "conv-1", "caller-agent", otherReply)
	if res2.err != nil {
		t.Fatalf("caller turn 2 (final): %v", res2.err)
	}
	final := res2.msg

	if got, want := text(final.Data.Message), "saw: pong"; got != want {
		t.Errorf("final reply text = %q, want %q", got, want)
	}
}

// TestAgentHarness_RedeliveredReplyToConsumedTokenIsRejected confirms a
// correlation token matched once and then redelivered under a different
// StepID is rejected, the same as a token this process never saw.
func TestAgentHarness_RedeliveredReplyToConsumedTokenIsRejected(t *testing.T) {
	client := startTestHarness(t, NewAgentHarness(a2asrv.AgentExecutorFunc(func(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			c, err := newClient(execCtx, "other-agent")
			if err != nil {
				yield(nil, err)
				return
			}
			for i := 1; i <= 2; i++ {
				if _, err := c.SendMessage(ctx, &a2a.SendMessageRequest{
					Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart(fmt.Sprintf("call-%d", i))),
				}); err != nil {
					yield(nil, err)
					return
				}
			}
			yield(a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("done")), nil)
		}
	})))
	ctx := context.Background()

	bootstrap := envNew(a2a.MessageRoleUser, "caller", "caller-agent", "start")
	res1 := sendTurn(t, ctx, client, "conv-1", "caller-agent", bootstrap)
	if res1.err != nil {
		t.Fatalf("turn 1 (call-1 outbound): %v", res1.err)
	}
	if got, want := text(res1.msg.Data.Message), "call-1"; got != want {
		t.Fatalf("turn 1 outbound text = %q, want %q", got, want)
	}

	// call-1's own reply matches its token and moves the invocation on to
	// call-2.
	reply1 := envReply(res1.msg, a2a.MessageRoleAgent, "caller-agent", "pong-1")
	reply1.From = "other-agent"
	reply1.Correlation = res1.msg.Correlation
	res2 := sendTurn(t, ctx, client, "conv-1", "caller-agent", reply1)
	if res2.err != nil {
		t.Fatalf("turn 2 (call-2 outbound): %v", res2.err)
	}
	if got, want := text(res2.msg.Data.Message), "call-2"; got != want {
		t.Fatalf("turn 2 outbound text = %q, want %q -- the invocation should have moved on to its next call", got, want)
	}

	// Redeliver call-1's reply again under a fresh StepID (a genuinely
	// stale redelivery's shape) with the same, now-consumed token.
	reply1Again := envReply(res1.msg, a2a.MessageRoleAgent, "caller-agent", "pong-1")
	reply1Again.From = "other-agent"
	reply1Again.Correlation = res1.msg.Correlation
	res2Again := sendTurn(t, ctx, client, "conv-1", "caller-agent", reply1Again)
	if res2Again.err == nil {
		t.Fatalf("redelivering call-1's own reply: got nil error and reply %+v, want an error", res2Again.msg)
	}
	if !strings.Contains(res2Again.err.Error(), "no invocation waiting for correlation token") {
		t.Errorf("redelivery error = %v, want it to name the lost-correlation condition", res2Again.err)
	}

	// Confirm the rejection above didn't corrupt this actor's ongoing
	// state: it's still waiting on call-2's own token.
	reply2 := envReply(res2.msg, a2a.MessageRoleAgent, "caller-agent", "pong-2")
	reply2.From = "other-agent"
	reply2.Correlation = res2.msg.Correlation
	res3 := sendTurn(t, ctx, client, "conv-1", "caller-agent", reply2)
	if res3.err != nil {
		t.Fatalf("turn 3 (final): %v", res3.err)
	}
	if got, want := text(res3.msg.Data.Message), "done"; got != want {
		t.Errorf("final reply text = %q, want %q", got, want)
	}
}

func TestAgentHarness_FreshRequestNotMisdeliveredToWaitingInvocation(t *testing.T) {
	client := startTestHarness(t, NewAgentHarness(a2asrv.AgentExecutorFunc(func(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		// Any message starting with "wait:" triggers the call-and-wait
		// path; anything else is answered immediately -- lets one actor
		// simulate both a genuinely pending invocation and a fresh,
		// unrelated one arriving concurrently.
		if strings.HasPrefix(text(execCtx.Message), "wait:") {
			return callAndWaitExecutor(ctx, execCtx)
		}
		return echoExecutor(ctx, execCtx)
	})))
	ctx := context.Background()

	// Start an invocation that blocks waiting for a reply (its own
	// outbound call to "other-agent").
	waiting := make(chan turnResult, 1)
	go func() {
		bootstrap := envNew(a2a.MessageRoleUser, "caller", "actor-a", "wait:go")
		waiting <- sendTurn(t, ctx, client, "conv-a", "actor-a", bootstrap)
	}()

	// Give the goroutine above a moment to actually reach the blocked
	// state (best-effort; the real correctness property below doesn't
	// depend on precise timing, just on this fresh call arriving while
	// *something* may or may not be pending).
	time.Sleep(50 * time.Millisecond)

	// A second, entirely unrelated, fresh request arrives at the exact
	// same actor (same ConversationId/AgentId) -- this must spawn its own,
	// independent invocation, not be fed into the first one's pending wait
	// (which is keyed to a correlation token this fresh request carries
	// none of -- see actorRuntime.routeOrSpawn).
	fresh := envNew(a2a.MessageRoleUser, "someone-else", "actor-a", "fresh-hello")
	freshRes := sendTurn(t, ctx, client, "conv-a", "actor-a", fresh)
	if freshRes.err != nil {
		t.Fatalf("fresh request: %v", freshRes.err)
	}
	if got, want := text(freshRes.msg.Data.Message), "echo: fresh-hello"; got != want {
		t.Fatalf("fresh request got %q, want %q -- it was misrouted into the pending invocation instead of spawning its own", got, want)
	}

	// The first invocation must still be genuinely pending -- confirm by
	// now delivering its own correlated reply and checking it completes
	// correctly, proving the fresh request above never consumed it.
	res1 := <-waiting
	if res1.err != nil {
		t.Fatalf("turn 1: %v", res1.err)
	}
	otherReply := envReply(res1.msg, a2a.MessageRoleAgent, "actor-a", "real-pong")
	otherReply.From = "other-agent"
	otherReply.Correlation = res1.msg.Correlation
	res2 := sendTurn(t, ctx, client, "conv-a", "actor-a", otherReply)
	if res2.err != nil {
		t.Fatalf("turn 2: %v", res2.err)
	}
	if got, want := text(res2.msg.Data.Message), "got: real-pong"; got != want {
		t.Errorf("turn 2 (final) reply text = %q, want %q", got, want)
	}
}

// TestAgentHarness_LostInvocationStateRejectsOrphanedReply confirms an
// envelope marked Reply with no matching waiting invocation, and no
// matching cached StepID either, fails the turn outright rather than
// spawning a fresh invocation fed the reply as its own bootstrap input.
func TestAgentHarness_LostInvocationStateRejectsOrphanedReply(t *testing.T) {
	client := startTestHarness(t, NewAgentHarness(a2asrv.AgentExecutorFunc(echoExecutor)))
	ctx := context.Background()

	orphaned := envNew(a2a.MessageRoleAgent, "other-agent", "actor-a", "real-pong")
	orphaned.Reply = true
	orphaned.Correlation = "a-token-nobody-minted-in-this-process"
	res := sendTurn(t, ctx, client, "conv-a", "actor-a", orphaned)
	if res.err == nil {
		t.Fatalf("orphaned reply: got nil error and reply %+v, want an error -- it must never be treated as a fresh bootstrap turn", res.msg)
	}
	if !strings.Contains(res.err.Error(), "no invocation waiting for correlation token") {
		t.Errorf("orphaned reply error = %v, want it to name the lost-correlation condition", res.err)
	}
}

// TestAgentHarness_FreshCallIgnoresStrayCorrelation confirms an envelope
// not marked Reply always spawns a fresh, independent invocation, even if
// it carries a non-empty Correlation token.
func TestAgentHarness_FreshCallIgnoresStrayCorrelation(t *testing.T) {
	client := startTestHarness(t, NewAgentHarness(a2asrv.AgentExecutorFunc(echoExecutor)))
	ctx := context.Background()

	fresh := envNew(a2a.MessageRoleUser, "caller", "actor-a", "hello")
	fresh.Correlation = "some-caller-minted-token"
	res := sendTurn(t, ctx, client, "conv-a", "actor-a", fresh)
	if res.err != nil {
		t.Fatalf("fresh call: %v", res.err)
	}
	if got, want := text(res.msg.Data.Message), "echo: hello"; got != want {
		t.Errorf("fresh call reply text = %q, want %q", got, want)
	}
}

// TestAgentHarness_StaleReplyAfterActorMovedOnIsRejected tests that a stale
// reply for a call this actor's invocation had already moved past (having
// reissued the equivalent call under a new token) must fail the turn.
func TestAgentHarness_StaleReplyAfterActorMovedOnIsRejected(t *testing.T) {
	client := startTestHarness(t, NewAgentHarness(a2asrv.AgentExecutorFunc(func(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			c, err := newClient(execCtx, "other-agent")
			if err != nil {
				yield(nil, err)
				return
			}
			for i := 1; i <= 2; i++ {
				if _, err := c.SendMessage(ctx, &a2a.SendMessageRequest{
					Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart(fmt.Sprintf("call-%d", i))),
				}); err != nil {
					yield(nil, err)
					return
				}
			}
			yield(a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("done")), nil)
		}
	})))
	ctx := context.Background()

	bootstrap := envNew(a2a.MessageRoleUser, "caller", "caller-agent", "start")
	res1 := sendTurn(t, ctx, client, "conv-1", "caller-agent", bootstrap)
	if res1.err != nil {
		t.Fatalf("turn 1 (call-1 outbound): %v", res1.err)
	}

	// call-1's own reply moves the invocation on to call-2.
	reply1 := envReply(res1.msg, a2a.MessageRoleAgent, "caller-agent", "pong-1")
	reply1.From = "other-agent"
	reply1.Correlation = res1.msg.Correlation
	res2 := sendTurn(t, ctx, client, "conv-1", "caller-agent", reply1)
	if res2.err != nil {
		t.Fatalf("turn 2 (call-2 outbound): %v", res2.err)
	}
	if got, want := text(res2.msg.Data.Message), "call-2"; got != want {
		t.Fatalf("turn 2 outbound text = %q, want %q", got, want)
	}

	// A retry of call-1's reply, but stamped with a token this process
	// never minted at all -- must fail outright, not duplicate call-2.
	staleReply := envReply(res1.msg, a2a.MessageRoleAgent, "caller-agent", "pong-1")
	staleReply.From = "other-agent"
	staleReply.Correlation = "a-token-this-process-genuinely-never-saw"
	res3 := sendTurn(t, ctx, client, "conv-1", "caller-agent", staleReply)
	if res3.err == nil {
		t.Fatalf("stale reply: got nil error and reply %+v, want an error", res3.msg)
	}
	if !strings.Contains(res3.err.Error(), "no invocation waiting for correlation token") {
		t.Errorf("stale reply error = %v, want it to name the lost-correlation condition", res3.err)
	}

	// Confirm the rejection above didn't corrupt this actor's ongoing
	// state: it's still waiting on call-2's own token.
	reply2 := envReply(res2.msg, a2a.MessageRoleAgent, "caller-agent", "pong-2")
	reply2.From = "other-agent"
	reply2.Correlation = res2.msg.Correlation
	res4 := sendTurn(t, ctx, client, "conv-1", "caller-agent", reply2)
	if res4.err != nil {
		t.Fatalf("turn 3 (final): %v", res4.err)
	}
	if got, want := text(res4.msg.Data.Message), "done"; got != want {
		t.Errorf("final reply text = %q, want %q", got, want)
	}
}

// TestAgentHarness_SendMessageWaitsForReplyThenHonorsCtx confirms
// SendMessage never abandons its wait early on ctx cancellation: it stays
// blocked, with its registry entry alive, until the target's real reply
// arrives, then returns ctx.Err() and cleans the entry up normally.
// Abandoning early would leave the target's own eventual reply rejected as
// orphaned, with no waiting entry left to match.
func TestAgentHarness_SendMessageWaitsForReplyThenHonorsCtx(t *testing.T) {
	unblocked := make(chan struct{})
	done := make(chan error, 1)
	h := NewAgentHarness(a2asrv.AgentExecutorFunc(func(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			c, err := newClient(execCtx, "other-agent")
			if err != nil {
				done <- err
				yield(a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("unobserved")), nil)
				return
			}
			shortCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
			defer cancel()
			_, err = c.SendMessage(shortCtx, &a2a.SendMessageRequest{
				Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("ping")),
			})
			close(unblocked)
			done <- err
			// Nothing will ever call this actor again in this test, so
			// this goroutine's own eventual yield is never observed;
			// yield something so it terminates cleanly regardless.
			yield(a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("unobserved")), nil)
		}
	}))
	rt, ok := h.(*actorRuntime)
	if !ok {
		t.Fatalf("NewAgentHarness did not return a *actorRuntime")
	}
	client := startTestHarness(t, h)
	ctx := context.Background()

	const target = "other-agent"
	bootstrap := envNew(a2a.MessageRoleUser, "caller", "actor-timeout", "go")
	res := sendTurn(t, ctx, client, "conv-timeout", "actor-timeout", bootstrap)
	if res.err != nil {
		t.Fatalf("sendTurn: %v", res.err)
	}
	if got := res.msg.To; got != target {
		t.Fatalf("turn 1 reply To = %q, want %q", got, target)
	}
	token := res.msg.Correlation
	if token == "" {
		t.Fatalf("outbound message carries no correlation token")
	}

	// Well past the 20ms ctx deadline: SendMessage must still be blocked,
	// with its registry entry still alive.
	select {
	case <-unblocked:
		t.Fatal("SendMessage returned before its target's own reply ever arrived")
	case <-time.After(200 * time.Millisecond):
	}
	rt.mu.Lock()
	_, pending := rt.waiting[token]
	rt.mu.Unlock()
	if !pending {
		t.Fatalf("registry entry for %q was removed before the target's own reply ever arrived", token)
	}

	// Deliver the target's own real reply -- this invocation only
	// unblocks now, and returns ctx.Err() rather than this reply's own
	// content, since ctx was already done well before it landed.
	reply := envReply(res.msg, a2a.MessageRoleAgent, "actor-timeout", "pong")
	reply.From = target
	reply.Correlation = token
	res2 := sendTurn(t, ctx, client, "conv-timeout", "actor-timeout", reply)
	if res2.err != nil {
		t.Fatalf("delivering the reply: %v", res2.err)
	}

	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("SendMessage returned %v, want context.DeadlineExceeded", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SendMessage never unblocked even after its target's own reply was delivered")
	}

	rt.mu.Lock()
	_, stillPending := rt.waiting[token]
	rt.mu.Unlock()
	if stillPending {
		t.Errorf("registry entry for %q was never cleaned up after its reply was delivered", token)
	}
}

// TestAgentHarness_MultiStepClientForSequence confirms a single invocation
// can block on more than one sequential call through the same client
// before finally yielding, each call its own genuine turn boundary.
func TestAgentHarness_MultiStepClientForSequence(t *testing.T) {
	executor := a2asrv.AgentExecutorFunc(func(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			client, err := newClient(execCtx, "echo-b")
			if err != nil {
				yield(nil, err)
				return
			}
			first, err := client.SendMessage(ctx, &a2a.SendMessageRequest{
				Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("one: "+text(execCtx.Message))),
			})
			if err != nil {
				yield(nil, err)
				return
			}
			firstMsg := first.(*a2a.Message)
			second, err := client.SendMessage(ctx, &a2a.SendMessageRequest{
				Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("two: "+text(firstMsg))),
			})
			if err != nil {
				yield(nil, err)
				return
			}
			secondMsg := second.(*a2a.Message)
			yield(a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("done: "+text(secondMsg))), nil)
		}
	})

	client := startTestHarness(t, NewAgentHarness(executor))
	ctx := context.Background()

	bootstrap := envNew(a2a.MessageRoleUser, "caller", "wf", "hi")
	res1 := sendTurn(t, ctx, client, "conv-wf", "wf", bootstrap)
	if res1.err != nil {
		t.Fatalf("turn 1: %v", res1.err)
	}
	if got, want := text(res1.msg.Data.Message), "one: hi"; got != want {
		t.Fatalf("turn 1 reply text = %q, want %q", got, want)
	}

	echoBReply1 := envReply(res1.msg, a2a.MessageRoleAgent, "wf", "echoed-"+text(res1.msg.Data.Message))
	echoBReply1.From = "echo-b"
	echoBReply1.Correlation = res1.msg.Correlation
	res2 := sendTurn(t, ctx, client, "conv-wf", "wf", echoBReply1)
	if res2.err != nil {
		t.Fatalf("turn 2: %v", res2.err)
	}
	if got, want := text(res2.msg.Data.Message), "two: echoed-one: hi"; got != want {
		t.Fatalf("turn 2 reply text = %q, want %q", got, want)
	}

	echoBReply2 := envReply(res2.msg, a2a.MessageRoleAgent, "wf", "echoed-"+text(res2.msg.Data.Message))
	echoBReply2.From = "echo-b"
	echoBReply2.Correlation = res2.msg.Correlation
	res3 := sendTurn(t, ctx, client, "conv-wf", "wf", echoBReply2)
	if res3.err != nil {
		t.Fatalf("turn 3: %v", res3.err)
	}
	if got, want := text(res3.msg.Data.Message), "done: echoed-two: echoed-one: hi"; got != want {
		t.Errorf("final reply text = %q, want %q", got, want)
	}
	// "caller", not "echo-b" (the last agent this workflow talked to): a
	// resumed reply wakes the same invocation rather than spawning a fresh
	// one, so the default stays the original bootstrap sender.
	if got, want := res3.msg.To, "caller"; got != want {
		t.Errorf("final reply To = %q, want %q (runInvocation defaults an unaddressed reply back to whoever sent this invocation's original bootstrap)", got, want)
	}
}

// TestAgentHarness_NoYieldIsEmptyMessage confirms an executor that never
// yields anything gets treated as a legitimate, if uninteresting, empty
// message reply -- not an error.
func TestAgentHarness_NoYieldIsEmptyMessage(t *testing.T) {
	executor := a2asrv.AgentExecutorFunc(func(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			// Never yields anything.
		}
	})
	client := startTestHarness(t, NewAgentHarness(executor))
	ctx := context.Background()

	bootstrap := envNew(a2a.MessageRoleUser, "caller", "no-yield", "hi")
	res := sendTurn(t, ctx, client, "conv-no-yield", "no-yield", bootstrap)
	if res.err != nil {
		t.Fatalf("sendTurn: %v, want success (an empty yield is a valid, if uninteresting, message reply)", res.err)
	}
	if res.msg.Data.Type != hop.DataTypeUnspecified {
		t.Errorf("Data.Type = %q, want %q", res.msg.Data.Type, hop.DataTypeUnspecified)
	}
	if res.msg.Data.Message != nil || res.msg.Data.Task != nil {
		t.Errorf("Data = %+v, want entirely empty", res.msg.Data)
	}
}

// TestAgentHarness_YieldedTaskDecomposedForCaller confirms an agent that
// yields a real *a2a.Task with a terminal state has it decomposed the same
// way a bare TaskStatusUpdateEvent+Message pair would be, and the reply
// addressed back to the caller since Completed isn't Working/Submitted.
func TestAgentHarness_YieldedTaskDecomposedForCaller(t *testing.T) {
	executor := a2asrv.AgentExecutorFunc(func(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			yield(&a2a.Task{
				Status: a2a.TaskStatus{
					State:   a2a.TaskStateCompleted,
					Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("done: "+text(execCtx.Message))),
				},
			}, nil)
		}
	})
	client := startTestHarness(t, NewAgentHarness(executor))
	ctx := context.Background()

	bootstrap := envNew(a2a.MessageRoleUser, "caller", "returns-task", "hi")
	res := sendTurn(t, ctx, client, "conv-returns-task", "returns-task", bootstrap)
	if res.err != nil {
		t.Fatalf("sendTurn: %v", res.err)
	}
	if got, want := res.msg.To, "caller"; got != want {
		t.Errorf("reply To = %q, want %q (Completed hands back to the caller)", got, want)
	}
	if got, want := taskState(res.msg.Data), a2a.TaskStateCompleted; got != want {
		t.Errorf("reply Data.State = %q, want %q", got, want)
	}
	if got, want := text(hopMessage(res.msg.Data)), "done: hi"; got != want {
		t.Errorf("reply text = %q, want %q", got, want)
	}
}

// TestAgentHarness_YieldedTaskGetsContextIDWhenAgentOmitsIt (regression
// test for CORE-MULTI-005) confirms a brand-new Task the agent never
// stamped a ContextID onto still comes back with one.
func TestAgentHarness_YieldedTaskGetsContextIDWhenAgentOmitsIt(t *testing.T) {
	executor := a2asrv.AgentExecutorFunc(func(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			yield(&a2a.Task{
				Status: a2a.TaskStatus{
					State:   a2a.TaskStateCompleted,
					Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("done")),
				},
			}, nil)
		}
	})
	client := startTestHarness(t, NewAgentHarness(executor))
	ctx := context.Background()

	bootstrap := envNew(a2a.MessageRoleUser, "caller", "no-context-id", "hi")
	res := sendTurn(t, ctx, client, "conv-no-context-id", "no-context-id", bootstrap)
	if res.err != nil {
		t.Fatalf("sendTurn: %v", res.err)
	}
	if res.msg.Data.Task == nil {
		t.Fatal("reply Data.Task is nil, want a Task")
	}
	if res.msg.Data.Task.ContextID == "" {
		t.Error("reply Task.ContextID is empty, want a minted id")
	}
}

// TestAgentHarness_YieldedTaskInputRequiredHandsBackToCaller confirms
// InputRequired -- despite not being Terminal() -- still hands control
// back to the caller instead of self-addressing.
func TestAgentHarness_YieldedTaskInputRequiredHandsBackToCaller(t *testing.T) {
	executor := a2asrv.AgentExecutorFunc(func(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			yield(&a2a.Task{Status: a2a.TaskStatus{State: a2a.TaskStateInputRequired}}, nil)
		}
	})
	client := startTestHarness(t, NewAgentHarness(executor))
	ctx := context.Background()

	bootstrap := envNew(a2a.MessageRoleUser, "caller", "pauses", "hi")
	res := sendTurn(t, ctx, client, "conv-pauses", "pauses", bootstrap)
	if res.err != nil {
		t.Fatalf("sendTurn: %v", res.err)
	}
	if got, want := res.msg.To, "caller"; got != want {
		t.Errorf("reply To = %q, want %q (InputRequired must hand back, not self-address)", got, want)
	}
	if got, want := taskState(res.msg.Data), a2a.TaskStateInputRequired; got != want {
		t.Errorf("reply Data.State = %q, want %q", got, want)
	}
}

// TestAgentHarness_SingleExecuteCallFlushesEachWorkingReportWithoutReturning
// confirms a single Execute() call that reports TaskStateWorking, then
// keeps running without returning, resumes on the same call stack: its own
// local state survives the flush like any other Go local variable would.
func TestAgentHarness_SingleExecuteCallFlushesEachWorkingReportWithoutReturning(t *testing.T) {
	// progress is an ordinary Go local: reaching the second yield already
	// incremented is proof this is the same call stack resuming.
	progress := 0
	executor := a2asrv.AgentExecutorFunc(func(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			progress++
			if !yield(&a2a.Task{Status: a2a.TaskStatus{State: a2a.TaskStateWorking}}, nil) {
				return
			}
			progress++
			yield(&a2a.Task{
				Status: a2a.TaskStatus{
					State:   a2a.TaskStateCompleted,
					Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("done")),
				},
			}, nil)
		}
	})
	client := startTestHarness(t, NewAgentHarness(executor))
	ctx := context.Background()

	bootstrap := envNew(a2a.MessageRoleUser, "caller", "single-call", "hi")
	res1 := sendTurn(t, ctx, client, "conv-single-call", "single-call", bootstrap)
	if res1.err != nil {
		t.Fatalf("turn 1: %v", res1.err)
	}
	if got, want := res1.msg.To, "single-call"; got != want {
		t.Fatalf("turn 1 reply To = %q, want %q (self-addressed while Working)", got, want)
	}
	if !res1.msg.Reply {
		t.Fatalf("turn 1 reply Reply = false, want true (a mid-call flush registration, see callContext.flushSelf)")
	}
	if got, want := taskState(res1.msg.Data), a2a.TaskStateWorking; got != want {
		t.Fatalf("turn 1 reply Data.State = %q, want %q", got, want)
	}

	// Redelivering res1 wakes the same still-running Execute() call, which
	// resumes past the flush and yields Completed next.
	res2 := sendTurn(t, ctx, client, "conv-single-call", "single-call", res1.msg, relayHop(res1.msg))
	if res2.err != nil {
		t.Fatalf("turn 2: %v", res2.err)
	}
	if got, want := res2.msg.To, "caller"; got != want {
		t.Errorf("turn 2 reply To = %q, want %q (back to the original external caller)", got, want)
	}
	if got, want := taskState(res2.msg.Data), a2a.TaskStateCompleted; got != want {
		t.Errorf("turn 2 reply Data.State = %q, want %q", got, want)
	}
	if got, want := text(hopMessage(res2.msg.Data)), "done"; got != want {
		t.Errorf("turn 2 reply text = %q, want %q", got, want)
	}
	// A fresh spawn would have re-run from the top (progress == 1 again);
	// reaching 2 proves this was the same call resumed, not a new one.
	if got, want := progress, 2; got != want {
		t.Errorf("progress = %d, want %d (executor.Execute should have been called exactly once, resumed in place)", got, want)
	}
}

// TestAgentHarness_ReturningWhileSelfContinuingIsRejected confirms
// NewAgentHarness rejects outright an Execute() call that returns while
// still reporting TaskStateWorking, instead of relying on a later, fresh
// Execute() call to resume -- not a real a2a-go contract, since the
// reference SDK's own execution pipeline never re-invokes Execute() on its
// own.
func TestAgentHarness_ReturningWhileSelfContinuingIsRejected(t *testing.T) {
	executor := a2asrv.AgentExecutorFunc(func(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			// Deliberately returns right after, instead of continuing to
			// yield further events the way callContext.flushSelf's own
			// resume expects.
			yield(&a2a.Task{Status: a2a.TaskStatus{State: a2a.TaskStateWorking}}, nil)
		}
	})
	client := startTestHarness(t, NewAgentHarness(executor))
	ctx := context.Background()

	// Turn 1 is still just the mid-call flush registration itself (see
	// callContext.flushSelf) -- a real, unremarkable hop, no error: the
	// violation isn't reporting Working at all, it's returning with
	// nothing further once resumed.
	bootstrap := envNew(a2a.MessageRoleUser, "caller", "returns-while-working", "hi")
	res1 := sendTurn(t, ctx, client, "conv-returns-while-working", "returns-while-working", bootstrap)
	if res1.err != nil {
		t.Fatalf("turn 1: %v", res1.err)
	}
	if !res1.msg.Reply {
		t.Fatalf("turn 1 reply Reply = false, want true (a mid-call flush registration)")
	}

	// Redelivering res1 wakes the still-parked invocation, which has
	// nothing further queued up: fn returns right here, still reporting
	// TaskStateWorking -- exactly the contract violation this rejects.
	res2 := sendTurn(t, ctx, client, "conv-returns-while-working", "returns-while-working", res1.msg, relayHop(res1.msg))
	if res2.err == nil {
		t.Fatal("turn 2: sendTurn succeeded, want an error (Execute() returned while still reporting TaskStateWorking)")
	}
}

// TestAgentHarness_ResumedArtifactOnlyTurnClassifiesAsTask confirms a
// resumed invocation that yields only a *a2a.TaskArtifactUpdateEvent,
// never touching Status.State, still classifies its hop.Data.Type as
// hop.DataTypeTask -- what checkpointd's own relay checks to decide
// whether a blocking external SendMessage call should unblock.
func TestAgentHarness_ResumedArtifactOnlyTurnClassifiesAsTask(t *testing.T) {
	executor := a2asrv.AgentExecutorFunc(func(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			if execCtx.StoredTask == nil {
				yield(&a2a.Task{Status: a2a.TaskStatus{State: a2a.TaskStateInputRequired}}, nil)
				return
			}
			// A genuinely separate, external-client-continued turn
			// (StoredTask.Status.State carried forward via seedTask,
			// never touched here): reports incremental progress via an
			// artifact chunk only, no Task/TaskStatusUpdateEvent of its
			// own this call.
			yield(&a2a.TaskArtifactUpdateEvent{
				Artifact: &a2a.Artifact{ID: "progress", Parts: a2a.ContentParts{a2a.NewTextPart("chunk")}},
			}, nil)
		}
	})
	client := startTestHarness(t, NewAgentHarness(executor))
	ctx := context.Background()

	bootstrap := envNew(a2a.MessageRoleUser, "caller", "artifact-only-turn", "hi")
	res1 := sendTurn(t, ctx, client, "conv-artifact-only-turn", "artifact-only-turn", bootstrap)
	if res1.err != nil {
		t.Fatalf("turn 1: %v", res1.err)
	}
	if got, want := taskState(res1.msg.Data), a2a.TaskStateInputRequired; got != want {
		t.Fatalf("turn 1 reply Data.State = %q, want %q", got, want)
	}

	more := envNew(a2a.MessageRoleUser, "caller", "artifact-only-turn", "continue")
	res2 := sendTurn(t, ctx, client, "conv-artifact-only-turn", "artifact-only-turn", res1.msg, more)
	if res2.err != nil {
		t.Fatalf("turn 2: %v", res2.err)
	}
	if got, want := res2.msg.To, "caller"; got != want {
		t.Fatalf("turn 2 reply To = %q, want %q", got, want)
	}
	if got, want := taskState(res2.msg.Data), a2a.TaskStateInputRequired; got != want {
		t.Fatalf("turn 2 reply Data.State = %q, want %q", got, want)
	}
	if got, want := res2.msg.Data.Type, hop.DataTypeTask; got != want {
		t.Errorf("turn 2 reply Data.Type = %q, want %q (an artifact-only turn against an already-established Task must still classify as Task-shaped)", got, want)
	}
	if got, want := len(res2.msg.Data.Task.Artifacts), 1; got != want {
		t.Errorf("turn 2 Artifacts = %d entries, want %d", got, want)
	}
}

// TestAgentHarness_ArtifactAfterSelfContinuingEstablishToleratesEmptyTaskID
// confirms an artifact event yielded after a self-continuing establish,
// with its own TaskID/ContextID left empty (the executor has no way to
// learn the real minted ID at that point), still merges correctly instead
// of failing ApplyArtifactUpdate's exact-match check.
func TestAgentHarness_ArtifactAfterSelfContinuingEstablishToleratesEmptyTaskID(t *testing.T) {
	executor := a2asrv.AgentExecutorFunc(func(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			if !yield(&a2a.Task{Status: a2a.TaskStatus{State: a2a.TaskStateWorking}}, nil) {
				return
			}
			// Deliberately leaves TaskID/ContextID unset: this executor
			// never learns the real ID flushSelf just minted.
			if !yield(&a2a.TaskArtifactUpdateEvent{
				Artifact: &a2a.Artifact{ID: "art-1", Parts: a2a.ContentParts{a2a.NewTextPart("chunk")}},
			}, nil) {
				return
			}
			yield(&a2a.Task{
				Status: a2a.TaskStatus{
					State:   a2a.TaskStateCompleted,
					Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("done")),
				},
			}, nil)
		}
	})
	client := startTestHarness(t, NewAgentHarness(executor))
	ctx := context.Background()

	bootstrap := envNew(a2a.MessageRoleUser, "caller", "artifact-after-flush", "hi")
	res1 := sendTurn(t, ctx, client, "conv-artifact-after-flush", "artifact-after-flush", bootstrap)
	if res1.err != nil {
		t.Fatalf("turn 1: %v", res1.err)
	}
	if got, want := taskState(res1.msg.Data), a2a.TaskStateWorking; got != want {
		t.Fatalf("turn 1 reply Data.State = %q, want %q (a mid-call flush registration)", got, want)
	}

	res2 := sendTurn(t, ctx, client, "conv-artifact-after-flush", "artifact-after-flush", res1.msg, relayHop(res1.msg))
	if res2.err != nil {
		t.Fatalf("turn 2: %v", res2.err)
	}
	if got, want := taskState(res2.msg.Data), a2a.TaskStateCompleted; got != want {
		t.Fatalf("turn 2 reply Data.State = %q, want %q", got, want)
	}
	artifacts := res2.msg.Data.Task.Artifacts
	if len(artifacts) != 1 || len(artifacts[0].Parts) != 1 || artifacts[0].Parts[0].Text() != "chunk" {
		t.Errorf("turn 2 Artifacts = %+v, want one artifact with text %q", artifacts, "chunk")
	}
}

// TestAgentHarness_PlainMessageRoutedToBoundTarget confirms a client built
// from a card + WithTransport addresses every message it sends to its bound
// target automatically -- a plain a2a.NewMessage, no hop package needed.
func TestAgentHarness_PlainMessageRoutedToBoundTarget(t *testing.T) {
	client := startTestHarness(t, NewAgentHarness(a2asrv.AgentExecutorFunc(func(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			c, err := newClient(execCtx, "specific-target")
			if err != nil {
				yield(nil, err)
				return
			}
			// Plain a2a.NewMessage, no hop package needed.
			_, err = c.SendMessage(ctx, &a2a.SendMessageRequest{
				Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("plain message, no hop or messageutil")),
			})
			// Never replied to -- this blocks forever, but only the
			// outbound message captured by turn 1 below is under test.
			_ = err
		}
	})))
	ctx := context.Background()

	bootstrap := envNew(a2a.MessageRoleUser, "caller", "clientfor-agent", "start")
	res := sendTurn(t, ctx, client, "conv-1", "clientfor-agent", bootstrap)
	if res.err != nil {
		t.Fatalf("sendTurn: %v", res.err)
	}
	if got, want := res.msg.To, "specific-target"; got != want {
		t.Errorf("outbound message To = %q, want %q", got, want)
	}
	if got, want := text(res.msg.Data.Message), "plain message, no hop or messageutil"; got != want {
		t.Errorf("outbound message text = %q, want %q", got, want)
	}
}

// TestRealCardWithCheckpointdInterface_RoutesThroughCheckpointdNotRealEndpoint
// confirms a card carrying both a checkpointd: interface and a real,
// unreachable http(s) one selects the checkpointd: transport, not the real
// endpoint, while still carrying the card's own metadata.
func TestRealCardWithCheckpointdInterface_RoutesThroughCheckpointdNotRealEndpoint(t *testing.T) {
	realCard := &a2a.AgentCard{
		Name: "Real Agent",
		SupportedInterfaces: []*a2a.AgentInterface{
			a2a.NewAgentInterface(hop.CheckpointdURLPrefix+"real-agent", hop.CheckpointdTransportProtocol),
			a2a.NewAgentInterface("http://example.invalid/agents/real-agent/", a2a.TransportProtocolJSONRPC),
		},
	}

	client := startTestHarness(t, NewAgentHarness(a2asrv.AgentExecutorFunc(func(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			c, err := a2aclient.NewFromCard(context.Background(), realCard,
				a2aclient.WithDefaultsDisabled(), WithTransport(execCtx))
			if err != nil {
				yield(nil, err)
				return
			}
			if got := c.Card().Name; got != "Real Agent" {
				yield(nil, fmt.Errorf("client.Card().Name = %q, want %q", got, "Real Agent"))
				return
			}
			_, err = c.SendMessage(ctx, &a2a.SendMessageRequest{
				Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("hi")),
			})
			_ = err // never replied to -- see TestAgentHarness_PlainMessageRoutedToBoundTarget's own note
		}
	})))
	ctx := context.Background()

	bootstrap := envNew(a2a.MessageRoleUser, "caller", "local-interface-agent", "start")
	res := sendTurn(t, ctx, client, "conv-1", "local-interface-agent", bootstrap)
	if res.err != nil {
		t.Fatalf("sendTurn: %v", res.err)
	}
	if got, want := res.msg.To, "real-agent"; got != want {
		t.Errorf("outbound message To = %q, want %q (routed through the real endpoint instead of checkpointd's own transport)", got, want)
	}
}

// TestWithTransport_FailsForForeignExecutorContext confirms
// a2aclient.NewFromCard surfaces an ordinary error when WithTransport is
// given an ExecutorContext NewAgentHarness never built.
func TestWithTransport_FailsForForeignExecutorContext(t *testing.T) {
	if _, err := newClient(&a2asrv.ExecutorContext{}, "target"); err == nil {
		t.Error("NewFromCard with WithTransport on a bare ExecutorContext = nil error, want one")
	}
}

// relayHop reconstructs res the way checkpointd's own relay loop does for
// an internal hop -- a faithful in-test stand-in for a real relay hop,
// rather than the more permissive pass-through shortcut the single-actor
// tests above use. Message, Task, and Type are all forwarded verbatim.
func relayHop(res *hop.Envelope) *hop.Envelope {
	return &hop.Envelope{
		From: res.From, To: res.To, Correlation: res.Correlation, Reply: res.Reply, StepID: uuid.NewString(),
		Data: hop.Data{
			Message: res.Data.Message,
			Task:    res.Data.Task,
			Type:    res.Data.Type,
		},
	}
}

// testExternalIdentity is a reserved hop.From/To value standing in for
// "whoever originally bootstrapped this task, outside this test's own
// harnesses" -- fakeRelay treats a reply addressed back to it as terminal.
const testExternalIdentity = "outside-caller"

// fakeRelay is a minimal in-test stand-in for checkpointd's own relay
// loop: dispatches bootstrap to harnesses[startAgent], then keeps relaying
// each reply (reconstructed via relayHop) to whichever harness hop.To
// names next, until a reply carries no further recipient.
func fakeRelay(t *testing.T, ctx context.Context, harnesses map[string]proto.HarnessServiceClient, startAgent string, bootstrap *hop.Envelope) *hop.Envelope {
	t.Helper()
	agent := startAgent
	res := bootstrap
	for i := 0; i < 100; i++ {
		client, ok := harnesses[agent]
		if !ok {
			t.Fatalf("fakeRelay: no harness registered for agent %q", agent)
		}
		result := sendTurn(t, ctx, client, "conv", agent, res)
		if result.err != nil {
			t.Fatalf("fakeRelay: hop to %s: %v", agent, result.err)
		}
		res = result.msg
		if to := res.To; to == "" || to == testExternalIdentity {
			return res
		}
		agent = res.To
		res = relayHop(res)
	}
	t.Fatalf("fakeRelay: exceeded hop limit -- possible infinite loop")
	return nil
}

// TestNestedMutualCallBetweenSameTwoAgents is the A->B->A scenario: A calls
// B and waits; while handling that, B calls A back with a brand-new,
// unrelated request, and only replies to A's original call once that
// nested call returns. Only a per-call correlation token, matched and
// carried forward end-to-end, routes this correctly.
func TestNestedMutualCallBetweenSameTwoAgents(t *testing.T) {
	ctx := context.Background()

	clientA := startTestHarness(t, NewAgentHarness(a2asrv.AgentExecutorFunc(func(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			switch text(execCtx.Message) {
			case "please give me X":
				// B's nested request, while A's own call to B is still
				// pending -- must spawn a fresh invocation, not confuse
				// with A's own wait.
				yield(a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("X")), nil)
			case "start":
				// A's own top-level entry point: call B and wait.
				c, err := newClient(execCtx, "B")
				if err != nil {
					yield(nil, err)
					return
				}
				result, err := c.SendMessage(ctx, &a2a.SendMessageRequest{
					Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("please handle this")),
				})
				if err != nil {
					yield(nil, err)
					return
				}
				bReply := text(result.(*a2a.Message))
				yield(a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("A-final: "+bReply)), nil)
			default:
				yield(nil, errors.New("A: unexpected message: "+text(execCtx.Message)))
			}
		}
	})))

	clientB := startTestHarness(t, NewAgentHarness(a2asrv.AgentExecutorFunc(func(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			// A new, independent request back to A, not a reply to what A
			// just sent.
			c, err := newClient(execCtx, "A")
			if err != nil {
				yield(nil, err)
				return
			}
			result, err := c.SendMessage(ctx, &a2a.SendMessageRequest{
				Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("please give me X")),
			})
			if err != nil {
				yield(nil, err)
				return
			}
			x := text(result.(*a2a.Message))
			yield(a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("B-result-using-"+x)), nil)
		}
	})))

	harnesses := map[string]proto.HarnessServiceClient{"A": clientA, "B": clientB}

	// Standing in for whatever bootstraps A's task; immediately calls B and
	// waits (clientA's "start" case above).
	bootstrap := envNew(a2a.MessageRoleUser, testExternalIdentity, "A", "start")
	final := fakeRelay(t, ctx, harnesses, "A", bootstrap)

	if got, want := text(final.Data.Message), "A-final: B-result-using-X"; got != want {
		t.Errorf("final reply text = %q, want %q -- a wrong value means the two calls were misdelivered into each other's pending call", got, want)
	}
}

// TestActorRuntime_DuplicateRedeliveryReturnsSameOutputWithoutReprocessing
// covers Connect's duplicate-turn guard on the simplest case: a fresh-spawn
// turn whose input gets redelivered with the identical StepID must return
// the same output already produced, without invoking the executor again.
func TestActorRuntime_DuplicateRedeliveryReturnsSameOutputWithoutReprocessing(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	executor := a2asrv.AgentExecutorFunc(func(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			mu.Lock()
			calls++
			n := calls
			mu.Unlock()
			reply := a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart(fmt.Sprintf("call %d", n)))
			yield(reply, nil)
		}
	})
	client := startTestHarness(t, NewAgentHarness(executor))
	ctx := context.Background()

	bootstrap := envNew(a2a.MessageRoleUser, "caller", "actor", "hi")
	res1 := sendTurn(t, ctx, client, "conv-dup", "actor", bootstrap)
	if res1.err != nil {
		t.Fatalf("turn 1: %v", res1.err)
	}
	if got, want := text(res1.msg.Data.Message), "call 1"; got != want {
		t.Fatalf("turn 1 reply = %q, want %q", got, want)
	}

	// Redeliver the exact same *a2a.Message (same ID) as a second, separate
	// turn.
	res2 := sendTurn(t, ctx, client, "conv-dup", "actor", bootstrap)
	if res2.err != nil {
		t.Fatalf("turn 2 (duplicate): %v", res2.err)
	}
	if got, want := text(res2.msg.Data.Message), "call 1"; got != want {
		t.Errorf("duplicate redelivery reply = %q, want %q (should resend turn 1's own output, not reprocess)", got, want)
	}
	mu.Lock()
	got := calls
	mu.Unlock()
	if got != 1 {
		t.Errorf("executor invoked %d times, want 1 (duplicate redelivery must not reprocess)", got)
	}
}

// TestActorRuntime_DuplicateRedeliveryOfFailedTurnReturnsSameErrorWithoutReexecuting
// is the previous test's counterpart for a turn that failed outright: a
// redelivery of the same, already-failed StepID must return the identical
// cached error, not re-invoke the executor or surface a different one.
func TestActorRuntime_DuplicateRedeliveryOfFailedTurnReturnsSameErrorWithoutReexecuting(t *testing.T) {
	calls := 0
	executor := a2asrv.AgentExecutorFunc(func(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			calls++
			// A deterministic yield-order violation: a bare
			// *a2a.TaskStatusUpdateEvent can never legally be an
			// executor's first event (see harness/agent.go's own
			// NewAgentHarness) -- fn returns an error every single time
			// this runs, unconditionally.
			yield(&a2a.TaskStatusUpdateEvent{Status: a2a.TaskStatus{State: a2a.TaskStateWorking}}, nil)
		}
	})
	client := startTestHarness(t, NewAgentHarness(executor))
	ctx := context.Background()

	bootstrap := envNew(a2a.MessageRoleUser, "caller", "bad-agent", "hi")
	res1 := sendTurn(t, ctx, client, "conv-failed-dup", "bad-agent", bootstrap)
	if res1.err == nil {
		t.Fatal("turn 1: sendTurn succeeded, want an error (bare TaskStatusUpdateEvent can't be the first event)")
	}

	// Redeliver the exact same envelope (same StepID) as a second, separate
	// turn -- simulating retryExec's own indiscriminate resend after an
	// error.
	res2 := sendTurn(t, ctx, client, "conv-failed-dup", "bad-agent", bootstrap)
	if res2.err == nil {
		t.Fatal("turn 2 (duplicate): sendTurn succeeded, want an error")
	}
	if got, want := res2.err.Error(), res1.err.Error(); got != want {
		t.Errorf("turn 2 error = %q, want the identical cached error %q (a redelivered, already-failed StepID must not surface a different error)", got, want)
	}
	if calls != 1 {
		t.Errorf("executor invoked %d times, want 1 (duplicate redelivery of an already-failed turn must not re-execute it)", calls)
	}
}

// TestActorRuntime_DuplicateRedeliveryOfResumeInputDoesNotMisroute covers
// a sharper case: redelivering the exact message that resumed an
// already-blocked invocation, after that invocation has since moved on to
// a new correlation token, must still return the resumed turn's own
// output rather than being misrouted as a fresh invocation.
func TestActorRuntime_DuplicateRedeliveryOfResumeInputDoesNotMisroute(t *testing.T) {
	client := startTestHarness(t, NewAgentHarness(a2asrv.AgentExecutorFunc(callAndWaitExecutor)))
	ctx := context.Background()

	bootstrap := envNew(a2a.MessageRoleUser, "caller", "caller-agent", "start")
	res1 := sendTurn(t, ctx, client, "conv-dup2", "caller-agent", bootstrap)
	if res1.err != nil {
		t.Fatalf("turn 1: %v", res1.err)
	}

	otherReply := envReply(res1.msg, a2a.MessageRoleAgent, "caller-agent", "pong")
	otherReply.From = "other-agent"
	otherReply.Correlation = res1.msg.Correlation
	res2 := sendTurn(t, ctx, client, "conv-dup2", "caller-agent", otherReply)
	if res2.err != nil {
		t.Fatalf("turn 2: %v", res2.err)
	}
	if got, want := text(res2.msg.Data.Message), "got: pong"; got != want {
		t.Fatalf("turn 2 reply = %q, want %q", got, want)
	}

	// Redeliver turn 2's exact same input (otherReply) a second time.
	res3 := sendTurn(t, ctx, client, "conv-dup2", "caller-agent", otherReply)
	if res3.err != nil {
		t.Fatalf("turn 3 (duplicate): %v", res3.err)
	}
	if got, want := text(res3.msg.Data.Message), "got: pong"; got != want {
		t.Errorf("duplicate redelivery reply = %q, want %q -- a different result means it was misrouted as a fresh invocation instead of resending turn 2's own output", got, want)
	}
}

// TestActorRuntime_RedeliveredFirstRoundAttachesInsteadOfDuplicating covers
// a controller retry that redelivers a top-level input while the original
// invocation is still doing local work, before producing any output.
// Neither Connect's completed-turn dedup nor correlation-token matching
// catches this; routeOrAwait's own bookkeeping must attach the redelivery
// to the still-running invocation instead of spawning a concurrent
// duplicate.
func TestActorRuntime_RedeliveredFirstRoundAttachesInsteadOfDuplicating(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	started := make(chan struct{}, 1)
	release := make(chan struct{})

	executor := a2asrv.AgentExecutorFunc(func(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			mu.Lock()
			calls++
			n := calls
			mu.Unlock()
			select {
			case started <- struct{}{}:
			default:
			}
			<-release // still doing local work -- nothing checkpointed yet
			reply := a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart(fmt.Sprintf("call %d", n)))
			yield(reply, nil)
		}
	})
	client := startTestHarness(t, NewAgentHarness(executor))
	ctx := context.Background()

	bootstrap := envNew(a2a.MessageRoleUser, "caller", "actor", "hi")

	res1Ch := make(chan turnResult, 1)
	go func() { res1Ch <- sendTurn(t, ctx, client, "conv-leak", "actor", bootstrap) }()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("executor never started")
	}

	// The redelivery: same conversation, same bootstrap message (same ID),
	// while the original invocation above is still blocked on release.
	res2Ch := make(chan turnResult, 1)
	go func() { res2Ch <- sendTurn(t, ctx, client, "conv-leak", "actor", bootstrap) }()

	// No precise signal for "redelivery reached routeOrAwait" exists; the
	// assertions below check the eventual outcome, not this timing.
	time.Sleep(50 * time.Millisecond)
	close(release)

	res1 := <-res1Ch
	res2 := <-res2Ch

	if res1.err != nil {
		t.Fatalf("turn 1: %v", res1.err)
	}
	if res2.err != nil {
		t.Fatalf("turn 2 (redelivery): %v", res2.err)
	}
	if got, want := text(res1.msg.Data.Message), "call 1"; got != want {
		t.Errorf("turn 1 reply = %q, want %q", got, want)
	}
	if got, want := text(res2.msg.Data.Message), "call 1"; got != want {
		t.Errorf("redelivered turn's reply = %q, want %q -- a different value means it was spawned as a second, concurrent invocation instead of attaching to the first", got, want)
	}

	mu.Lock()
	got := calls
	mu.Unlock()
	if got != 1 {
		t.Errorf("executor invoked %d times, want 1 -- the redelivery spawned a duplicate invocation of the same logical turn instead of attaching to the still-running one", got)
	}
}

// TestActorRuntime_RedeliveredReplyAttachesInsteadOfDuplicating is the
// previous test's sharper case: the redelivered message is a reply the
// invocation already received and is now doing local work with, not a
// fresh top-level turn. deliverOnce's own correlation-token match can't
// catch this alone, since that entry was already consumed by the original
// delivery -- routeOrAwait's guard must still attach it correctly.
func TestActorRuntime_RedeliveredReplyAttachesInsteadOfDuplicating(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	started := make(chan struct{}, 1)
	release := make(chan struct{})

	executor := a2asrv.AgentExecutorFunc(func(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			client, err := newClient(execCtx, "other-agent")
			if err != nil {
				yield(nil, err)
				return
			}
			result, err := client.SendMessage(ctx, &a2a.SendMessageRequest{
				Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("ping")),
			})
			if err != nil {
				yield(nil, err)
				return
			}
			otherReply, ok := result.(*a2a.Message)
			if !ok {
				yield(nil, errors.New("unexpected reply type"))
				return
			}
			// Received the reply -- now doing local work with it, before
			// producing the next output. Nothing checkpointed for this
			// round yet.
			mu.Lock()
			calls++
			n := calls
			mu.Unlock()
			select {
			case started <- struct{}{}:
			default:
			}
			<-release
			reply := a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart(fmt.Sprintf("call %d got: %s", n, text(otherReply))))
			yield(reply, nil)
		}
	})
	client := startTestHarness(t, NewAgentHarness(executor))
	ctx := context.Background()

	bootstrap := envNew(a2a.MessageRoleUser, "caller", "caller-agent", "start")
	res1 := sendTurn(t, ctx, client, "conv-leak2", "caller-agent", bootstrap)
	if res1.err != nil {
		t.Fatalf("turn 1: %v", res1.err)
	}

	otherReply := envReply(res1.msg, a2a.MessageRoleAgent, "caller-agent", "pong")
	otherReply.From = "other-agent"
	otherReply.Correlation = res1.msg.Correlation

	// Deliver the reply: this Connect() call ends up waiting for the
	// executor's own next output, which is now blocked on release.
	res2Ch := make(chan turnResult, 1)
	go func() { res2Ch <- sendTurn(t, ctx, client, "conv-leak2", "caller-agent", otherReply) }()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("executor never reached its post-reply local work")
	}

	// The redelivery: same reply message (same ID, same correlation
	// token), while the original round above is still blocked on release.
	res3Ch := make(chan turnResult, 1)
	go func() { res3Ch <- sendTurn(t, ctx, client, "conv-leak2", "caller-agent", otherReply) }()

	// No precise signal for "redelivery reached routeOrAwait" exists; the
	// assertions below check the eventual outcome, not this timing.
	time.Sleep(50 * time.Millisecond)
	close(release)

	res2 := <-res2Ch
	res3 := <-res3Ch

	if res2.err != nil {
		t.Fatalf("turn 2 (reply delivery): %v", res2.err)
	}
	if res3.err != nil {
		t.Fatalf("turn 3 (redelivered reply): %v", res3.err)
	}
	want := "call 1 got: pong"
	if got := text(res2.msg.Data.Message); got != want {
		t.Errorf("turn 2 reply = %q, want %q", got, want)
	}
	if got := text(res3.msg.Data.Message); got != want {
		t.Errorf("redelivered reply's own turn = %q, want %q -- a different value means it was misrouted as a fresh top-level invocation instead of attaching to the original reply-delivery round", got, want)
	}

	mu.Lock()
	got := calls
	mu.Unlock()
	if got != 1 {
		t.Errorf("executor's post-reply local work ran %d times, want 1 -- the redelivered reply was misrouted as a fresh top-level invocation instead of attaching to the still-running one", got)
	}
}

// TestAgentHarness_ArtifactAppendMergesWithinSameCall confirms two
// *a2a.TaskArtifactUpdateEvents for the same artifact ID within one
// Execute() call, the second with Append: true, merge into one artifact
// with both chunks concatenated, not two separate entries.
func TestAgentHarness_ArtifactAppendMergesWithinSameCall(t *testing.T) {
	executor := a2asrv.AgentExecutorFunc(func(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			if !yield(&a2a.Task{}, nil) {
				return
			}
			if !yield(&a2a.TaskArtifactUpdateEvent{
				Artifact: &a2a.Artifact{ID: "art-1", Parts: a2a.ContentParts{a2a.NewTextPart("chunk1-")}},
			}, nil) {
				return
			}
			yield(&a2a.TaskArtifactUpdateEvent{
				Append:   true,
				Artifact: &a2a.Artifact{ID: "art-1", Parts: a2a.ContentParts{a2a.NewTextPart("chunk2")}},
			}, nil)
		}
	})
	client := startTestHarness(t, NewAgentHarness(executor))
	ctx := context.Background()

	bootstrap := envNew(a2a.MessageRoleUser, "caller", "artifact-merge", "hi")
	res := sendTurn(t, ctx, client, "conv-artifact-merge", "artifact-merge", bootstrap)
	if res.err != nil {
		t.Fatalf("sendTurn: %v", res.err)
	}
	artifacts := res.msg.Data.Task.Artifacts
	if len(artifacts) != 1 {
		t.Fatalf("Artifacts = %d entries, want 1 (a naive append would leave 2)", len(artifacts))
	}
	parts := artifacts[0].Parts
	if len(parts) != 2 {
		t.Fatalf("merged artifact has %d parts, want 2", len(parts))
	}
	if got, want := parts[0].Text()+parts[1].Text(), "chunk1-chunk2"; got != want {
		t.Errorf("merged artifact text = %q, want %q", got, want)
	}
}

// TestAgentHarness_ArtifactMergesAcrossTurns is the previous test's
// cross-turn counterpart: the first chunk is yielded on turn 1 (pausing at
// InputRequired), and the Append: true second chunk on a genuinely
// separate turn 2 -- seedTask must consult execCtx.StoredTask so turn 2
// merges onto turn 1's chunk instead of starting from an empty Task.
func TestAgentHarness_ArtifactMergesAcrossTurns(t *testing.T) {
	executor := a2asrv.AgentExecutorFunc(func(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			if execCtx.StoredTask == nil {
				if !yield(&a2a.Task{}, nil) {
					return
				}
				if !yield(&a2a.TaskArtifactUpdateEvent{
					Artifact: &a2a.Artifact{ID: "art-1", Parts: a2a.ContentParts{a2a.NewTextPart("chunk1-")}},
				}, nil) {
					return
				}
				yield(&a2a.TaskStatusUpdateEvent{Status: a2a.TaskStatus{State: a2a.TaskStateInputRequired}}, nil)
				return
			}
			// TaskID must be stamped explicitly: unlike
			// *a2a.TaskStatusUpdateEvent, ApplyArtifactUpdate rejects a
			// blank TaskID rather than treating it as "don't touch it."
			if !yield(&a2a.TaskArtifactUpdateEvent{
				TaskID:   execCtx.TaskID,
				Append:   true,
				Artifact: &a2a.Artifact{ID: "art-1", Parts: a2a.ContentParts{a2a.NewTextPart("chunk2")}},
			}, nil) {
				return
			}
			yield(&a2a.TaskStatusUpdateEvent{Status: a2a.TaskStatus{
				State:   a2a.TaskStateCompleted,
				Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("done")),
			}}, nil)
		}
	})
	client := startTestHarness(t, NewAgentHarness(executor))
	ctx := context.Background()

	bootstrap := envNew(a2a.MessageRoleUser, "caller", "artifact-cross-turn", "hi")
	res1 := sendTurn(t, ctx, client, "conv-artifact-cross-turn", "artifact-cross-turn", bootstrap)
	if res1.err != nil {
		t.Fatalf("turn 1: %v", res1.err)
	}
	if got, want := taskState(res1.msg.Data), a2a.TaskStateInputRequired; got != want {
		t.Fatalf("turn 1 reply Data.State = %q, want %q", got, want)
	}
	if got, want := len(res1.msg.Data.Task.Artifacts), 1; got != want {
		t.Fatalf("turn 1 Artifacts = %d entries, want %d", got, want)
	}

	more := envNew(a2a.MessageRoleUser, "caller", "artifact-cross-turn", "continue")
	more.Data.Message.TaskID = res1.msg.Data.Task.ID
	res2 := sendTurn(t, ctx, client, "conv-artifact-cross-turn", "artifact-cross-turn", res1.msg, more)
	if res2.err != nil {
		t.Fatalf("turn 2: %v", res2.err)
	}
	if got, want := res2.msg.To, "caller"; got != want {
		t.Errorf("turn 2 reply To = %q, want %q", got, want)
	}
	if got, want := taskState(res2.msg.Data), a2a.TaskStateCompleted; got != want {
		t.Errorf("turn 2 reply Data.State = %q, want %q", got, want)
	}
	artifacts := res2.msg.Data.Task.Artifacts
	if len(artifacts) != 1 {
		t.Fatalf("turn 2 Artifacts = %d entries, want 1 (turn 1's chunk must merge, not sit alongside turn 2's as a duplicate)", len(artifacts))
	}
	parts := artifacts[0].Parts
	if len(parts) != 2 {
		t.Fatalf("merged artifact has %d parts, want 2", len(parts))
	}
	if got, want := parts[0].Text()+parts[1].Text(), "chunk1-chunk2"; got != want {
		t.Errorf("merged artifact text = %q, want %q", got, want)
	}
}

// TestAgentHarness_ResumedStatusUpdateCarriesForwardPriorArtifacts confirms
// a bare *a2a.TaskStatusUpdateEvent on turn 2, never touching an artifact
// at all, still carries turn 1's already-established artifact forward
// rather than silently dropping it.
func TestAgentHarness_ResumedStatusUpdateCarriesForwardPriorArtifacts(t *testing.T) {
	executor := a2asrv.AgentExecutorFunc(func(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			if execCtx.StoredTask == nil {
				if !yield(&a2a.Task{}, nil) {
					return
				}
				if !yield(&a2a.TaskArtifactUpdateEvent{
					Artifact: &a2a.Artifact{ID: "art-1", Parts: a2a.ContentParts{a2a.NewTextPart("only-chunk")}},
				}, nil) {
					return
				}
				yield(&a2a.TaskStatusUpdateEvent{Status: a2a.TaskStatus{State: a2a.TaskStateInputRequired}}, nil)
				return
			}
			// Deliberately yields no artifact event at all this turn.
			yield(&a2a.TaskStatusUpdateEvent{Status: a2a.TaskStatus{
				State:   a2a.TaskStateCompleted,
				Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("done")),
			}}, nil)
		}
	})
	client := startTestHarness(t, NewAgentHarness(executor))
	ctx := context.Background()

	bootstrap := envNew(a2a.MessageRoleUser, "caller", "artifact-carry-forward", "hi")
	res1 := sendTurn(t, ctx, client, "conv-artifact-carry-forward", "artifact-carry-forward", bootstrap)
	if res1.err != nil {
		t.Fatalf("turn 1: %v", res1.err)
	}

	more := envNew(a2a.MessageRoleUser, "caller", "artifact-carry-forward", "continue")
	res2 := sendTurn(t, ctx, client, "conv-artifact-carry-forward", "artifact-carry-forward", res1.msg, more)
	if res2.err != nil {
		t.Fatalf("turn 2: %v", res2.err)
	}
	if got, want := taskState(res2.msg.Data), a2a.TaskStateCompleted; got != want {
		t.Fatalf("turn 2 reply Data.State = %q, want %q", got, want)
	}
	artifacts := res2.msg.Data.Task.Artifacts
	if len(artifacts) != 1 || len(artifacts[0].Parts) != 1 || artifacts[0].Parts[0].Text() != "only-chunk" {
		t.Errorf("turn 2 Artifacts = %+v, want turn 1's single artifact carried forward unchanged", artifacts)
	}
}

// TestAgentHarness_TaskStatePreservedAcrossMultipleTurns confirms
// execCtx.StoredTask correctly reflects a prior turn's own
// Task.Status.State across three external-caller-driven turns, each
// yielding a whole *a2a.Task -- a wrong or missing seed would make every
// turn behave as if it were the first.
func TestAgentHarness_TaskStatePreservedAcrossMultipleTurns(t *testing.T) {
	const askName = "what's your name?"
	const askAge = "what's your age?"
	executor := a2asrv.AgentExecutorFunc(func(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			switch {
			case execCtx.StoredTask == nil:
				yield(&a2a.Task{Status: a2a.TaskStatus{
					State:   a2a.TaskStateInputRequired,
					Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart(askName)),
				}}, nil)
			case execCtx.StoredTask.Status.State == a2a.TaskStateInputRequired && text(execCtx.StoredTask.Status.Message) == askName:
				yield(&a2a.Task{Status: a2a.TaskStatus{
					State:   a2a.TaskStateInputRequired,
					Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart(askAge)),
				}}, nil)
			default:
				yield(&a2a.Task{Status: a2a.TaskStatus{
					State: a2a.TaskStateCompleted,
					Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart(
						"done: prior state was "+string(execCtx.StoredTask.Status.State))),
				}}, nil)
			}
		}
	})
	client := startTestHarness(t, NewAgentHarness(executor))
	ctx := context.Background()

	bootstrap := envNew(a2a.MessageRoleUser, "caller", "task-state", "hi")
	res1 := sendTurn(t, ctx, client, "conv-task-state", "task-state", bootstrap)
	if res1.err != nil {
		t.Fatalf("turn 1: %v", res1.err)
	}
	if got, want := taskState(res1.msg.Data), a2a.TaskStateInputRequired; got != want {
		t.Fatalf("turn 1 reply Data.State = %q, want %q", got, want)
	}
	if got, want := text(res1.msg.Data.Task.Status.Message), askName; got != want {
		t.Fatalf("turn 1 reply message = %q, want %q", got, want)
	}

	msg2 := envNew(a2a.MessageRoleUser, "caller", "task-state", "Alice")
	res2 := sendTurn(t, ctx, client, "conv-task-state", "task-state", res1.msg, msg2)
	if res2.err != nil {
		t.Fatalf("turn 2: %v", res2.err)
	}
	if got, want := res2.msg.To, "caller"; got != want {
		t.Fatalf("turn 2 reply To = %q, want %q", got, want)
	}
	if got, want := taskState(res2.msg.Data), a2a.TaskStateInputRequired; got != want {
		t.Fatalf("turn 2 reply Data.State = %q, want %q -- StoredTask.Status.State from turn 1 was not seen, agent treated this as a fresh turn 1", got, want)
	}
	if got, want := text(res2.msg.Data.Task.Status.Message), askAge; got != want {
		t.Fatalf("turn 2 reply message = %q, want %q", got, want)
	}

	msg3 := envNew(a2a.MessageRoleUser, "caller", "task-state", "30")
	res3 := sendTurn(t, ctx, client, "conv-task-state", "task-state", res1.msg, msg2, res2.msg, msg3)
	if res3.err != nil {
		t.Fatalf("turn 3: %v", res3.err)
	}
	if got, want := res3.msg.To, "caller"; got != want {
		t.Fatalf("turn 3 reply To = %q, want %q", got, want)
	}
	if got, want := taskState(res3.msg.Data), a2a.TaskStateCompleted; got != want {
		t.Fatalf("turn 3 reply Data.State = %q, want %q", got, want)
	}
	if got, want := text(res3.msg.Data.Task.Status.Message), "done: prior state was "+string(a2a.TaskStateInputRequired); got != want {
		t.Errorf("turn 3 reply message = %q, want %q -- StoredTask.Status.State from turn 2 was not seen on turn 3", got, want)
	}
}
