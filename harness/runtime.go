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
	"log"
	"sync"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/google/uuid"
	"github.com/ktock/checkpointd/internal/hop"
	"github.com/ktock/checkpointd/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// invocationFunc is a driven-to-completion AgentExecutor call.
type invocationFunc func(cc *callContext, current *a2a.Message, history storedHistory) (*hop.Data, error)

// storedHistory reconstructs a2asrv.ExecutorContext.StoredTask from
// checkpointd's own event log.
type storedHistory struct {
	// messages is the flattened text history
	messages []*a2a.Message
	// task is whichever prior hop most recently carried a real Task-shaped
	// claim, nil if none ever did.
	task     *a2a.Task
}

// callContext bundles what one invocation needs to build a client on demand.
type callContext struct {
	rt      *actorRuntime
	inv     *invocation
	agentID string
	// current is this invocation's own original spawn input, fixed for its entire life
	current *hop.Envelope
}

// replyOrErr is what an invocation's own goroutine sends on its reply channel.
type replyOrErr struct {
	env *hop.Envelope
	err error
}

// invocation is one running invocationFunc call.
type invocation struct {
	reply chan replyOrErr // buffered, capacity 1
	// stepID is the StepID of whichever current most recently unblocked
	// this invocation.
	stepID string
}

// actorRuntime is the shared proto.HarnessServiceServer behind NewAgentHarness.
type actorRuntime struct {
	proto.UnimplementedHarnessServiceServer
	fn invocationFunc

	// processID identifies this process instance, for diagnostics.
	processID string

	mu       sync.Mutex
	waiting  map[string]*pendingCall // keyed by Envelope.Correlation
	inFlight map[string]*roundResult // keyed by hop.Envelope.StepID

	// mintedTaskIDs remembers every TaskID this process has freshly minted.
	mintedTaskIDs map[a2a.TaskID]struct{}

	// mintedContextIDs remembers every ContextID this process has freshly minted.
	mintedContextIDs map[string]struct{}

	// lastInputID/lastOutput/lastErr remember the most recently completed
	// turn's own StepID and its outcome so a redelivery of that StepID replays
	// the same result.
	lastInputID string
	lastOutput  *hop.Envelope
	lastErr     error
}

// pendingCall is one outstanding SendMessage call.
type pendingCall struct {
	// replyCh delivers its eventual repl
	replyCh chan *hop.Envelope // buffered, capacity 1
	// inv is the invocation waiting on it
	inv     *invocation
}

// roundResult lets a redelivered message share an already-in-flight round's own output.
type roundResult struct {
	done   chan struct{} // closed once result is set
	result replyOrErr
}

func newActorRuntime(fn invocationFunc) *actorRuntime {
	processID := uuid.NewString()
	log.Printf("harness: actorRuntime process instance %s starting", processID)
	return &actorRuntime{processID: processID, fn: fn, waiting: make(map[string]*pendingCall), inFlight: make(map[string]*roundResult), mintedTaskIDs: make(map[a2a.TaskID]struct{}), mintedContextIDs: make(map[string]struct{})}
}

// Connect drives one turn.
func (rt *actorRuntime) Connect(stream proto.HarnessService_ConnectServer) error {
	req, err := stream.Recv()
	if err != nil {
		return err
	}
	convID := req.GetConversationId()
	agentID := req.GetAgentId()

	steps := req.GetStart().GetSteps()
	if len(steps) == 0 {
		return status.Error(codes.InvalidArgument, "no input message")
	}
	envs := make([]*hop.Envelope, 0, len(steps))
	for _, step := range steps {
		text := firstText([]*proto.Step{step})
		env, ok := hop.Parse(text)
		if !ok {
			return status.Errorf(codes.InvalidArgument, "message must be an A2A-subset JSON envelope, got: %s", text)
		}
		envs = append(envs, env)
	}
	current := envs[len(envs)-1]
	if current.StepID == "" {
		return status.Error(codes.InvalidArgument, "current envelope's own StepID must be set")
	}
	var history storedHistory
	for _, env := range envs[:len(envs)-1] {
		switch {
		case env.From == agentID && env.Reply:
			// This actor's own logged claim completing a round trip
			// (flushSelf's self-continuation, or runInvocation's terminal
			// reply).
			if env.Data.Task != nil {
				if history.task != nil {
					history.messages = append(history.messages, history.task.Status.Message)
				}
				history.task = env.Data.Task
				continue
			}
			if history.task != nil {
				// Reject a bare *a2a.Message once a Task already
				// exists for the conversation -- NewAgentHarness's own
				// "established" check already enforces this on the
				// agent's own yields (see agent.go), so a logged reply
				// violating it here means the event log itself is corrupt.
				return status.Errorf(codes.Internal, "agent %s's own logged reply (stepID=%s) reverted to a bare Message after already claiming a Task: %v", agentID, env.StepID, a2a.ErrInvalidAgentResponse)
			}
			// A legitimate Message-only reply -- no Task ever claimed
			// this turn, so history.task stays nil and this content is
			// never surfaced as StoredTask history either way.
			continue
		case env.From != agentID && env.Reply:
			// Another agent's own reply completing a sub-call this actor
			// itself initiated.
			continue
		case env.From == agentID && !env.Reply:
			// This actor's own outbound sub-call request.
			continue
		default:
			// env.From != agentID && !env.Reply: a genuinely fresh input
			// delivered to this actor (the external caller's own
			// bootstrap/continuation, or another agent's own fresh call
			// into this one).
			var msg *a2a.Message
			switch {
			case env.Data.Task != nil:
				msg = env.Data.Task.Status.Message
			case env.Data.Message != nil:
				msg = env.Data.Message
			default:
				continue
			}
			history.messages = append(history.messages, msg)
		}
	}

	var output *hop.Envelope
	var dupErr error
	rt.mu.Lock()
	duplicate := current.StepID == rt.lastInputID
	if duplicate {
		output, dupErr = rt.lastOutput, rt.lastErr
	}
	rt.mu.Unlock()
	log.Printf("harness: process %s Connect stepID=%q reply=%v correlation=%q from=%q dedup=%v", rt.processID, current.StepID, current.Reply, current.Correlation, current.From, duplicate)

	if duplicate {
		if dupErr != nil {
			return dupErr
		}
	} else {
		env, err := rt.routeOrAwait(agentID, current, history)
		if err != nil {
			return err
		}
		output = env
	}
	log.Printf("harness: process %s Connect stepID=%q output correlation=%q to=%q", rt.processID, current.StepID, output.Correlation, output.To)

	b, err := json.Marshal(output)
	if err != nil {
		return fmt.Errorf("encoding output: %w", err)
	}
	if err := stream.Send(&proto.HarnessResponse{
		ConversationId: convID,
		Type: &proto.HarnessResponse_Outputs{
			Outputs: &proto.HarnessOutputs{Steps: []*proto.Step{textStep(string(b))}},
		},
	}); err != nil {
		return err
	}
	return stream.Send(&proto.HarnessResponse{
		ConversationId: convID,
		Type: &proto.HarnessResponse_End{
			End: &proto.HarnessEnd{State: proto.State_STATE_COMPLETED},
		},
	})
}

// routeOrAwait delivers current to whichever invocation should handle it.
func (rt *actorRuntime) routeOrAwait(agentID string, current *hop.Envelope, history storedHistory) (*hop.Envelope, error) {
	rt.mu.Lock()
	if fr, ok := rt.inFlight[current.StepID]; ok {
		rt.mu.Unlock()
		<-fr.done
		return fr.result.env, fr.result.err
	}
	fr := &roundResult{done: make(chan struct{})}
	rt.inFlight[current.StepID] = fr
	rt.mu.Unlock()

	env, err := rt.deliverOnce(agentID, current, history)

	rt.mu.Lock()
	rt.lastInputID = current.StepID
	if err == nil {
		env.StepID = current.StepID
		rt.lastOutput = env
		rt.lastErr = nil
	} else {
		rt.lastOutput = nil
		rt.lastErr = err
	}
	fr.result = replyOrErr{env: env, err: err}
	delete(rt.inFlight, current.StepID)
	close(fr.done)
	rt.mu.Unlock()

	return env, err
}

// deliverOnce routes current to whichever invocation is genuinely blocked
// waiting for exactly this reply. If it isn't marked as a reply, spawns a
// new invocation to handle it as a fresh turn.
func (rt *actorRuntime) deliverOnce(agentID string, current *hop.Envelope, history storedHistory) (*hop.Envelope, error) {
	if current.Reply {
		token := current.Correlation
		rt.mu.Lock()
		p, ok := rt.waiting[token]
		if ok {
			delete(rt.waiting, token)
			// Refreshed before the invocation runs further, so
			// SendMessage can reuse it as its own next call's token.
			p.inv.stepID = current.StepID
		}
		if !ok {
			var stillWaiting []string
			for t := range rt.waiting {
				stillWaiting = append(stillWaiting, t)
			}
			rt.mu.Unlock()
			return nil, fmt.Errorf("harness: process %s: no invocation waiting for correlation token %q (reply from %q), and this actor has never answered this exact call either: this actor's in-flight invocation state for that call is gone -- refusing to spawn a fresh invocation fed this reply as its own bootstrap input (still waiting on: %v)", rt.processID, token, current.From, stillWaiting)
		}
		rt.mu.Unlock()
		p.replyCh <- current
		r := <-p.inv.reply
		log.Printf("harness: process %s matched token %q", rt.processID, token)
		return r.env, r.err
	}
	return rt.spawn(agentID, current, history)
}

// spawn starts a brand-new invocation for current.
func (rt *actorRuntime) spawn(agentID string, current *hop.Envelope, history storedHistory) (*hop.Envelope, error) {
	inv := &invocation{reply: make(chan replyOrErr, 1), stepID: current.StepID}
	cc := &callContext{rt: rt, inv: inv, agentID: agentID, current: current}
	go runInvocation(inv, rt.fn, cc, current, history)
	r := <-inv.reply
	return r.env, r.err
}

// interrupted reports whether state means the task is paused waiting on
// the external caller.
func interrupted(state a2a.TaskState) bool {
	return state == a2a.TaskStateInputRequired || state == a2a.TaskStateAuthRequired
}

// selfContinue reports whether state means the agent has more of its own
// work left to do. In practice this is just Working and Submitted.
func selfContinue(state a2a.TaskState) bool {
	if state == a2a.TaskStateUnspecified {
		return false
	}
	return !state.Terminal() && !interrupted(state)
}

// flushSelf suspends the calling invocation mid-Execute(), handing data
// back as a self-addressed hop, then blocks until checkpointd redelivers
// the matching reply.
func (cc *callContext) flushSelf(data *hop.Data) {
	// Filled in here too: a Working/Submitted report can reach the wire
	// mid-call, well before runInvocation's own post-loop fill-in runs.
	ensureTaskID(cc.rt, cc.current, data)
	ensureContextID(cc.rt, cc.current, data)

	replyCh := make(chan *hop.Envelope, 1)
	token := cc.inv.stepID
	cc.rt.mu.Lock()
	cc.rt.waiting[token] = &pendingCall{replyCh: replyCh, inv: cc.inv}
	cc.rt.mu.Unlock()

	cc.inv.reply <- replyOrErr{env: &hop.Envelope{
		From:        cc.agentID,
		To:          cc.agentID,
		Data:        *data,
		Reply:       true,
		Correlation: token,
	}}

	<-replyCh
}

// ensureTaskID mints or reuses data.Task's own ID if the agent hasn't
// claimed one yet.
func ensureTaskID(rt *actorRuntime, current *hop.Envelope, data *hop.Data) {
	if data.Task == nil || data.Task.ID != "" {
		return
	}
	if id := currentTaskID(current.Data); id != "" {
		data.Task.ID = id
		return
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	var id a2a.TaskID
	for {
		id = a2a.NewTaskID()
		if _, exists := rt.mintedTaskIDs[id]; !exists {
			rt.mintedTaskIDs[id] = struct{}{}
			break
		}
	}
	data.Task.ID = id
}

// ensureContextID mints or reuses data.Task's own ContextID if the agent
// hasn't claimed one yet.
func ensureContextID(rt *actorRuntime, current *hop.Envelope, data *hop.Data) {
	if data.Task == nil || data.Task.ContextID != "" {
		return
	}
	if id := currentContextID(current.Data); id != "" {
		data.Task.ContextID = id
		return
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	var id string
	for {
		id = a2a.NewContextID()
		if _, exists := rt.mintedContextIDs[id]; !exists {
			rt.mintedContextIDs[id] = struct{}{}
			break
		}
	}
	data.Task.ContextID = id
}

// runInvocation drives fn to completion.
func runInvocation(inv *invocation, fn invocationFunc, cc *callContext, current *hop.Envelope, history storedHistory) {
	data, err := fn(cc, hopMessage(current.Data), history)
	if err != nil {
		log.Printf("harness: invocation failed: %v", err)
		inv.reply <- replyOrErr{err: fmt.Errorf("invocation failed: %w", err)}
		return
	}
	// Fills in a fresh TaskID/ContextID if the agent never claimed one.
	ensureTaskID(cc.rt, current, data)
	ensureContextID(cc.rt, current, data)
	// StepID is stamped by Connect, not here.
	env := &hop.Envelope{
		From: cc.agentID,
		To: current.From,
		Data: *data,
		Reply: true,
		// Carries current's own correlation token forward, so whoever's
		// waiting on this call recognizes the reply.
		Correlation: current.Correlation,
	}
	inv.reply <- replyOrErr{env: env}
}

// errTransportNotImplemented is returned by every pipeTransport method
// this package doesn't need yet.
var errTransportNotImplemented = errors.New("harness: not implemented")

// notImplementedEvents is the iter.Seq2-shaped form of
// errTransportNotImplemented, for pipeTransport's streaming methods.
func notImplementedEvents(yield func(a2a.Event, error) bool) {
	yield(nil, errTransportNotImplemented)
}

// pipeTransport is the a2aclient.Transport backing one invocation's own
// client.SendMessage calls.
type pipeTransport struct {
	rt      *actorRuntime
	inv     *invocation
	agentID string
	// target is who SendMessage always addresses the outbound message to.
	target string
	// taskID/contextID are this invocation's own identity, used as a
	// fallback when the agent's own outbound message doesn't set them.
	taskID    a2a.TaskID
	contextID string
}

var _ a2aclient.Transport = (*pipeTransport)(nil)

// SendMessage wraps req.Message in a fresh Envelope addressed to target
// with a fresh correlation token, then blocks until that reply arrives.
func (t *pipeTransport) SendMessage(ctx context.Context, params a2aclient.ServiceParams, req *a2a.SendMessageRequest) (a2a.SendMessageResult, error) {
	msg := req.Message
	if msg.TaskID == "" || msg.ContextID == "" {
		// Fills in this invocation's own identity when the agent's own
		// message left it empty.
		m := *msg
		if m.TaskID == "" {
			m.TaskID = t.taskID
		}
		if m.ContextID == "" {
			m.ContextID = t.contextID
		}
		msg = &m
	}
	replyCh := make(chan *hop.Envelope, 1)
	t.rt.mu.Lock()
	token := t.inv.stepID
	t.rt.waiting[token] = &pendingCall{replyCh: replyCh, inv: t.inv}
	t.rt.mu.Unlock()
	log.Printf("harness: process %s registered token %q (call to %q)", t.rt.processID, token, t.target)

	env := &hop.Envelope{
		From: t.agentID, To: t.target, Correlation: token,
		Data: hop.Data{Message: msg, Type: hop.DataTypeMessage},
	}

	t.inv.reply <- replyOrErr{env: env}

	reply := <-replyCh
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("harness: waiting for reply from %q: %w", t.target, err)
	}
	data := reply.Data
	switch data.Type {
	case hop.DataTypeTask:
		// Already the callee's own aggregated Task claim.
		task := *data.Task
		return &task, nil
	default: // hop.DataTypeMessage, hop.DataTypeUnspecified
		// Already SendMessageResult-shaped, so it goes back as yielded.
		if data.Message == nil {
			// The callee's own Execute() call yielded nothing at all.
			return nil, a2a.ErrInvalidAgentResponse
		}
		return data.Message, nil
	}
}

func (t *pipeTransport) GetTask(context.Context, a2aclient.ServiceParams, *a2a.GetTaskRequest) (*a2a.Task, error) {
	return nil, errTransportNotImplemented
}

func (t *pipeTransport) ListTasks(context.Context, a2aclient.ServiceParams, *a2a.ListTasksRequest) (*a2a.ListTasksResponse, error) {
	return nil, errTransportNotImplemented
}

func (t *pipeTransport) CancelTask(context.Context, a2aclient.ServiceParams, *a2a.CancelTaskRequest) (*a2a.Task, error) {
	return nil, errTransportNotImplemented
}

func (t *pipeTransport) SubscribeToTask(context.Context, a2aclient.ServiceParams, *a2a.SubscribeToTaskRequest) iter.Seq2[a2a.Event, error] {
	return notImplementedEvents
}

func (t *pipeTransport) SendStreamingMessage(context.Context, a2aclient.ServiceParams, *a2a.SendMessageRequest) iter.Seq2[a2a.Event, error] {
	return notImplementedEvents
}

func (t *pipeTransport) GetTaskPushConfig(context.Context, a2aclient.ServiceParams, *a2a.GetTaskPushConfigRequest) (*a2a.PushConfig, error) {
	return nil, errTransportNotImplemented
}

func (t *pipeTransport) ListTaskPushConfigs(context.Context, a2aclient.ServiceParams, *a2a.ListTaskPushConfigRequest) ([]*a2a.PushConfig, error) {
	return nil, errTransportNotImplemented
}

func (t *pipeTransport) CreateTaskPushConfig(context.Context, a2aclient.ServiceParams, *a2a.PushConfig) (*a2a.PushConfig, error) {
	return nil, errTransportNotImplemented
}

func (t *pipeTransport) DeleteTaskPushConfig(context.Context, a2aclient.ServiceParams, *a2a.DeleteTaskPushConfigRequest) error {
	return errTransportNotImplemented
}

func (t *pipeTransport) GetExtendedAgentCard(context.Context, a2aclient.ServiceParams, *a2a.GetExtendedAgentCardRequest) (*a2a.AgentCard, error) {
	return nil, errTransportNotImplemented
}

func (t *pipeTransport) Destroy() error { return nil }

func hopMessage(d hop.Data) *a2a.Message {
	if d.Task != nil {
		return d.Task.Status.Message
	}
	return d.Message
}

func currentTaskID(d hop.Data) a2a.TaskID {
	if d.Task != nil {
		return d.Task.ID
	}
	if d.Message != nil {
		return d.Message.TaskID
	}
	return ""
}

func currentContextID(d hop.Data) string {
	if d.Task != nil {
		return d.Task.ContextID
	}
	if d.Message != nil {
		return d.Message.ContextID
	}
	return ""
}

func firstText(steps []*proto.Step) string {
	for _, step := range steps {
		content := step.GetContent()
		if content == nil {
			continue
		}
		for _, c := range content.Content {
			if text := c.GetText().GetText(); text != "" {
				return text
			}
		}
	}
	return ""
}

func textStep(text string) *proto.Step {
	return &proto.Step{
		Type: &proto.Step_Content{
			Content: &proto.ContentStep{
				Role: "assistant",
				Content: []*proto.Content{{
					Type: &proto.Content_Text{Text: &proto.TextContent{Text: text}},
				}},
			},
		},
	}
}
