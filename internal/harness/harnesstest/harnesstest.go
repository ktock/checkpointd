// Copyright 2026 Google LLC
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

package harnesstest

// Shared in-process mocks for the harness tests: a mock Substrate Control server
// (the substrate control plane), a mock HarnessService server (the harness
// inside an actor), a recording Handler, and message builders.

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/ktock/checkpointd/internal/harness"
	"github.com/ktock/checkpointd/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// MockControlServer is an in-process ateapipb.ControlServer that records the
// actor lifecycle calls SubstrateHarness makes and lets tests steer the
// CreateActor/ResumeActor responses. Only the RPCs SubstrateHarness uses are
// implemented; the rest come from the embedded Unimplemented server.
type MockControlServer struct {
	ateapipb.UnimplementedControlServer

	mu                sync.Mutex
	createCalls       []string
	resumeCalls       []string
	suspendCalls      []string
	deleteCalls       []string
	deleteWorkerCalls []string
	getCalls          []string
	revertCalls       []string

	CreateErr        error  // returned from CreateActor when non-nil
	EgressPolicyErr  error  // returned from CreateActorEgressPolicy when non-nil
	ResumeErr        error  // returned from ResumeActor when non-nil
	RevertErr        error  // returned from RevertActor when non-nil
	ResumeIP         string // WorkerAssignment.WorkerPodIp returned from ResumeActor
	ResumeWorkerName string // WorkerAssignment.Worker.Name returned from ResumeActor, if set
	// PostRecoveryResumeIP, once set, is returned as
	// WorkerAssignment.WorkerPodIp once DeleteWorker has been called at
	// least once, standing in for a force-recovered actor landing on a
	// different, healthy worker.
	PostRecoveryResumeIP string
	ResumeNilActor       bool  // when true, ResumeActor returns a nil Actor
	SuspendErr           error // returned from SuspendActor when non-nil

	// SuspendSucceedsWithoutAdvancing simulates SuspendActor reporting
	// success and moving the actor to SUSPENDED without its external snapshot
	// actually advancing.
	SuspendSucceedsWithoutAdvancing bool

	// Templates is what ListActorTemplates serves.
	Templates []*ateapipb.ActorTemplate
	// TemplatePageSize caps each ListActorTemplates page when positive.
	TemplatePageSize int
	listedAtespaces  []string

	// egressPolicies holds each actor's policy and sequence records the order of policy creations and resumes.
	egressPolicies map[string]*ateapipb.EgressPolicy
	sequence       []string

	// actors backs the actor RPCs below; only populated by tests that call the
	// SetXxx helpers.
	actors map[string]*ateapipb.Actor
}

// setActor tracks name in state, with snapshotURI as its external snapshot
// when non-empty and workerName as its worker when non-empty.
func (f *MockControlServer) setActor(name string, state ateapipb.ActorState, snapshotURI, workerName string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.actors == nil {
		f.actors = make(map[string]*ateapipb.Actor)
	}
	st := &ateapipb.ActorStatus{State: state}
	if snapshotURI != "" {
		st.ExternalSnapshot = &ateapipb.ExternalSnapshot{SnapshotUri: snapshotURI}
	}
	if workerName != "" {
		st.WorkerAssignment = &ateapipb.WorkerAssignment{Worker: &ateapipb.ObjectRef{Name: workerName}}
	}
	f.actors[name] = &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Name: name}, Status: st}
}

// SetCrashed marks name as ACTOR_STATE_CRASHED with snapshotURI as its
// external snapshot, so a later ResumeActor call for it fails until it is
// reverted.
func (f *MockControlServer) SetCrashed(name, snapshotURI string) {
	f.setActor(name, ateapipb.ActorState_ACTOR_STATE_CRASHED, snapshotURI, "")
}

// SetReverting marks name as ACTOR_STATE_REVERTING, standing in for a revert
// that was interrupted partway through.
func (f *MockControlServer) SetReverting(name, snapshotURI string) {
	f.setActor(name, ateapipb.ActorState_ACTOR_STATE_REVERTING, snapshotURI, "")
}

// ExternalSnapshotURI returns name's currently-tracked external snapshot URI,
// or "" if it has none.
func (f *MockControlServer) ExternalSnapshotURI(name string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.actors[name].GetStatus().GetExternalSnapshot().GetSnapshotUri()
}

// ActorState returns name's currently-tracked state.
func (f *MockControlServer) ActorState(name string) ateapipb.ActorState {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.actors[name].GetStatus().GetState()
}

// SetSuspending marks name as ACTOR_STATE_SUSPENDING, so a later ResumeActor
// call for it fails until something re-invokes SuspendActor.
func (f *MockControlServer) SetSuspending(name, snapshotURI string) {
	f.setActor(name, ateapipb.ActorState_ACTOR_STATE_SUSPENDING, snapshotURI, "")
}

// SetSuspendingWithWorker marks name as ACTOR_STATE_SUSPENDING like
// SetSuspending, but leaves it assigned to workerName until DeleteWorker
// crashes it.
func (f *MockControlServer) SetSuspendingWithWorker(name, snapshotURI, workerName string) {
	f.setActor(name, ateapipb.ActorState_ACTOR_STATE_SUSPENDING, snapshotURI, workerName)
}

// SetResuming marks name as ACTOR_STATE_RESUMING, so a later ResumeActor call
// for it retries against Substrate's own re-entrant resume workflow instead
// of being declined.
func (f *MockControlServer) SetResuming(name, snapshotURI string) {
	f.setActor(name, ateapipb.ActorState_ACTOR_STATE_RESUMING, snapshotURI, "")
}

// SetPausing marks name as ACTOR_STATE_PAUSING, a state resumeActor's own
// switch doesn't specially recognize, so it reports a plain retryable error.
func (f *MockControlServer) SetPausing(name, snapshotURI string) {
	f.setActor(name, ateapipb.ActorState_ACTOR_STATE_PAUSING, snapshotURI, "")
}

// SetRunning marks name as ACTOR_STATE_RUNNING, standing in for an actor that
// already completed a prior turn/checkpoint.
func (f *MockControlServer) SetRunning(name, snapshotURI string) {
	f.setActor(name, ateapipb.ActorState_ACTOR_STATE_RUNNING, snapshotURI, "")
}

// SetDeleting marks name as ACTOR_STATE_DELETING with no worker, which resumeActor cannot recover.
func (f *MockControlServer) SetDeleting(name, snapshotURI string) {
	f.setActor(name, ateapipb.ActorState_ACTOR_STATE_DELETING, snapshotURI, "")
}

// SetDeletingWithWorker marks name as ACTOR_STATE_DELETING while it still holds workerName, as after a delete whose atelet terminate failed.
func (f *MockControlServer) SetDeletingWithWorker(name, snapshotURI, workerName string) {
	f.setActor(name, ateapipb.ActorState_ACTOR_STATE_DELETING, snapshotURI, workerName)
}

func (f *MockControlServer) CreateAtespace(_ context.Context, req *ateapipb.CreateAtespaceRequest) (*ateapipb.Atespace, error) {
	return &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: req.GetAtespace().GetMetadata().GetName()}}, nil
}

func (f *MockControlServer) CreateActor(_ context.Context, req *ateapipb.CreateActorRequest) (*ateapipb.Actor, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := req.GetActor().GetMetadata().GetName()
	f.createCalls = append(f.createCalls, name)
	if f.CreateErr != nil {
		return nil, f.CreateErr
	}
	// An actor that already exists is AlreadyExists, never silently
	// overwritten.
	if _, exists := f.actors[name]; exists {
		return nil, status.Errorf(codes.AlreadyExists, "actor %s already exists", name)
	}
	actor := &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Name: name}}
	if f.actors == nil {
		f.actors = make(map[string]*ateapipb.Actor)
	}
	f.actors[name] = actor
	return actor, nil
}

func (f *MockControlServer) GetActor(_ context.Context, req *ateapipb.GetActorRequest) (*ateapipb.Actor, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := req.GetActor().GetName()
	f.getCalls = append(f.getCalls, name)
	actor, ok := f.actors[name]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "actor %s not found", name)
	}
	return actor, nil
}

func (f *MockControlServer) ResumeActor(_ context.Context, req *ateapipb.ResumeActorRequest) (*ateapipb.ResumeActorResponse, error) {
	f.mu.Lock()
	name := req.GetActor().GetName()
	f.resumeCalls = append(f.resumeCalls, name)
	f.sequence = append(f.sequence, "resume:"+name)
	state := f.actors[name].GetStatus().GetState()
	// Carried into the response below like the real Control API does.
	externalSnapshot := f.actors[name].GetStatus().GetExternalSnapshot()
	resumeIP := f.ResumeIP
	if f.PostRecoveryResumeIP != "" && len(f.deleteWorkerCalls) > 0 {
		resumeIP = f.PostRecoveryResumeIP
	}
	var worker *ateapipb.ObjectRef
	if f.ResumeWorkerName != "" {
		worker = &ateapipb.ObjectRef{Name: f.ResumeWorkerName}
	}
	f.mu.Unlock()
	if f.ResumeErr != nil {
		return nil, f.ResumeErr
	}
	// Mirrors the real Control API: FailedPrecondition for any state other than
	// SUSPENDED, PAUSED, or an already RESUMING/RUNNING actor.
	switch state {
	case ateapipb.ActorState_ACTOR_STATE_CRASHED,
		ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
		ateapipb.ActorState_ACTOR_STATE_DELETING,
		ateapipb.ActorState_ACTOR_STATE_REVERTING:
		return nil, status.Errorf(codes.FailedPrecondition, "actor %s is %s, want SUSPENDED or PAUSED", name, state)
	}
	if f.ResumeNilActor {
		return &ateapipb.ResumeActorResponse{}, nil
	}
	respActor := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Name: name},
		Status: &ateapipb.ActorStatus{
			State:            ateapipb.ActorState_ACTOR_STATE_RUNNING,
			WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPodIp: resumeIP, Worker: worker},
			ExternalSnapshot: externalSnapshot,
		},
	}
	f.mu.Lock()
	// Persisted so a later DeleteWorker call can find this actor by its
	// assigned worker's name.
	if actor, ok := f.actors[name]; ok {
		if actor.Status == nil {
			actor.Status = &ateapipb.ActorStatus{}
		}
		actor.Status.State = respActor.Status.State
		actor.Status.WorkerAssignment = respActor.Status.WorkerAssignment
	}
	f.mu.Unlock()
	return &ateapipb.ResumeActorResponse{Actor: respActor}, nil
}

// RevertActor mirrors the real Control API: it accepts RUNNING, PAUSED,
// CRASHED, or an interrupted REVERTING actor, and returns it to SUSPENDED with
// no worker and its external snapshot untouched.
func (f *MockControlServer) RevertActor(_ context.Context, req *ateapipb.RevertActorRequest) (*ateapipb.RevertActorResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := req.GetActor().GetName()
	f.revertCalls = append(f.revertCalls, name)
	if f.RevertErr != nil {
		return nil, f.RevertErr
	}
	actor, ok := f.actors[name]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "actor %s not found", name)
	}
	switch state := actor.GetStatus().GetState(); state {
	case ateapipb.ActorState_ACTOR_STATE_RUNNING,
		ateapipb.ActorState_ACTOR_STATE_PAUSED,
		ateapipb.ActorState_ACTOR_STATE_CRASHED,
		ateapipb.ActorState_ACTOR_STATE_REVERTING:
	default:
		return nil, status.Errorf(codes.FailedPrecondition, "actor %s is not in a revertable state (got: %s)", name, state)
	}
	actor.Status.State = ateapipb.ActorState_ACTOR_STATE_SUSPENDED
	actor.Status.WorkerAssignment = nil
	return &ateapipb.RevertActorResponse{Actor: actor}, nil
}

func (f *MockControlServer) DeleteActor(_ context.Context, req *ateapipb.DeleteActorRequest) (*ateapipb.Actor, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := req.GetActor().GetName()
	f.deleteCalls = append(f.deleteCalls, name)
	// Fails while the actor still carries a worker assignment, mirroring
	// the real Control API, until DeleteWorker clears it.
	if actor, ok := f.actors[name]; ok && actor.GetStatus().GetWorkerAssignment() != nil {
		return nil, status.Errorf(codes.Internal, "while terminating actor on atelet: worker unreachable")
	}
	delete(f.actors, name)
	return &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Name: name}}, nil
}

// DeleteWorker deregisters workerName, crashing whatever non-SUSPENDED actor
// is currently assigned to it.
func (f *MockControlServer) DeleteWorker(_ context.Context, req *ateapipb.DeleteWorkerRequest) (*ateapipb.Worker, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	workerName := req.GetWorker().GetName()
	f.deleteWorkerCalls = append(f.deleteWorkerCalls, workerName)
	for _, actor := range f.actors {
		if actor.GetStatus().GetWorkerAssignment().GetWorker().GetName() == workerName &&
			actor.Status.State != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
			actor.Status.WorkerAssignment = nil
			// Mirrors the real Control API: transitions the actor straight
			// to CRASHED, like a real pod deletion would.
			actor.Status.State = ateapipb.ActorState_ACTOR_STATE_CRASHED
		}
	}
	return &ateapipb.Worker{Metadata: &ateapipb.ResourceMetadata{Name: workerName}}, nil
}

func (f *MockControlServer) SuspendActor(_ context.Context, req *ateapipb.SuspendActorRequest) (*ateapipb.SuspendActorResponse, error) {
	f.mu.Lock()
	name := req.GetActor().GetName()
	f.suspendCalls = append(f.suspendCalls, name)
	if f.SuspendErr != nil {
		f.mu.Unlock()
		// Left tracked as SUSPENDING, not advanced to SUSPENDED, standing
		// in for a checkpoint that never completes.
		return nil, f.SuspendErr
	}
	// A genuinely successful checkpoint always mints a fresh external snapshot,
	// like the real Control API does. SuspendSucceedsWithoutAdvancing
	// simulates a checkpoint that moves the actor to SUSPENDED without one.
	snapshot := &ateapipb.ExternalSnapshot{SnapshotUri: fmt.Sprintf("mock-snapshot-%d", len(f.suspendCalls))}
	if actor, ok := f.actors[name]; ok {
		if f.SuspendSucceedsWithoutAdvancing {
			actor.Status = &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED, ExternalSnapshot: actor.GetStatus().GetExternalSnapshot()}
			snapshot = actor.Status.ExternalSnapshot
		} else {
			actor.Status = &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED, ExternalSnapshot: snapshot}
		}
	}
	f.mu.Unlock()
	return &ateapipb.SuspendActorResponse{
		Actor: &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Name: name}, Status: &ateapipb.ActorStatus{ExternalSnapshot: snapshot}},
	}, nil
}

// CreateActorEgressPolicy stores the actor's policy, failing with AlreadyExists if it already has one like the real Control API.
func (f *MockControlServer) CreateActorEgressPolicy(_ context.Context, req *ateapipb.CreateActorEgressPolicyRequest) (*ateapipb.EgressPolicy, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := req.GetActor().GetName()
	f.sequence = append(f.sequence, "egress-policy:"+name)
	if f.EgressPolicyErr != nil {
		return nil, f.EgressPolicyErr
	}
	if _, exists := f.egressPolicies[name]; exists {
		return nil, status.Errorf(codes.AlreadyExists, "egress policy for actor %s already exists", name)
	}
	if f.egressPolicies == nil {
		f.egressPolicies = make(map[string]*ateapipb.EgressPolicy)
	}
	f.egressPolicies[name] = req.GetEgressPolicy()
	return req.GetEgressPolicy(), nil
}

// EgressPolicyOf returns the policy stored for the actor name, or nil if it has none.
func (f *MockControlServer) EgressPolicyOf(name string) *ateapipb.EgressPolicy {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.egressPolicies[name]
}

// Sequence returns the order of policy creations and resumes as "egress-policy:<actor>" and "resume:<actor>".
func (f *MockControlServer) Sequence() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.sequence...)
}

// ListActorTemplates serves Templates filtered by atespace, paginated by TemplatePageSize with the next index as the page token.
func (f *MockControlServer) ListActorTemplates(_ context.Context, req *ateapipb.ListActorTemplatesRequest) (*ateapipb.ListActorTemplatesResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listedAtespaces = append(f.listedAtespaces, req.GetAtespace())
	var matching []*ateapipb.ActorTemplate
	for _, t := range f.Templates {
		if req.GetAtespace() == "" || t.GetMetadata().GetAtespace() == req.GetAtespace() {
			matching = append(matching, t)
		}
	}
	start := 0
	if tok := req.GetPageToken(); tok != "" {
		if _, err := fmt.Sscanf(tok, "%d", &start); err != nil || start < 0 || start > len(matching) {
			return nil, status.Errorf(codes.InvalidArgument, "bad page token %q", tok)
		}
	}
	end := len(matching)
	next := ""
	if f.TemplatePageSize > 0 && start+f.TemplatePageSize < end {
		end = start + f.TemplatePageSize
		next = fmt.Sprintf("%d", end)
	}
	return &ateapipb.ListActorTemplatesResponse{ActorTemplates: matching[start:end], NextPageToken: next}, nil
}

// ListedAtespaces returns the atespace of every ListActorTemplates call so far.
func (f *MockControlServer) ListedAtespaces() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.listedAtespaces...)
}

// Calls returns copies of the recorded call lists.
func (f *MockControlServer) Calls() (create, resume, suspend []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.createCalls...),
		append([]string(nil), f.resumeCalls...),
		append([]string(nil), f.suspendCalls...)
}

// CrashRecoveryCalls returns copies of the recorded call lists for the RPCs
// only crash recovery uses.
func (f *MockControlServer) CrashRecoveryCalls() (get, del, revert []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.getCalls...),
		append([]string(nil), f.deleteCalls...),
		append([]string(nil), f.revertCalls...)
}

// DeleteWorkerCalls returns a copy of the recorded DeleteWorker call list.
func (f *MockControlServer) DeleteWorkerCalls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.deleteWorkerCalls...)
}

// mockHarnessServer is an in-process proto.HarnessServiceServer standing in for
// the harness running inside an actor (substrate) or a local subprocess
// (antigravity). It records the start frame and emits its configured outputs
// followed by a terminal HarnessEnd.
type MockHarnessServer struct {
	proto.UnimplementedHarnessServiceServer

	// Outputs are the steps emitted (in a single Outputs frame) before the
	// terminal HarnessEnd. When nil, each input is echoed as "ack: <input>".
	Outputs []*proto.Step
	// FailConnect makes Connect return an RPC error before any frame.
	FailConnect bool
	// FailFrame makes Connect terminate the turn with HarnessEnd{STATE_FAILED}.
	FailFrame bool
	// ErrCode is the error code used by FailFrame.
	ErrCode int32
	// ErrMessage is the error text used by FailConnect/FailFrame.
	ErrMessage string

	mu             sync.Mutex
	gotConvID      string
	gotHarnessID   string
	gotAgentConfig []byte
	gotInputs      []string
}

func (s *MockHarnessServer) Connect(stream proto.HarnessService_ConnectServer) error {
	if s.FailConnect {
		return status.Error(codes.Internal, s.ErrMessage)
	}

	req, err := stream.Recv()
	if err != nil {
		return err
	}

	var inputs []string
	for _, step := range req.GetStart().GetSteps() {
		if contentStep := step.GetContent(); contentStep != nil {
			for _, c := range contentStep.Content {
				if text := c.GetText().GetText(); text != "" {
					inputs = append(inputs, text)
				}
			}
		}
	}
	s.mu.Lock()
	s.gotConvID = req.GetConversationId()
	s.gotHarnessID = req.GetAgentId()
	s.gotAgentConfig = req.GetStart().GetAgentConfig()
	s.gotInputs = inputs
	s.mu.Unlock()

	convID := req.GetConversationId()
	if s.FailFrame {
		return stream.Send(&proto.HarnessResponse{
			ConversationId: convID,
			Type: &proto.HarnessResponse_End{
				End: &proto.HarnessEnd{
					State: proto.State_STATE_FAILED,
					Error: &proto.Error{
						Code:        s.ErrCode,
						Description: s.ErrMessage,
					},
				},
			},
		})
	}

	steps := s.Outputs
	if steps == nil {
		for _, in := range inputs {
			steps = append(steps, AssistantStep("ack: "+in))
		}
	}
	if len(steps) > 0 {
		if err := stream.Send(&proto.HarnessResponse{
			ConversationId: convID,
			Type: &proto.HarnessResponse_Outputs{
				Outputs: &proto.HarnessOutputs{Steps: steps},
			},
		}); err != nil {
			return err
		}
	}
	return stream.Send(&proto.HarnessResponse{
		ConversationId: convID,
		Type:           &proto.HarnessResponse_End{End: &proto.HarnessEnd{State: proto.State_STATE_COMPLETED}},
	})
}

// Received returns a copy of the start frame the server received.
func (s *MockHarnessServer) Received() (convID, harnessID string, agentConfig []byte, inputs []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gotConvID, s.gotHarnessID, append([]byte(nil), s.gotAgentConfig...), append([]string(nil), s.gotInputs...)
}

// MockHandler records the steps and completion streamed during a turn.
type MockHandler struct {
	mu       sync.Mutex
	steps    []*proto.Step
	complete bool
}

var _ harness.Handler = (*MockHandler)(nil)

func (h *MockHandler) OnMessage(_ context.Context, _ string, step *proto.Step) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.steps = append(h.steps, step)
	return nil
}

func (h *MockHandler) OnComplete(_ context.Context, _ string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.complete = true
	return nil
}

func (h *MockHandler) IsDone() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.complete
}

// Collected returns a copy of the steps received via OnMessage.
func (h *MockHandler) Collected() []*proto.Step {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]*proto.Step(nil), h.steps...)
}

// Texts returns the text content of each received step, in order.
func (h *MockHandler) Texts() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []string
	for _, s := range h.steps {
		if contentStep := s.GetContent(); contentStep != nil {
			for _, part := range contentStep.Content {
				out = append(out, part.GetText().GetText())
			}
		}
	}
	return out
}

func UserStep(text string) *proto.Step {
	return &proto.Step{
		Type: &proto.Step_Content{
			Content: &proto.ContentStep{
				Role:    "user",
				Content: []*proto.Content{{Type: &proto.Content_Text{Text: &proto.TextContent{Text: text}}}},
			},
		},
	}
}

func AssistantStep(text string) *proto.Step {
	return &proto.Step{
		Type: &proto.Step_Content{
			Content: &proto.ContentStep{
				Role:    "assistant",
				Content: []*proto.Content{{Type: &proto.Content_Text{Text: &proto.TextContent{Text: text}}}},
			},
		},
	}
}

func ThoughtStep(summary string) *proto.Step {
	return &proto.Step{
		Type: &proto.Step_Thought{
			Thought: &proto.ThoughtStep{
				Summary: []*proto.Content{
					{Type: &proto.Content_Text{Text: &proto.TextContent{Text: summary}}},
				},
			},
		},
	}
}

// StartHarnessServer starts a HarnessService + health server (status SERVING)
// on a random local port and returns its address.
func StartHarnessServer(t *testing.T, srv *MockHarnessServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	proto.RegisterHarnessServiceServer(s, srv)
	hs := health.NewServer()
	hs.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(s, hs)
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)
	return lis.Addr().String()
}

// StartControlServer starts a mock Substrate Control server on a random local port.
func StartControlServer(t *testing.T, srv *MockControlServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	ateapipb.RegisterControlServer(s, srv)
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)
	return lis.Addr().String()
}
