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

// mockControlServer is an in-process ateapipb.ControlServer that records the
// actor lifecycle calls SubstrateHarness makes and lets tests steer the
// CreateActor/ResumeActor responses. Only the three RPCs SubstrateHarness uses
// are implemented; the rest come from the embedded Unimplemented server.
type MockControlServer struct {
	ateapipb.UnimplementedControlServer

	mu                sync.Mutex
	createCalls       []string
	resumeCalls       []string
	suspendCalls      []string
	deleteCalls       []string
	deleteWorkerCalls []string
	getCalls          []string
	tagCreateCalls    []string
	tagDeleteCalls    []string

	CreateErr        error  // returned from CreateActor when non-nil
	ResumeErr        error  // returned from ResumeActor when non-nil
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
	// success and moving the actor to SUSPENDED without LatestSnapshot
	// actually advancing.
	SuspendSucceedsWithoutAdvancing bool

	// actors and tags back the actor/tag RPCs below; only populated by
	// tests that call the SetXxx/SeedSnapshotTag helpers.
	actors map[string]*ateapipb.Actor
	tags   map[string]*ateapipb.ObjectRef
}

// SetCrashed marks name as ACTOR_STATE_CRASHED with the given
// LatestSnapshot, so a later ResumeActor call for it fails until something
// deletes and recreates it.
func (f *MockControlServer) SetCrashed(name string, snapshot *ateapipb.ObjectRef) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.actors == nil {
		f.actors = make(map[string]*ateapipb.Actor)
	}
	f.actors[name] = &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Name: name},
		Status: &ateapipb.ActorStatus{
			State:          ateapipb.ActorState_ACTOR_STATE_CRASHED,
			LatestSnapshot: snapshot,
		},
	}
}

// LatestSnapshot returns name's currently-tracked LatestSnapshot, or nil if
// it was never set.
func (f *MockControlServer) LatestSnapshot(name string) *ateapipb.ObjectRef {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.actors[name].GetStatus().GetLatestSnapshot()
}

// SetSuspending marks name as ACTOR_STATE_SUSPENDING with the given
// LatestSnapshot, so a later ResumeActor call for it fails until something
// re-invokes SuspendActor.
func (f *MockControlServer) SetSuspending(name string, snapshot *ateapipb.ObjectRef) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.actors == nil {
		f.actors = make(map[string]*ateapipb.Actor)
	}
	f.actors[name] = &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Name: name},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDING, LatestSnapshot: snapshot},
	}
}

// SetResuming marks name as ACTOR_STATE_RESUMING with the given
// LatestSnapshot, so a later ResumeActor call for it retries against
// Substrate's own re-entrant resume workflow instead of being declined.
func (f *MockControlServer) SetResuming(name string, snapshot *ateapipb.ObjectRef) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.actors == nil {
		f.actors = make(map[string]*ateapipb.Actor)
	}
	f.actors[name] = &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Name: name},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RESUMING, LatestSnapshot: snapshot},
	}
}

// SetPausing marks name as ACTOR_STATE_PAUSING, a state resumeActor's own
// switch doesn't specially recognize, so it reports a plain retryable
// error.
func (f *MockControlServer) SetPausing(name string, snapshot *ateapipb.ObjectRef) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.actors == nil {
		f.actors = make(map[string]*ateapipb.Actor)
	}
	f.actors[name] = &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Name: name},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_PAUSING, LatestSnapshot: snapshot},
	}
}

// SetRunning marks name as ACTOR_STATE_RUNNING with the given
// LatestSnapshot, standing in for an actor that already completed a prior
// turn/checkpoint.
func (f *MockControlServer) SetRunning(name string, snapshot *ateapipb.ObjectRef) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.actors == nil {
		f.actors = make(map[string]*ateapipb.Actor)
	}
	f.actors[name] = &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Name: name},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING, LatestSnapshot: snapshot},
	}
}

// SetDeleting marks name as ACTOR_STATE_DELETING with the given
// LatestSnapshot, so a later ResumeActor call for it fails until something
// re-invokes DeleteActor.
func (f *MockControlServer) SetDeleting(name string, snapshot *ateapipb.ObjectRef) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.actors == nil {
		f.actors = make(map[string]*ateapipb.Actor)
	}
	f.actors[name] = &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Name: name},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_DELETING, LatestSnapshot: snapshot},
	}
}

// SetDeletingWithStuckWorker marks name as ACTOR_STATE_DELETING like
// SetDeleting, but leaves it assigned to workerName; this mock's own
// DeleteActor then fails until DeleteWorker clears that assignment.
func (f *MockControlServer) SetDeletingWithStuckWorker(name string, snapshot *ateapipb.ObjectRef, workerName string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.actors == nil {
		f.actors = make(map[string]*ateapipb.Actor)
	}
	f.actors[name] = &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Name: name},
		Status: &ateapipb.ActorStatus{
			State:            ateapipb.ActorState_ACTOR_STATE_DELETING,
			LatestSnapshot:   snapshot,
			WorkerAssignment: &ateapipb.WorkerAssignment{Worker: &ateapipb.ObjectRef{Name: workerName}},
		},
	}
}

// SeedSnapshotTag pre-populates tagName as already existing, pointing at
// snapshot, so a test can exercise recovery finding a stale tag rather than
// an absent one.
func (f *MockControlServer) SeedSnapshotTag(tagName string, snapshot *ateapipb.ObjectRef) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.tags == nil {
		f.tags = make(map[string]*ateapipb.ObjectRef)
	}
	f.tags[tagName] = snapshot
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
	if _, exists := f.actors[name]; exists && req.GetActor().GetSourceSnapshotTag() == nil {
		return nil, status.Errorf(codes.AlreadyExists, "actor %s already exists", name)
	}
	actor := &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Name: name}}
	// A source_snapshot_tag seeds the new actor's own LatestSnapshot from
	// the tagged snapshot, like the real Control API's CreateActor.
	if tagRef := req.GetActor().GetSourceSnapshotTag(); tagRef != nil {
		snapshot, ok := f.tags[tagRef.GetName()]
		if !ok {
			return nil, status.Errorf(codes.NotFound, "snapshot tag %s not found", tagRef.GetName())
		}
		actor.Status = &ateapipb.ActorStatus{
			State:          ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
			LatestSnapshot: snapshot,
		}
	}
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
	state := f.actors[name].GetStatus().GetState()
	// Carried into the response below like the real Control API does.
	latestSnapshot := f.actors[name].GetStatus().GetLatestSnapshot()
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
	// Mirrors the real Control API: FailedPrecondition for CRASHED,
	// SUSPENDING, or DELETING.
	switch state {
	case ateapipb.ActorState_ACTOR_STATE_CRASHED:
		return nil, status.Errorf(codes.FailedPrecondition, "actor %s is CRASHED, want SUSPENDED or PAUSED", name)
	case ateapipb.ActorState_ACTOR_STATE_SUSPENDING:
		return nil, status.Errorf(codes.FailedPrecondition, "actor %s is SUSPENDING, want SUSPENDED or PAUSED", name)
	case ateapipb.ActorState_ACTOR_STATE_DELETING:
		return nil, status.Errorf(codes.FailedPrecondition, "actor %s is DELETING, want SUSPENDED or PAUSED", name)
	}
	if f.ResumeNilActor {
		return &ateapipb.ResumeActorResponse{}, nil
	}
	respActor := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Name: name},
		Status: &ateapipb.ActorStatus{
			State:            ateapipb.ActorState_ACTOR_STATE_RUNNING,
			WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPodIp: resumeIP, Worker: worker},
			LatestSnapshot:   latestSnapshot,
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

// DeleteWorker deregisters workerName, clearing the WorkerAssignment of
// whatever actor is currently assigned to it.
func (f *MockControlServer) DeleteWorker(_ context.Context, req *ateapipb.DeleteWorkerRequest) (*ateapipb.Worker, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	workerName := req.GetWorker().GetName()
	f.deleteWorkerCalls = append(f.deleteWorkerCalls, workerName)
	for _, actor := range f.actors {
		if actor.GetStatus().GetWorkerAssignment().GetWorker().GetName() == workerName {
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
	// A genuinely successful checkpoint always mints a fresh LatestSnapshot,
	// like the real Control API does. SuspendSucceedsWithoutAdvancing
	// simulates a checkpoint that moves the actor to SUSPENDED without one.
	snapshot := &ateapipb.ObjectRef{Name: fmt.Sprintf("mock-snapshot-%d", len(f.suspendCalls))}
	if actor, ok := f.actors[name]; ok {
		if f.SuspendSucceedsWithoutAdvancing {
			actor.Status = &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED, LatestSnapshot: actor.GetStatus().GetLatestSnapshot()}
			snapshot = actor.Status.LatestSnapshot
		} else {
			actor.Status = &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED, LatestSnapshot: snapshot}
		}
	}
	f.mu.Unlock()
	return &ateapipb.SuspendActorResponse{
		Actor: &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Name: name}, Status: &ateapipb.ActorStatus{LatestSnapshot: snapshot}},
	}, nil
}

func (f *MockControlServer) CreateActorSnapshotTag(_ context.Context, req *ateapipb.CreateActorSnapshotTagRequest) (*ateapipb.ActorSnapshotTag, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	tag := req.GetActorSnapshotTag()
	name := tag.GetMetadata().GetName()
	f.tagCreateCalls = append(f.tagCreateCalls, name)
	// A pure create, never an upsert: a tag that already exists is
	// AlreadyExists.
	if f.tags == nil {
		f.tags = make(map[string]*ateapipb.ObjectRef)
	}
	if _, exists := f.tags[name]; exists {
		return nil, status.Errorf(codes.AlreadyExists, "ActorSnapshot tag %s already exists", name)
	}
	f.tags[name] = tag.GetSnapshot()
	return tag, nil
}

func (f *MockControlServer) GetActorSnapshotTag(_ context.Context, req *ateapipb.GetActorSnapshotTagRequest) (*ateapipb.ActorSnapshotTag, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := req.GetActorSnapshotTag().GetName()
	snapshot, ok := f.tags[name]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "ActorSnapshot tag %s not found", name)
	}
	return &ateapipb.ActorSnapshotTag{
		Metadata: &ateapipb.ResourceMetadata{Name: name},
		Snapshot: snapshot,
	}, nil
}

func (f *MockControlServer) DeleteActorSnapshotTag(_ context.Context, req *ateapipb.DeleteActorSnapshotTagRequest) (*ateapipb.ActorSnapshotTag, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := req.GetActorSnapshotTag().GetName()
	f.tagDeleteCalls = append(f.tagDeleteCalls, name)
	delete(f.tags, name)
	return &ateapipb.ActorSnapshotTag{}, nil
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
func (f *MockControlServer) CrashRecoveryCalls() (get, del, tagCreate, tagDelete []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.getCalls...),
		append([]string(nil), f.deleteCalls...),
		append([]string(nil), f.tagCreateCalls...),
		append([]string(nil), f.tagDeleteCalls...)
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
