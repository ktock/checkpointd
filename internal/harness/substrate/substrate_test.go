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

package substrate

import (
	"bytes"
	"context"
	"net"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/ktock/checkpointd/internal/ate"
	"github.com/ktock/checkpointd/internal/harness/harnesstest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

var substrateAgentConfig = []byte(`{"model":"gemini-2.5-pro"}`)

func TestNew_SuspendActorTimeoutDefaulting(t *testing.T) {
	h, err := New("harness-id", "", "", "", 0, 0, ControlAPIOptions{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := h.resolvedSuspendActorTimeout(); got != DefaultSuspendActorTimeout {
		t.Fatalf("resolvedSuspendActorTimeout() = %v, want %v (zero input should fall back to the default)", got, DefaultSuspendActorTimeout)
	}

	const custom = 3 * time.Minute
	h, err = New("harness-id", "", "", "", 0, custom, ControlAPIOptions{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := h.resolvedSuspendActorTimeout(); got != custom {
		t.Fatalf("resolvedSuspendActorTimeout() = %v, want %v (explicit override)", got, custom)
	}
}

// startHealthTestServer starts a gRPC server on a random local port. If hs is
// non-nil the standard health service is registered. Returns the listen address.
func startHealthTestServer(t *testing.T, hs *health.Server) string {
	t.Helper()
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	if hs != nil {
		grpc_health_v1.RegisterHealthServer(s, hs)
	}
	go func() {
		_ = s.Serve(lis)
	}()
	t.Cleanup(s.Stop)
	return lis.Addr().String()
}

func dialTestConn(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial %s: %v", addr, err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func TestWaitForHealthy_Serving(t *testing.T) {
	hs := health.NewServer()
	hs.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	conn := dialTestConn(t, startHealthTestServer(t, hs))

	if err := waitForHealthy(context.Background(), conn, 5*time.Second); err != nil {
		t.Fatalf("expected healthy, got %v", err)
	}
}

func TestWaitForHealthy_UnimplementedProceeds(t *testing.T) {
	// Server is up but does not register the health service.
	conn := dialTestConn(t, startHealthTestServer(t, nil))

	if err := waitForHealthy(context.Background(), conn, 5*time.Second); err != nil {
		t.Fatalf("expected to proceed when health is unimplemented, got %v", err)
	}
}

func TestWaitForHealthy_TimesOut(t *testing.T) {
	hs := health.NewServer()
	hs.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	conn := dialTestConn(t, startHealthTestServer(t, hs))

	if err := waitForHealthy(context.Background(), conn, 500*time.Millisecond); err == nil {
		t.Fatal("expected timeout error while NOT_SERVING, got nil")
	}
}

func TestWaitForHealthy_StatusChange(t *testing.T) {
	hs := health.NewServer()
	hs.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	conn := dialTestConn(t, startHealthTestServer(t, hs))

	go func() {
		time.Sleep(150 * time.Millisecond)
		hs.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	}()

	if err := waitForHealthy(context.Background(), conn, 5*time.Second); err != nil {
		t.Fatalf("expected healthy after status flip, got %v", err)
	}
}

func TestWaitForHealthy_ServerDown(t *testing.T) {
	// Reserve a port then release it so nothing is listening there.
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	addr := lis.Addr().String()
	lis.Close()
	conn := dialTestConn(t, addr)

	if err := waitForHealthy(context.Background(), conn, 500*time.Millisecond); err == nil {
		t.Fatal("expected timeout error when server is down, got nil")
	}
}

// newTestSubstrateHarness builds a SubstrateHarness wired to the mock control
// server and the mock harness server. It constructs the struct directly (rather
// than via NewSubstrateHarness) so the control client can use insecure
// credentials instead of the TLS that NewSubstrateHarness hard-codes.
func newTestSubstrateHarness(t *testing.T, ctrlAddr, harnessAddr string) *SubstrateHarness {
	t.Helper()
	_, portStr, err := net.SplitHostPort(harnessAddr)
	if err != nil {
		t.Fatalf("bad harness addr %q: %v", harnessAddr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("bad harness port %q: %v", portStr, err)
	}
	client, err := ate.NewClient("ax", "antigravity-template", ctrlAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create ate client: %v", err)
	}
	return &SubstrateHarness{
		harnessID: "antigravity",
		ateClient: client,
		port:      port,
		dialOpts:  []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	}
}

// Test full SubstrateHarness Start -> Run -> Close flow against the shared
// in-process mocks (see mocks_test.go).
// They lock in the wiring that a substrate bump or an ax-side change could silently
// break: create/resume idempotency, worker-IP extraction, the health gate, the
// Connect streaming protocol, and suspend-on-close.
func TestSubstrateHarness_StartAndRun(t *testing.T) {
	ctrl := &harnesstest.MockControlServer{ResumeIP: "127.0.0.1"}
	srv := &harnesstest.MockHarnessServer{}
	h := newTestSubstrateHarness(t, harnesstest.StartControlServer(t, ctrl), harnesstest.StartHarnessServer(t, srv))

	ctx := context.Background()
	exec, err := h.Start(ctx, "conv-1", substrateAgentConfig)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := exec.Queue(ctx, harnesstest.UserStep("hi")); err != nil {
		t.Fatalf("Queue: %v", err)
	}
	handler := &harnesstest.MockHandler{}
	if err := exec.Run(ctx, handler); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// The harness server received the start frame with the right identifiers.
	convID, harnessID, agentConfig, inputs := srv.Received()
	if convID != "conv-1" || harnessID != "antigravity" {
		t.Errorf("server got convID=%q harnessID=%q, want conv-1/antigravity", convID, harnessID)
	}
	if !bytes.Equal(agentConfig, substrateAgentConfig) {
		t.Errorf("server got agentConfig=%q, want %q", agentConfig, substrateAgentConfig)
	}
	if !slices.Equal(inputs, []string{"hi"}) {
		t.Errorf("server got inputs=%v, want [hi]", inputs)
	}

	// The handler streamed the output and completed.
	if !handler.IsDone() {
		t.Error("handler did not complete")
	}
	if got := handler.Texts(); !slices.Equal(got, []string{"ack: hi"}) {
		t.Errorf("handler messages=%v, want [ack: hi]", got)
	}

	// CreateActor then ResumeActor ran for the conversation; no suspend yet.
	create, resume, suspend := ctrl.Calls()
	want := []string{"conv-1"}
	if !slices.Equal(create, want) || !slices.Equal(resume, want) {
		t.Errorf("create=%v resume=%v, want %v each", create, resume, want)
	}
	if len(suspend) != 0 {
		t.Errorf("suspend called before Checkpoint: %v", suspend)
	}

	// Checkpoint suspends the actor.
	if err := exec.Checkpoint(ctx); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if _, _, suspend = ctrl.Calls(); !slices.Equal(suspend, want) {
		t.Errorf("suspend=%v, want %v", suspend, want)
	}

	// Close does not suspend again; it only releases the connection.
	if err := exec.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, _, suspend = ctrl.Calls(); !slices.Equal(suspend, want) {
		t.Errorf("suspend after Close=%v, want unchanged %v", suspend, want)
	}
}

// TestSubstrateHarness_CheckpointDetectsSilentNonAdvance is the regression
// test for a Substrate race: a worker pod disappearing can clear an
// in-flight SuspendActor call's own checkpoint bookkeeping between the
// checkpoint RPC succeeding and the finalize step re-reading it, so
// SuspendActor reports success but LatestSnapshot never advances. Checkpoint
// must detect the unchanged LatestSnapshot and fail the turn rather than let
// the caller commit stale state as durable.
func TestSubstrateHarness_CheckpointDetectsSilentNonAdvance(t *testing.T) {
	ctrl := &harnesstest.MockControlServer{ResumeIP: "127.0.0.1", SuspendSucceedsWithoutAdvancing: true}
	h := newTestSubstrateHarness(t, harnesstest.StartControlServer(t, ctrl), harnesstest.StartHarnessServer(t, &harnesstest.MockHarnessServer{}))

	ctx := context.Background()
	exec, err := h.Start(ctx, "conv-1", substrateAgentConfig)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = exec.Close(ctx) })
	if err := exec.Queue(ctx, harnesstest.UserStep("hi")); err != nil {
		t.Fatalf("Queue: %v", err)
	}
	handler := &harnesstest.MockHandler{}
	if err := exec.Run(ctx, handler); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if err := exec.Checkpoint(ctx); err == nil {
		t.Fatal("Checkpoint: got nil error, want one -- SuspendActor reported success but LatestSnapshot never advanced, which must fail this turn rather than let the caller commit it as durable")
	}
}

func TestSubstrateHarness_CreateAlreadyExistsTolerated(t *testing.T) {
	ctrl := &harnesstest.MockControlServer{
		ResumeIP:  "127.0.0.1",
		CreateErr: status.Error(codes.AlreadyExists, "exists"),
	}
	h := newTestSubstrateHarness(t, harnesstest.StartControlServer(t, ctrl), harnesstest.StartHarnessServer(t, &harnesstest.MockHarnessServer{}))

	ctx := context.Background()
	exec, err := h.Start(ctx, "conv-1", substrateAgentConfig)
	if err != nil {
		t.Fatalf("Start should tolerate AlreadyExists: %v", err)
	}
	t.Cleanup(func() { _ = exec.Close(ctx) })

	if err := exec.Queue(ctx, harnesstest.UserStep("hi")); err != nil {
		t.Fatalf("Queue: %v", err)
	}
	handler := &harnesstest.MockHandler{}
	if err := exec.Run(ctx, handler); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !handler.IsDone() {
		t.Error("handler did not complete")
	}
	if _, resume, _ := ctrl.Calls(); !slices.Equal(resume, []string{"conv-1"}) {
		t.Errorf("resume=%v, want [conv-1]", resume)
	}
}

func TestSubstrateHarness_ResumeNoWorkerIP(t *testing.T) {
	ctrl := &harnesstest.MockControlServer{ResumeIP: ""} // empty AteomPodIp
	h := newTestSubstrateHarness(t, harnesstest.StartControlServer(t, ctrl), harnesstest.StartHarnessServer(t, &harnesstest.MockHarnessServer{}))

	_, err := h.Start(context.Background(), "conv-1", substrateAgentConfig)
	if err == nil {
		t.Fatal("expected error for empty worker IP, got nil")
	}
	if !strings.Contains(err.Error(), "no active worker IP") {
		t.Errorf("error = %v, want it to mention 'no active worker IP'", err)
	}
}

func TestSubstrateHarness_ResumeNilActor(t *testing.T) {
	ctrl := &harnesstest.MockControlServer{ResumeNilActor: true}
	h := newTestSubstrateHarness(t, harnesstest.StartControlServer(t, ctrl), harnesstest.StartHarnessServer(t, &harnesstest.MockHarnessServer{}))

	_, err := h.Start(context.Background(), "conv-1", substrateAgentConfig)
	if err == nil {
		t.Fatal("expected error for nil actor, got nil")
	}
	if !strings.Contains(err.Error(), "nil actor") {
		t.Errorf("error = %v, want it to mention 'nil actor'", err)
	}
}

// TestSubstrateHarness_RecoversFromCrashedActor confirms Start recovers an
// ACTOR_STATE_CRASHED actor: it notices ResumeActor's FailedPrecondition,
// restores the actor from its last snapshot via a tag-and-recreate cycle,
// cleans up the recovery tag, and retries the resume so a normal turn can run.
func TestSubstrateHarness_RecoversFromCrashedActor(t *testing.T) {
	ctrl := &harnesstest.MockControlServer{ResumeIP: "127.0.0.1"}
	snapshot := &ateapipb.ObjectRef{Atespace: "ax", Name: "snap-before-crash"}
	ctrl.SetCrashed("conv-1", snapshot)
	h := newTestSubstrateHarness(t, harnesstest.StartControlServer(t, ctrl), harnesstest.StartHarnessServer(t, &harnesstest.MockHarnessServer{}))

	ctx := context.Background()
	exec, err := h.Start(ctx, "conv-1", substrateAgentConfig)
	if err != nil {
		t.Fatalf("Start should recover from CRASHED and succeed: %v", err)
	}
	t.Cleanup(func() { _ = exec.Close(ctx) })

	get, del, tagCreate, tagDelete := ctrl.CrashRecoveryCalls()
	if !slices.Equal(get, []string{"conv-1"}) {
		t.Errorf("GetActor calls = %v, want [conv-1]", get)
	}
	if !slices.Equal(del, []string{"conv-1"}) {
		t.Errorf("DeleteActor calls = %v, want [conv-1] (the crashed actor)", del)
	}
	if len(tagCreate) != 1 {
		t.Fatalf("CreateActorSnapshotTag calls = %v, want exactly 1", tagCreate)
	}
	if len(tagDelete) != 1 {
		t.Errorf("DeleteActorSnapshotTag calls = %v, want exactly 1 (best-effort cleanup)", tagDelete)
	}
	if tagCreate[0] != tagDelete[0] {
		t.Errorf("created tag %q but cleaned up a different one %q", tagCreate[0], tagDelete[0])
	}

	// The recreated actor carries forward the crashed one's own last
	// snapshot, not a fresh/unrelated one.
	if got := ctrl.LatestSnapshot("conv-1"); got.GetName() != snapshot.GetName() || got.GetAtespace() != snapshot.GetAtespace() {
		t.Errorf("recreated actor's LatestSnapshot = %v, want %v", got, snapshot)
	}

	// And the recovered actor runs a normal turn afterward.
	if err := exec.Queue(ctx, harnesstest.UserStep("hi")); err != nil {
		t.Fatalf("Queue: %v", err)
	}
	handler := &harnesstest.MockHandler{}
	if err := exec.Run(ctx, handler); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !handler.IsDone() {
		t.Error("handler did not complete after recovery")
	}
}

// TestSubstrateHarness_RecoversFromCrashedActor_NoPriorSnapshot confirms
// Start recovers a CRASHED actor with no LatestSnapshot at all (it crashed
// before ever completing a checkpoint) by discarding it and letting it be
// recreated blank -- safe since the caller's retry redelivers the
// conversation's bootstrap input from scratch anyway.
func TestSubstrateHarness_RecoversFromCrashedActor_NoPriorSnapshot(t *testing.T) {
	ctrl := &harnesstest.MockControlServer{ResumeIP: "127.0.0.1"}
	ctrl.SetCrashed("conv-1", nil) // crashed before ever completing a checkpoint
	h := newTestSubstrateHarness(t, harnesstest.StartControlServer(t, ctrl), harnesstest.StartHarnessServer(t, &harnesstest.MockHarnessServer{}))

	ctx := context.Background()
	exec, err := h.Start(ctx, "conv-1", substrateAgentConfig)
	if err != nil {
		t.Fatalf("Start should discard the actor and recreate it blank, not decline forever: %v", err)
	}
	t.Cleanup(func() { _ = exec.Close(ctx) })

	get, del, tagCreate, tagDelete := ctrl.CrashRecoveryCalls()
	if !slices.Equal(get, []string{"conv-1"}) {
		t.Errorf("GetActor calls = %v, want [conv-1]", get)
	}
	if !slices.Equal(del, []string{"conv-1"}) {
		t.Errorf("DeleteActor calls = %v, want [conv-1] (discarding the snapshot-less crashed actor)", del)
	}
	// No snapshot means nothing to tag -- the usual tag-and-restore
	// machinery must not run at all here.
	if len(tagCreate) != 0 || len(tagDelete) != 0 {
		t.Errorf("tag machinery ran for a no-snapshot crash: tagCreates=%v tagDeletes=%v, want none", tagCreate, tagDelete)
	}

	// The recreated actor runs a normal turn afterward, same as any other
	// brand-new actor.
	if err := exec.Queue(ctx, harnesstest.UserStep("hi")); err != nil {
		t.Fatalf("Queue: %v", err)
	}
	handler := &harnesstest.MockHandler{}
	if err := exec.Run(ctx, handler); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !handler.IsDone() {
		t.Error("handler did not complete after recovery")
	}
}

// TestSubstrateHarness_RecoversFromStuckSuspending confirms Start recovers
// an actor stuck ACTOR_STATE_SUSPENDING (Checkpoint's own SuspendActor
// timeout fired while a genuine checkpoint was still running server-side) by
// re-invoking SuspendActor, which Substrate's workflow is safe to re-enter.
func TestSubstrateHarness_RecoversFromStuckSuspending(t *testing.T) {
	ctrl := &harnesstest.MockControlServer{ResumeIP: "127.0.0.1"}
	ctrl.SetSuspending("conv-1", nil) // standing in for Checkpoint's own timeout firing mid-checkpoint, before this actor's own first completed checkpoint
	h := newTestSubstrateHarness(t, harnesstest.StartControlServer(t, ctrl), harnesstest.StartHarnessServer(t, &harnesstest.MockHarnessServer{}))

	ctx := context.Background()
	exec, err := h.Start(ctx, "conv-1", substrateAgentConfig)
	if err != nil {
		t.Fatalf("Start should re-enter the stuck suspend and then resume successfully: %v", err)
	}
	t.Cleanup(func() { _ = exec.Close(ctx) })

	_, resumeCalls, suspendCalls := ctrl.Calls()
	if !slices.Equal(suspendCalls, []string{"conv-1"}) {
		t.Errorf("SuspendActor calls = %v, want [conv-1] (re-entering the stuck suspend)", suspendCalls)
	}
	if len(resumeCalls) != 1 {
		t.Errorf("ResumeActor calls = %v, want 1 (resumeActor's own GetActor-first switch only ever calls ResumeActor once it already knows the actor is resumable, never speculatively before knowing that)", resumeCalls)
	}

	// The now-resumed actor runs a normal turn afterward.
	if err := exec.Queue(ctx, harnesstest.UserStep("hi")); err != nil {
		t.Fatalf("Queue: %v", err)
	}
	handler := &harnesstest.MockHandler{}
	if err := exec.Run(ctx, handler); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !handler.IsDone() {
		t.Error("handler did not complete after recovery")
	}
}

// TestSubstrateHarness_RecoversFromStuckSuspending_ResuspendFails confirms
// resumeActor falls back to discardAndRecoverFromSnapshot, using the
// actor's own last snapshot, when re-entering a stuck SuspendActor call
// itself errors (e.g. the sandbox stopped mid-checkpoint and can never
// resume, and Substrate never transitions it to CRASHED either) -- the same
// recovery path as a genuine crash.
func TestSubstrateHarness_RecoversFromStuckSuspending_ResuspendFails(t *testing.T) {
	ctrl := &harnesstest.MockControlServer{ResumeIP: "127.0.0.1"}
	snapshot := &ateapipb.ObjectRef{Atespace: "ax", Name: "snap-before-stuck-suspend"}
	ctrl.SetSuspending("conv-1", snapshot)
	ctrl.SuspendErr = status.Error(codes.Unknown, "while running `runsc checkpoint`: exit status 128")
	h := newTestSubstrateHarness(t, harnesstest.StartControlServer(t, ctrl), harnesstest.StartHarnessServer(t, &harnesstest.MockHarnessServer{}))

	ctx := context.Background()
	exec, err := h.Start(ctx, "conv-1", substrateAgentConfig)
	if err != nil {
		t.Fatalf("Start should fall back to recovering from the last snapshot when re-suspending itself fails: %v", err)
	}
	t.Cleanup(func() { _ = exec.Close(ctx) })

	_, _, suspendCalls := ctrl.Calls()
	if !slices.Equal(suspendCalls, []string{"conv-1"}) {
		t.Errorf("SuspendActor calls = %v, want [conv-1] (attempting to re-enter the stuck suspend once before falling back)", suspendCalls)
	}

	get, del, tagCreate, tagDelete := ctrl.CrashRecoveryCalls()
	if !slices.Equal(get, []string{"conv-1"}) {
		t.Errorf("GetActor calls = %v, want [conv-1]", get)
	}
	if !slices.Equal(del, []string{"conv-1"}) {
		t.Errorf("DeleteActor calls = %v, want [conv-1] (discarding the un-resuspendable actor)", del)
	}
	if len(tagCreate) != 1 || len(tagDelete) != 1 {
		t.Errorf("tag create/delete calls = %v/%v, want exactly 1 each (the usual tag-and-restore cycle, same as a genuine crash)", tagCreate, tagDelete)
	}

	// The recreated actor carries forward the stuck one's own last
	// completed checkpoint, not a fresh/unrelated one.
	if got := ctrl.LatestSnapshot("conv-1"); got.GetName() != snapshot.GetName() || got.GetAtespace() != snapshot.GetAtespace() {
		t.Errorf("recreated actor's LatestSnapshot = %v, want %v", got, snapshot)
	}

	// The recovered actor runs a normal turn afterward.
	if err := exec.Queue(ctx, harnesstest.UserStep("hi")); err != nil {
		t.Fatalf("Queue: %v", err)
	}
	handler := &harnesstest.MockHandler{}
	if err := exec.Run(ctx, handler); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !handler.IsDone() {
		t.Error("handler did not complete after recovery")
	}
}

// TestSubstrateHarness_RecoversFromStuckSuspending_AbortedNotDiscarded
// confirms a re-entered SuspendActor call failing with codes.Aborted is
// reported as a plain retryable error, never discarded: Aborted means the
// original suspend attempt is still genuinely in flight (holding the
// actor's lease) or just landed a concurrent write, so recovering from a
// snapshot here would roll back state the original attempt may still commit.
func TestSubstrateHarness_RecoversFromStuckSuspending_AbortedNotDiscarded(t *testing.T) {
	ctrl := &harnesstest.MockControlServer{ResumeIP: "127.0.0.1"}
	snapshot := &ateapipb.ObjectRef{Atespace: "ax", Name: "snap-before-still-in-flight-suspend"}
	ctrl.SetSuspending("conv-1", snapshot)
	ctrl.SuspendErr = status.Error(codes.Aborted, "another operation is in progress for this actor")
	h := newTestSubstrateHarness(t, harnesstest.StartControlServer(t, ctrl), harnesstest.StartHarnessServer(t, &harnesstest.MockHarnessServer{}))

	ctx := context.Background()
	if _, err := h.Start(ctx, "conv-1", substrateAgentConfig); err == nil {
		t.Fatal("Start should report the still-in-flight original suspend as a retryable error, not succeed")
	}

	_, _, suspendCalls := ctrl.Calls()
	if !slices.Equal(suspendCalls, []string{"conv-1"}) {
		t.Errorf("SuspendActor calls = %v, want [conv-1] (attempting to re-enter once before reporting it as still in flight)", suspendCalls)
	}

	_, del, tagCreate, tagDelete := ctrl.CrashRecoveryCalls()
	if len(del) != 0 || len(tagCreate) != 0 || len(tagDelete) != 0 {
		t.Errorf("discardAndRecoverFromSnapshot must never run for a lease/version conflict: delete=%v tagCreate=%v tagDelete=%v", del, tagCreate, tagDelete)
	}

	// Nothing was discarded: the actor's own LatestSnapshot is untouched.
	if got := ctrl.LatestSnapshot("conv-1"); got.GetName() != snapshot.GetName() || got.GetAtespace() != snapshot.GetAtespace() {
		t.Errorf("actor's LatestSnapshot = %v, want unchanged %v", got, snapshot)
	}
}

// TestSubstrateHarness_RecoversFromStuckDeleting confirms Start recovers an
// actor stuck ACTOR_STATE_DELETING (an earlier discardAndRecoverFromSnapshot
// attempt's own DeleteActor call failed partway through) by simply
// re-entering discardAndRecoverFromSnapshot, since DeleteActor's own
// workflow is idempotent.
func TestSubstrateHarness_RecoversFromStuckDeleting(t *testing.T) {
	ctrl := &harnesstest.MockControlServer{ResumeIP: "127.0.0.1"}
	snapshot := &ateapipb.ObjectRef{Atespace: "ax", Name: "snap-before-stuck-delete"}
	ctrl.SetDeleting("conv-1", snapshot)
	h := newTestSubstrateHarness(t, harnesstest.StartControlServer(t, ctrl), harnesstest.StartHarnessServer(t, &harnesstest.MockHarnessServer{}))

	ctx := context.Background()
	exec, err := h.Start(ctx, "conv-1", substrateAgentConfig)
	if err != nil {
		t.Fatalf("Start should re-enter the stuck delete and then resume successfully: %v", err)
	}
	t.Cleanup(func() { _ = exec.Close(ctx) })

	get, del, tagCreate, tagDelete := ctrl.CrashRecoveryCalls()
	if !slices.Equal(get, []string{"conv-1"}) {
		t.Errorf("GetActor calls = %v, want [conv-1]", get)
	}
	if !slices.Equal(del, []string{"conv-1"}) {
		t.Errorf("DeleteActor calls = %v, want [conv-1] (re-entering the stuck delete)", del)
	}
	if len(tagCreate) != 1 || len(tagDelete) != 1 {
		t.Errorf("tag create/delete calls = %v/%v, want exactly 1 each (the usual tag-and-restore cycle, same as a genuine crash)", tagCreate, tagDelete)
	}

	// The recreated actor carries forward the stuck one's own last
	// completed checkpoint, not a fresh/unrelated one.
	if got := ctrl.LatestSnapshot("conv-1"); got.GetName() != snapshot.GetName() || got.GetAtespace() != snapshot.GetAtespace() {
		t.Errorf("recreated actor's LatestSnapshot = %v, want %v", got, snapshot)
	}

	// The recovered actor runs a normal turn afterward.
	if err := exec.Queue(ctx, harnesstest.UserStep("hi")); err != nil {
		t.Fatalf("Queue: %v", err)
	}
	handler := &harnesstest.MockHandler{}
	if err := exec.Run(ctx, handler); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !handler.IsDone() {
		t.Error("handler did not complete after recovery")
	}
}

// TestSubstrateHarness_RecoversFromStuckDeleting_WorkerNeverReleases
// confirms resumeActor releases a still-assigned worker directly
// (DeleteWorker) before re-entering DeleteActor, for the case where
// DeleteActor's own atelet-terminate step can never succeed because the
// worker itself is wedged and unreachable.
func TestSubstrateHarness_RecoversFromStuckDeleting_WorkerNeverReleases(t *testing.T) {
	ctrl := &harnesstest.MockControlServer{ResumeIP: "127.0.0.1"}
	snapshot := &ateapipb.ObjectRef{Atespace: "ax", Name: "snap-before-stuck-delete"}
	ctrl.SetDeletingWithStuckWorker("conv-1", snapshot, "worker-with-wedged-sandbox")
	h := newTestSubstrateHarness(t, harnesstest.StartControlServer(t, ctrl), harnesstest.StartHarnessServer(t, &harnesstest.MockHarnessServer{}))

	ctx := context.Background()
	exec, err := h.Start(ctx, "conv-1", substrateAgentConfig)
	if err != nil {
		t.Fatalf("Start should release the stuck worker directly and then recover successfully: %v", err)
	}
	t.Cleanup(func() { _ = exec.Close(ctx) })

	if got := ctrl.DeleteWorkerCalls(); !slices.Equal(got, []string{"worker-with-wedged-sandbox"}) {
		t.Errorf("DeleteWorker calls = %v, want [worker-with-wedged-sandbox] (releasing the actor from its stuck worker directly, bypassing that worker's own atelet)", got)
	}

	get, del, tagCreate, tagDelete := ctrl.CrashRecoveryCalls()
	if !slices.Equal(get, []string{"conv-1"}) {
		t.Errorf("GetActor calls = %v, want [conv-1]", get)
	}
	if !slices.Equal(del, []string{"conv-1"}) {
		t.Errorf("DeleteActor calls = %v, want [conv-1] (succeeding once the worker assignment was cleared)", del)
	}
	if len(tagCreate) != 1 || len(tagDelete) != 1 {
		t.Errorf("tag create/delete calls = %v/%v, want exactly 1 each (the usual tag-and-restore cycle, same as a genuine crash)", tagCreate, tagDelete)
	}

	if got := ctrl.LatestSnapshot("conv-1"); got.GetName() != snapshot.GetName() || got.GetAtespace() != snapshot.GetAtespace() {
		t.Errorf("recreated actor's LatestSnapshot = %v, want %v", got, snapshot)
	}

	if err := exec.Queue(ctx, harnesstest.UserStep("hi")); err != nil {
		t.Fatalf("Queue: %v", err)
	}
	handler := &harnesstest.MockHandler{}
	if err := exec.Run(ctx, handler); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !handler.IsDone() {
		t.Error("handler did not complete after recovery")
	}
}

// TestSubstrateHarness_RecoversFromUnreachableRunningWorker confirms Start
// recovers an actor Substrate still reports ACTOR_STATE_RUNNING when its
// worker is actually unreachable (e.g. the guest process panicked inside a
// sandbox whose host pod never restarts) -- a gap plain ResumeActor retries
// can never fix, since ResumeActor is a no-op on an already-RUNNING actor.
// Start itself only force-releases the dead worker (DeleteWorker has no
// precondition on the actor's own state and transitions it straight to
// CRASHED) and reports this attempt as failed; recovery from the actor's
// own last snapshot happens on the *next* Start call (standing in for the
// relay's own backoff retry), via resumeActor's ordinary CRASHED case.
func TestSubstrateHarness_RecoversFromUnreachableRunningWorker(t *testing.T) {
	ctrl := &harnesstest.MockControlServer{
		// 127.0.0.2 is unreachable: the mock harness server only binds
		// 127.0.0.1, standing in for a guest process that died inside its
		// still-alive sandbox/pod.
		ResumeIP:             "127.0.0.2",
		ResumeWorkerName:     "worker-with-dead-guest",
		PostRecoveryResumeIP: "127.0.0.1",
	}
	snapshot := &ateapipb.ObjectRef{Atespace: "ax", Name: "snap-before-worker-died"}
	ctrl.SetRunning("conv-1", snapshot)
	h := newTestSubstrateHarness(t, harnesstest.StartControlServer(t, ctrl), harnesstest.StartHarnessServer(t, &harnesstest.MockHarnessServer{}))
	// Shrinks the health-check budget well below its 60s default so the
	// first, permanently-doomed health check gives up quickly.
	h.healthCheckTimeout = 500 * time.Millisecond

	ctx := context.Background()

	// Attempt 1: discovers the worker is unreachable, force-releases it,
	// and reports failure -- an outer retry loop (checkpointd's own relay
	// backoff, not this function) is what drives the second attempt below.
	if _, err := h.Start(ctx, "conv-1", substrateAgentConfig); err == nil {
		t.Fatal("Start (attempt 1) should report the unreachable worker as failed, not silently succeed")
	}
	if got := ctrl.DeleteWorkerCalls(); !slices.Equal(got, []string{"worker-with-dead-guest"}) {
		t.Errorf("DeleteWorker calls = %v, want [worker-with-dead-guest] (force-releasing the actor from its unreachable worker)", got)
	}

	// Attempt 2 (standing in for the relay's own retry): the actor is now
	// CRASHED (per DeleteWorker's own real-Substrate semantics), so
	// resumeActor's ordinary CRASHED case recovers it from its last
	// snapshot.
	exec, err := h.Start(ctx, "conv-1", substrateAgentConfig)
	if err != nil {
		t.Fatalf("Start (attempt 2) should recover the now-CRASHED actor from its last snapshot and succeed: %v", err)
	}
	t.Cleanup(func() { _ = exec.Close(context.Background()) })

	_, del, tagCreate, tagDelete := ctrl.CrashRecoveryCalls()
	if !slices.Equal(del, []string{"conv-1"}) {
		t.Errorf("DeleteActor calls = %v, want [conv-1] (re-creating the actor after force-releasing its dead worker)", del)
	}
	if len(tagCreate) != 1 || len(tagDelete) != 1 {
		t.Errorf("tag create/delete calls = %v/%v, want exactly 1 each (the usual tag-and-restore cycle, same as a genuine crash)", tagCreate, tagDelete)
	}

	if got := ctrl.LatestSnapshot("conv-1"); got.GetName() != snapshot.GetName() || got.GetAtespace() != snapshot.GetAtespace() {
		t.Errorf("recreated actor's LatestSnapshot = %v, want %v", got, snapshot)
	}

	if err := exec.Queue(ctx, harnesstest.UserStep("hi")); err != nil {
		t.Fatalf("Queue: %v", err)
	}
	handler := &harnesstest.MockHandler{}
	if err := exec.Run(ctx, handler); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !handler.IsDone() {
		t.Error("handler did not complete after recovery")
	}
}

// TestSubstrateHarness_RecoversFromCrashedActor_StaleTagFromEarlierCrash
// confirms ensureSnapshotTag detects and discards a stale tag left by an
// earlier, already-resolved crash of the same actor (crashRecoveryTagName is
// deterministic per conversationID, not per crash), re-tagging with the
// current crash's own snapshot instead of silently recovering from the
// wrong one.
func TestSubstrateHarness_RecoversFromCrashedActor_StaleTagFromEarlierCrash(t *testing.T) {
	ctrl := &harnesstest.MockControlServer{ResumeIP: "127.0.0.1"}
	staleSnapshot := &ateapipb.ObjectRef{Atespace: "ax", Name: "snap-from-earlier-crash"}
	currentSnapshot := &ateapipb.ObjectRef{Atespace: "ax", Name: "snap-from-this-crash"}

	tagName := crashRecoveryTagName("conv-1")
	ctrl.SeedSnapshotTag(tagName, staleSnapshot) // leaked by an earlier, already-resolved crash
	ctrl.SetCrashed("conv-1", currentSnapshot)   // conv-1 has crashed again since then
	h := newTestSubstrateHarness(t, harnesstest.StartControlServer(t, ctrl), harnesstest.StartHarnessServer(t, &harnesstest.MockHarnessServer{}))

	ctx := context.Background()
	exec, err := h.Start(ctx, "conv-1", substrateAgentConfig)
	if err != nil {
		t.Fatalf("Start should recover past the stale tag and succeed: %v", err)
	}
	t.Cleanup(func() { _ = exec.Close(ctx) })

	// The critical assertion: recovery must have used THIS crash's own
	// snapshot, not the stale one left over from the earlier crash.
	if got := ctrl.LatestSnapshot("conv-1"); got.GetName() != currentSnapshot.GetName() {
		t.Fatalf("recreated actor's LatestSnapshot = %v, want %v (got the STALE tag's snapshot instead -- a silent rollback)", got, currentSnapshot)
	}

	_, _, tagCreate, tagDelete := ctrl.CrashRecoveryCalls()
	// One attempt that hits AlreadyExists against the stale tag, one that
	// succeeds after discarding it.
	if len(tagCreate) != 2 {
		t.Errorf("CreateActorSnapshotTag calls = %v, want exactly 2 (one AlreadyExists against the stale tag, one after discarding it)", tagCreate)
	}
	// One delete for the stale tag, one for the final best-effort cleanup
	// of the tag this recovery itself created.
	if len(tagDelete) != 2 {
		t.Errorf("DeleteActorSnapshotTag calls = %v, want exactly 2 (discarding the stale tag, then final cleanup)", tagDelete)
	}
	for _, name := range tagCreate {
		if name != tagName {
			t.Errorf("CreateActorSnapshotTag call for %q, want the deterministic name %q every time", name, tagName)
		}
	}

	if err := exec.Queue(ctx, harnesstest.UserStep("hi")); err != nil {
		t.Fatalf("Queue: %v", err)
	}
	handler := &harnesstest.MockHandler{}
	if err := exec.Run(ctx, handler); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !handler.IsDone() {
		t.Error("handler did not complete after recovery")
	}
}

// TestSubstrateHarness_RecoversFromCrashedActor_TagAlreadyMatchesCurrentCrash
// confirms ensureSnapshotTag treats an existing tag that already points at
// the current crash's own snapshot as correct, not stale -- a genuine retry
// of recovering the same crash skips the needless discard-and-recreate.
func TestSubstrateHarness_RecoversFromCrashedActor_TagAlreadyMatchesCurrentCrash(t *testing.T) {
	ctrl := &harnesstest.MockControlServer{ResumeIP: "127.0.0.1"}
	snapshot := &ateapipb.ObjectRef{Atespace: "ax", Name: "snap-before-crash"}

	tagName := crashRecoveryTagName("conv-1")
	ctrl.SeedSnapshotTag(tagName, snapshot) // a prior attempt already tagged this exact crash
	ctrl.SetCrashed("conv-1", snapshot)
	h := newTestSubstrateHarness(t, harnesstest.StartControlServer(t, ctrl), harnesstest.StartHarnessServer(t, &harnesstest.MockHarnessServer{}))

	ctx := context.Background()
	exec, err := h.Start(context.Background(), "conv-1", substrateAgentConfig)
	if err != nil {
		t.Fatalf("Start should recover successfully: %v", err)
	}
	t.Cleanup(func() { _ = exec.Close(ctx) })

	if got := ctrl.LatestSnapshot("conv-1"); got.GetName() != snapshot.GetName() {
		t.Errorf("recreated actor's LatestSnapshot = %v, want %v", got, snapshot)
	}

	_, _, tagCreate, tagDelete := ctrl.CrashRecoveryCalls()
	// Just the one attempt, which hits AlreadyExists -- since it already
	// points at the right snapshot, there's nothing to discard/redo.
	if len(tagCreate) != 1 {
		t.Errorf("CreateActorSnapshotTag calls = %v, want exactly 1 (no needless retry against an already-correct tag)", tagCreate)
	}
	// Just the one final best-effort cleanup delete -- no extra
	// discard-and-recreate cycle.
	if len(tagDelete) != 1 {
		t.Errorf("DeleteActorSnapshotTag calls = %v, want exactly 1 (final cleanup only)", tagDelete)
	}
}

// TestSubstrateHarness_ReclaimsInterruptedRecovery confirms resumeActor
// finishes a recovery interrupted between discardAndRecoverFromSnapshot's
// own DeleteActor and CreateActorFromSnapshotTag calls, where
// conversationID has no actor but a durable recovery tag still points at
// its last snapshot: its own "not found" branch checks for that tag before
// ever calling CreateActor, so it recovers from the tag directly instead of
// booting a blank actor and discarding the conversation's history.
func TestSubstrateHarness_ReclaimsInterruptedRecovery(t *testing.T) {
	ctrl := &harnesstest.MockControlServer{ResumeIP: "127.0.0.1"}
	snapshot := &ateapipb.ObjectRef{Atespace: "ax", Name: "snap-before-checkpointd-itself-crashed"}
	tagName := crashRecoveryTagName("conv-1")
	ctrl.SeedSnapshotTag(tagName, snapshot) // left behind by the interrupted recovery attempt
	h := newTestSubstrateHarness(t, harnesstest.StartControlServer(t, ctrl), harnesstest.StartHarnessServer(t, &harnesstest.MockHarnessServer{}))

	ctx := context.Background()
	exec, err := h.Start(ctx, "conv-1", substrateAgentConfig)
	if err != nil {
		t.Fatalf("Start should finish the interrupted recovery and succeed: %v", err)
	}
	t.Cleanup(func() { _ = exec.Close(ctx) })

	// The critical assertion: the actor Start ends up running must carry
	// the tag's own snapshot, not a blank/golden one.
	if got := ctrl.LatestSnapshot("conv-1"); got.GetName() != snapshot.GetName() {
		t.Fatalf("actor's LatestSnapshot = %v, want %v (got a blank actor instead -- the interrupted recovery's tag was silently discarded)", got, snapshot)
	}

	get, del, tagCreate, tagDelete := ctrl.CrashRecoveryCalls()
	// resumeActor's own GetActor comes back NotFound; checking for a
	// leftover tag before ever calling CreateActor (unlike the old
	// reclaimInterruptedRecovery, which ran only after CreateActor had
	// already produced a bogus blank actor) means no blank actor is ever
	// created here to discard in the first place.
	if !slices.Equal(get, []string{"conv-1"}) {
		t.Errorf("GetActor calls = %v, want [conv-1]", get)
	}
	if len(del) != 0 {
		t.Errorf("DeleteActor calls = %v, want none -- no blank actor was ever created to discard", del)
	}
	// No new tag creation -- the existing one is reused as-is.
	if len(tagCreate) != 0 {
		t.Errorf("CreateActorSnapshotTag calls = %v, want none -- the existing tag is reused, not recreated", tagCreate)
	}
	if !slices.Equal(tagDelete, []string{tagName}) {
		t.Errorf("DeleteActorSnapshotTag calls = %v, want [%s] (final cleanup)", tagDelete, tagName)
	}

	// And the recovered actor runs a normal turn afterward.
	if err := exec.Queue(ctx, harnesstest.UserStep("hi")); err != nil {
		t.Fatalf("Queue: %v", err)
	}
	handler := &harnesstest.MockHandler{}
	if err := exec.Run(ctx, handler); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !handler.IsDone() {
		t.Error("handler did not complete after recovery")
	}
}

// TestSubstrateHarness_NoInterruptedRecovery_OrdinaryFreshActor confirms
// resumeActor's own GetActorSnapshotTag check (in its "not found" branch)
// doesn't change behavior for the overwhelmingly common case: a genuinely
// brand new conversationID, with no crash-recovery tag ever created for it.
func TestSubstrateHarness_NoInterruptedRecovery_OrdinaryFreshActor(t *testing.T) {
	ctrl := &harnesstest.MockControlServer{ResumeIP: "127.0.0.1"}
	h := newTestSubstrateHarness(t, harnesstest.StartControlServer(t, ctrl), harnesstest.StartHarnessServer(t, &harnesstest.MockHarnessServer{}))

	ctx := context.Background()
	exec, err := h.Start(ctx, "conv-1", substrateAgentConfig)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = exec.Close(ctx) })

	_, del, tagCreate, tagDelete := ctrl.CrashRecoveryCalls()
	if len(del) != 0 || len(tagCreate) != 0 || len(tagDelete) != 0 {
		t.Errorf("crash-recovery machinery ran for a genuinely fresh actor: deletes=%v tagCreates=%v tagDeletes=%v, want none of it", del, tagCreate, tagDelete)
	}
}

// TestSubstrateHarness_CrashDuringConnectionRetryStillRecovers confirms
// Start carries no state between independent calls: an earlier attempt that
// failed on a plain transient error (not yet a visible CRASHED state) leaves
// nothing behind that could block a later attempt's own recovery once the
// actor's crash does become visible, the same as cmd/checkpointd's own
// outer retry loop calling Start fresh on every attempt.
func TestSubstrateHarness_CrashDuringConnectionRetryStillRecovers(t *testing.T) {
	ctrl := &harnesstest.MockControlServer{
		ResumeIP:  "127.0.0.1",
		ResumeErr: status.Error(codes.Unavailable, "upstream request timeout"), // a transient connection failure, not yet a CRASHED signal
	}
	h := newTestSubstrateHarness(t, harnesstest.StartControlServer(t, ctrl), harnesstest.StartHarnessServer(t, &harnesstest.MockHarnessServer{}))
	ctx := context.Background()

	// Attempt 1 (standing in for checkpointd's first ax exec try): fails, but
	// not with anything recovery should ever act on.
	if _, err := h.Start(ctx, "conv-1", substrateAgentConfig); err == nil {
		t.Fatal("Start should have failed on the transient connection error")
	} else if !strings.Contains(err.Error(), "upstream request timeout") {
		t.Errorf("error = %v, want it to mention the transient failure", err)
	}
	// resumeActor's own GetActor-first design looks the actor up on every
	// Start call regardless of outcome -- that alone isn't recovery, just
	// how it learns the actor's state; only del/tagCreate/tagDelete below
	// are the actual (destructive) recovery machinery.
	if _, del, tagCreate, tagDelete := ctrl.CrashRecoveryCalls(); len(del) != 0 || len(tagCreate) != 0 || len(tagDelete) != 0 {
		t.Errorf("crash-recovery machinery ran for a plain transient connection error: deletes=%v tagCreates=%v tagDeletes=%v, want none of it", del, tagCreate, tagDelete)
	}

	// The transient issue clears, and the actor's crash -- which may well
	// have caused attempt 1's own failure -- is now reflected in
	// Substrate's state.
	ctrl.ResumeErr = nil
	snapshot := &ateapipb.ObjectRef{Atespace: "ax", Name: "snap-before-crash"}
	ctrl.SetCrashed("conv-1", snapshot)

	// Attempt 2 (checkpointd's retry): must recover cleanly, exactly as if
	// this had been the very first attempt.
	exec, err := h.Start(ctx, "conv-1", substrateAgentConfig)
	if err != nil {
		t.Fatalf("Start (retry) should recover from CRASHED and succeed: %v", err)
	}
	t.Cleanup(func() { _ = exec.Close(ctx) })

	if got := ctrl.LatestSnapshot("conv-1"); got.GetName() != snapshot.GetName() {
		t.Errorf("recovered actor's LatestSnapshot = %v, want %v", got, snapshot)
	}

	if err := exec.Queue(ctx, harnesstest.UserStep("hi")); err != nil {
		t.Fatalf("Queue: %v", err)
	}
	handler := &harnesstest.MockHandler{}
	if err := exec.Run(ctx, handler); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !handler.IsDone() {
		t.Error("handler did not complete after recovery")
	}
}

// TestSubstrateHarness_ResumeUnrecognizedState_ReportsRetryableError
// confirms resumeActor declines to guess a recovery for a state its own
// declarative switch doesn't specially recognize (only SUSPENDED/RUNNING,
// RESUMING, CRASHED, DELETING, and SUSPENDING are) -- reporting a plain
// retryable error instead, for the relay's own backoff loop to retry
// later.
func TestSubstrateHarness_ResumeUnrecognizedState_ReportsRetryableError(t *testing.T) {
	ctrl := &harnesstest.MockControlServer{ResumeIP: "127.0.0.1"}
	ctrl.SetPausing("conv-1", nil)
	h := newTestSubstrateHarness(t, harnesstest.StartControlServer(t, ctrl), harnesstest.StartHarnessServer(t, &harnesstest.MockHarnessServer{}))

	_, err := h.Start(context.Background(), "conv-1", substrateAgentConfig)
	if err == nil {
		t.Fatal("expected Start to fail, got nil")
	}
	if !strings.Contains(err.Error(), "conv-1") {
		t.Errorf("error = %v, want it to name the actor", err)
	}
	// resumeActor's own GetActor comes back the actor directly (found, not
	// NotFound); it declines to guess a recovery for its unrecognized
	// state -- no ResumeActor call, and definitely no destructive recovery
	// attempt.
	_, resumeCalls, _ := ctrl.Calls()
	if len(resumeCalls) != 0 {
		t.Errorf("ResumeActor calls = %v, want none -- an unrecognized state must never be resumed", resumeCalls)
	}
	_, del, tagCreate, tagDelete := ctrl.CrashRecoveryCalls()
	if len(del) != 0 || len(tagCreate) != 0 || len(tagDelete) != 0 {
		t.Errorf("recovery machinery ran for an unrecognized state: delete=%v tagCreate=%v tagDelete=%v, want none of it", del, tagCreate, tagDelete)
	}
}

// TestSubstrateHarness_ResumeRetriesResumingActor confirms resumeActor
// retries ResumeActor for an actor already RESUMING (e.g. left that way by
// a checkpointd restart mid-resume), instead of declining like it does for
// a genuinely unrecognized state -- Substrate's own ResumeActor workflow is
// designed to be safely re-entered for exactly this case.
func TestSubstrateHarness_ResumeRetriesResumingActor(t *testing.T) {
	ctrl := &harnesstest.MockControlServer{ResumeIP: "127.0.0.1"}
	ctrl.SetResuming("conv-1", nil)
	h := newTestSubstrateHarness(t, harnesstest.StartControlServer(t, ctrl), harnesstest.StartHarnessServer(t, &harnesstest.MockHarnessServer{}))

	ctx := context.Background()
	exec, err := h.Start(ctx, "conv-1", substrateAgentConfig)
	if err != nil {
		t.Fatalf("Start should retry the RESUMING actor and then resume successfully: %v", err)
	}
	t.Cleanup(func() { _ = exec.Close(ctx) })

	_, resumeCalls, _ := ctrl.Calls()
	if !slices.Equal(resumeCalls, []string{"conv-1"}) {
		t.Errorf("ResumeActor calls = %v, want [conv-1] (retrying the RESUMING actor directly)", resumeCalls)
	}
}

func TestSubstrateHarness_HarnessFailedFrame(t *testing.T) {
	ctrl := &harnesstest.MockControlServer{ResumeIP: "127.0.0.1"}
	srv := &harnesstest.MockHarnessServer{FailFrame: true, ErrCode: 13, ErrMessage: "boom"}
	h := newTestSubstrateHarness(t, harnesstest.StartControlServer(t, ctrl), harnesstest.StartHarnessServer(t, srv))

	ctx := context.Background()
	exec, err := h.Start(ctx, "conv-1", substrateAgentConfig)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = exec.Close(ctx) })
	if err := exec.Queue(ctx, harnesstest.UserStep("hi")); err != nil {
		t.Fatalf("Queue: %v", err)
	}
	if err := exec.Run(ctx, &harnesstest.MockHandler{}); err == nil {
		t.Fatal("expected error from failed harness frame, got nil")
	} else if !strings.Contains(err.Error(), "harness failed") {
		t.Errorf("error = %v, want it to mention 'harness failed'", err)
	}
}
