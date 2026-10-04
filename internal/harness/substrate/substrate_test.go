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
	"bufio"
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/ktock/checkpointd/internal/ate"
	"github.com/ktock/checkpointd/internal/harness"
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
// SuspendActor reports success but the external snapshot never advances. Checkpoint
// must detect the unchanged snapshot and fail the turn rather than let
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
		t.Fatal("Checkpoint: got nil error, want one -- SuspendActor reported success but the external snapshot never advanced, which must fail this turn rather than let the caller commit it as durable")
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

// runTurn queues one user message on exec and fails the test unless the turn completes.
func runTurn(t *testing.T, ctx context.Context, exec harness.Execution) {
	t.Helper()
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

// TestSubstrateHarness_RecoversFromCrashedActor confirms Start reverts a CRASHED actor in place and then resumes it.
func TestSubstrateHarness_RecoversFromCrashedActor(t *testing.T) {
	ctrl := &harnesstest.MockControlServer{ResumeIP: "127.0.0.1"}
	ctrl.SetCrashed("conv-1", "snap-before-crash")
	h := newTestSubstrateHarness(t, harnesstest.StartControlServer(t, ctrl), harnesstest.StartHarnessServer(t, &harnesstest.MockHarnessServer{}))

	ctx := context.Background()
	exec, err := h.Start(ctx, "conv-1", substrateAgentConfig)
	if err != nil {
		t.Fatalf("Start should recover from CRASHED and succeed: %v", err)
	}
	t.Cleanup(func() { _ = exec.Close(ctx) })

	get, del, revert := ctrl.CrashRecoveryCalls()
	if !slices.Equal(get, []string{"conv-1"}) {
		t.Errorf("GetActor calls = %v, want [conv-1]", get)
	}
	if !slices.Equal(revert, []string{"conv-1"}) {
		t.Errorf("RevertActor calls = %v, want [conv-1]", revert)
	}
	if len(del) != 0 {
		t.Errorf("DeleteActor calls = %v, want none because the same actor is reverted in place", del)
	}
	if create, _, _ := ctrl.Calls(); len(create) != 0 {
		t.Errorf("CreateActor calls = %v, want none because the same actor is reverted in place", create)
	}
	if got := ctrl.ExternalSnapshotURI("conv-1"); got != "snap-before-crash" {
		t.Errorf("external snapshot = %q, want the crashed actor's own last snapshot", got)
	}
	runTurn(t, ctx, exec)
}

// TestSubstrateHarness_RecoversFromCrashedActor_NoPriorSnapshot confirms an actor that crashed before its first checkpoint is reverted the same way.
func TestSubstrateHarness_RecoversFromCrashedActor_NoPriorSnapshot(t *testing.T) {
	ctrl := &harnesstest.MockControlServer{ResumeIP: "127.0.0.1"}
	ctrl.SetCrashed("conv-1", "")
	h := newTestSubstrateHarness(t, harnesstest.StartControlServer(t, ctrl), harnesstest.StartHarnessServer(t, &harnesstest.MockHarnessServer{}))

	ctx := context.Background()
	exec, err := h.Start(ctx, "conv-1", substrateAgentConfig)
	if err != nil {
		t.Fatalf("Start should revert the snapshot-less actor and succeed: %v", err)
	}
	t.Cleanup(func() { _ = exec.Close(ctx) })

	_, del, revert := ctrl.CrashRecoveryCalls()
	if !slices.Equal(revert, []string{"conv-1"}) {
		t.Errorf("RevertActor calls = %v, want [conv-1]", revert)
	}
	if len(del) != 0 {
		t.Errorf("DeleteActor calls = %v, want none", del)
	}
	runTurn(t, ctx, exec)
}

// TestSubstrateHarness_RecoversFromInterruptedRevert confirms Start re-enters a revert that was interrupted partway through.
func TestSubstrateHarness_RecoversFromInterruptedRevert(t *testing.T) {
	ctrl := &harnesstest.MockControlServer{ResumeIP: "127.0.0.1"}
	ctrl.SetReverting("conv-1", "snap-before-crash")
	h := newTestSubstrateHarness(t, harnesstest.StartControlServer(t, ctrl), harnesstest.StartHarnessServer(t, &harnesstest.MockHarnessServer{}))

	ctx := context.Background()
	exec, err := h.Start(ctx, "conv-1", substrateAgentConfig)
	if err != nil {
		t.Fatalf("Start should re-enter the interrupted revert and succeed: %v", err)
	}
	t.Cleanup(func() { _ = exec.Close(ctx) })

	if _, _, revert := ctrl.CrashRecoveryCalls(); !slices.Equal(revert, []string{"conv-1"}) {
		t.Errorf("RevertActor calls = %v, want [conv-1]", revert)
	}
	runTurn(t, ctx, exec)
}

// TestSubstrateHarness_RevertFailureIsRetryable confirms a failed revert is reported without ever calling ResumeActor.
func TestSubstrateHarness_RevertFailureIsRetryable(t *testing.T) {
	ctrl := &harnesstest.MockControlServer{ResumeIP: "127.0.0.1", RevertErr: status.Error(codes.Aborted, "concurrent update conflict, please retry")}
	ctrl.SetCrashed("conv-1", "snap-before-crash")
	h := newTestSubstrateHarness(t, harnesstest.StartControlServer(t, ctrl), harnesstest.StartHarnessServer(t, &harnesstest.MockHarnessServer{}))

	_, err := h.Start(context.Background(), "conv-1", substrateAgentConfig)
	if err == nil {
		t.Fatal("Start should report the failed revert, got nil")
	}
	if !strings.Contains(err.Error(), "revert") {
		t.Errorf("error = %v, want it to mention the failed revert", err)
	}
	if _, resume, _ := ctrl.Calls(); len(resume) != 0 {
		t.Errorf("ResumeActor calls = %v, want none because the actor was never reverted", resume)
	}
}

// TestSubstrateHarness_RecoversFromStuckSuspending confirms Start re-enters a stuck SuspendActor and then resumes the actor.
func TestSubstrateHarness_RecoversFromStuckSuspending(t *testing.T) {
	ctrl := &harnesstest.MockControlServer{ResumeIP: "127.0.0.1"}
	ctrl.SetSuspending("conv-1", "")
	h := newTestSubstrateHarness(t, harnesstest.StartControlServer(t, ctrl), harnesstest.StartHarnessServer(t, &harnesstest.MockHarnessServer{}))

	ctx := context.Background()
	exec, err := h.Start(ctx, "conv-1", substrateAgentConfig)
	if err != nil {
		t.Fatalf("Start should re-enter the stuck suspend and then resume successfully: %v", err)
	}
	t.Cleanup(func() { _ = exec.Close(ctx) })

	_, resumeCalls, suspendCalls := ctrl.Calls()
	if !slices.Equal(suspendCalls, []string{"conv-1"}) {
		t.Errorf("SuspendActor calls = %v, want [conv-1]", suspendCalls)
	}
	if len(resumeCalls) != 1 {
		t.Errorf("ResumeActor calls = %v, want exactly 1", resumeCalls)
	}
	if _, _, revert := ctrl.CrashRecoveryCalls(); len(revert) != 0 {
		t.Errorf("RevertActor calls = %v, want none because the suspend completed", revert)
	}
	runTurn(t, ctx, exec)
}

// TestSubstrateHarness_RecoversFromStuckSuspending_ResuspendFails confirms a suspend that cannot be re-entered is forced to CRASHED via its worker and then reverted.
func TestSubstrateHarness_RecoversFromStuckSuspending_ResuspendFails(t *testing.T) {
	ctrl := &harnesstest.MockControlServer{ResumeIP: "127.0.0.1"}
	ctrl.SetSuspendingWithWorker("conv-1", "snap-before-stuck-suspend", "worker-with-wedged-sandbox")
	ctrl.SuspendErr = status.Error(codes.Unknown, "while running `runsc checkpoint`: exit status 128")
	h := newTestSubstrateHarness(t, harnesstest.StartControlServer(t, ctrl), harnesstest.StartHarnessServer(t, &harnesstest.MockHarnessServer{}))

	ctx := context.Background()
	exec, err := h.Start(ctx, "conv-1", substrateAgentConfig)
	if err != nil {
		t.Fatalf("Start should fall back to reverting the actor when re-suspending itself fails: %v", err)
	}
	t.Cleanup(func() { _ = exec.Close(ctx) })

	if _, _, suspendCalls := ctrl.Calls(); !slices.Equal(suspendCalls, []string{"conv-1"}) {
		t.Errorf("SuspendActor calls = %v, want [conv-1] (one attempt to re-enter the stuck suspend)", suspendCalls)
	}
	if got := ctrl.DeleteWorkerCalls(); !slices.Equal(got, []string{"worker-with-wedged-sandbox"}) {
		t.Errorf("DeleteWorker calls = %v, want [worker-with-wedged-sandbox] (forcing the actor to CRASHED)", got)
	}
	_, del, revert := ctrl.CrashRecoveryCalls()
	if !slices.Equal(revert, []string{"conv-1"}) {
		t.Errorf("RevertActor calls = %v, want [conv-1]", revert)
	}
	if len(del) != 0 {
		t.Errorf("DeleteActor calls = %v, want none", del)
	}
	if got := ctrl.ExternalSnapshotURI("conv-1"); got != "snap-before-stuck-suspend" {
		t.Errorf("external snapshot = %q, want the actor's own last completed checkpoint", got)
	}
	runTurn(t, ctx, exec)
}

// TestSubstrateHarness_RecoversFromStuckSuspending_AbortedNotReverted confirms an Aborted re-suspend is reported as retryable and never triggers a revert.
func TestSubstrateHarness_RecoversFromStuckSuspending_AbortedNotReverted(t *testing.T) {
	ctrl := &harnesstest.MockControlServer{ResumeIP: "127.0.0.1"}
	ctrl.SetSuspendingWithWorker("conv-1", "snap-before-still-in-flight-suspend", "worker-1")
	ctrl.SuspendErr = status.Error(codes.Aborted, "another operation is in progress for this actor")
	h := newTestSubstrateHarness(t, harnesstest.StartControlServer(t, ctrl), harnesstest.StartHarnessServer(t, &harnesstest.MockHarnessServer{}))

	ctx := context.Background()
	if _, err := h.Start(ctx, "conv-1", substrateAgentConfig); err == nil {
		t.Fatal("Start should report the still-in-flight original suspend as a retryable error, not succeed")
	}

	if _, _, suspendCalls := ctrl.Calls(); !slices.Equal(suspendCalls, []string{"conv-1"}) {
		t.Errorf("SuspendActor calls = %v, want [conv-1]", suspendCalls)
	}
	_, del, revert := ctrl.CrashRecoveryCalls()
	if len(del) != 0 || len(revert) != 0 {
		t.Errorf("recovery must never run for a lease or version conflict: delete=%v revert=%v", del, revert)
	}
	if got := ctrl.DeleteWorkerCalls(); len(got) != 0 {
		t.Errorf("DeleteWorker calls = %v, want none because the original suspend may still complete", got)
	}
	if got := ctrl.ExternalSnapshotURI("conv-1"); got != "snap-before-still-in-flight-suspend" {
		t.Errorf("external snapshot = %q, want it unchanged", got)
	}
}

// TestSubstrateHarness_DeletingActorWithWorkerIsDeletedThenRetried confirms a DELETING actor holding a worker is released from it and deleted, and that the next attempt starts from a fresh actor.
func TestSubstrateHarness_DeletingActorWithWorkerIsDeletedThenRetried(t *testing.T) {
	ctrl := &harnesstest.MockControlServer{ResumeIP: "127.0.0.1"}
	ctrl.SetDeletingWithWorker("conv-1", "snap-before-stuck-delete", "worker-with-wedged-sandbox")
	h := newTestSubstrateHarness(t, harnesstest.StartControlServer(t, ctrl), harnesstest.StartHarnessServer(t, &harnesstest.MockHarnessServer{}))

	ctx := context.Background()
	if _, err := h.Start(ctx, "conv-1", substrateAgentConfig); err == nil || !strings.Contains(err.Error(), "retry") {
		t.Fatalf("first Start error = %v, want a retryable one after finishing the deletion", err)
	}
	if got := ctrl.DeleteWorkerCalls(); !slices.Equal(got, []string{"worker-with-wedged-sandbox"}) {
		t.Errorf("DeleteWorker calls = %v, want [worker-with-wedged-sandbox]", got)
	}
	_, del, revert := ctrl.CrashRecoveryCalls()
	if !slices.Equal(del, []string{"conv-1"}) || len(revert) != 0 {
		t.Errorf("DeleteActor calls = %v, RevertActor calls = %v, want [conv-1] and none", del, revert)
	}
	if create, resume, _ := ctrl.Calls(); len(create) != 0 || len(resume) != 0 {
		t.Errorf("the failing attempt must not create or resume anything: create=%v resume=%v", create, resume)
	}

	exec, err := h.Start(ctx, "conv-1", substrateAgentConfig)
	if err != nil {
		t.Fatalf("retried Start should create a fresh actor and succeed: %v", err)
	}
	t.Cleanup(func() { _ = exec.Close(ctx) })
	if create, _, _ := ctrl.Calls(); !slices.Equal(create, []string{"conv-1"}) {
		t.Errorf("CreateActor calls = %v, want [conv-1]", create)
	}
	runTurn(t, ctx, exec)
}

// TestSubstrateHarness_DeletingActorWithoutWorkerIsDeletedThenRetried confirms a DELETING actor with no worker has its deletion finished without touching any worker.
func TestSubstrateHarness_DeletingActorWithoutWorkerIsDeletedThenRetried(t *testing.T) {
	ctrl := &harnesstest.MockControlServer{ResumeIP: "127.0.0.1"}
	ctrl.SetDeleting("conv-1", "snap-before-delete")
	h := newTestSubstrateHarness(t, harnesstest.StartControlServer(t, ctrl), harnesstest.StartHarnessServer(t, &harnesstest.MockHarnessServer{}))

	ctx := context.Background()
	if _, err := h.Start(ctx, "conv-1", substrateAgentConfig); err == nil || !strings.Contains(err.Error(), "retry") {
		t.Fatalf("first Start error = %v, want a retryable one after finishing the deletion", err)
	}
	_, del, revert := ctrl.CrashRecoveryCalls()
	if !slices.Equal(del, []string{"conv-1"}) || len(revert) != 0 || len(ctrl.DeleteWorkerCalls()) != 0 {
		t.Errorf("delete=%v revert=%v deleteWorker=%v, want [conv-1] and none", del, revert, ctrl.DeleteWorkerCalls())
	}

	exec, err := h.Start(ctx, "conv-1", substrateAgentConfig)
	if err != nil {
		t.Fatalf("retried Start should create a fresh actor and succeed: %v", err)
	}
	t.Cleanup(func() { _ = exec.Close(ctx) })
	runTurn(t, ctx, exec)
}

// TestSubstrateHarness_StuckSuspendingWithoutWorkerIsReportedNotReverted confirms a SUSPENDING actor with no worker to release is retried rather than sent to a revert Substrate would reject.
func TestSubstrateHarness_StuckSuspendingWithoutWorkerIsReportedNotReverted(t *testing.T) {
	ctrl := &harnesstest.MockControlServer{ResumeIP: "127.0.0.1"}
	ctrl.SetSuspending("conv-1", "snap-before-stuck-suspend")
	ctrl.SuspendErr = status.Error(codes.Unknown, "while running `runsc checkpoint`: exit status 128")
	h := newTestSubstrateHarness(t, harnesstest.StartControlServer(t, ctrl), harnesstest.StartHarnessServer(t, &harnesstest.MockHarnessServer{}))

	_, err := h.Start(context.Background(), "conv-1", substrateAgentConfig)
	if err == nil || !strings.Contains(err.Error(), "without a worker") {
		t.Fatalf("Start error = %v, want one explaining there is no worker to release", err)
	}
	if _, _, revert := ctrl.CrashRecoveryCalls(); len(revert) != 0 {
		t.Errorf("RevertActor calls = %v, want none", revert)
	}
	if got := ctrl.DeleteWorkerCalls(); len(got) != 0 {
		t.Errorf("DeleteWorker calls = %v, want none", got)
	}
}

// TestSubstrateHarness_RecoversFromUnreachableRunningWorker confirms a RUNNING actor with an unreachable worker is force-crashed on the first Start and reverted on the retry.
func TestSubstrateHarness_RecoversFromUnreachableRunningWorker(t *testing.T) {
	ctrl := &harnesstest.MockControlServer{
		// 127.0.0.2 is unreachable because the mock harness server only binds 127.0.0.1.
		ResumeIP:             "127.0.0.2",
		ResumeWorkerName:     "worker-with-dead-guest",
		PostRecoveryResumeIP: "127.0.0.1",
	}
	ctrl.SetRunning("conv-1", "snap-before-worker-died")
	h := newTestSubstrateHarness(t, harnesstest.StartControlServer(t, ctrl), harnesstest.StartHarnessServer(t, &harnesstest.MockHarnessServer{}))
	// Keeps the permanently doomed first health check short.
	h.healthCheckTimeout = 500 * time.Millisecond

	ctx := context.Background()

	// Attempt 1 force-releases the dead worker and reports failure, and the relay's own backoff drives attempt 2.
	if _, err := h.Start(ctx, "conv-1", substrateAgentConfig); err == nil {
		t.Fatal("Start (attempt 1) should report the unreachable worker as failed, not silently succeed")
	}
	if got := ctrl.DeleteWorkerCalls(); !slices.Equal(got, []string{"worker-with-dead-guest"}) {
		t.Errorf("DeleteWorker calls = %v, want [worker-with-dead-guest]", got)
	}
	if got := ctrl.ActorState("conv-1"); got != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		t.Errorf("actor state after attempt 1 = %s, want CRASHED", got)
	}

	exec, err := h.Start(ctx, "conv-1", substrateAgentConfig)
	if err != nil {
		t.Fatalf("Start (attempt 2) should revert the now-CRASHED actor and succeed: %v", err)
	}
	t.Cleanup(func() { _ = exec.Close(context.Background()) })

	_, del, revert := ctrl.CrashRecoveryCalls()
	if !slices.Equal(revert, []string{"conv-1"}) {
		t.Errorf("RevertActor calls = %v, want [conv-1]", revert)
	}
	if len(del) != 0 {
		t.Errorf("DeleteActor calls = %v, want none", del)
	}
	if got := ctrl.ExternalSnapshotURI("conv-1"); got != "snap-before-worker-died" {
		t.Errorf("external snapshot = %q, want the actor's own last snapshot", got)
	}
	runTurn(t, ctx, exec)
}

// TestSubstrateHarness_FreshActorSkipsRecovery confirms a brand new conversation never touches the recovery machinery.
func TestSubstrateHarness_FreshActorSkipsRecovery(t *testing.T) {
	ctrl := &harnesstest.MockControlServer{ResumeIP: "127.0.0.1"}
	h := newTestSubstrateHarness(t, harnesstest.StartControlServer(t, ctrl), harnesstest.StartHarnessServer(t, &harnesstest.MockHarnessServer{}))

	ctx := context.Background()
	exec, err := h.Start(ctx, "conv-1", substrateAgentConfig)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = exec.Close(ctx) })

	_, del, revert := ctrl.CrashRecoveryCalls()
	if len(del) != 0 || len(revert) != 0 || len(ctrl.DeleteWorkerCalls()) != 0 {
		t.Errorf("recovery ran for a fresh actor: deletes=%v reverts=%v deleteWorkers=%v, want none", del, revert, ctrl.DeleteWorkerCalls())
	}
}

// TestSubstrateHarness_CrashDuringConnectionRetryStillRecovers confirms Start carries no state between calls, so a transient failure never blocks a later recovery.
func TestSubstrateHarness_CrashDuringConnectionRetryStillRecovers(t *testing.T) {
	ctrl := &harnesstest.MockControlServer{
		ResumeIP:  "127.0.0.1",
		ResumeErr: status.Error(codes.Unavailable, "upstream request timeout"),
	}
	h := newTestSubstrateHarness(t, harnesstest.StartControlServer(t, ctrl), harnesstest.StartHarnessServer(t, &harnesstest.MockHarnessServer{}))
	ctx := context.Background()

	if _, err := h.Start(ctx, "conv-1", substrateAgentConfig); err == nil {
		t.Fatal("Start should have failed on the transient connection error")
	} else if !strings.Contains(err.Error(), "upstream request timeout") {
		t.Errorf("error = %v, want it to mention the transient failure", err)
	}
	if _, del, revert := ctrl.CrashRecoveryCalls(); len(del) != 0 || len(revert) != 0 {
		t.Errorf("recovery ran for a plain transient connection error: deletes=%v reverts=%v, want none", del, revert)
	}

	// The transient issue clears and the actor's crash is now visible.
	ctrl.ResumeErr = nil
	ctrl.SetCrashed("conv-1", "snap-before-crash")

	exec, err := h.Start(ctx, "conv-1", substrateAgentConfig)
	if err != nil {
		t.Fatalf("Start (retry) should recover from CRASHED and succeed: %v", err)
	}
	t.Cleanup(func() { _ = exec.Close(ctx) })

	if got := ctrl.ExternalSnapshotURI("conv-1"); got != "snap-before-crash" {
		t.Errorf("recovered actor's external snapshot = %q, want %q", got, "snap-before-crash")
	}
	runTurn(t, ctx, exec)
}

// TestSubstrateHarness_ResumeUnrecognizedState_ReportsRetryableError confirms resumeActor declines to guess a recovery for a state it does not recognize.
func TestSubstrateHarness_ResumeUnrecognizedState_ReportsRetryableError(t *testing.T) {
	ctrl := &harnesstest.MockControlServer{ResumeIP: "127.0.0.1"}
	ctrl.SetPausing("conv-1", "")
	h := newTestSubstrateHarness(t, harnesstest.StartControlServer(t, ctrl), harnesstest.StartHarnessServer(t, &harnesstest.MockHarnessServer{}))

	_, err := h.Start(context.Background(), "conv-1", substrateAgentConfig)
	if err == nil {
		t.Fatal("expected Start to fail, got nil")
	}
	if !strings.Contains(err.Error(), "conv-1") {
		t.Errorf("error = %v, want it to name the actor", err)
	}
	_, resumeCalls, _ := ctrl.Calls()
	if len(resumeCalls) != 0 {
		t.Errorf("ResumeActor calls = %v, want none because an unrecognized state must never be resumed", resumeCalls)
	}
	_, del, revert := ctrl.CrashRecoveryCalls()
	if len(del) != 0 || len(revert) != 0 {
		t.Errorf("recovery ran for an unrecognized state: delete=%v revert=%v, want none", del, revert)
	}
}

// TestSubstrateHarness_ResumeRetriesResumingActor confirms resumeActor
// retries ResumeActor for an actor already RESUMING (e.g. left that way by
// a checkpointd restart mid-resume), instead of declining like it does for
// a genuinely unrecognized state -- Substrate's own ResumeActor workflow is
// designed to be safely re-entered for exactly this case.
func TestSubstrateHarness_ResumeRetriesResumingActor(t *testing.T) {
	ctrl := &harnesstest.MockControlServer{ResumeIP: "127.0.0.1"}
	ctrl.SetResuming("conv-1", "")
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

// fakeRouter accepts one CONNECT request, records it, and answers with status.
func fakeRouter(t *testing.T, status string) (addr string, got <-chan *http.Request) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lis.Close() })
	reqs := make(chan *http.Request, 1)
	go func() {
		conn, err := lis.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		req, err := http.ReadRequest(bufio.NewReader(conn))
		if err != nil {
			return
		}
		reqs <- req
		_, _ = io.WriteString(conn, "HTTP/1.1 "+status+"\r\nContent-Length: 0\r\n\r\n")
		_, _ = io.Copy(conn, conn)
	}()
	return lis.Addr().String(), reqs
}

// TestDialThroughRouter_NamesTheActorInTheHeader confirms the CONNECT request routes on the target-actor header and carries the port in its authority.
func TestDialThroughRouter_NamesTheActorInTheHeader(t *testing.T) {
	addr, got := fakeRouter(t, "200 OK")
	conn, err := dialThroughRouter(context.Background(), addr, "my-atespace", "conv-1", "8080")
	if err != nil {
		t.Fatalf("dialThroughRouter: %v", err)
	}
	defer conn.Close()

	req := <-got
	if req.Method != http.MethodConnect {
		t.Errorf("method = %s, want CONNECT", req.Method)
	}
	if hdr := req.Header.Get("ate-target-actor"); hdr != "my-atespace/conv-1" {
		t.Errorf("ate-target-actor = %q, want %q", hdr, "my-atespace/conv-1")
	}
	if _, port, err := net.SplitHostPort(req.Host); err != nil || port != "8080" {
		t.Errorf("authority = %q, want a host with port 8080", req.Host)
	}
}

// TestDialThroughRouter_ReportsARejectedTunnel confirms a non-2xx CONNECT response is an error naming the status.
func TestDialThroughRouter_ReportsARejectedTunnel(t *testing.T) {
	addr, _ := fakeRouter(t, "404 Not Found")
	_, err := dialThroughRouter(context.Background(), addr, "my-atespace", "conv-1", "8080")
	if err == nil || !strings.Contains(err.Error(), "404") {
		t.Errorf("error = %v, want one mentioning the 404 status", err)
	}
}

func testEgressPolicy() *ateapipb.EgressPolicy {
	return &ateapipb.EgressPolicy{Rules: []*ateapipb.EgressRule{{
		Hostnames: &ateapipb.HostnameRule{Patterns: []string{"testserver.ns.svc"}},
	}}}
}

// TestSubstrateHarness_GivesActorItsEgressPolicyBeforeResuming confirms a configured policy is created in the actor's atespace under the name Substrate requires, ahead of the first resume.
func TestSubstrateHarness_GivesActorItsEgressPolicyBeforeResuming(t *testing.T) {
	ctrl := &harnesstest.MockControlServer{ResumeIP: "127.0.0.1"}
	h := newTestSubstrateHarness(t, harnesstest.StartControlServer(t, ctrl), harnesstest.StartHarnessServer(t, &harnesstest.MockHarnessServer{}))
	h.SetEgressPolicy(testEgressPolicy())

	ctx := context.Background()
	exec, err := h.Start(ctx, "conv-1", substrateAgentConfig)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = exec.Close(ctx) })

	got := ctrl.EgressPolicyOf("conv-1")
	if got == nil {
		t.Fatal("actor conv-1 has no egress policy")
	}
	if got.GetMetadata().GetName() != "default" || got.GetMetadata().GetAtespace() != "ax" {
		t.Errorf("policy metadata = %v, want atespace ax and name default", got.GetMetadata())
	}
	if len(got.GetRules()) != 1 || got.GetRules()[0].GetHostnames().GetPatterns()[0] != "testserver.ns.svc" {
		t.Errorf("policy rules = %v, want the configured rule", got.GetRules())
	}
	if want := []string{"egress-policy:conv-1", "resume:conv-1"}; !slices.Equal(ctrl.Sequence(), want) {
		t.Errorf("order = %v, want %v so the policy exists before the actor runs", ctrl.Sequence(), want)
	}
}

// TestSubstrateHarness_EgressPolicyAlreadyExistsIsTolerated confirms resuming an actor that already has its policy succeeds.
func TestSubstrateHarness_EgressPolicyAlreadyExistsIsTolerated(t *testing.T) {
	ctrl := &harnesstest.MockControlServer{ResumeIP: "127.0.0.1"}
	h := newTestSubstrateHarness(t, harnesstest.StartControlServer(t, ctrl), harnesstest.StartHarnessServer(t, &harnesstest.MockHarnessServer{}))
	h.SetEgressPolicy(testEgressPolicy())

	ctx := context.Background()
	for i := 1; i <= 2; i++ {
		exec, err := h.Start(ctx, "conv-1", substrateAgentConfig)
		if err != nil {
			t.Fatalf("Start #%d: %v", i, err)
		}
		if err := exec.Checkpoint(ctx); err != nil {
			t.Fatalf("Checkpoint #%d: %v", i, err)
		}
		_ = exec.Close(ctx)
	}
}

// TestSubstrateHarness_NoEgressPolicyConfiguredCreatesNone confirms an agent without a configured policy never calls the policy RPC.
func TestSubstrateHarness_NoEgressPolicyConfiguredCreatesNone(t *testing.T) {
	ctrl := &harnesstest.MockControlServer{ResumeIP: "127.0.0.1"}
	h := newTestSubstrateHarness(t, harnesstest.StartControlServer(t, ctrl), harnesstest.StartHarnessServer(t, &harnesstest.MockHarnessServer{}))

	ctx := context.Background()
	exec, err := h.Start(ctx, "conv-1", substrateAgentConfig)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = exec.Close(ctx) })

	if want := []string{"resume:conv-1"}; !slices.Equal(ctrl.Sequence(), want) {
		t.Errorf("order = %v, want just %v", ctrl.Sequence(), want)
	}
}

// TestSubstrateHarness_EgressPolicyFailureBlocksResume confirms a failed policy creation is reported without resuming an actor that would be denied all egress.
func TestSubstrateHarness_EgressPolicyFailureBlocksResume(t *testing.T) {
	ctrl := &harnesstest.MockControlServer{ResumeIP: "127.0.0.1", EgressPolicyErr: status.Error(codes.Unavailable, "control API restarting")}
	h := newTestSubstrateHarness(t, harnesstest.StartControlServer(t, ctrl), harnesstest.StartHarnessServer(t, &harnesstest.MockHarnessServer{}))
	h.SetEgressPolicy(testEgressPolicy())

	_, err := h.Start(context.Background(), "conv-1", substrateAgentConfig)
	if err == nil || !strings.Contains(err.Error(), "egress policy") {
		t.Fatalf("Start error = %v, want one mentioning the egress policy", err)
	}
	if _, resume, _ := ctrl.Calls(); len(resume) != 0 {
		t.Errorf("ResumeActor calls = %v, want none", resume)
	}
}
