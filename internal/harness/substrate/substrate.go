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
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base32"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/uuid"
	"github.com/ktock/checkpointd/internal/ate"
	"github.com/ktock/checkpointd/internal/harness"
	"github.com/ktock/checkpointd/proto"
	protolib "google.golang.org/protobuf/proto"
)

// Compile-time interface assertions.
var _ harness.Harness = (*SubstrateHarness)(nil)
var _ harness.Execution = (*substrateExecution)(nil)

// healthCheckTimeout defines the maximum time Start waits for a freshly
// created/resumed actor's harness to become reachable and ready.
const healthCheckTimeout = 60 * time.Second

// workerKeepaliveParams is applied to every gRPC connection to a worker's
// HarnessService. Without it, a Run call's stream.Recv() loop has no read
// deadline of its own and can block indefinitely if the worker pod is
// killed while a call is in flight.
var workerKeepaliveParams = keepalive.ClientParameters{
	Time:                20 * time.Second,
	Timeout:             10 * time.Second,
	PermitWithoutStream: true,
}

// DefaultSuspendActorTimeout is the client-side timeout Checkpoint applies
// to its SuspendActor call when New is not given an explicit override.
const DefaultSuspendActorTimeout = 11 * time.Minute

// SubstrateHarness manages execution in a SubstrATE sandboxed actor over gRPC HarnessService.
type SubstrateHarness struct {
	harnessID string
	namespace string // the actor's atespace, used to route to it
	ateClient *ate.Client
	port      int
	dialOpts  []grpc.DialOption
	// suspendActorTimeout is the client-side timeout Checkpoint applies to
	// SuspendActor.
	suspendActorTimeout time.Duration
	// healthCheckTimeout overrides the package default when non-zero. Only
	// tests set it, to shrink the wait for a permanently unreachable worker.
	healthCheckTimeout time.Duration
	tokenFile          string
	routerAddr         string
}

func (h *SubstrateHarness) resolvedHealthCheckTimeout() time.Duration {
	if h.healthCheckTimeout <= 0 {
		return healthCheckTimeout
	}
	return h.healthCheckTimeout
}

// ControlAPIOptions configures how New's control-API client authenticates to,
// and verifies the identity of, Agent Substrate's control API.
type ControlAPIOptions struct {
	// CAFile is a PEM file containing the control API's own trust bundle.
	// Empty disables server certificate verification.
	CAFile string
	// TokenFile is a bearer token file. Start CONNECT-tunnels through
	// atenet-router when set, or dials a worker pod IP directly when not.
	TokenFile string
	// RouterAddr is atenet-router's CONNECT-ingress address, used to reach
	// actor worker ports when TokenFile is set. Empty falls back to
	// defaultActorRouterAddr.
	RouterAddr string
}

// New creates a new SubstrateHarness.
func New(harnessID string, endpoint string, namespace string, template string, port int, suspendActorTimeout time.Duration, ctrlOpts ControlAPIOptions, opts ...grpc.DialOption) (*SubstrateHarness, error) {
	if port == 0 {
		port = 50053 // Default HarnessService port
	}
	if namespace == "" {
		namespace = "ax"
	}
	if template == "" {
		template = "ax-harness-antigravity-template"
	}
	if suspendActorTimeout <= 0 {
		suspendActorTimeout = DefaultSuspendActorTimeout
	}
	routerAddr := ctrlOpts.RouterAddr
	if routerAddr == "" {
		routerAddr = defaultActorRouterAddr
	}
	controlOpts, err := controlDialOptions(ctrlOpts)
	if err != nil {
		return nil, err
	}
	client, err := ate.NewClient(namespace, template, endpoint, controlOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create ATE client: %w", err)
	}
	if len(opts) == 0 {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	opts = append(opts, grpc.WithStatsHandler(otelgrpc.NewClientHandler()), grpc.WithKeepaliveParams(workerKeepaliveParams))
	return &SubstrateHarness{
		harnessID:           harnessID,
		namespace:           namespace,
		ateClient:           client,
		port:                port,
		dialOpts:            opts,
		suspendActorTimeout: suspendActorTimeout,
		tokenFile:           ctrlOpts.TokenFile,
		routerAddr:          routerAddr,
	}, nil
}

// controlKeepaliveParams is applied to the control-API connection.
// api.ate-system.svc is a headless Service (no ClusterIP/kube-proxy VIP in
// front of it), so DNS hands back its backend pods' own IPs directly and
// gRPC's pick_first policy pins this connection to one of them for its
// whole lifetime. Without keepalive, that pinned backend going away (a
// restart, a reschedule) leaves the connection silently stale: nothing
// detects it until a real RPC tries to use it and stalls for several
// seconds while TCP/gRPC discovers the connection is dead and reconnects.
var controlKeepaliveParams = keepalive.ClientParameters{
	Time:                6 * time.Minute,
	Timeout:             10 * time.Second,
	PermitWithoutStream: true,
}

// controlDialOptions returns the dial options New uses to authenticate to
// the real Agent Substrate Control API.
func controlDialOptions(ctrlOpts ControlAPIOptions) ([]grpc.DialOption, error) {
	tlsConfig := &tls.Config{}
	if ctrlOpts.CAFile == "" {
		tlsConfig.InsecureSkipVerify = true
	} else {
		pem, err := os.ReadFile(ctrlOpts.CAFile)
		if err != nil {
			return nil, fmt.Errorf("reading substrate control API CA file %q: %w", ctrlOpts.CAFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("substrate control API CA file %q contains no valid certificates", ctrlOpts.CAFile)
		}
		tlsConfig.RootCAs = pool
	}
	controlCreds := grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	opts := append([]grpc.DialOption{controlCreds}, controlAPIBearerTokenOption(ctrlOpts.TokenFile)...)
	return append(opts, grpc.WithKeepaliveParams(controlKeepaliveParams)), nil
}

// tokenFileReadable reports whether path is non-empty and currently stat-able.
func tokenFileReadable(path string) bool {
	if path == "" {
		return false
	}
	_, err := os.Stat(path)
	return err == nil
}

// controlAPIBearerTokenOption returns a PerRPCCredentials dial option that
// attaches a bearer token read from tokenFile to every control API call, or
// nil if tokenFile is empty or unreadable.
func controlAPIBearerTokenOption(tokenFile string) []grpc.DialOption {
	if !tokenFileReadable(tokenFile) {
		return nil
	}
	return []grpc.DialOption{grpc.WithPerRPCCredentials(bearerTokenFileCreds(tokenFile))}
}

// bearerTokenFileCreds implements credentials.PerRPCCredentials, re-reading
// the token file on every call so a kubelet-refreshed projected token is
// picked up without restarting.
type bearerTokenFileCreds string

func (c bearerTokenFileCreds) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	b, err := os.ReadFile(string(c))
	if err != nil {
		return nil, fmt.Errorf("read substrate control API bearer token file %q: %w", c, err)
	}
	token := strings.TrimSpace(string(b))
	if token == "" {
		return nil, fmt.Errorf("substrate control API bearer token file %q is empty", c)
	}
	return map[string]string{"authorization": "Bearer " + token}, nil
}

func (c bearerTokenFileCreds) RequireTransportSecurity() bool { return true }

// defaultActorRouterAddr is atenet-router's in-cluster CONNECT listener,
// used when ControlAPIOptions.RouterAddr is empty. A
// WorkerPool's own NetworkPolicy restricts ingress on worker pods to
// app=atenet-router in ate-system only, so a plain client outside the
// substrate mesh cannot dial a worker pod IP directly (it gets a bare TCP
// timeout) and must CONNECT-tunnel through the router instead, the same
// path real actor traffic takes.
const defaultActorRouterAddr = "atenet-router.ate-system.svc:8081"

// actorDNSSuffix is the fixed suffix of an actor's routable DNS name, which
// atenet-router uses to resolve a CONNECT authority to a specific actor:
// "<name>.<atespace>.actors.resources.substrate.ate.dev".
const actorDNSSuffix = "actors.resources.substrate.ate.dev"

// actorDNSName returns the DNS name atenet-router routes on for the actor
// named name in atespace.
func actorDNSName(atespace, name string) string {
	return name + "." + atespace + "." + actorDNSSuffix
}

// dialThroughRouter opens a plaintext connection to authority (an actor's DNS
// name plus port) by CONNECT-tunneling through atenet-router, since a worker
// pod is not reachable directly (see defaultActorRouterAddr).
func dialThroughRouter(ctx context.Context, routerAddr, authority string) (net.Conn, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", routerAddr)
	if err != nil {
		return nil, fmt.Errorf("dialing atenet-router at %s: %w", routerAddr, err)
	}
	req := &http.Request{
		Method: http.MethodConnect,
		URL:    &url.URL{Host: authority},
		Host:   authority,
	}
	if err := req.Write(conn); err != nil {
		conn.Close()
		return nil, fmt.Errorf("writing CONNECT request to atenet-router: %w", err)
	}
	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, req)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("reading CONNECT response from atenet-router: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		resp.Body.Close()
		conn.Close()
		msg := strings.TrimSpace(string(body))
		if msg == "" {
			msg = resp.Status
		}
		return nil, fmt.Errorf("atenet-router rejected CONNECT to %s: %s: %s", authority, resp.Status, msg)
	}
	// http.ReadResponse may buffer bytes past the header boundary into
	// reader; wrap conn so a caller's Read sees them instead of losing them.
	return &bufferedConn{Conn: conn, reader: reader}, nil
}

type bufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *bufferedConn) Read(p []byte) (int, error) { return c.reader.Read(p) }

// DeleteActor deletes the Substrate actor backing conversationID.
func (h *SubstrateHarness) DeleteActor(ctx context.Context, conversationID string) error {
	return h.ateClient.DeleteActor(ctx, conversationID)
}

// DeleteCrashRecoveryTag deletes conversationID's ActorSnapshotTag.
func (h *SubstrateHarness) DeleteCrashRecoveryTag(ctx context.Context, conversationID string) error {
	return h.ateClient.DeleteActorSnapshotTag(ctx, crashRecoveryTagName(conversationID))
}

// resumeActor ensures conversationID's actor is running with an assigned
// worker, declaratively driven by whatever state GetActor reports.
func (h *SubstrateHarness) resumeActor(ctx context.Context, conversationID string) (*ateapipb.Actor, error) {
	actor, err := h.ateClient.GetActor(ctx, conversationID)
	if err != nil {
		if status.Code(err) != codes.NotFound {
			return nil, fmt.Errorf("failed to look up actor %s: %w", conversationID, err)
		}
		tagName := crashRecoveryTagName(conversationID)
		if _, tagErr := h.ateClient.GetActorSnapshotTag(ctx, tagName); tagErr != nil {
			if status.Code(tagErr) != codes.NotFound {
				return nil, fmt.Errorf("failed to check %s for a leftover crash-recovery tag: %w", conversationID, tagErr)
			}
			// The ordinary case: a genuinely new conversation.
			if _, err := h.ateClient.CreateActor(ctx, conversationID); err != nil && status.Code(err) != codes.AlreadyExists {
				return nil, fmt.Errorf("failed to create substrate actor %s: %w", conversationID, err)
			}
		} else {
			// Not found, a crash-recovery tag exists. A prior recovery attempt
			// crashed before completion.
			slog.WarnContext(ctx, "crash recovery: found a leftover recovery tag for an actor that doesn't exist -- a prior recovery was interrupted after deleting the crashed actor but before recreating it from its own snapshot; finishing recovery from the tag",
				slog.String("conversation_id", conversationID), slog.String("tag", tagName))
			if err := h.finishRecoveryFromTag(ctx, conversationID, tagName); err != nil {
				return nil, err
			}
		}
		return h.doResumeActor(ctx, conversationID)
	}

	switch actor.GetStatus().GetState() {
	case ateapipb.ActorState_ACTOR_STATE_SUSPENDED, ateapipb.ActorState_ACTOR_STATE_RUNNING:
		// Already resumable as-is.
	case ateapipb.ActorState_ACTOR_STATE_RESUMING:
		// Substrate's own ResumeActor workflow is designed to be safely
		// re-entered for an actor already RESUMING: it revalidates the
		// existing worker assignment and either continues the resume to
		// completion or crashes the actor if that worker is gone, handled
		// as CRASHED on a later retry. It never resolves this on its own,
		// so retry ResumeActor directly instead of declining below.
	case ateapipb.ActorState_ACTOR_STATE_CRASHED:
		// Recover from its own last completed checkpoint, then ResumeActor
		slog.WarnContext(ctx, "crash recovery: actor CRASHED; recovering from its own last completed checkpoint",
			slog.String("conversation_id", conversationID),
			slog.Int64("actor_version", actor.GetMetadata().GetVersion()),
			slog.String("latest_snapshot", actor.GetStatus().GetLatestSnapshot().GetName()))
		if err := h.discardAndRecoverFromSnapshot(ctx, conversationID, actor); err != nil {
			return nil, err
		}
	case ateapipb.ActorState_ACTOR_STATE_DELETING:
		// Release the worker assignment if the actor still holds one,
		// recover from its own last completed checkpoint, then ResumeActor
		if worker := actor.GetStatus().GetWorkerAssignment().GetWorker(); worker != nil {
			slog.WarnContext(ctx, "crash recovery: actor stuck DELETING still holds a worker assignment; releasing it directly (bypassing that worker's own atelet) before retrying delete",
				slog.String("conversation_id", conversationID), slog.String("worker", worker.GetName()))
			if err := h.ateClient.DeleteWorker(ctx, worker); err != nil {
				return nil, fmt.Errorf("failed to release actor %s from its stuck worker %s: %w", conversationID, worker.GetName(), err)
			}
		}
		if err := h.discardAndRecoverFromSnapshot(ctx, conversationID, actor); err != nil {
			return nil, err
		}
	case ateapipb.ActorState_ACTOR_STATE_SUSPENDING:
		// SuspendActor's own workflow is designed to be safely re-entered,
		// so retrying it here usually either lets an in-progress
		// checkpoint finish or discovers the sandbox is genuinely wedged
		// and crashes the actor, handled as CRASHED on a later retry.
		if _, err := h.ateClient.SuspendActor(ctx, conversationID); err != nil {
			if status.Code(err) == codes.Aborted {
				// The original SuspendActor call may still be legitimately
				// in flight (holding the actor's lease) or have just
				// landed a concurrent write, not stuck. Recovering from a
				// snapshot here would roll the actor back past state that
				// call may still go on to commit, so report this as
				// retryable instead.
				return nil, fmt.Errorf("actor %s's own suspend is still genuinely in progress (%s); not recovering from a snapshot while it might still complete: %w", conversationID, err.Error(), err)
			}
			slog.WarnContext(ctx, "crash recovery: actor stuck SUSPENDING could not be re-suspended either; recovering from its own last completed checkpoint instead",
				slog.String("conversation_id", conversationID), slog.Any("error", err))
			if err := h.discardAndRecoverFromSnapshot(ctx, conversationID, actor); err != nil {
				return nil, err
			}
		}
	default:
		// Transient state expected to resolve on its own.
		return nil, fmt.Errorf("actor %s is in state %s, neither resumable nor a recognized stuck state; declining to recover it. Retry later.",
			conversationID, actor.GetStatus().GetState())
	}
	return h.doResumeActor(ctx, conversationID)
}

// doResumeActor calls ResumeActor for conversationID and returns the
// resulting actor.
func (h *SubstrateHarness) doResumeActor(ctx context.Context, conversationID string) (*ateapipb.Actor, error) {
	resumeResp, err := h.ateClient.ResumeActor(ctx, conversationID)
	if err != nil {
		return nil, fmt.Errorf("failed to resume substrate actor %s: %w", conversationID, err)
	}
	actor := resumeResp.Actor
	if actor == nil {
		return nil, fmt.Errorf("received nil actor in response for %s", conversationID)
	}
	return actor, nil
}

// dialAndWaitHealthy dials actor's assigned worker's HarnessService.
// It CONNECT-tunnels through atenet-router, since a worker pod IP is not
// directly reachable.
func (h *SubstrateHarness) dialAndWaitHealthy(ctx context.Context, conversationID string, actor *ateapipb.Actor) (*grpc.ClientConn, error) {
	workerPodIP := actor.GetStatus().GetWorkerAssignment().GetWorkerPodIp()
	if workerPodIP == "" {
		return nil, fmt.Errorf("actor %s has no active worker IP address", conversationID)
	}
	dialTarget := fmt.Sprintf("%s:%d", workerPodIP, h.port)
	dialOpts := h.dialOpts
	viaRouter := ""
	if tokenFileReadable(h.tokenFile) {
		dialTarget = fmt.Sprintf("%s:%d", actorDNSName(h.namespace, conversationID), h.port)
		dialOpts = append([]grpc.DialOption{
			grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
				return dialThroughRouter(ctx, h.routerAddr, addr)
			}),
		}, h.dialOpts...)
		viaRouter = fmt.Sprintf(" (via atenet-router %s)", h.routerAddr)
	}
	conn, err := grpc.NewClient("passthrough:///"+dialTarget, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to dial remote harness service at %s%s: %w", dialTarget, viaRouter, err)
	}
	if err := waitForHealthy(ctx, conn, h.resolvedHealthCheckTimeout()); err != nil {
		conn.Close()
		return nil, fmt.Errorf("harness for %s not ready at %s%s: %w", conversationID, dialTarget, viaRouter, err)
	}
	return conn, nil
}

// Start implements Harness interface. It creates/resumes the target actor.
func (h *SubstrateHarness) Start(ctx context.Context, conversationID string, config []byte) (harness.Execution, error) {
	if conversationID == "" {
		return nil, errors.New("SubstrateHarness needs valid conversationID")
	}

	actor, err := h.resumeActor(ctx, conversationID)
	if err != nil {
		return nil, err
	}

	conn, err := h.dialAndWaitHealthy(ctx, conversationID, actor)
	if err != nil {
		// Substrate reports this actor RUNNING with a seemingly valid
		// WorkerAssignment, yet its worker never became reachable (e.g.
		// a guest process that panics and dies in-process inside a
		// sandbox whose host pod never restarts). Forcibly delete the worker
		// to move the state to CRASHED and report this as retryable. The relay's
		// own backoff loop retries this Start call, and resumeActor's own CRASHED
		// case recovers it then.
		if worker := actor.GetStatus().GetWorkerAssignment().GetWorker(); worker != nil {
			slog.WarnContext(ctx, "crash recovery: actor RUNNING but its worker never became reachable; releasing it for recovery on retry",
				slog.String("conversation_id", conversationID),
				slog.String("worker", worker.GetName()),
				slog.Int64("actor_version", actor.GetMetadata().GetVersion()),
				slog.String("latest_snapshot", actor.GetStatus().GetLatestSnapshot().GetName()))
			if delErr := h.ateClient.DeleteWorker(ctx, worker); delErr != nil {
				return nil, fmt.Errorf("%w (also failed to release its unreachable worker %s: %v)", err, worker.GetName(), delErr)
			}
		}
		return nil, fmt.Errorf("substrate actor %s's worker unreachable: %w", conversationID, err)
	}

	return &substrateExecution{
		harness:               h,
		conversationID:        conversationID,
		execID:                uuid.NewString(),
		conn:                  conn,
		client:                proto.NewHarnessServiceClient(conn),
		config:                config,
		suspendActorTimeout:   h.resolvedSuspendActorTimeout(),
		preCheckpointSnapshot: actor.GetStatus().GetLatestSnapshot().GetName(),
	}, nil
}

func (h *SubstrateHarness) resolvedSuspendActorTimeout() time.Duration {
	if h.suspendActorTimeout <= 0 {
		return DefaultSuspendActorTimeout
	}
	return h.suspendActorTimeout
}

// crashRecoveryTagName derives a deterministic ActorSnapshotTag name from
// conversationID.
func crashRecoveryTagName(conversationID string) string {
	sum := sha256.Sum256([]byte(conversationID))
	// encode 32bytes digest to string using base32 so get
	// a shorter string representation (5bits per char so 52
	// chars w/o padding). So it fit in 63 chars label limit.
	enc := base32.StdEncoding.WithPadding(base32.NoPadding)
	return "crash-" + strings.ToLower(enc.EncodeToString(sum[:]))
}

// discardAndRecoverFromSnapshot recovers conversationID's actor being
// unrecoverable (CRASHED, or stuck DELETING/SUSPENDING). It reads the
// actor's own LatestSnapshot, pins it under a durable tag, deletes the
// actor record, and recreates it from that tag, which comes back SUSPENDED
// at its last completed hop.
func (h *SubstrateHarness) discardAndRecoverFromSnapshot(ctx context.Context, conversationID string, actor *ateapipb.Actor) error {
	tagName := crashRecoveryTagName(conversationID)

	snapshot := actor.GetStatus().GetLatestSnapshot()
	if snapshot == nil {
		// No checkpoint was ever completed for this actor, so there's
		// nothing to preserve; deleting and recreating it blank is safe,
		// since the caller's retry redelivers this conversation's
		// bootstrap input anyway.
		if err := h.ateClient.DeleteActor(ctx, conversationID); err != nil {
			return fmt.Errorf("failed to delete crashed actor %s (no prior snapshot to recover from): %w", conversationID, err)
		}
		if _, err := h.ateClient.CreateActor(ctx, conversationID); err != nil && status.Code(err) != codes.AlreadyExists {
			return fmt.Errorf("failed to recreate actor %s blank after a crash with no prior snapshot: %w", conversationID, err)
		}
		return nil
	}
	if err := h.ensureSnapshotTag(ctx, conversationID, tagName, snapshot); err != nil {
		return err
	}
	if err := h.ateClient.DeleteActor(ctx, conversationID); err != nil {
		return fmt.Errorf("failed to delete crashed actor %s: %w", conversationID, err)
	}
	return h.finishRecoveryFromTag(ctx, conversationID, tagName)
}

// finishRecoveryFromTag recreates conversationID's actor from tagName.
func (h *SubstrateHarness) finishRecoveryFromTag(ctx context.Context, conversationID, tagName string) error {
	if _, err := h.ateClient.CreateActorFromSnapshotTag(ctx, conversationID, tagName); err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("failed to recreate actor %s from its own last snapshot: %w", conversationID, err)
	}
	if err := h.ateClient.DeleteActorSnapshotTag(ctx, tagName); err != nil {
		// Best-effort: the new actor already has its own copy of the
		// snapshot reference, so a leaked tag only costs tidiness. A later
		// cleanup pass makes another attempt once the task completes.
		slog.WarnContext(ctx, "crash recovery: failed to clean up snapshot tag",
			slog.String("conversation_id", conversationID), slog.String("tag", tagName), slog.Any("error", err))
	}
	return nil
}

// ensureSnapshotTag creates tagName pointing at snapshot, tolerating a stale
// tag under this same deterministic name.
func (h *SubstrateHarness) ensureSnapshotTag(ctx context.Context, conversationID, tagName string, snapshot *ateapipb.ObjectRef) error {
	if _, err := h.ateClient.CreateActorSnapshotTag(ctx, tagName, snapshot); err != nil {
		if status.Code(err) != codes.AlreadyExists {
			return fmt.Errorf("failed to tag crashed actor %s's own last snapshot: %w", conversationID, err)
		}
		existing, getErr := h.ateClient.GetActorSnapshotTag(ctx, tagName)
		if getErr != nil {
			return fmt.Errorf("failed to check existing snapshot tag %s for crashed actor %s: %w", tagName, conversationID, getErr)
		}
		if !protolib.Equal(existing.GetSnapshot(), snapshot) {
			slog.WarnContext(ctx, "crash recovery: found a stale snapshot tag left over from an earlier crash of this actor, repointing it at the current crash's own snapshot",
				slog.String("conversation_id", conversationID), slog.String("tag", tagName))
			if err := h.ateClient.DeleteActorSnapshotTag(ctx, tagName); err != nil {
				return fmt.Errorf("failed to discard stale snapshot tag %s for crashed actor %s: %w", tagName, conversationID, err)
			}
			if _, err := h.ateClient.CreateActorSnapshotTag(ctx, tagName, snapshot); err != nil {
				return fmt.Errorf("failed to re-tag crashed actor %s's own last snapshot after discarding a stale tag: %w", conversationID, err)
			}
		}
	}
	return nil
}

// waitForHealthy blocks until the harness behind conn reports SERVING via the
// standard gRPC health protocol until timeout. A harness that is reachable
// but does not implement the health service (Unimplemented) is treated as
// ready; connection failures (Unavailable) and NOT_SERVING are retried.
func waitForHealthy(ctx context.Context, conn *grpc.ClientConn, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	client := grpc_health_v1.NewHealthClient(conn)
	const maxBackoff = 2 * time.Second
	backoff := 100 * time.Millisecond
	for {
		resp, err := client.Check(ctx, &grpc_health_v1.HealthCheckRequest{Service: ""})
		if err == nil && resp.GetStatus() == grpc_health_v1.HealthCheckResponse_SERVING {
			return nil
		}
		if status.Code(err) == codes.Unimplemented {
			// Reachable but no health service: the port is up, proceed.
			return nil
		}
		if err != nil {
			// conn's first connection attempt can fail before the worker is
			// ready (e.g. a 502 from a router proxying to a not-yet-listening
			// backend), pushing its subchannel into its own, much longer
			// reconnect backoff. Reset it so this loop's retry isn't stuck
			// waiting out that timer.
			conn.ResetConnectBackoff()
		}

		select {
		case <-ctx.Done():
			if err != nil {
				return fmt.Errorf("harness not healthy within %s: %w", timeout, err)
			}
			return fmt.Errorf("harness not healthy within %s (last status: %s)", timeout, resp.GetStatus())
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

type substrateExecution struct {
	harness             *SubstrateHarness
	conversationID      string
	execID              string
	conn                *grpc.ClientConn
	client              proto.HarnessServiceClient
	config              []byte
	suspendActorTimeout time.Duration

	// preCheckpointSnapshot is this actor's own LatestSnapshot name before
	// this turn ran.
	preCheckpointSnapshot string

	mu      sync.Mutex
	pending []*proto.Step
}

func (e *substrateExecution) ID() string {
	return e.execID
}

func (e *substrateExecution) Queue(ctx context.Context, steps ...*proto.Step) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.pending = append(e.pending, steps...)
	return nil
}

func (e *substrateExecution) Run(ctx context.Context, handler harness.Handler) error {
	ctx, span := otel.Tracer("substrate-harness").Start(ctx, "Run")
	defer span.End()

	e.mu.Lock()
	inputs := e.pending
	e.pending = nil
	e.mu.Unlock()

	stream, err := e.client.Connect(ctx)
	if err != nil {
		return fmt.Errorf("failed to open harness service stream: %w", err)
	}

	// Send a HarnessRequest to initiate the turn.
	start := &proto.HarnessRequest{
		ConversationId: e.conversationID,
		AgentId:        e.harness.harnessID,
		Type: &proto.HarnessRequest_Start{
			Start: &proto.HarnessStart{
				AgentConfig: e.config,
				Steps:       inputs,
			},
		},
	}
	// A server that fails before reading the start frame makes Send/CloseSend
	// report io.EOF; the real status is surfaced by DrainStream's Recv below, so
	// only treat non-EOF errors as send failures.
	if err := stream.Send(start); err != nil && err != io.EOF {
		return fmt.Errorf("failed to send harness start: %w", err)
	}

	// Close send direction to trigger server processing.
	if err := stream.CloseSend(); err != nil && err != io.EOF {
		return fmt.Errorf("failed to close stream send direction: %w", err)
	}

	// Drain HarnessResponse frames until the terminal HarnessEnd.
	return harness.DrainStream(ctx, stream, e.execID, handler)
}

func (e *substrateExecution) Checkpoint(ctx context.Context) error {
	slog.InfoContext(ctx, "Suspending SubstrATE actor",
		slog.String("conversation_id", e.conversationID),
		slog.String("exec_id", e.execID),
	)
	start := time.Now()
	timeout := e.suspendActorTimeout
	if timeout <= 0 {
		timeout = DefaultSuspendActorTimeout
	}
	suspendCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	resp, err := e.harness.ateClient.SuspendActor(suspendCtx, e.conversationID)
	elapsed := time.Since(start)
	if err != nil {
		// Diagnostic only: this timeout is set longer than Substrate's own
		// server-side ceiling on SuspendActor, so deadline_exceeded here
		// means Substrate itself gave up first, not that this call was
		// merely slow.
		slog.WarnContext(ctx, "SuspendActor call itself failed or timed out",
			slog.String("conversation_id", e.conversationID),
			slog.String("exec_id", e.execID),
			slog.Duration("elapsed", elapsed),
			slog.Duration("timeout", timeout),
			slog.Bool("deadline_exceeded", errors.Is(suspendCtx.Err(), context.DeadlineExceeded)),
			slog.Any("error", err))
		return fmt.Errorf("failed to suspend substrate actor %s: %w", e.conversationID, err)
	}
	if elapsed > timeout/2 {
		slog.WarnContext(ctx, "SuspendActor call succeeded but took most of its own client-side timeout budget",
			slog.String("conversation_id", e.conversationID),
			slog.String("exec_id", e.execID),
			slog.Duration("elapsed", elapsed),
			slog.Duration("timeout", timeout))
	}
	// An unchanged name here, despite SuspendActor reporting success, means
	// this turn's progress was silently not recorded (e.g. because of worker
	// crash). Returning an error here keeps the turn uncommitted (PENDING),
	// so the caller's retry loop re-attempts the whole turn from a fresh Start.
	got := resp.GetActor().GetStatus().GetLatestSnapshot().GetName()
	if got == "" || got == e.preCheckpointSnapshot {
		slog.WarnContext(ctx, "SuspendActor reported success but this actor's own LatestSnapshot did not advance -- treating as a checkpoint failure",
			slog.String("conversation_id", e.conversationID),
			slog.String("exec_id", e.execID),
			slog.String("pre_checkpoint_snapshot", e.preCheckpointSnapshot),
			slog.String("post_checkpoint_snapshot", got))
		return fmt.Errorf("SuspendActor for substrate actor %s reported success but its own LatestSnapshot did not advance (still %q): this turn's own state was not actually recorded durably", e.conversationID, got)
	}
	return nil
}

func (e *substrateExecution) Close(ctx context.Context) error {
	if e.conn != nil {
		e.conn.Close()
	}
	return nil
}
