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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
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
)

// Compile-time interface assertions.
var _ harness.Harness = (*SubstrateHarness)(nil)
var _ harness.Execution = (*substrateExecution)(nil)

// healthCheckTimeout defines the maximum time Start waits for a freshly
// created/resumed actor's harness to become reachable and ready.
const healthCheckTimeout = 10 * time.Second

// workerKeepaliveParams is applied to every gRPC connection to a worker's
// HarnessService. Without it, a Run call's stream.Recv() loop has no read
// deadline of its own and can block indefinitely if the worker pod is
// killed while a call is in flight.
var workerKeepaliveParams = keepalive.ClientParameters{
	Time:                20 * time.Second,
	Timeout:             10 * time.Second,
	PermitWithoutStream: true,
}

// DefaultSuspendActorTimeout is the client-side SuspendActor timeout, kept one minute above Substrate's 10-minute maxRPCDeadline in cmd/ateapi/main.go.
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
	// egressPolicy, when set, is given to every actor before it resumes, since Substrate denies all egress to an actor without one.
	egressPolicy *ateapipb.EgressPolicy
}

// SetEgressPolicy makes every actor of this harness get policy before it resumes.
func (h *SubstrateHarness) SetEgressPolicy(policy *ateapipb.EgressPolicy) {
	h.egressPolicy = policy
}

// EgressPolicy returns the policy every actor of this harness gets, or nil if it has none.
func (h *SubstrateHarness) EgressPolicy() *ateapipb.EgressPolicy {
	return h.egressPolicy
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

// NewTemplateLister returns a client that lists ActorTemplates from the Control API at endpoint, authenticated the same way as New's harnesses.
func NewTemplateLister(endpoint string, ctrlOpts ControlAPIOptions) (*ate.TemplateLister, error) {
	controlOpts, err := controlDialOptions(ctrlOpts)
	if err != nil {
		return nil, err
	}
	return ate.NewTemplateLister(endpoint, controlOpts...)
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

// targetActorHeader names the actor atenet-router routes a CONNECT request to, as "<atespace>/<actor>".
const targetActorHeader = "ate-target-actor"

// dialThroughRouter opens a plaintext connection to the given port of the actor named name in atespace by
// CONNECT-tunneling through atenet-router, since a worker pod is not reachable directly (see defaultActorRouterAddr).
func dialThroughRouter(ctx context.Context, routerAddr, atespace, name, port string) (net.Conn, error) {
	// The router routes on the target-actor header and takes only the port from the authority.
	authority := net.JoinHostPort(name+"."+atespace, port)
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", routerAddr)
	if err != nil {
		return nil, fmt.Errorf("dialing atenet-router at %s: %w", routerAddr, err)
	}
	req := &http.Request{
		Method: http.MethodConnect,
		URL:    &url.URL{Host: authority},
		Host:   authority,
		Header: http.Header{targetActorHeader: {atespace + "/" + name}},
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

// resumeActor ensures conversationID's actor is running with an assigned
// worker, declaratively driven by whatever state GetActor reports.
func (h *SubstrateHarness) resumeActor(ctx context.Context, conversationID string) (*ateapipb.Actor, error) {
	actor, err := h.ateClient.GetActor(ctx, conversationID)
	if err != nil {
		if status.Code(err) != codes.NotFound {
			return nil, fmt.Errorf("failed to look up actor %s: %w", conversationID, err)
		}
		if _, err := h.ateClient.CreateActor(ctx, conversationID); err != nil && status.Code(err) != codes.AlreadyExists {
			return nil, fmt.Errorf("failed to create substrate actor %s: %w", conversationID, err)
		}
		return h.doResumeActor(ctx, conversationID)
	}

	switch actor.GetStatus().GetState() {
	case ateapipb.ActorState_ACTOR_STATE_SUSPENDED, ateapipb.ActorState_ACTOR_STATE_RUNNING:
		// Already resumable as-is.
	case ateapipb.ActorState_ACTOR_STATE_RESUMING:
		// Substrate's ResumeActor is safe to re-enter, so retry it directly.
	case ateapipb.ActorState_ACTOR_STATE_CRASHED, ateapipb.ActorState_ACTOR_STATE_REVERTING:
		// An interrupted revert is re-entered the same way as a fresh one.
		slog.WarnContext(ctx, "crash recovery: actor CRASHED; reverting it to its own last completed checkpoint",
			slog.String("conversation_id", conversationID),
			slog.Int64("actor_version", actor.GetMetadata().GetVersion()),
			slog.String("external_snapshot", actor.GetStatus().GetExternalSnapshot().GetSnapshotUri()))
		if err := h.revertActor(ctx, conversationID); err != nil {
			return nil, err
		}
	case ateapipb.ActorState_ACTOR_STATE_SUSPENDING:
		// Substrate's SuspendActor is safe to re-enter, so retry it before giving up.
		if _, err := h.ateClient.SuspendActor(ctx, conversationID); err != nil {
			if status.Code(err) == codes.Aborted {
				// Reverting now could roll back state that the still-running original suspend may commit.
				return nil, fmt.Errorf("actor %s's own suspend is still genuinely in progress (%s); not reverting it while it might still complete: %w", conversationID, err.Error(), err)
			}
			if actor.GetStatus().GetWorkerAssignment().GetWorker() == nil {
				// RevertActor rejects SUSPENDING and there is no worker whose release would crash the actor into a revertable state.
				return nil, fmt.Errorf("actor %s is stuck SUSPENDING without a worker to release, which Substrate cannot revert; retrying the suspend: %w", conversationID, err)
			}
			slog.WarnContext(ctx, "crash recovery: actor stuck SUSPENDING could not be re-suspended either; reverting it to its own last completed checkpoint instead",
				slog.String("conversation_id", conversationID), slog.Any("error", err))
			if err := h.releaseWorker(ctx, conversationID, actor); err != nil {
				return nil, err
			}
			if err := h.revertActor(ctx, conversationID); err != nil {
				return nil, err
			}
		}
	case ateapipb.ActorState_ACTOR_STATE_DELETING:
		// Substrate's DeleteActor releases the actor's snapshots even when its terminate step fails, so a DELETING actor cannot be recovered and its deletion is finished instead.
		slog.WarnContext(ctx, "actor is DELETING because an earlier delete of it never finished; finishing that delete so the next attempt starts from a fresh actor",
			slog.String("conversation_id", conversationID))
		if err := h.releaseWorker(ctx, conversationID, actor); err != nil {
			return nil, err
		}
		if err := h.ateClient.DeleteActor(ctx, conversationID); err != nil {
			return nil, fmt.Errorf("failed to finish deleting actor %s: %w", conversationID, err)
		}
		return nil, fmt.Errorf("actor %s was being deleted and its deletion is now finished; retry to start from a fresh actor", conversationID)
	default:
		// Transient state expected to resolve on its own.
		return nil, fmt.Errorf("actor %s is in state %s, neither resumable nor a recognized stuck state; declining to recover it. Retry later.",
			conversationID, actor.GetStatus().GetState())
	}
	return h.doResumeActor(ctx, conversationID)
}

// releaseWorker deletes the worker hosting actor, which is the only way to force an actor with an unreachable worker into CRASHED.
func (h *SubstrateHarness) releaseWorker(ctx context.Context, conversationID string, actor *ateapipb.Actor) error {
	worker := actor.GetStatus().GetWorkerAssignment().GetWorker()
	if worker == nil {
		return nil
	}
	if err := h.ateClient.DeleteWorker(ctx, worker); err != nil {
		return fmt.Errorf("failed to release actor %s from its worker %s: %w", conversationID, worker.GetName(), err)
	}
	return nil
}

// revertActor returns conversationID's actor to SUSPENDED at its last completed snapshot, ready for ResumeActor.
func (h *SubstrateHarness) revertActor(ctx context.Context, conversationID string) error {
	if _, err := h.ateClient.RevertActor(ctx, conversationID); err != nil {
		return fmt.Errorf("failed to revert actor %s to its own last snapshot: %w", conversationID, err)
	}
	return nil
}

// doResumeActor calls ResumeActor for conversationID and returns the
// resulting actor.
func (h *SubstrateHarness) doResumeActor(ctx context.Context, conversationID string) (*ateapipb.Actor, error) {
	if err := h.ensureEgressPolicy(ctx, conversationID); err != nil {
		return nil, err
	}
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

// ensureEgressPolicy gives conversationID's actor the configured egress policy, which is a no-op if it already has one.
// It runs before every resume so an actor whose first attempt failed after creation still ends up with its policy.
func (h *SubstrateHarness) ensureEgressPolicy(ctx context.Context, conversationID string) error {
	if h.egressPolicy == nil {
		return nil
	}
	if err := h.ateClient.CreateActorEgressPolicy(ctx, conversationID, h.egressPolicy); err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("failed to give actor %s its egress policy: %w", conversationID, err)
	}
	return nil
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
		dialTarget = fmt.Sprintf("%s.%s:%d", conversationID, h.namespace, h.port)
		dialOpts = append([]grpc.DialOption{
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return dialThroughRouter(ctx, h.routerAddr, h.namespace, conversationID, strconv.Itoa(h.port))
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
				slog.String("external_snapshot", actor.GetStatus().GetExternalSnapshot().GetSnapshotUri()))
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
		preCheckpointSnapshot: actor.GetStatus().GetExternalSnapshot().GetSnapshotUri(),
	}, nil
}

func (h *SubstrateHarness) resolvedSuspendActorTimeout() time.Duration {
	if h.suspendActorTimeout <= 0 {
		return DefaultSuspendActorTimeout
	}
	return h.suspendActorTimeout
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

	// preCheckpointSnapshot is this actor's own external snapshot URI before
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
	got := resp.GetActor().GetStatus().GetExternalSnapshot().GetSnapshotUri()
	if got == "" || got == e.preCheckpointSnapshot {
		slog.WarnContext(ctx, "SuspendActor reported success but this actor's own external snapshot did not advance -- treating as a checkpoint failure",
			slog.String("conversation_id", e.conversationID),
			slog.String("exec_id", e.execID),
			slog.String("pre_checkpoint_snapshot", e.preCheckpointSnapshot),
			slog.String("post_checkpoint_snapshot", got))
		return fmt.Errorf("SuspendActor for substrate actor %s reported success but its own external snapshot did not advance (still %q): this turn's own state was not actually recorded durably", e.conversationID, got)
	}
	return nil
}

func (e *substrateExecution) Close(ctx context.Context) error {
	if e.conn != nil {
		e.conn.Close()
	}
	return nil
}
