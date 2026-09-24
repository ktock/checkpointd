// Copyright 2026 Google LLC
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

package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2asrv"
	config "github.com/ktock/checkpointd/internal/config/checkpointd"
	"github.com/ktock/checkpointd/internal/controller"
	"github.com/ktock/checkpointd/internal/controller/eventlog"
	"github.com/ktock/checkpointd/internal/harness/substrate"
	"github.com/ktock/checkpointd/internal/hop"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	serveConfigFile                string
	serveAddr                      string
	serveBaseURL                   string
	serveDiscover                  bool
	serveDiscoveryInterval         time.Duration
	serveSubstrateEndpoint         string
	serveSubstrateCAFile           string
	serveSubstrateTokenFile        string
	serveSubstrateRouterAddr       string
	serveLogRetentionPeriod        time.Duration
	serveLogRetentionSweepInterval time.Duration
	serveLogLevel                  string
	servePodName                   string
	servePodUID                    string
	serveSalvageSweepInterval      time.Duration
)

func init() {
	rootCmd.Flags().StringVar(&serveLogLevel, "log-level", "info", "log level: panic, fatal, error, warn, info, debug, or trace")
	rootCmd.Flags().StringVar(&serveConfigFile, "config", "/etc/checkpointd/checkpointd.yaml", "Path to the configuration file")
	rootCmd.Flags().StringVar(&serveAddr, "addr", ":80", "address to serve on")
	rootCmd.Flags().StringVar(&serveBaseURL, "base-url", "http://localhost:80", "this server's own externally-reachable base URL, stamped into each agent's AgentCard (e.g. http://checkpointd.<namespace>.svc:80)")
	rootCmd.Flags().BoolVar(&serveDiscover, "discover", false, "discover agents from Kubernetes ActorTemplate CRDs instead of the config file. Requires an in-cluster ServiceAccount granted get/list/watch on actortemplates.ate.dev")
	rootCmd.Flags().DurationVar(&serveDiscoveryInterval, "discovery-interval", 30*time.Second, "how often to re-list ActorTemplate CRDs when --discover is set")
	rootCmd.Flags().StringVar(&serveSubstrateEndpoint, "substrate-endpoint", "", "Agent Substrate Control API address each discovered agent's harness dials; empty uses the ate package's own default")
	rootCmd.Flags().StringVar(&serveSubstrateCAFile, "substrate-ca-file", "", "PEM file with the Agent Substrate Control API's trust bundle (e.g. a mounted clusterTrustBundle projected volume), used to verify its server certificate; empty trusts any certificate, which is only appropriate for local testing.")
	rootCmd.Flags().StringVar(&serveSubstrateTokenFile, "substrate-token-file", "/var/run/secrets/ate-system/token", "bearer token file for the Agent Substrate Control API, re-read on every call; its existence also decides whether a worker is dialed directly (file absent: local/testing) or CONNECT-tunneled through atenet-router (file present: a real cluster's NetworkPolicy requires it)")
	rootCmd.Flags().StringVar(&serveSubstrateRouterAddr, "substrate-router-addr", "atenet-router.ate-system.svc:8081", "atenet-router's in-cluster CONNECT-ingress address, used to reach actor worker ports when --substrate-token-file exists")
	rootCmd.Flags().DurationVar(&serveLogRetentionPeriod, "log-retention-period", 0, "how long to keep a completed session's history after it becomes terminal. 0 (default) disables retention and keeps every session's history forever")
	rootCmd.Flags().DurationVar(&serveLogRetentionSweepInterval, "log-retention-sweep-interval", time.Hour, "how often to check for expired session history when --log-retention-period is set")
	rootCmd.Flags().StringVar(&servePodName, "pod-name", "", "this replica's own stable identity, durably recorded as each session's owner so ownership survives a restart under the same identity -- pass the pod's own name via the Kubernetes Downward API. Required when the event log is Postgres-backed (multi-replica); ignored for SQLite (single-replica only, ownership is implicit)")
	rootCmd.Flags().StringVar(&servePodUID, "pod-uid", "", "this replica's own pod's Kubernetes UID (Downward API metadata.uid), durably recorded as each session's owner alongside --pod-name. Required when the event log is Postgres-backed (multi-replica); ignored for SQLite, same as --pod-name")
	rootCmd.Flags().DurationVar(&serveSalvageSweepInterval, "salvage-sweep-interval", time.Minute, "how often this instance scans sessions cluster-wide to find sessions whose recorded owner pod no longer exists (crashed, evicted, or force-deleted without a graceful drain) and salvages them by claiming and resuming their relay loop locally. A no-op unless both --pod-name and --pod-uid are set")
}

func runServe(cmd *cobra.Command, args []string) error {
	level, err := logrus.ParseLevel(serveLogLevel)
	if err != nil {
		return fmt.Errorf("invalid --log-level %q: %w", serveLogLevel, err)
	}
	setLogLevel(level)

	ctx, cancel := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg, err := config.LoadFromFile(serveConfigFile)
	if err != nil {
		return fmt.Errorf("loading config %q: %w", serveConfigFile, err)
	}
	if cfg.EventLog.PostgresConfig.DSN != "" && (servePodName == "" || servePodUID == "") {
		return fmt.Errorf("--pod-name and --pod-uid are both required when eventlog is Postgres-backed (multi-replica)")
	}
	ctrlOpts := substrate.ControlAPIOptions{
		CAFile:     serveSubstrateCAFile,
		TokenFile:  serveSubstrateTokenFile,
		RouterAddr: serveSubstrateRouterAddr,
	}

	c, el, err := newController(ctx, cfg, ctrlOpts)
	if err != nil {
		return fmt.Errorf("creating controller: %w", err)
	}
	defer c.Close()

	db, dialect, ok := eventlog.SQLDB(el)
	if !ok {
		return fmt.Errorf("checkpointd requires a SQL-backed event log (sqlite or postgres), not %T", el)
	}
	store, err := newSQLTaskStore(db, dialect, el)
	if err != nil {
		return fmt.Errorf("creating task store: %w", err)
	}

	registry := newTaskRegistry()

	if err := resumeServerSessions(ctx, c, el, store, registry, servePodName, servePodUID); err != nil {
		return fmt.Errorf("resuming server sessions: %w", err)
	}

	if servePodName != "" && servePodUID != "" {
		checker, err := newPodExistenceChecker()
		if err != nil {
			return fmt.Errorf("creating salvage pod existence checker: %w", err)
		}
		namespace := os.Getenv(checkpointdNamespaceEnv)
		log.Infof("pod %s (uid %s): starting the orphan-session salvage loop (interval %s)", servePodName, servePodUID, serveSalvageSweepInterval)
		go runSalvageLoop(ctx, c, el, store, registry, checker, namespace, servePodName, servePodUID, serveSalvageSweepInterval)
	} else {
		log.Infof("pod %s: --pod-name/--pod-uid not set, not running the orphan-session salvage loop (SQLite/single-replica)", servePodName)
	}

	if serveLogRetentionPeriod > 0 {
		go runRetentionLoop(ctx, db, registry, serveLogRetentionPeriod, serveLogRetentionSweepInterval)
	}

	var cards *agentCardStore
	if serveDiscover {
		disc, err := newDiscoveryClient()
		if err != nil {
			return fmt.Errorf("creating discovery client: %w", err)
		}
		cards = newAgentCardStore(nil)
		discState := &discoveryState{}
		if err := discoverAndApply(ctx, disc, serveSubstrateEndpoint, ctrlOpts, c.Registry(), cards, discState); err != nil {
			return fmt.Errorf("initial agent discovery: %w", err)
		}
		go runDiscoveryLoop(ctx, disc, serveSubstrateEndpoint, ctrlOpts, c.Registry(), cards, serveDiscoveryInterval, discState)
	} else {
		agentCards := make(map[string]*config.AgentCardConfig, len(cfg.Registry.Substrate))
		for _, sc := range cfg.Registry.Substrate {
			if sc.AgentCard == nil {
				return fmt.Errorf("substrate harness %q: agent_card is required", sc.ID)
			}
			agentCards[sc.ID] = sc.AgentCard
		}
		cards = newAgentCardStore(agentCards)
	}
	draining := &atomic.Bool{}
	mux := buildServerMux(cards, serveBaseURL, c, el, store, registry, servePodName, servePodUID, draining)

	srv := &http.Server{Addr: serveAddr, Handler: checkTransportPreconditions(mux)}
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-ctx.Done()
		// Stop accepting new work first, then wait for every session this
		// instance still owns to reach a stopping point and release on its
		// own.
		draining.Store(true)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = srv.Shutdown(shutdownCtx)
		cancel()
		if servePodName == "" {
			return
		}
		for {
			n, err := store.CountOwnedRunningSessions(context.Background(), servePodName)
			if err != nil {
				log.Infof("drain: counting owned running sessions: %v", err)
				return
			}
			if n == 0 {
				return
			}
			log.Infof("drain: waiting for %d owned running session(s) to finish", n)
			time.Sleep(3 * time.Second)
		}
	}()

	log.Infof("server listening on %s", serveAddr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	<-shutdownDone
	return nil
}

// agentCardStore holds the current set of externally exposed agents'
// config.AgentCardConfig, keyed by agent id.
type agentCardStore struct {
	v atomic.Pointer[map[string]*config.AgentCardConfig]
}

func newAgentCardStore(initial map[string]*config.AgentCardConfig) *agentCardStore {
	s := &agentCardStore{}
	if initial != nil {
		s.store(initial)
	}
	return s
}

func (s *agentCardStore) store(cards map[string]*config.AgentCardConfig) {
	s.v.Store(&cards)
}

// get returns name's config.AgentCardConfig and whether it's currently known.
func (s *agentCardStore) get(name string) (*config.AgentCardConfig, bool) {
	m := s.v.Load()
	if m == nil {
		return nil, false
	}
	c, ok := (*m)[name]
	return c, ok
}

// ready reports whether the store has completed its first sync, whether or
// not that sync actually found any agents -- for --discover, that's its
// first pass (run synchronously before this server ever starts accepting
// connections); for a static --config list, this is true from the moment
// the server starts serving at all.
func (s *agentCardStore) ready() bool {
	return s.v.Load() != nil
}

func buildServerMux(cards *agentCardStore, baseURL string, c *controller.Controller, el eventlog.EventLog, store *sqlTaskStore, registry *taskRegistry, podName, podUID string, draining *atomic.Bool) http.Handler {
	mux := http.NewServeMux()
	base := strings.TrimSuffix(baseURL, "/")

	// /ready is a Kubernetes readinessProbe target: 200 once cards.ready(),
	// 503 otherwise.
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		if !cards.ready() {
			http.Error(w, "no agents discovered/configured yet", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	// /healthz is a Kubernetes livenessProbe target: a bounded-time DB ping,
	// so a hung instance (e.g. a wedged connection pool) fails fast instead
	// of hanging the probe itself.
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		if err := store.Ping(ctx); err != nil {
			http.Error(w, fmt.Sprintf("database unreachable: %v", err), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/agents/{name}"+a2asrv.WellKnownAgentCardPath, func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		content, ok := cards.get(name)
		if !ok {
			http.NotFound(w, r)
			return
		}
		card := buildAgentCard(content, name, base+"/agents/"+name+"/")
		a2asrv.NewStaticAgentCardHandler(card).ServeHTTP(w, r)
	})
	mux.HandleFunc("/agents/{name}/", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		if _, ok := cards.get(name); !ok {
			http.NotFound(w, r)
			return
		}
		handler := &serverRequestHandler{agent: name, c: c, el: el, store: store, registry: registry, podName: podName, podUID: podUID, draining: draining}
		a2asrv.NewJSONRPCHandler(handler).ServeHTTP(w, r)
	})
	return mux
}

// resumeServerSessions re-discovers every session this instance still owns
// by name (owner_pod == podName) that isn't already TERMINATED and
// resumes each one in its own goroutine -- called once on startup, so a
// restart picks back up every relay loop this same instance was still
// driving. Never touches a session already TERMINATED, nor one owned by a
// different instance or already released (owner_pod == ""):
// ListResumableSessions' own filter excludes both.
func resumeServerSessions(ctx context.Context, c *controller.Controller, el eventlog.EventLog, store *sqlTaskStore, registry *taskRegistry, podName, podUID string) error {
	pageToken := ""
	for {
		sessions, next, err := store.ListResumableSessions(ctx, pageToken, podName)
		if err != nil {
			return fmt.Errorf("listing resumable sessions: %w", err)
		}
		for _, sess := range sessions {
			restamped, err := store.ClaimOrphanedSession(ctx, sess.id, podName, podUID, sess.ownerUID)
			if err != nil {
				return fmt.Errorf("re-stamping owner_uid for session %s: %w", sess.id, err)
			}
			if !restamped {
				log.Infof("session %s: owner_uid changed before this restart could re-stamp it (claimed by a concurrent salvage sweep instead), not resuming locally", sess.id)
				continue
			}
			if err := resumeClaimedSession(ctx, c, el, store, registry, sess.id, sess.state); err != nil {
				log.Infof("session %s: resuming after restart: %v", sess.id, err)
				mustRevertClaim(context.Background(), store, sess.id, podName, sess.ownerUID, podUID)
			}
		}
		if next == "" {
			break
		}
		pageToken = next
	}
	return nil
}

// mustRevertClaim reverts sessionID's ownership from claimedUID back to
// (revertOwnerPod, revertOwnerUID) -- its owner immediately before the
// caller's own now-failed claim -- rather than blanking it via
// ReleaseSession: a blank owner_pod is indistinguishable from a session
// that's legitimately idle at InputRequired, so the salvage sweep's own
// ListDistinctActiveOwners would permanently exclude it. Reverting to
// the prior (often still-dead) owner keeps the row a valid salvage
// candidate, so this failure is retried on a later sweep (this instance's
// or another's) instead of silently
// stranding the session forever.
func mustRevertClaim(ctx context.Context, store *sqlTaskStore, sessionID, revertOwnerPod, revertOwnerUID, claimedUID string) {
	if _, err := store.ClaimOrphanedSession(ctx, sessionID, revertOwnerPod, revertOwnerUID, claimedUID); err != nil {
		// forcibly lose ownership of the session. Needs restartPolicy: Never to keep
		// Kubernetes from restarting the container in place with the same UID.
		panic(fmt.Sprintf("session %s: reverting ownership to (%s, %s): %v", sessionID, revertOwnerPod, revertOwnerUID, err))
	}
}

// mustReleaseSession releases sessionID's ownership or panics trying.
func mustReleaseSession(ctx context.Context, store *sqlTaskStore, sessionID string) {
	if err := store.ReleaseSession(ctx, sessionID); err != nil {
		// forcibly lose ownership of the session. Needs restartPolicy: Never to keep
		// Kubernetes from restarting the container in place with the same UID.
		panic(fmt.Sprintf("session %s: releasing ownership: %v", sessionID, err))
	}
}

// resumeClaimedSession acquires the local registry entry for a session this
// process now durably owns and spawns the goroutine that actually resumes
// driving it. Returns a non-nil error on every path that doesn't end up
// spawning that goroutine, EXCEPT a registry conflict (see below), which
// panics instead; the caller -- which already knows this claim's own prior
// owner -- is responsible for reverting it via mustRevertClaim on a returned
// error.
func resumeClaimedSession(ctx context.Context, c *controller.Controller, el eventlog.EventLog, store *sqlTaskStore, registry *taskRegistry, sessionID, state string) error {
	bk, bootstrap, ok, err := loadServerSession(ctx, store, el, sessionID)
	if err != nil {
		return fmt.Errorf("loading session %s: %w", sessionID, err)
	}
	if !ok {
		return fmt.Errorf("session %s: row vanished before it could be resumed", sessionID)
	}
	taskCtx, release, ok := registry.acquire(context.Background(), sessionID)
	if !ok {
		// This can't happen at startup (nothing else has run yet) or from a
		// correctly functioning salvage sweep (sweeps never run
		// concurrently with each other or with resumeServerSessions on the
		// same instance, and a genuinely dead prior owner can't have a
		// live goroutine anywhere).
		panic(fmt.Sprintf("session %s: registry already has an active entry, but this call just won a fresh database-level ownership claim for it -- the single-owner invariant is broken", sessionID))
	}
	if state == sessionStateTerminating {
		// A TERMINATING session is a session whose relay loop already
		// durably decided it's done.
		log.Infof("resuming session %s (%s): finishing pending termination", sessionID, bk.ownerAgent)
		go func() {
			defer release()
			defer mustReleaseSession(context.Background(), store, sessionID)
			finishTermination(taskCtx, c, bk)
		}()
		return nil
	}
	log.Debugf("resuming session %s (%s)", sessionID, bk.ownerAgent)
	go func() {
		defer release()
		defer mustReleaseSession(context.Background(), store, sessionID)
		// No caller is waiting on this run, so the qualifies-based
		// early-return signal has nothing to notify.
		res, err := runRelayLoop(taskCtx, c, el, bk, bk.ownerAgent, bootstrap, func(*hop.Envelope) {})
		if err != nil {
			log.Infof("session %s: %v", bk.sessionID, err)
			return
		}
		msg := res.Data.Message
		if res.Data.Task != nil {
			msg = res.Data.Task.Status.Message
		}
		var text string
		if msg != nil && len(msg.Parts) > 0 {
			text = msg.Parts[0].Text()
		}
		log.Debugf("session %s: completed (%s)", bk.sessionID, text)
	}()
	return nil
}
