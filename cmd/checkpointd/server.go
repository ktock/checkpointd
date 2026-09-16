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

	if err := resumeServerSessions(ctx, c, el, store, registry); err != nil {
		return fmt.Errorf("resuming server sessions: %w", err)
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
	mux := buildServerMux(cards, serveBaseURL, c, el, store, registry)

	srv := &http.Server{Addr: serveAddr, Handler: checkTransportPreconditions(mux)}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Infof("server listening on %s", serveAddr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
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

func buildServerMux(cards *agentCardStore, baseURL string, c *controller.Controller, el eventlog.EventLog, store *sqlTaskStore, registry *taskRegistry) http.Handler {
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
		handler := &serverRequestHandler{agent: name, c: c, el: el, store: store, registry: registry}
		a2asrv.NewJSONRPCHandler(handler).ServeHTTP(w, r)
	})
	return mux
}

// resumeServerSessions re-discovers every session checkpointd server has
// created that isn't already TERMINATED and resumes each one in its own
// goroutine -- called once on startup, so a restart picks back up every
// relay loop that was still in flight. Never touches a session already
// TERMINATED at all.
func resumeServerSessions(ctx context.Context, c *controller.Controller, el eventlog.EventLog, store *sqlTaskStore, registry *taskRegistry) error {
	pageToken := ""
	for {
		sessions, next, err := store.ListResumableSessions(ctx, pageToken)
		if err != nil {
			return fmt.Errorf("listing resumable sessions: %w", err)
		}
		for _, sess := range sessions {
			bk, bootstrap, ok, err := loadServerSession(ctx, store, el, sess.id)
			if err != nil {
				return fmt.Errorf("loading session %s: %w", sess.id, err)
			}
			if !ok {
				continue
			}
			taskCtx, release, ok := registry.acquire(context.Background(), sess.id)
			if !ok {
				// Can't happen at startup (nothing else has run yet), but
				// don't silently double-drive a session if it somehow did.
				log.Infof("session %s: registry already has an active entry at startup, skipping resume", sess.id)
				continue
			}
			if sess.state == sessionStateTerminating {
				// A TERMINATING session is a session whose relay loop
				// already durably decided it's done.
				log.Infof("resuming session %s (%s): finishing pending termination", sess.id, bk.ownerAgent)
				go func() {
					defer release()
					finishTermination(taskCtx, c, bk)
				}()
				continue
			}
			log.Debugf("resuming session %s (%s)", sess.id, bk.ownerAgent)
			go func() {
				defer release()
				// No caller is waiting on this run, so the
				// qualifies-based early-return signal has nothing to
				// notify.
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
		}
		if next == "" {
			break
		}
		pageToken = next
	}
	return nil
}
