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

// Package stub provides a fake Agent Substrate control API for testing.
package stub

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"log"
	"math/big"
	"net"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/uuid"
	"github.com/ktock/checkpointd/harness"
	"github.com/ktock/checkpointd/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

// ServeOptions configures Serve.
type ServeOptions struct {
	// HarnessAddr is where the given proto.HarnessServiceServer is served,
	// in plaintext, like a real actor's HarnessService worker port
	HarnessAddr string
	// ControlAddr is where the fake Agent Substrate control API is served.
	ControlAddr string
}

func (o ServeOptions) withDefaults() ServeOptions {
	if o.HarnessAddr == "" {
		o.HarnessAddr = "127.0.0.1:50060"
	}
	if o.ControlAddr == "" {
		o.ControlAddr = "127.0.0.1:50061"
	}
	return o
}

// Serve starts the fake Agent Substrate control API.
func Serve(opts ServeOptions, h proto.HarnessServiceServer) error {
	opts = opts.withDefaults()

	cert, err := selfSignedCert()
	if err != nil {
		return fmt.Errorf("generate self-signed cert: %w", err)
	}

	controlLis, err := net.Listen("tcp", opts.ControlAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", opts.ControlAddr, err)
	}
	controlSrv := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
	})))
	host, _, err := net.SplitHostPort(opts.HarnessAddr)
	if err != nil || host == "" || host == "0.0.0.0" {
		host = "127.0.0.1"
	}
	ateapipb.RegisterControlServer(controlSrv, &controlServer{workerHost: host})

	harnessLis, err := net.Listen("tcp", opts.HarnessAddr)
	if err != nil {
		controlLis.Close()
		return fmt.Errorf("listen on %s: %w", opts.HarnessAddr, err)
	}
	harnessSrv := grpc.NewServer(grpc.KeepaliveEnforcementPolicy(harness.KeepaliveEnforcementPolicy))
	proto.RegisterHarnessServiceServer(harnessSrv, h)

	errCh := make(chan error, 2)
	go func() {
		log.Printf("fake control API listening on %s (TLS)", opts.ControlAddr)
		errCh <- controlSrv.Serve(controlLis)
	}()
	go func() {
		log.Printf("HarnessService listening on %s", opts.HarnessAddr)
		errCh <- harnessSrv.Serve(harnessLis)
	}()
	return <-errCh
}

func selfSignedCert() (tls.Certificate, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("generate key: %w", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("generate serial: %w", err)
	}
	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "ax harness local control API stub"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:              []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &priv.PublicKey, priv)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("create certificate: %w", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("marshal key: %w", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return tls.X509KeyPair(certPEM, keyPEM)
}

// controlServer is a minimal stand-in for the Agent Substrate Control API.
type controlServer struct {
	ateapipb.UnimplementedControlServer
	workerHost string
	mu        sync.Mutex
	snapshots map[string]string
	actors    map[string]*ateapipb.ActorStatus
}

func (c *controlServer) CreateAtespace(_ context.Context, req *ateapipb.CreateAtespaceRequest) (*ateapipb.Atespace, error) {
	return &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: req.GetAtespace().GetMetadata().GetName()}}, nil
}

func (c *controlServer) CreateActor(_ context.Context, req *ateapipb.CreateActorRequest) (*ateapipb.Actor, error) {
	name := req.GetActor().GetMetadata().GetName()
	c.mu.Lock()
	if c.actors == nil {
		c.actors = make(map[string]*ateapipb.ActorStatus)
	}
	if _, exists := c.actors[name]; !exists {
		c.actors[name] = &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED}
	}
	c.mu.Unlock()
	return &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Name: name}}, nil
}

// GetActor reports the actor's own last-recorded state.
func (c *controlServer) GetActor(_ context.Context, req *ateapipb.GetActorRequest) (*ateapipb.Actor, error) {
	name := req.GetActor().GetName()
	c.mu.Lock()
	defer c.mu.Unlock()
	st, ok := c.actors[name]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "actor %s not found", name)
	}
	return &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Name: name}, Status: st}, nil
}

func (c *controlServer) ResumeActor(_ context.Context, req *ateapipb.ResumeActorRequest) (*ateapipb.ResumeActorResponse, error) {
	name := req.GetActor().GetName()
	c.mu.Lock()
	latest := c.snapshots[name]
	var latestSnapshot *ateapipb.ObjectRef
	if latest != "" {
		latestSnapshot = &ateapipb.ObjectRef{Name: latest}
	}
	st := &ateapipb.ActorStatus{
		State:            ateapipb.ActorState_ACTOR_STATE_RUNNING,
		WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPodIp: c.workerHost},
		LatestSnapshot:   latestSnapshot,
	}
	if c.actors == nil {
		c.actors = make(map[string]*ateapipb.ActorStatus)
	}
	c.actors[name] = st
	c.mu.Unlock()
	return &ateapipb.ResumeActorResponse{Actor: &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Name: name},
		Status:   st,
	}}, nil
}

func (c *controlServer) SuspendActor(_ context.Context, req *ateapipb.SuspendActorRequest) (*ateapipb.SuspendActorResponse, error) {
	name := req.GetActor().GetName()
	// A genuinely successful checkpoint always advances LatestSnapshot to a
	// fresh name (see this struct's own snapshots field doc comment) -- mint
	// one here too.
	snapshot := uuid.NewString()
	c.mu.Lock()
	if c.snapshots == nil {
		c.snapshots = make(map[string]string)
	}
	c.snapshots[name] = snapshot
	st := &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED, LatestSnapshot: &ateapipb.ObjectRef{Name: snapshot}}
	if c.actors == nil {
		c.actors = make(map[string]*ateapipb.ActorStatus)
	}
	c.actors[name] = st
	c.mu.Unlock()
	return &ateapipb.SuspendActorResponse{Actor: &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Name: name},
		Status:   st,
	}}, nil
}

func (c *controlServer) DeleteActor(_ context.Context, req *ateapipb.DeleteActorRequest) (*ateapipb.Actor, error) {
	name := req.GetActor().GetName()
	c.mu.Lock()
	delete(c.actors, name)
	c.mu.Unlock()
	return &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Name: name}}, nil
}

// GetActorSnapshotTag always reports NotFound.
func (c *controlServer) GetActorSnapshotTag(_ context.Context, req *ateapipb.GetActorSnapshotTagRequest) (*ateapipb.ActorSnapshotTag, error) {
	return nil, status.Errorf(codes.NotFound, "ActorSnapshot tag %s not found", req.GetActorSnapshotTag().GetName())
}
