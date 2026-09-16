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

// Package ate provides a client for the Agent Substrate Control API.
package ate

import (
	"context"
	"fmt"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type Client struct {
	namespace string
	template  string
	conn      *grpc.ClientConn
}

// NewClient creates a new actor client.
func NewClient(ns, template, target string, opts ...grpc.DialOption) (*Client, error) {
	if ns == "" {
		return nil, fmt.Errorf("namespace cannot be empty")
	}
	if template == "" {
		return nil, fmt.Errorf("template cannot be empty")
	}
	if target == "" {
		target = "api.ate-system.svc:443"
	}
	if len(opts) == 0 {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		return nil, fmt.Errorf("error when creating Control client: %w", err)
	}
	return &Client{
		namespace: ns,
		template:  template,
		conn:      conn,
	}, nil
}

// CreateActor creates a new actor.
func (c *Client) CreateActor(ctx context.Context, id string) (*ateapipb.Actor, error) {
	client := ateapipb.NewControlClient(c.conn)
	// TODO(wjjclaud): Configure atespace in manifests instead of reusing the namespace.
	if _, err := client.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{
		Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: c.namespace}},
	}); err != nil && status.Code(err) != codes.AlreadyExists {
		return nil, fmt.Errorf("error when calling Control.CreateAtespace: %w", err)
	}
	actor, err := client.CreateActor(ctx, &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata:               &ateapipb.ResourceMetadata{Atespace: c.namespace, Name: id},
			ActorTemplateNamespace: c.namespace,
			ActorTemplateName:      c.template,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("error when calling Control.CreateActor: %w", err)
	}
	return actor, nil
}

// GetActor retrieves the current state of the actor backing conversationID.
func (c *Client) GetActor(ctx context.Context, id string) (*ateapipb.Actor, error) {
	client := ateapipb.NewControlClient(c.conn)
	actor, err := client.GetActor(ctx, &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: c.namespace, Name: id},
	})
	if err != nil {
		return nil, fmt.Errorf("error when calling Control.GetActor: %w", err)
	}
	return actor, nil
}

// CreateActorSnapshotTag pins snapshot under tagName, a durable alias that
// survives deleting the actor that produced it.
func (c *Client) CreateActorSnapshotTag(ctx context.Context, tagName string, snapshot *ateapipb.ObjectRef) (*ateapipb.ActorSnapshotTag, error) {
	client := ateapipb.NewControlClient(c.conn)
	tag, err := client.CreateActorSnapshotTag(ctx, &ateapipb.CreateActorSnapshotTagRequest{
		ActorSnapshotTag: &ateapipb.ActorSnapshotTag{
			Metadata: &ateapipb.ResourceMetadata{Atespace: c.namespace, Name: tagName},
			Snapshot: snapshot,
			Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("error when calling Control.CreateActorSnapshotTag: %w", err)
	}
	return tag, nil
}

// GetActorSnapshotTag retrieves tagName's current definition, including which
// ActorSnapshot it points at.
func (c *Client) GetActorSnapshotTag(ctx context.Context, tagName string) (*ateapipb.ActorSnapshotTag, error) {
	client := ateapipb.NewControlClient(c.conn)
	tag, err := client.GetActorSnapshotTag(ctx, &ateapipb.GetActorSnapshotTagRequest{
		ActorSnapshotTag: &ateapipb.ObjectRef{Atespace: c.namespace, Name: tagName},
	})
	if err != nil {
		return nil, fmt.Errorf("error when calling Control.GetActorSnapshotTag: %w", err)
	}
	return tag, nil
}

// DeleteActorSnapshotTag removes tagName, treating an already-gone tag as success.
func (c *Client) DeleteActorSnapshotTag(ctx context.Context, tagName string) error {
	client := ateapipb.NewControlClient(c.conn)
	_, err := client.DeleteActorSnapshotTag(ctx, &ateapipb.DeleteActorSnapshotTagRequest{
		ActorSnapshotTag: &ateapipb.ObjectRef{Atespace: c.namespace, Name: tagName},
	})
	if err != nil && status.Code(err) != codes.NotFound {
		return fmt.Errorf("error when calling Control.DeleteActorSnapshotTag: %w", err)
	}
	return nil
}

// CreateActorFromSnapshotTag creates a new actor named id, already SUSPENDED and seeded
// from the ActorSnapshot tagName points at.
func (c *Client) CreateActorFromSnapshotTag(ctx context.Context, id, tagName string) (*ateapipb.Actor, error) {
	client := ateapipb.NewControlClient(c.conn)
	actor, err := client.CreateActor(ctx, &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata:               &ateapipb.ResourceMetadata{Atespace: c.namespace, Name: id},
			ActorTemplateNamespace: c.namespace,
			ActorTemplateName:      c.template,
			SourceSnapshotTag:      &ateapipb.ObjectRef{Atespace: c.namespace, Name: tagName},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("error when calling Control.CreateActor (from snapshot tag): %w", err)
	}
	return actor, nil
}

// ResumeActor resumes the actor, scheduling it onto a worker. The returned
// actor carries the worker IP once it is running.
func (c *Client) ResumeActor(ctx context.Context, id string) (*ateapipb.ResumeActorResponse, error) {
	client := ateapipb.NewControlClient(c.conn)
	resp, err := client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: c.namespace, Name: id},
	})
	if err != nil {
		return nil, fmt.Errorf("error when calling Control.ResumeActor: %w", err)
	}
	return resp, nil
}

// DeleteActor deletes the actor regardless of its current state, treating NotFound as success.
func (c *Client) DeleteActor(ctx context.Context, id string) error {
	client := ateapipb.NewControlClient(c.conn)
	_, err := client.DeleteActor(ctx, &ateapipb.DeleteActorRequest{
		Actor:    &ateapipb.ObjectRef{Atespace: c.namespace, Name: id},
		AnyState: true,
	})
	if err != nil && status.Code(err) != codes.NotFound {
		return fmt.Errorf("error when calling Control.DeleteActor: %w", err)
	}
	return nil
}

// DeleteWorker deregisters worker via a control-plane update.
func (c *Client) DeleteWorker(ctx context.Context, worker *ateapipb.ObjectRef) error {
	client := ateapipb.NewControlClient(c.conn)
	_, err := client.DeleteWorker(ctx, &ateapipb.DeleteWorkerRequest{Worker: worker})
	if err != nil && status.Code(err) != codes.NotFound {
		return fmt.Errorf("error when calling Control.DeleteWorker: %w", err)
	}
	return nil
}

// SuspendActor suspends the actor.
func (c *Client) SuspendActor(ctx context.Context, id string) (*ateapipb.SuspendActorResponse, error) {
	client := ateapipb.NewControlClient(c.conn)
	resp, err := client.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: c.namespace, Name: id},
	})
	if err != nil {
		return nil, fmt.Errorf("error when calling Control.SuspendActor: %w", err)
	}
	return resp, nil
}

// Close closes the gRPC connection.
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
