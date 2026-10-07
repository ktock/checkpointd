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

package ate

import (
	"context"
	"fmt"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TemplateLister lists ActorTemplates through the Agent Substrate Control API.
type TemplateLister struct {
	conn *grpc.ClientConn
}

// NewTemplateLister connects to the Control API at target, which defaults to the in-cluster address when empty.
func NewTemplateLister(target string, opts ...grpc.DialOption) (*TemplateLister, error) {
	if target == "" {
		target = DefaultTarget
	}
	if len(opts) == 0 {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		return nil, fmt.Errorf("error when creating Control client: %w", err)
	}
	return &TemplateLister{conn: conn}, nil
}

// ListActorTemplates returns every ActorTemplate in atespace, or in all atespaces when atespace is empty.
func (l *TemplateLister) ListActorTemplates(ctx context.Context, atespace string) ([]*ateapipb.ActorTemplate, error) {
	client := ateapipb.NewControlClient(l.conn)
	var templates []*ateapipb.ActorTemplate
	pageToken := ""
	for {
		resp, err := client.ListActorTemplates(ctx, &ateapipb.ListActorTemplatesRequest{Atespace: atespace, PageToken: pageToken})
		if err != nil {
			return nil, fmt.Errorf("error when calling Control.ListActorTemplates: %w", err)
		}
		templates = append(templates, resp.GetActorTemplates()...)
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			return templates, nil
		}
	}
}

// Close closes the gRPC connection.
func (l *TemplateLister) Close() error {
	return l.conn.Close()
}
