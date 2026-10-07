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
	"slices"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/ktock/checkpointd/internal/harness/harnesstest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func template(atespace, name string) *ateapipb.ActorTemplate {
	return &ateapipb.ActorTemplate{Metadata: &ateapipb.ResourceMetadata{Atespace: atespace, Name: name}}
}

func names(templates []*ateapipb.ActorTemplate) []string {
	var out []string
	for _, t := range templates {
		out = append(out, t.GetMetadata().GetAtespace()+"/"+t.GetMetadata().GetName())
	}
	return out
}

func newLister(t *testing.T, ctrl *harnesstest.MockControlServer) *TemplateLister {
	t.Helper()
	l, err := NewTemplateLister(harnesstest.StartControlServer(t, ctrl), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("NewTemplateLister: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

func TestTemplateLister_FollowsEveryPage(t *testing.T) {
	ctrl := &harnesstest.MockControlServer{
		Templates:        []*ateapipb.ActorTemplate{template("ns", "a"), template("ns", "b"), template("ns", "c"), template("ns", "d"), template("ns", "e")},
		TemplatePageSize: 2,
	}
	got, err := newLister(t, ctrl).ListActorTemplates(context.Background(), "ns")
	if err != nil {
		t.Fatalf("ListActorTemplates: %v", err)
	}
	if want := []string{"ns/a", "ns/b", "ns/c", "ns/d", "ns/e"}; !slices.Equal(names(got), want) {
		t.Errorf("templates = %v, want %v", names(got), want)
	}
	if calls := ctrl.ListedAtespaces(); len(calls) != 3 {
		t.Errorf("ListActorTemplates calls = %v, want 3 pages", calls)
	}
}

func TestTemplateLister_EmptyAtespaceListsEveryAtespace(t *testing.T) {
	ctrl := &harnesstest.MockControlServer{Templates: []*ateapipb.ActorTemplate{template("one", "a"), template("two", "b")}}
	got, err := newLister(t, ctrl).ListActorTemplates(context.Background(), "")
	if err != nil {
		t.Fatalf("ListActorTemplates: %v", err)
	}
	if want := []string{"one/a", "two/b"}; !slices.Equal(names(got), want) {
		t.Errorf("templates = %v, want %v", names(got), want)
	}
}

func TestTemplateLister_FiltersByAtespace(t *testing.T) {
	ctrl := &harnesstest.MockControlServer{Templates: []*ateapipb.ActorTemplate{template("one", "a"), template("two", "b")}}
	got, err := newLister(t, ctrl).ListActorTemplates(context.Background(), "two")
	if err != nil {
		t.Fatalf("ListActorTemplates: %v", err)
	}
	if want := []string{"two/b"}; !slices.Equal(names(got), want) {
		t.Errorf("templates = %v, want %v", names(got), want)
	}
}
