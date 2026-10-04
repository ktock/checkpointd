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
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/ktock/checkpointd/internal/harness/harnesstest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const yamlPolicy = `
rules:
  - hostnames:
      patterns: ["testserver.ns.svc", "*.example.com"]
`

func TestParseEgressPolicy_YAML(t *testing.T) {
	p, err := ParseEgressPolicy([]byte(yamlPolicy))
	if err != nil {
		t.Fatalf("ParseEgressPolicy: %v", err)
	}
	rule := p.GetRules()[0].GetHostnames()
	if len(rule.GetPatterns()) != 2 || rule.GetPatterns()[1] != "*.example.com" {
		t.Errorf("rule = %v, want the two hostname patterns", rule)
	}
}

func TestParseEgressPolicy_JSON(t *testing.T) {
	p, err := ParseEgressPolicy([]byte(`{"rules":[{"all":{}}]}`))
	if err != nil {
		t.Fatalf("ParseEgressPolicy: %v", err)
	}
	if p.GetRules()[0].GetAll() == nil {
		t.Errorf("rule = %v, want the match-everything rule", p.GetRules()[0])
	}
}

// TestParseEgressPolicy_AcceptsNoRules confirms a policy without rules parses, since Substrate accepts it and it simply denies everything.
func TestParseEgressPolicy_AcceptsNoRules(t *testing.T) {
	for _, in := range []string{`{}`, `rules: []`, `{"rules":[]}`} {
		p, err := ParseEgressPolicy([]byte(in))
		if err != nil {
			t.Errorf("ParseEgressPolicy(%q): %v", in, err)
			continue
		}
		if len(p.GetRules()) != 0 {
			t.Errorf("ParseEgressPolicy(%q) rules = %v, want none", in, p.GetRules())
		}
	}
}

func TestParseEgressPolicy_Rejects(t *testing.T) {
	for name, tc := range map[string]struct{ in, want string }{
		"unknown field":   {`{"rulez":[]}`, "rulez"},
		"empty":           {"", "manifest is empty"},
		"blank":           {"  \n", "manifest is empty"},
		"null document":   {"null", "manifest is empty"},
		"two documents":   {"rules: []\n---\nrules: []\n", "more than one document"},
		"wrong rule type": {`{"rules":[{"htp":{}}]}`, "htp"},
		"not a document":  {`: : :`, "invalid egress policy"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseEgressPolicy([]byte(tc.in)); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("ParseEgressPolicy(%q) error = %v, want one mentioning %q", tc.in, err, tc.want)
			}
		})
	}
}

// TestParseEgressPolicy_AcceptsMetadata confirms a policy pasted from `kubectl ate get egress-policy -o yaml` parses, as kubectl-ate accepts it.
func TestParseEgressPolicy_AcceptsMetadata(t *testing.T) {
	p, err := ParseEgressPolicy([]byte(`
metadata:
  atespace: team
  name: default
rules:
  - hostnames:
      patterns: ["a.example.com"]
`))
	if err != nil {
		t.Fatalf("ParseEgressPolicy: %v", err)
	}
	if p.GetMetadata().GetName() != "default" || p.GetMetadata().GetAtespace() != "team" {
		t.Errorf("metadata = %v, want it kept as written", p.GetMetadata())
	}
}

func newClient(t *testing.T, ctrl *harnesstest.MockControlServer) *Client {
	t.Helper()
	c, err := NewClient("team", "tmpl", harnesstest.StartControlServer(t, ctrl), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func policyWithMetadata(atespace, name string) *ateapipb.EgressPolicy {
	p := &ateapipb.EgressPolicy{Rules: []*ateapipb.EgressRule{{Hostnames: &ateapipb.HostnameRule{Patterns: []string{"a.example.com"}}}}}
	if atespace != "" || name != "" {
		p.Metadata = &ateapipb.ResourceMetadata{Atespace: atespace, Name: name, Uid: "stale-uid", Version: 7}
	}
	return p
}

// TestCreateActorEgressPolicy_Metadata confirms metadata may be omitted or match the actor, is replaced by the actor's own, and is rejected when it names another atespace or policy.
func TestCreateActorEgressPolicy_Metadata(t *testing.T) {
	for name, tc := range map[string]struct {
		policy  *ateapipb.EgressPolicy
		wantErr string
	}{
		"omitted":             {policy: policyWithMetadata("", "")},
		"matching":            {policy: policyWithMetadata("team", "default")},
		"only the name":       {policy: policyWithMetadata("", "default")},
		"another atespace":    {policy: policyWithMetadata("other", "default"), wantErr: "does not match the actor's atespace"},
		"another policy name": {policy: policyWithMetadata("team", "custom"), wantErr: `must be "default"`},
	} {
		t.Run(name, func(t *testing.T) {
			ctrl := &harnesstest.MockControlServer{}
			err := newClient(t, ctrl).CreateActorEgressPolicy(context.Background(), "conv-1", tc.policy)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error = %v, want one mentioning %q", err, tc.wantErr)
				}
				if ctrl.EgressPolicyOf("conv-1") != nil {
					t.Error("a rejected policy must not reach Substrate")
				}
				return
			}
			if err != nil {
				t.Fatalf("CreateActorEgressPolicy: %v", err)
			}
			md := ctrl.EgressPolicyOf("conv-1").GetMetadata()
			if md.GetAtespace() != "team" || md.GetName() != "default" || md.GetUid() != "" || md.GetVersion() != 0 {
				t.Errorf("stored metadata = %v, want only the actor's atespace and the name default", md)
			}
		})
	}
}
