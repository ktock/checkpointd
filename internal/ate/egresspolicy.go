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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

// egressPolicyName is the only name Substrate accepts for an actor's egress policy.
const egressPolicyName = "default"

// ParseEgressPolicy parses an EgressPolicy from a single JSON or YAML document in Substrate's protojson shape.
// Like kubectl-ate, it rejects unknown fields, an empty manifest and more than one document, and it leaves metadata optional.
func ParseEgressPolicy(raw []byte) (*ateapipb.EgressPolicy, error) {
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	var doc any
	if err := dec.Decode(&doc); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, errors.New("invalid egress policy: manifest is empty")
		}
		return nil, fmt.Errorf("invalid egress policy: %w", err)
	}
	var extra yaml.Node
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("invalid egress policy: manifest holds more than one document, expected one")
	}
	jsonData, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("invalid egress policy: %w", err)
	}
	if string(jsonData) == "null" {
		return nil, errors.New("invalid egress policy: manifest is empty")
	}
	policy := &ateapipb.EgressPolicy{}
	if err := protojson.Unmarshal(jsonData, policy); err != nil {
		return nil, fmt.Errorf("invalid egress policy: %w", err)
	}
	return policy, nil
}

// CreateActorEgressPolicy gives the actor id the egress policy, naming it and placing it in the client's atespace.
func (c *Client) CreateActorEgressPolicy(ctx context.Context, id string, policy *ateapipb.EgressPolicy) error {
	if md := policy.GetMetadata(); md.GetName() != "" && md.GetName() != egressPolicyName {
		return fmt.Errorf("egress policy metadata.name %q must be %q", md.GetName(), egressPolicyName)
	} else if md.GetAtespace() != "" && md.GetAtespace() != c.namespace {
		return fmt.Errorf("egress policy metadata.atespace %q does not match the actor's atespace %q", md.GetAtespace(), c.namespace)
	}
	policy = proto.Clone(policy).(*ateapipb.EgressPolicy)
	policy.Metadata = &ateapipb.ResourceMetadata{Atespace: c.namespace, Name: egressPolicyName}
	client := ateapipb.NewControlClient(c.conn)
	_, err := client.CreateActorEgressPolicy(ctx, &ateapipb.CreateActorEgressPolicyRequest{
		Actor:        &ateapipb.ObjectRef{Atespace: c.namespace, Name: id},
		EgressPolicy: policy,
	})
	if err != nil {
		return fmt.Errorf("error when calling Control.CreateActorEgressPolicy: %w", err)
	}
	return nil
}
