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

package hop

import (
	"encoding/json"

	"github.com/a2aproject/a2a-go/v2/a2a"
)

// Data is everything one hop's sender produced.
type Data struct {
	// Message is an A2A Message.
	Message *a2a.Message `json:"message,omitempty"`
	// Task is an A2A Task.
	Task *a2a.Task `json:"task,omitempty"`
	// Type records the type this Data represents.
	Type DataType `json:"type,omitempty"`
}

// DataType is the type this Data represents.
type DataType string

const (
	// DataTypeUnspecified means nothing was yielded.
	DataTypeUnspecified DataType = ""
	// DataTypeMessage means *a2a.Message.
	DataTypeMessage DataType = "message"
	// DataTypeTask means a *a2a.Task.
	DataTypeTask DataType = "task"
)

// Envelope is checkpointd's own wire shape for one hop.
type Envelope struct {
	// From is who sent this hop: an agent id, or checkpointdIdentity ("")
	// for checkpointd's own bootstrap/continuation identity.
	From string `json:"from"`
	// To is who should receive this hop next, or "" for a final reply.
	To string `json:"to,omitempty"`
	// Correlation identifies one outbound call from the harness so its
	// eventual reply can be matched back to it.
	Correlation string `json:"correlation,omitempty"`
	// StepID identifies this specific delivery: checkpointd's relay mints
	// a fresh one on each turn. Used for message deduplication in the harness.
	StepID string `json:"stepId,omitempty"`
	// Reply is true when this message completes a round trip.
	Reply bool `json:"reply,omitempty"`
	// Data is what this hop actually carries.
	Data Data `json:"data"`
}

// Parse reports whether text is a JSON-encoded Envelope.
func Parse(text string) (*Envelope, bool) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(text), &raw); err != nil {
		return nil, false
	}
	// requires the "from" key to be present. From == "" is
	// itself a legitimate, checkpointd-identity value
	if _, ok := raw["from"]; !ok {
		return nil, false
	}
	var env Envelope
	if err := json.Unmarshal([]byte(text), &env); err != nil {
		return nil, false
	}
	return &env, true
}

// CheckpointdTransportProtocol names harness's transport binding.
const CheckpointdTransportProtocol a2a.TransportProtocol = "checkpointd"

// CheckpointdURLPrefix is the URL scheme an a2a.AgentInterface uses to name
// a CheckpointdTransportProtocol interface.
const CheckpointdURLPrefix = "checkpointd:"
