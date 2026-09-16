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
	"strings"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
)

func TestParse_EmptyDataIsStillAValidEnvelope(t *testing.T) {
	env := &Envelope{From: "agent-a", To: "agent-b"}
	b, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, ok := Parse(string(b))
	if !ok {
		t.Fatalf("Parse(%s) = (_, false), want (_, true) -- an empty Data is a legitimate, if uninteresting, Envelope", b)
	}
	if got.Data.Type != DataTypeUnspecified {
		t.Errorf("Data.Type = %q, want %q", got.Data.Type, DataTypeUnspecified)
	}
}

func TestParse_NoFromIsInvalid(t *testing.T) {
	if _, ok := Parse(`{"to":"agent-b","data":{}}`); ok {
		t.Error("Parse with no From = (_, true), want (_, false)")
	}
}

// TestParse_EmptyFromIsValidIfKeyPresent confirms Parse tells an absent
// "from" key apart from a present-but-empty one (checkpointdIdentity, a
// genuine value) by checking for the key itself, not the decoded field's
// non-emptiness.
func TestParse_EmptyFromIsValidIfKeyPresent(t *testing.T) {
	env := &Envelope{From: "", To: "agent-b"}
	b, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if !strings.Contains(string(b), `"from":""`) {
		t.Fatalf(`marshaled Envelope = %s, want it to include an explicit "from":"" (no omitempty on Envelope.From)`, b)
	}
	got, ok := Parse(string(b))
	if !ok {
		t.Fatalf("Parse(%s) = (_, false), want (_, true) -- an empty From is checkpointdIdentity, a genuine value, not evidence of missing routing", b)
	}
	if got.From != "" {
		t.Errorf("From = %q, want \"\"", got.From)
	}
}

func TestParse_ArbitraryTextIsInvalid(t *testing.T) {
	if _, ok := Parse("not json at all"); ok {
		t.Error("Parse(garbage) = (_, true), want (_, false)")
	}
}

func TestParse_RoundTripsType(t *testing.T) {
	env := &Envelope{From: "agent-a", To: "", Data: Data{
		Task: &a2a.Task{Status: a2a.TaskStatus{State: a2a.TaskStateWorking}},
		Type: DataTypeTask,
	}}
	b, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, ok := Parse(string(b))
	if !ok {
		t.Fatalf("Parse(%s) = (_, false), want (_, true)", b)
	}
	if got.Data.Type != DataTypeTask {
		t.Errorf("Data.Type = %q, want %q", got.Data.Type, DataTypeTask)
	}
}
