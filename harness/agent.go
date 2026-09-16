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

// Package harness lets an agent be implemented as a real a2asrv.AgentExecutor.
package harness

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/a2aproject/a2a-go/v2/a2aevent"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/ktock/checkpointd/internal/hop"
	"github.com/ktock/checkpointd/proto"
)

// NewAgentHarness drives every invocation as one call to executor.Execute.
func NewAgentHarness(executor a2asrv.AgentExecutor) proto.HarnessServiceServer {
	return newActorRuntime(func(cc *callContext, current *a2a.Message, history storedHistory) (*hop.Data, error) {
		// current is nil when the triggering hop yielded nothing at all.
		var taskID a2a.TaskID
		var contextID string
		if current != nil {
			taskID = current.TaskID
			contextID = current.ContextID
		}
		execCtx := &a2asrv.ExecutorContext{
			Message:   current,
			TaskID:    taskID,
			ContextID: contextID,
			Metadata: map[string]any{
				metaCallContext: cc,
			},
		}
		// history.task is non-nil only if some prior turn genuinely
		// yielded a Task-shaped event; its History is always overwritten
		// with the freshly flattened message history.
		if history.task != nil {
			t := *history.task
			t.History = history.messages
			execCtx.StoredTask = &t
		}

		var data hop.Data
		established := execCtx.StoredTask != nil
		var terminal bool
		for event, err := range executor.Execute(context.Background(), execCtx) {
			if err != nil {
				return nil, err
			}
			if terminal {
				return nil, fmt.Errorf("agent yielded a %T after its task already reached a terminal state: %w", event, a2a.ErrInvalidAgentResponse)
			}
			switch v := event.(type) {
			case *a2a.Message:
				if established {
					return nil, fmt.Errorf("agent yielded a Message after already claiming a Task: %w", a2a.ErrInvalidAgentResponse)
				}
				data.Message = v
				data.Type = hop.DataTypeMessage
			case *a2a.TaskStatusUpdateEvent:
				if !established {
					return nil, fmt.Errorf("agent's first event must be a Task or a Message, got %T: %w", v, a2a.ErrInvalidAgentResponse)
				}
				t := seedTask(execCtx, &data.Task)
				t.Status.State = v.Status.State
				if v.Status.Message != nil {
					t.Status.Message = v.Status.Message
				}
				if v.TaskID != "" {
					t.ID = v.TaskID
				}
				if v.ContextID != "" {
					t.ContextID = v.ContextID
				}
				mergeMetadata(t, v.Metadata)
				data.Type = hop.DataTypeTask
				terminal = v.Status.State.Terminal()
				if selfContinue(v.Status.State) {
					cc.flushSelf(&data)
				}
			case *a2a.TaskArtifactUpdateEvent:
				if !established {
					return nil, fmt.Errorf("agent's first event must be a Task or a Message, got %T: %w", v, a2a.ErrInvalidAgentResponse)
				}
				t := seedTask(execCtx, &data.Task)
				// TaskID/ContextID default from t when the event leaves
				// them empty because ApplyArtifactUpdate requires an exact match.
				if v.TaskID != "" {
					t.ID = v.TaskID
				} else {
					v.TaskID = t.ID
				}
				if v.ContextID != "" {
					t.ContextID = v.ContextID
				} else {
					v.ContextID = t.ContextID
				}
				merged, err := a2aevent.ApplyArtifactUpdate(t, v)
				if err != nil {
					return nil, fmt.Errorf("applying artifact update: %w: %w", err, a2a.ErrInvalidAgentResponse)
				}
				data.Task = merged
				data.Type = hop.DataTypeTask
			case *a2a.Task:
				seedTask(execCtx, &data.Task)
				adoptTask(&data.Task, v)
				data.Type = hop.DataTypeTask
				data.Message = nil
				established = true
				terminal = data.Task.Status.State.Terminal()
				if selfContinue(data.Task.Status.State) {
					cc.flushSelf(&data)
				}
			default:
				return nil, fmt.Errorf("agent yielded a %T event; checkpointd supports only a2a.Message, a2a.Task, a2a.TaskStatusUpdateEvent, and a2a.TaskArtifactUpdateEvent", event)
			}
		}
		// An Execute() call must not return while still reporting
		// Working/Submitted.
		if data.Task != nil && selfContinue(data.Task.Status.State) {
			return nil, fmt.Errorf("agent's Execute() call returned while still reporting %s -- keep running (yield further events) until reaching a terminal or interrupted state instead: %w", data.Task.Status.State, a2a.ErrInvalidAgentResponse)
		}
		// data.Type is set directly by whichever case above ran; left at
		// DataTypeUnspecified only if nothing was ever yielded at all.
		if data.Type == hop.DataTypeUnspecified {
			slog.WarnContext(context.Background(), "agent yielded nothing this turn -- treating as an empty message")
		}
		return &data, nil
	})
}

// seedTask initializes *task from execCtx.StoredTask when resuming an
// already-established task, or a fresh, empty Task otherwise.
func seedTask(execCtx *a2asrv.ExecutorContext, task **a2a.Task) *a2a.Task {
	if *task != nil {
		return *task
	}
	if execCtx.StoredTask == nil {
		*task = &a2a.Task{}
		return *task
	}
	t := *execCtx.StoredTask
	t.Artifacts = append([]*a2a.Artifact{}, execCtx.StoredTask.Artifacts...)
	// History is checkpointd's own derived view, never echoed back out.
	t.History = nil
	*task = &t
	return *task
}

func adoptTask(dst **a2a.Task, v *a2a.Task) {
	task := *v
	if *dst != nil {
		if len((*dst).Artifacts) > 0 {
			task.Artifacts = append(append([]*a2a.Artifact{}, (*dst).Artifacts...), task.Artifacts...)
		}
		if len((*dst).Metadata) > 0 {
			merged := make(map[string]any, len((*dst).Metadata)+len(task.Metadata))
			for k, val := range (*dst).Metadata {
				merged[k] = val
			}
			for k, val := range task.Metadata {
				merged[k] = val
			}
			task.Metadata = merged
		}
	}
	*dst = &task
}

// mergeMetadata merges extra into task.Metadata
func mergeMetadata(task *a2a.Task, extra map[string]any) {
	if len(extra) == 0 {
		return
	}
	if task.Metadata == nil {
		task.Metadata = make(map[string]any, len(extra))
	}
	for k, v := range extra {
		task.Metadata[k] = v
	}
}

// metaCallContext is the ExecutorContext.Metadata key holding this
// invocation's own *callContext.
const metaCallContext = "ax.dev/harness-call-context"

func callContextOf(execCtx *a2asrv.ExecutorContext) *callContext {
	cc, _ := execCtx.Metadata[metaCallContext].(*callContext)
	return cc
}

// WithTransport plugs execCtx's own durable transport into
// a2aclient.NewFromCard; pass it alongside a2aclient.WithDefaultsDisabled()
// so a real card's own http(s) interface is never selected instead.
func WithTransport(execCtx *a2asrv.ExecutorContext) a2aclient.FactoryOption {
	cc := callContextOf(execCtx)
	// SendMessage needs a fallback TaskID/ContextID for a
	// nested call's own outbound request.
	taskID, contextID := execCtx.TaskID, execCtx.ContextID
	return a2aclient.WithTransport(hop.CheckpointdTransportProtocol, a2aclient.TransportFactoryFn(
		func(ctx context.Context, card *a2a.AgentCard, iface *a2a.AgentInterface) (a2aclient.Transport, error) {
			if cc == nil {
				return nil, fmt.Errorf("harness: execCtx not built by NewAgentHarness's own adapter")
			}
			target := strings.TrimPrefix(iface.URL, hop.CheckpointdURLPrefix)
			return &pipeTransport{rt: cc.rt, inv: cc.inv, agentID: cc.agentID, target: target, taskID: taskID, contextID: contextID}, nil
		},
	))
}
