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
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"strings"
	"sync/atomic"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/a2aproject/a2a-go/v2/a2asrv/taskstore"
	"github.com/google/uuid"
	"github.com/ktock/checkpointd/internal/controller"
	"github.com/ktock/checkpointd/internal/controller/eventlog"
	"github.com/ktock/checkpointd/internal/hop"
	"github.com/ktock/checkpointd/proto"
)

// cancelWaitTimeout bounds how long CancelTask waits for an actively
// driving goroutine to confirm it has actually stopped before recording
// the cancellation anyway.
const cancelWaitTimeout = 5 * time.Second

// serverRequestHandler is a2asrv.RequestHandler for one configured agent's
// own /agents/{name}/ path.
type serverRequestHandler struct {
	agent    string
	c        *controller.Controller
	el       eventlog.EventLog
	store    *sqlTaskStore
	registry *taskRegistry
	podName  string
	podUID   string
	// draining, when set and true, means this instance is scaling in: no
	// brand-new sessions, and no new work for a session it doesn't already
	// have actively running locally. nil (e.g. in tests that construct this
	// struct directly) behaves as "never draining".
	draining *atomic.Bool
}

// isDraining reports whether this instance is currently scaling in.
func (h *serverRequestHandler) isDraining() bool {
	return h.draining != nil && h.draining.Load()
}

var _ a2asrv.RequestHandler = (*serverRequestHandler)(nil)

// SendMessage implements a2asrv.RequestHandler: starts a brand-new session if
// req.Message carries no TaskID, or delivers it as a continuation of an
// existing task if it does (see continueTask). Either way, this blocks until
// the relay loop reaches its first stopping point --
// req.Config.ReturnImmediately controls how early that stopping point is:
// unset/false waits for a terminal or interrupted state, true also stops at the
// first self-continuation report (TaskStateWorking/TaskStateSubmitted).
func (h *serverRequestHandler) SendMessage(ctx context.Context, req *a2a.SendMessageRequest) (a2a.SendMessageResult, error) {
	if req == nil || req.Message == nil {
		return nil, a2a.ErrInvalidParams
	}
	returnImmediately := req.Config != nil && req.Config.ReturnImmediately
	if req.Message.TaskID != "" {
		return h.continueTask(ctx, req.Message, req.Tenant, returnImmediately)
	}
	return h.startTask(ctx, req.Message, returnImmediately)
}

// shortServerSessionID returns a new session ID.
func shortServerSessionID() string {
	return strings.ReplaceAll(uuid.NewString(), "-", "")[:16]
}

// loadServerSession reads back a relay loop's *bookkeeping by its own
// session id.
func loadServerSession(ctx context.Context, store *sqlTaskStore, el eventlog.EventLog, sessionID string) (bk *bookkeeping, bootstrap *hop.Envelope, ok bool, err error) {
	sess, err := store.GetSession(ctx, sessionID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil, false, nil
		}
		return nil, nil, false, fmt.Errorf("loading session %s: %w", sessionID, err)
	}
	taskID, _, err := store.TaskIDForSession(ctx, sessionID)
	if err != nil {
		return nil, nil, false, err
	}
	bk = &bookkeeping{el: el, sessionID: sessionID, taskID: taskID, ownerAgent: sess.ownerAgent, store: store}

	// sess.bootstrap came back for free with the session row itself (see
	// CreateSession); establishRelayState is what decides whether to
	// actually use it (only when bk.lastHop finds nothing yet logged for
	// this session at all).
	return bk, sess.bootstrap, true, nil
}

// newServerSession durably creates a brand-new relay loop's session, owned
// from inception by (podName, podUID) along with its bootstrap message.
func newServerSession(ctx context.Context, store *sqlTaskStore, el eventlog.EventLog, agent, podName, podUID string, bootstrap *hop.Envelope) (*bookkeeping, error) {
	const maxSessionIDAttempts = 5
	var sessionID string
	var err error
	for attempt := 0; attempt < maxSessionIDAttempts; attempt++ {
		sessionID = shortServerSessionID()
		err = store.CreateSession(ctx, sessionID, agent, podName, podUID, bootstrap)
		if err == nil {
			break
		}
		if !errors.Is(err, errDuplicateSessionID) {
			return nil, fmt.Errorf("creating session: %w", err)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("creating session after %d attempts: %w", maxSessionIDAttempts, err)
	}
	return &bookkeeping{
		el: el, sessionID: sessionID, store: store,
		ownerAgent: agent,
	}, nil
}

// startTask starts a brand-new session for msg, durably recording it before
// returning, then hands it to awaitOrSubmit.
func (h *serverRequestHandler) startTask(ctx context.Context, msg *a2a.Message, returnImmediately bool) (a2a.SendMessageResult, error) {
	if h.isDraining() {
		return nil, fmt.Errorf("checkpointd instance %s is draining for scale-in, not accepting new sessions: %w", h.podName, a2a.ErrInvalidRequest)
	}
	bootstrap := &hop.Envelope{From: checkpointdIdentity, To: h.agent, StepID: uuid.NewString(), Data: hop.Data{Message: msg, Type: hop.DataTypeMessage}}

	bk, err := newServerSession(ctx, h.store, h.el, h.agent, h.podName, h.podUID, bootstrap)
	if err != nil {
		return nil, fmt.Errorf("creating session: %w", err)
	}

	// context.Background(), not ctx: this goroutine outlives the HTTP
	// request that started it.
	taskCtx, release, ok := h.registry.acquire(context.Background(), bk.sessionID)
	if !ok {
		// Can't happen for a freshly minted session id, but fail loudly
		// rather than silently drive two goroutines for the same session if
		// it somehow did.
		return nil, fmt.Errorf("session %s: registry already has an active entry for a brand-new id", bk.sessionID)
	}
	result, err := h.awaitOrSubmit(ctx, bk.sessionID, returnImmediately, release, func(onQualifying func(*hop.Envelope)) (*hop.Envelope, error) {
		return runRelayLoop(taskCtx, h.c, h.el, bk, h.agent, bootstrap, onQualifying)
	})
	return result, err
}

// continueTask delivers msg -- which carries an existing TaskID -- to that
// task as a genuine A2A multi-turn continuation: CORE-SEND-002 (reject a
// terminal task), CORE-MULTI-006 (reject a mismatching contextId), and
// CORE-MULTI-005 (infer contextId from the task when the client omits it)
// are all enforced here, before anything is handed to awaitOrSubmit.
//
// tenant is the client's own req.Tenant, expected to be the sessionID
// checkpointd stamped into the task's own Metadata on an earlier reply.
// With no tenant, this still resolves as long as (msg.TaskID, h.agent)
// is currently unambiguous; a genuine collision, or the wrong tenant for
// this task, resolves as not-found. Once resolved, this function reads the
// actual session id back off stored.SessionID.
func (h *serverRequestHandler) continueTask(ctx context.Context, msg *a2a.Message, tenant string, returnImmediately bool) (a2a.SendMessageResult, error) {
	stored, err := h.store.Get(ctx, msg.TaskID, h.agent, tenant)
	if err != nil {
		return nil, err
	}
	task := stored.Task
	if msg.ContextID != "" && msg.ContextID != task.ContextID {
		return nil, a2a.ErrInvalidParams
	}
	if task.Status.State.Terminal() {
		return nil, a2a.ErrUnsupportedOperation
	}
	msg.ContextID = task.ContextID

	sessionID := stored.SessionID
	if h.isDraining() && !h.registry.isActive(sessionID) {
		// Still service work this instance already has actively running
		// locally (matches "keeps processing existing relays" during
		// scale-in); refuse anything that would newly claim ownership.
		return nil, fmt.Errorf("checkpointd instance %s is draining for scale-in, not accepting new work for session %s: %w", h.podName, sessionID, a2a.ErrInvalidRequest)
	}
	bk, _, ok, err := loadServerSession(ctx, h.store, h.el, sessionID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("task %s: its own session %s not found (data inconsistency)", msg.TaskID, sessionID)
	}

	// Durably claim cross-instance ownership before the local, per-process
	// registry.acquire below: a session with owner_pod already set
	// (actively driven, here or on another replica) must be refused, not
	// silently double-driven.
	claimed, err := h.store.ClaimSession(ctx, sessionID, h.podName, h.podUID)
	if err != nil {
		return nil, err
	}
	if !claimed {
		return nil, fmt.Errorf("task %s is already being processed: %w", msg.TaskID, a2a.ErrInvalidRequest)
	}

	taskCtx, release, ok := h.registry.acquire(context.Background(), sessionID)
	if !ok {
		// Can't happen after a successful DB claim (this process just
		// established sole ownership), but if it somehow did, don't leak
		// the claim.
		mustReleaseSession(context.Background(), h.store, sessionID)
		return nil, fmt.Errorf("task %s is already being processed: %w", msg.TaskID, a2a.ErrInvalidRequest)
	}
	result, err := h.awaitOrSubmit(ctx, sessionID, returnImmediately, release, func(onQualifying func(*hop.Envelope)) (*hop.Envelope, error) {
		return continueRelayLoop(taskCtx, h.c, h.el, bk, msg, onQualifying)
	})
	return result, err
}

// awaitOrSubmit runs relay and waits for its own stopping point, or until
// reqCtx is done, whichever comes first. With returnImmediately, that stopping
// point is relay's own onQualifying callback firing for the first time. Without
// it (the common case), onQualifying's early firing is ignored and this waits
// for relay itself to return (a terminal or interrupted state).
func (h *serverRequestHandler) awaitOrSubmit(reqCtx context.Context, sessionID string, returnImmediately bool, release func(), relay func(onQualifying func(*hop.Envelope)) (*hop.Envelope, error)) (a2a.SendMessageResult, error) {
	var res *hop.Envelope
	done := make(chan struct{})
	// Buffered 1: onQualifying is called at most once (see driveRelayLoop's
	// own notified guard), and this send must never block the background
	// goroutine even if nobody ever reads it (returnImmediately is false, or
	// reqCtx ending first).
	qualifying := make(chan *hop.Envelope, 1)
	go func() {
		defer release()
		defer mustReleaseSession(context.Background(), h.store, sessionID)
		defer close(done)
		r, err := relay(func(q *hop.Envelope) { qualifying <- q })
		if err != nil {
			log.Infof("session %s: %v", sessionID, err)
			return
		}
		res = r
	}()

	var reply *hop.Envelope
	if returnImmediately {
		select {
		case reply = <-qualifying:
		case <-done:
			reply = res
		case <-reqCtx.Done():
		}
	} else {
		select {
		case <-done:
			reply = res
		case <-reqCtx.Done():
		}
	}

	taskID, hasTask, err := h.store.TaskIDForSession(context.Background(), sessionID)
	if err != nil {
		return nil, err
	}
	if !hasTask {
		// This relay loop has never made a real Task-shaped claim.
		if reply == nil {
			return nil, reqCtx.Err()
		}
		msg := reply.Data.Message
		if msg == nil {
			return nil, a2a.ErrInvalidAgentResponse
		}
		return msg, nil
	}
	stored, err := h.store.Get(context.Background(), a2a.TaskID(taskID), h.agent, sessionID)
	if err != nil {
		return nil, err
	}
	return stored.Task, nil
}

// GetTask implements a2asrv.RequestHandler. req.Tenant, when the caller
// supplies it, is expected to be the sessionID checkpointd stamped into the
// task's own Metadata on an earlier reply. A caller that never learned it
// still resolves normally as long as (req.ID, h.agent) is currently unambiguous.
// Only a genuine collision, or the wrong tenant for this task, gets a2a.ErrTaskNotFound.
func (h *serverRequestHandler) GetTask(ctx context.Context, req *a2a.GetTaskRequest) (*a2a.Task, error) {
	if req == nil || req.ID == "" {
		return nil, a2a.ErrInvalidParams
	}
	return h.store.GetWithHistoryLength(ctx, req.ID, h.agent, req.Tenant, req.HistoryLength)
}

// ListTasks implements a2asrv.RequestHandler, scoped to tasks bootstrapped
// through h.agent specifically.
func (h *serverRequestHandler) ListTasks(ctx context.Context, req *a2a.ListTasksRequest) (*a2a.ListTasksResponse, error) {
	return h.store.ListForAgent(ctx, h.agent, req)
}

// CancelTask implements a2asrv.RequestHandler: cancels req.ID's own
// session's actively driving goroutine if one exists, then durably, atomically
// marks the task Canceled and its session TERMINATING before this call returns.
// A task already in a terminal state returns ErrTaskNotCancelable.
func (h *serverRequestHandler) CancelTask(ctx context.Context, req *a2a.CancelTaskRequest) (*a2a.Task, error) {
	if req == nil || req.ID == "" {
		return nil, a2a.ErrInvalidParams
	}
	stored, err := h.store.Get(ctx, req.ID, h.agent, req.Tenant)
	if err != nil {
		return nil, err
	}
	task := stored.Task
	if task.Status.State.Terminal() {
		return nil, a2a.ErrTaskNotCancelable
	}
	sessionID := stored.SessionID
	bk, _, ok, err := loadServerSession(ctx, h.store, h.el, sessionID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("task %s: its own session %s not found (data inconsistency)", req.ID, sessionID)
	}

	h.registry.cancelAndWait(ctx, sessionID, cancelWaitTimeout)
	// Durably, atomically marks the task Canceled and its session
	// TERMINATING.
	if err := bk.store.CancelTaskAndSession(ctx, req.ID, bk.ownerAgent, bk.sessionID, stored.Version); err != nil {
		if errors.Is(err, taskstore.ErrConcurrentModification) {
			// Someone else's write landed first -- most plausibly a
			// racing, spec-mandated-idempotent CancelTask (see this
			// method's own doc comment), or the relay loop's own last hop
			// completing right as this call raced it. Either way, that's
			// not this call's own failure to report: re-check, and if the
			// task is now terminal (which it will be, either way), report
			// exactly what a second, slightly later CancelTask attempt
			// against an already-canceled task would.
			if restored, getErr := h.store.Get(ctx, req.ID, h.agent, req.Tenant); getErr == nil && restored.Task.Status.State.Terminal() {
				return nil, a2a.ErrTaskNotCancelable
			}
		}
		return nil, fmt.Errorf("recording cancellation: %w", err)
	}
	// Best-effort: append a Task-shaped Canceled entry to bk's own session,
	// so GetTask's History shows the cancellation as its own entry.
	if err := appendCancellationMarker(ctx, bk); err != nil {
		log.Infof("session %s: recording cancellation marker: %v", bk.sessionID, err)
	}

	// If some other instance is still actively driving it, this claim fails
	// and finishTermination is deliberately not run here -- CancelTaskAndSession
	// above already durably recorded the cancellation, so the client-visible result
	// is already correct; that instance's own driveRelayLoop will run finishTermination
	// itself the next time it reaches a stopping point.
	if claimed, err := h.store.ClaimSession(context.Background(), sessionID, h.podName, h.podUID); err != nil {
		log.Infof("session %s: claiming for post-cancel cleanup: %v", sessionID, err)
	} else if claimed {
		go func() {
			// this goroutine outlives the RPC so context.Background().
			taskCtx, release, ok := h.registry.acquireWait(context.Background(), sessionID)
			if !ok {
				// Only happens if context.Background() itself ended, which it
				// never does.
				return
			}
			defer release()
			defer mustReleaseSession(context.Background(), h.store, sessionID)
			finishTermination(taskCtx, h.c, bk)
		}()
	}

	stored, err = h.store.Get(ctx, req.ID, h.agent, req.Tenant)
	if err != nil {
		return nil, err
	}
	return stored.Task, nil
}

// appendCancellationMarker appends a Task-shaped Canceled entry to bk's own
// session, so GetTask's History also shows the cancellation as its own
// entry.
func appendCancellationMarker(ctx context.Context, bk *bookkeeping) error {
	actorID := actorConv(bk.sessionID, bk.ownerAgent)
	msg := &hop.Envelope{
		From: bk.ownerAgent,
		Data: hop.Data{
			Task: &a2a.Task{
				Status: a2a.TaskStatus{
					State: a2a.TaskStateCanceled,
				},
			},
			Type: hop.DataTypeTask,
		},
	}
	b, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("encoding cancellation marker: %w", err)
	}
	_, err = bk.el.Append(ctx, &proto.StepEvent{
		ConversationId: actorID,
		SessionId:      bk.sessionID,
		Steps:          []*proto.Step{bookkeepingStep(string(b))},
	})
	return err
}

// SendStreamingMessage implements a2asrv.RequestHandler.
func (h *serverRequestHandler) SendStreamingMessage(context.Context, *a2a.SendMessageRequest) iter.Seq2[a2a.Event, error] {
	return unsupportedEvents
}

// SubscribeToTask implements a2asrv.RequestHandler.
func (h *serverRequestHandler) SubscribeToTask(context.Context, *a2a.SubscribeToTaskRequest) iter.Seq2[a2a.Event, error] {
	return unsupportedEvents
}

// unsupportedEvents is the shared iter.Seq2[a2a.Event, error] body for every
// a2asrv.RequestHandler streaming method checkpointd doesn't implement.
func unsupportedEvents(yield func(a2a.Event, error) bool) {
	yield(nil, a2a.ErrUnsupportedOperation)
}

// GetTaskPushConfig implements a2asrv.RequestHandler.
func (h *serverRequestHandler) GetTaskPushConfig(context.Context, *a2a.GetTaskPushConfigRequest) (*a2a.PushConfig, error) {
	return nil, a2a.ErrPushNotificationNotSupported
}

// ListTaskPushConfigs implements a2asrv.RequestHandler.
func (h *serverRequestHandler) ListTaskPushConfigs(context.Context, *a2a.ListTaskPushConfigRequest) (*a2a.ListTaskPushConfigResponse, error) {
	return nil, a2a.ErrPushNotificationNotSupported
}

// CreateTaskPushConfig implements a2asrv.RequestHandler.
func (h *serverRequestHandler) CreateTaskPushConfig(context.Context, *a2a.PushConfig) (*a2a.PushConfig, error) {
	return nil, a2a.ErrPushNotificationNotSupported
}

// DeleteTaskPushConfig implements a2asrv.RequestHandler.
func (h *serverRequestHandler) DeleteTaskPushConfig(context.Context, *a2a.DeleteTaskPushConfigRequest) error {
	return a2a.ErrPushNotificationNotSupported
}

// GetExtendedAgentCard implements a2asrv.RequestHandler.
func (h *serverRequestHandler) GetExtendedAgentCard(context.Context, *a2a.GetExtendedAgentCardRequest) (*a2a.AgentCard, error) {
	return nil, a2a.ErrUnsupportedOperation
}
