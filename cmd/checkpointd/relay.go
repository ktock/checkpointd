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
	"crypto/sha256"
	"encoding/base32"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/google/uuid"
	"github.com/ktock/checkpointd/internal/controller"
	"github.com/ktock/checkpointd/internal/controller/eventlog"
	"github.com/ktock/checkpointd/internal/hop"
	"github.com/ktock/checkpointd/proto"
)

// actorIDPrefixBytes is how many raw bytes of agent's own name actorConv
// keeps.
const actorIDPrefixBytes = 10

// checkpointdIdentity is the reserved hop.From/hop.To value standing in for
// checkpointd itself, stamped as a task's bootstrap message's From and a
// continuation's own From.
const checkpointdIdentity = ""

// isTerminalReply reports whether res carries no further recipient.
func isTerminalReply(res *hop.Envelope) bool {
	return res.To == checkpointdIdentity
}

// qualifies reports whether res is the first point in a driven relay chain
// worth unblocking this call's own external, waiting caller for.
func qualifies(res *hop.Envelope, theFirstAgent string) bool {
	if isTerminalReply(res) {
		// the chain's final answer
		return true
	}
	// theFirstAgent -- the one agent this specific call is actually
	// talking to -- reporting real progress on itself: a Task-shaped
	// self-continuation (res.Data.Type == hop.DataTypeTask), addressed From and
	// To theFirstAgent
	return res.From == theFirstAgent && res.To == theFirstAgent && res.Data.Type == hop.DataTypeTask
}

// retryBackoff tracks retryExec's own per-error-class delay state
// across attempts of one hop.
type retryBackoff struct {
	// notFound backs off in the Kubernetes's crash-loop-like way. (10s, 20s, 40s, ..., 5m)
	notFound time.Duration
	// connFail backs off in the faster way with capped low (1s, 2s, 4s, ..., 30s)
	connFail time.Duration
}

const (
	notFoundInitialRetryDelay = 10 * time.Second
	notFoundMaxRetryDelay     = 5 * time.Minute

	connFailInitialRetryDelay = 1 * time.Second
	connFailMaxRetryDelay     = 30 * time.Second
)

// next classifies err (via errors.Is(err, controller.ErrHarnessNotFound)),
// advances that class's own counter and returns the resulting delay.
func (b *retryBackoff) next(err error) time.Duration {
	if errors.Is(err, controller.ErrHarnessNotFound) {
		b.notFound = advanceDelay(b.notFound, notFoundInitialRetryDelay, notFoundMaxRetryDelay)
		return b.notFound
	}
	b.connFail = advanceDelay(b.connFail, connFailInitialRetryDelay, connFailMaxRetryDelay)
	return b.connFail
}

// advanceDelay returns initial for a class's first-ever failure (current ==
// 0), or double current capped at max otherwise.
func advanceDelay(current, initial, max time.Duration) time.Duration {
	if current == 0 {
		return initial
	}
	next := current * 2
	if next > max {
		next = max
	}
	return next
}

// bookkeeping is one server-mode relay loop's small handle on its own
// identity and progress
type bookkeeping struct {
	el    eventlog.EventLog
	store *sqlTaskStore

	// session id
	sessionID string
	// external a2a.TaskID (empty until this relay loop's first Task-shaped reply)
	taskID string
	// this session's own checkpointd_sessions.owner_agent
	ownerAgent string
}

// recordTaskStatus durably records a hop's own just-received reply as this
// task's current status so GetTask/ListTasks see the result eagerly, right after
// every execA2A call succeeds. theFirstAgent is whoever this specific call's own
// relay chain is fundamentally addressed to.
func (bk *bookkeeping) recordTaskStatus(ctx context.Context, res *hop.Envelope, theFirstAgent string) error {
	if res.From != theFirstAgent {
		// some other agent's own internal claim which must never become
		// this task's own externally-visible status.
		return nil
	}
	if bk.taskID == "" {
		// checkpointd_tasks gets its row lazily, right here, the first time
		// theFirstAgent's own res is genuinely Task-shaped (res.Data.Type ==
		// hop.DataTypeTask). A non-Task-shaped res (a bare Message, forwarded
		// or otherwise) is not recorded.
		if res.Data.Type != hop.DataTypeTask {
			return nil
		}
		id := string(res.Data.Task.ID)
		if id == "" {
			return fmt.Errorf("agent %s yielded a Task with no ID: %w", theFirstAgent, a2a.ErrInvalidAgentResponse)
		}
		task := &a2a.Task{
			ID:        a2a.TaskID(id),
			ContextID: res.Data.Task.ContextID,
			Status:    a2a.TaskStatus{State: reconstructState(res)},
		}
		if _, err := bk.store.Create(ctx, task, bk.ownerAgent, bk.sessionID); err != nil {
			return fmt.Errorf("creating task row: %w", err)
		}
		bk.taskID = id
		return nil
	}
	if err := bk.store.UpdateStatus(ctx, a2a.TaskID(bk.taskID), bk.ownerAgent, bk.sessionID, reconstructState(res)); err != nil {
		return fmt.Errorf("recording hop result: %w", err)
	}
	return nil
}

// reconstructState infers a task's current lifecycle state from last, the
// live envelope a relay call just produced.
func reconstructState(last *hop.Envelope) a2a.TaskState {
	if last.Data.Task != nil && last.Data.Task.Status.State != a2a.TaskStateUnspecified {
		return last.Data.Task.Status.State
	}
	if isTerminalReply(last) {
		return a2a.TaskStateCompleted
	}
	return a2a.TaskStateWorking
}

// newestText returns the text of the last non-empty Content_Text step in
// steps.
func newestText(steps []*proto.Step) string {
	var text string
	for _, step := range steps {
		content := step.GetContent()
		if content == nil {
			continue
		}
		for _, c := range content.Content {
			if t := c.GetText().GetText(); t != "" {
				text = t
			}
		}
	}
	return text
}

// lastHop returns this relay loop's own most recently logged hop, or nil if
// nothing has been logged for this session yet.
func (bk *bookkeeping) lastHop(ctx context.Context) (*hop.Envelope, *proto.StepEvent, error) {
	events, err := bk.el.EventsBySessionID(ctx, bk.sessionID)
	if err != nil {
		return nil, nil, err
	}
	if len(events) == 0 {
		return nil, nil, nil
	}
	lastEvent := events[len(events)-1]
	for i := len(events) - 1; i >= 0; i-- {
		env, ok := hop.Parse(newestText(events[i].Steps))
		if !ok {
			continue
		}
		return env, lastEvent, nil
	}
	return nil, lastEvent, nil
}

// visitedActorsFromHistory returns the distinct set of (private actor id ->
// agent) pairs this relay loop has ever contacted, for cleanupActors to
// delete once it completes.
func visitedActorsFromHistory(ctx context.Context, el eventlog.EventLog, sessionID string) (map[string]string, error) {
	events, err := el.EventsBySessionID(ctx, sessionID)
	if err != nil {
		return nil, fmt.Errorf("loading history for session %s: %w", sessionID, err)
	}
	actors := make(map[string]string)
	for _, ev := range events {
		if isCheckpointdInputStep(ev) {
			// this is not from a real Substrate actor (e.g. bootstrap message).
			continue
		}
		env, ok := hop.Parse(newestText(ev.Steps))
		if !ok {
			continue
		}
		agent := env.From
		actors[actorConv(sessionID, agent)] = agent
	}
	return actors, nil
}

// isCheckpointdInputStep reports whether ev logged an input delivered to an
// actor (wrapped Role: "user" by userStep/historySteps) as opposed to
// that actor's own reply (wrapped Role: "assistant").
func isCheckpointdInputStep(ev *proto.StepEvent) bool {
	for _, s := range ev.Steps {
		if c := s.GetContent(); c != nil && c.Role == "user" {
			return true
		}
	}
	return false
}

// actorConv derives the private Substrate actor id this relay loop uses for
// agent. Private to this session via sessionID, so two checkpointd sessions
// that both talk to the same target agent id never share one Substrate actor.
func actorConv(sessionID, agent string) string {
	prefix := agent
	if len(prefix) > actorIDPrefixBytes {
		prefix = prefix[:actorIDPrefixBytes]
	}
	sum := sha256.Sum256([]byte("checkpointd-" + sessionID + "-" + agent))
	// encode 32bytes digest to string using base32 so get
	// a shorter string representation (5bits per char so 52
	// chars w/o padding).
	enc := base32.StdEncoding.WithPadding(base32.NoPadding)
	// 10 + 1 + 52 fit in 63 chars name limit
	return prefix + "-" + strings.ToLower(enc.EncodeToString(sum[:]))
}

// bookkeepingStep wraps data as a single content step, for bookkeeping's
// own durable records -- these never reach a real harness, so the role is
// arbitrary and chosen only to be recognizably not "user" or "assistant".
func bookkeepingStep(data string) *proto.Step {
	return &proto.Step{
		Type: &proto.Step_Content{
			Content: &proto.ContentStep{
				Role: "checkpointd",
				Content: []*proto.Content{{
					Type: &proto.Content_Text{Text: &proto.TextContent{Text: data}},
				}},
			},
		},
	}
}

// stepText returns the text of the first non-empty Content_Text found
// across steps, or "" if there is none.
func stepText(steps []*proto.Step) string {
	for _, step := range steps {
		content := step.GetContent()
		if content == nil {
			continue
		}
		for _, c := range content.Content {
			if text := c.GetText().GetText(); text != "" {
				return text
			}
		}
	}
	return ""
}

// runRelayLoop is the durable message-relay loop's core. It first tries to
// resume whatever this task last did. A restart re-enters here and picks up
// exactly where the event log left off. A genuinely first-ever invocation
// (nothing to resume) falls back to starting agent fresh with bootstrap as its
// input.
func runRelayLoop(ctx context.Context, c *controller.Controller, el eventlog.EventLog, bk *bookkeeping, agent string, bootstrap *hop.Envelope, onQualifying func(*hop.Envelope)) (*hop.Envelope, error) {
	res, err := establishRelayState(ctx, c, el, bk, agent, bootstrap)
	if err != nil {
		return nil, err
	}
	return driveRelayLoop(ctx, c, el, bk, agent, res, agent, onQualifying)
}

// continueRelayLoop delivers msg -- a genuinely new message from the
// external client, not an internal hop reply -- to bk's current last-hop
// agent as that hop's next input, then drives the same outer relay loop
// runRelayLoop does. Used for a2a's standard multi-turn continuation
// pattern: a client sending a follow-up message against an existing,
// non-terminal taskID.
//
// The caller is responsible for having already confirmed bk's last hop is
// paused (not still actively in flight, not terminal) before calling this.
func continueRelayLoop(ctx context.Context, c *controller.Controller, el eventlog.EventLog, bk *bookkeeping, msg *a2a.Message, onQualifying func(*hop.Envelope)) (*hop.Envelope, error) {
	last, _, err := bk.lastHop(ctx)
	if err != nil {
		return nil, err
	}
	if last == nil {
		return nil, fmt.Errorf("continueRelayLoop: no hop recorded yet for this task -- nothing to continue")
	}
	if !isTerminalReply(last) {
		return nil, fmt.Errorf("continueRelayLoop: last hop is still addressed to %q, not paused for external continuation", last.To)
	}
	agent := last.From
	// msg arrives from an external client with no checkpointd routing at
	// all. Wrapping it here is what makes it a valid hop.Envelope
	// downstream; driveRelayLoop's own loop delivers it exactly like any
	// other hop (execA2A mints its own StepID, records task status).
	env := &hop.Envelope{From: checkpointdIdentity, To: agent, Data: hop.Data{Message: msg, Type: hop.DataTypeMessage}}
	return driveRelayLoop(ctx, c, el, bk, agent, env, agent, onQualifying)
}

// establishRelayState returns what driveRelayLoop should resume from.
func establishRelayState(ctx context.Context, c *controller.Controller, el eventlog.EventLog, bk *bookkeeping, agent string, bootstrap *hop.Envelope) (*hop.Envelope, error) {
	last, lastStep, err := bk.lastHop(ctx)
	if err != nil {
		return nil, err
	}
	if last == nil {
		log.Debugf("nothing to resume; starting fresh with %s", agent)
		return bootstrap, nil
	}
	if lastStep.State == proto.State_STATE_COMPLETED {
		return last, nil
	}
	log.Debugf("completing still-pending turn for actor %s", lastStep.ConversationId)
	// SessionId must be set here exactly like execA2A sets it, since
	// Controller.Exec passes it into newLogger, which stamps every event
	// this retry commits with this session.
	out, err := retryExec(ctx, c, &proto.CreateInteractionEvent{
		ConversationId: lastStep.ConversationId,
		SessionId:      bk.sessionID,
	})
	if err != nil {
		return nil, err
	}
	res, valid := hop.Parse(out)
	if !valid {
		return nil, fmt.Errorf("resumed reply for actor %s is not an A2A message: %s", lastStep.ConversationId, out)
	}
	if err := bk.recordTaskStatus(ctx, res, agent); err != nil {
		return nil, err
	}
	return res, nil
}

// driveRelayLoop is the shared tail of runRelayLoop and continueRelayLoop:
// given res, the current hop's reply, keep relaying to whoever its
// hop.To until one carries no further recipient.
//
// Every hop's target agent gets its own private actor for this run. once the
// loop reaches a genuinely terminal reply, every such actor is deleted, since
// none of them will ever be revisited.
//
// onQualifying is called exactly once, synchronously, the first time some
// hop's own res satisfies qualifies(res, theFirstAgent), including possibly the
// very first one, res itself, before the loop below ever runs.
func driveRelayLoop(ctx context.Context, c *controller.Controller, el eventlog.EventLog, bk *bookkeeping, agent string, res *hop.Envelope, theFirstAgent string, onQualifying func(*hop.Envelope)) (*hop.Envelope, error) {
	notified := false
	notify := func(r *hop.Envelope) {
		if notified || !qualifies(r, theFirstAgent) {
			return
		}
		notified = true
		onQualifying(r)
	}
	notify(res)
	for !isTerminalReply(res) {
		agent = res.To
		log.Debugf("relaying %s -> %s", res.From, agent)
		actorID := actorConv(bk.sessionID, agent)
		// res is forwarded as-is; execA2A mints a fresh StepID if this one
		// would collide with the target actor's own history.
		var err error
		res, err = execA2A(ctx, c, el, agent, actorID, bk.sessionID, res)
		if err != nil {
			return nil, err
		}
		if err := bk.recordTaskStatus(ctx, res, theFirstAgent); err != nil {
			return nil, err
		}
		notify(res)
	}

	// A reply that carries no further recipient is usually genuinely done --
	// but not if the sender explicitly stamped a non-terminal state on it
	// (e.g. a2a.TaskStateInputRequired): that's this task pausing to wait
	// for its external caller specifically, not completing, and premature
	// cleanup here would delete an actor a later continuation still needs.
	var state a2a.TaskState
	if res.Data.Task != nil {
		state = res.Data.Task.Status.State
	}
	if state != a2a.TaskStateUnspecified && !state.Terminal() {
		return res, nil
	}

	// MarkTerminating durably records the decision itself, before any of
	// the teardown work it implies starts. Best-effort, logged rather than
	// propagated, the same as finishTermination's own errors below: this relay
	// loop's real, successful result (res) is already in hand by this point, and
	// failing the whole call over a bookkeeping write would be a worse outcome than
	// a session that stays RUNNING a little longer than it should.
	if err := bk.store.MarkTerminating(ctx, bk.sessionID); err != nil {
		log.Infof("marking session %s terminating: %v", bk.sessionID, err)
	}
	finishTermination(ctx, c, bk)
	return res, nil
}

// finishTermination tears down everything left for a session once it's
// known to be done: deletes every private actor it ever created and durably
// marks it TERMINATED.
func finishTermination(ctx context.Context, c *controller.Controller, bk *bookkeeping) {
	cleanupActors(ctx, c, bk)
	if err := bk.store.TerminateSession(ctx, bk.sessionID); err != nil {
		log.Infof("terminating session %s: %v", bk.sessionID, err)
	}
}

// actorDeleter is implemented by harnesses backed by a deletable resource
// (substrate.SubstrateHarness's Substrate actor).
type actorDeleter interface {
	DeleteActor(ctx context.Context, actorID string) error
}

// crashTagDeleter is implemented by harnesses that can leave behind a
// crash-recovery snapshot tag keyed off an actor id.
type crashTagDeleter interface {
	DeleteCrashRecoveryTag(ctx context.Context, actorID string) error
}

// cleanupActors deletes every private actor this run created, now that the
// run has reached a terminal reply and none of them will ever be revisited.
func cleanupActors(ctx context.Context, c *controller.Controller, bk *bookkeeping) {
	actors, err := visitedActorsFromHistory(ctx, bk.el, bk.sessionID)
	if err != nil {
		log.Infof("cleanup: could not derive visited actors for session %s: %v", bk.sessionID, err)
		return
	}
	for actorID, agent := range actors {
		h, err := c.Registry().Harness(agent)
		if err != nil {
			log.Infof("cleanup: could not look up harness %s: %v", agent, err)
			continue
		}
		if deleter, ok := h.(actorDeleter); ok {
			log.Infof("cleanup: deleting actor %s (%s)", actorID, agent)
			if err := deleter.DeleteActor(ctx, actorID); err != nil {
				log.Infof("cleanup: could not delete actor %s (%s): %v", actorID, agent, err)
			}
		}
		if tagDeleter, ok := h.(crashTagDeleter); ok {
			if err := tagDeleter.DeleteCrashRecoveryTag(ctx, actorID); err != nil {
				log.Infof("cleanup: could not delete crash-recovery tag for actor %s (%s): %v", actorID, agent, err)
			}
		}
	}
}

// execA2A JSON-encodes msg, sends it to agent's private actor alongside that
// actor's prior history, retrying indefinitely on failure, and parses the reply as
// an A2A message.
func execA2A(ctx context.Context, c *controller.Controller, el eventlog.EventLog, agent, actorID, sessionID string, msg *hop.Envelope) (*hop.Envelope, error) {
	events, err := el.Events(ctx, actorID)
	if err != nil {
		return nil, fmt.Errorf("loading history for %s: %w", agent, err)
	}
	history := historySteps(events)
	// A caller may hand this a fresh envelope with no StepID at all (empty
	// is not a valid one on the wire -- harness/runtime.go's Connect rejects
	// it outright): mint unconditionally in that case. Only re-mint an
	// already-set one on an actual collision with this actor's own history.
	const maxStepIDAttempts = 5
	for attempt := 0; msg.StepID == "" || stepIDInUse(events, msg.StepID); attempt++ {
		if attempt >= maxStepIDAttempts {
			return nil, fmt.Errorf("could not mint a unique StepID for %s after %d attempts", agent, maxStepIDAttempts)
		}
		msg.StepID = uuid.NewString()
	}

	b, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("encoding message to %s: %w", agent, err)
	}
	req := &proto.CreateInteractionEvent{
		AgentId:        agent,
		ConversationId: actorID,
		Inputs:         append(history, userStep(string(b))),
		SessionId:      sessionID,
	}
	var mismatchDelay time.Duration
	for {
		out, err := retryExec(ctx, c, req)
		if err != nil {
			return nil, err
		}
		reply, ok := hop.Parse(out)
		if !ok {
			return nil, fmt.Errorf("reply from %s is not an A2A message: %s", agent, out)
		}
		// harness/runtime.go's Connect stamps every reply for a turn with exactly
		// the StepID that turn was delivered under.
		if reply.StepID == msg.StepID {
			return reply, nil
		}
		mismatchDelay = advanceDelay(mismatchDelay, connFailInitialRetryDelay, connFailMaxRetryDelay)
		log.Infof("agent %s replied with StepID %q, want %q (this turn's own) -- retrying in %s",
			agent, reply.StepID, msg.StepID, mismatchDelay)
		if err := sleep(ctx, mismatchDelay); err != nil {
			return nil, err
		}
	}
}

// historySteps returns events re-wrapped as one user step per turn, each carrying
// that turn's own logged A2A message JSON verbatim. A real a2asrv.AgentExecutor
// reconstruct ExecutorContext.StoredTask from them.
func historySteps(events []*proto.StepEvent) []*proto.Step {
	var steps []*proto.Step
	for _, ev := range events {
		if text := stepText(ev.Steps); text != "" {
			steps = append(steps, userStep(text))
		}
	}
	return steps
}

// stepIDInUse reports whether events already contains an entry parsing as
// a hop.Envelope whose own StepID is stepID.
func stepIDInUse(events []*proto.StepEvent, stepID string) bool {
	for _, ev := range events {
		if env, ok := hop.Parse(newestText(ev.Steps)); ok && env.StepID == stepID {
			return true
		}
	}
	return false
}

// retryExec runs req against c, retrying indefinitely with capped
// exponential backoff on failure. Safe to resend: a failure here always
// means c.Exec never reached a durable commit, since it's a plain in-process
// call with no network hop of its own -- so a retry just resumes the same
// still-PENDING turn exactly where it left off (see Controller.Exec's own
// PENDING-state handling), never re-runs an already-completed one.
func retryExec(ctx context.Context, c *controller.Controller, req *proto.CreateInteractionEvent) (string, error) {
	var b retryBackoff
	for {
		out, err := rawExec(ctx, c, req)
		if err == nil {
			return out, nil
		}
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		delay := b.next(err)
		log.Infof("ax exec failed, retrying in %s: %v", delay, err)
		if err := sleep(ctx, delay); err != nil {
			return "", err
		}
	}
}

// sleep waits for delay or until ctx is done, whichever comes first.
func sleep(ctx context.Context, delay time.Duration) error {
	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
