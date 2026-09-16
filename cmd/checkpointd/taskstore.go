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
	"encoding/base64"
	"fmt"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv/taskstore"
	"github.com/ktock/checkpointd/internal/controller/eventlog"
	"github.com/ktock/checkpointd/internal/hop"
)

// checkpointdTaskMetaTenant is the a2a.Task.Metadata key assembleTask stamps
// the session id this task's relay loop runs under into.
const checkpointdTaskMetaTenant = "checkpointd-tenant"

// defaultListTasksHistoryLength bounds how much history ListTasks embeds in
// each returned task by default.
const defaultListTasksHistoryLength = 100

// sqlTaskStore is taskstore.Store backend.
type sqlTaskStore struct {
	db      *sql.DB
	el      eventlog.EventLog
	dialect string
}

// newSQLTaskStore creates checkpointd_tasks (if it doesn't exist yet) on db
// and returns a store backed by it. dialect must be "sqlite" or "postgres".
func newSQLTaskStore(db *sql.DB, dialect string, el eventlog.EventLog) (*sqlTaskStore, error) {
	var intType string
	if dialect == "postgres" {
		intType = "BIGINT"
	} else if dialect == "sqlite" {
		intType = "INTEGER"
	} else {
		return nil, fmt.Errorf("taskstore: unknown dialect %q", dialect)
	}
	// task_id is deliberately not its own PRIMARY KEY: TaskID is the agent's own
	// claim, never checkpointd-minted for uniqueness, so two different agents (or two
	// different sessions with the same agent) can genuinely claim the same
	// TaskID string without colliding.
	if _, err := db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS checkpointd_tasks (
			task_id TEXT NOT NULL,
			context_id TEXT NOT NULL,
			owner_agent TEXT NOT NULL,
			session_id TEXT NOT NULL,
			status TEXT NOT NULL,
			last_updated %s NOT NULL,
			version %s NOT NULL,
			PRIMARY KEY (task_id, owner_agent, session_id)
		)`, intType, intType)); err != nil {
		return nil, fmt.Errorf("taskstore: create checkpointd_tasks table: %w", err)
	}
	if err := createSessionsTable(db, dialect); err != nil {
		return nil, err
	}
	return &sqlTaskStore{db: db, el: el, dialect: dialect}, nil
}

// Create records a brand-new task row.
func (s *sqlTaskStore) Create(ctx context.Context, task *a2a.Task, ownerAgent, sessionID string) (taskstore.TaskVersion, error) {
	const version = taskstore.TaskVersion(1)
	_, err := s.db.ExecContext(ctx,
		"INSERT INTO checkpointd_tasks (task_id, context_id, owner_agent, session_id, status, last_updated, version) VALUES ($1, $2, $3, $4, $5, $6, $7)",
		string(task.ID), task.ContextID, ownerAgent, sessionID, string(task.Status.State), time.Now().UnixNano(), int64(version))
	if err != nil {
		return 0, fmt.Errorf("%w: %w", taskstore.ErrTaskAlreadyExists, err)
	}
	return version, nil
}

// Update is a conditional UPDATE keyed on (task_id, owner_agent, session_id)
// AND the caller's own PrevVersion.
func (s *sqlTaskStore) Update(ctx context.Context, req *taskstore.UpdateRequest, ownerAgent, sessionID string) (taskstore.TaskVersion, error) {
	newVersion := req.PrevVersion + 1
	result, err := s.db.ExecContext(ctx,
		"UPDATE checkpointd_tasks SET status = $1, last_updated = $2, version = $3 WHERE task_id = $4 AND owner_agent = $5 AND session_id = $6 AND version = $7",
		string(req.Task.Status.State), time.Now().UnixNano(), int64(newVersion), string(req.Task.ID), ownerAgent, sessionID, int64(req.PrevVersion))
	if err != nil {
		return 0, fmt.Errorf("taskstore: update: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("taskstore: update rows affected: %w", err)
	}
	if n == 0 {
		// Zero rows affected means either the scoping didn't match any row
		// (not found) or the version didn't (someone else's write landed first,
		// taskstore.ErrConcurrentModification)
		if _, getErr := s.getRow(ctx, req.Task.ID, ownerAgent, sessionID); getErr == a2a.ErrTaskNotFound {
			return 0, a2a.ErrTaskNotFound
		}
		return 0, taskstore.ErrConcurrentModification
	}
	return newVersion, nil
}

// storedTask is Get's return shape.
type storedTask struct {
	Task       *a2a.Task
	Version    taskstore.TaskVersion
	OwnerAgent string
	SessionID  string
}

// Get reads back taskID's row, scoped to ownerAgent/sessionID.
func (s *sqlTaskStore) Get(ctx context.Context, taskID a2a.TaskID, ownerAgent, sessionID string) (*storedTask, error) {
	row, err := s.getRow(ctx, taskID, ownerAgent, sessionID)
	if err != nil {
		return nil, err
	}
	task, err := s.assembleTask(ctx, row, nil, true)
	if err != nil {
		return nil, err
	}
	return &storedTask{Task: task, Version: row.version, OwnerAgent: row.ownerAgent, SessionID: row.sessionID}, nil
}

// GetWithHistoryLength is Get, but honoring a caller-supplied history
// length exactly (nil means the same defaultListTasksHistoryLength cap Get
// itself uses).
func (s *sqlTaskStore) GetWithHistoryLength(ctx context.Context, taskID a2a.TaskID, ownerAgent, sessionID string, historyLength *int) (*a2a.Task, error) {
	row, err := s.getRow(ctx, taskID, ownerAgent, sessionID)
	if err != nil {
		return nil, err
	}
	return s.assembleTask(ctx, row, historyLength, true)
}

// ListForAgent list tasks, scoping to one configured agent's own tasks.
func (s *sqlTaskStore) ListForAgent(ctx context.Context, agent string, req *a2a.ListTasksRequest) (*a2a.ListTasksResponse, error) {
	return s.list(ctx, agent, req)
}

type taskRow struct {
	taskID      string
	contextID   string
	ownerAgent  string
	sessionID   string
	status      string
	lastUpdated int64
	version     taskstore.TaskVersion
}

// rowScanner is the shared shape of *sql.Row and *sql.Rows' own Scan.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanTaskRow(s rowScanner) (*taskRow, error) {
	var r taskRow
	var version int64
	if err := s.Scan(&r.taskID, &r.contextID, &r.ownerAgent, &r.sessionID, &r.status, &r.lastUpdated, &version); err != nil {
		if err == sql.ErrNoRows {
			return nil, a2a.ErrTaskNotFound
		}
		return nil, fmt.Errorf("taskstore: get: %w", err)
	}
	r.version = taskstore.TaskVersion(version)
	return &r, nil
}

// getRow resolves taskID's row, always scoped to ownerAgent.
// sessionID, when supplied, is matched exactly.
func (s *sqlTaskStore) getRow(ctx context.Context, taskID a2a.TaskID, ownerAgent, sessionID string) (*taskRow, error) {
	if sessionID != "" {
		row := s.db.QueryRowContext(ctx,
			"SELECT task_id, context_id, owner_agent, session_id, status, last_updated, version FROM checkpointd_tasks WHERE task_id = $1 AND owner_agent = $2 AND session_id = $3",
			string(taskID), ownerAgent, sessionID)
		return scanTaskRow(row)
	}

	rows, err := s.db.QueryContext(ctx,
		"SELECT task_id, context_id, owner_agent, session_id, status, last_updated, version FROM checkpointd_tasks WHERE task_id = $1 AND owner_agent = $2",
		string(taskID), ownerAgent)
	if err != nil {
		return nil, fmt.Errorf("taskstore: get: %w", err)
	}
	defer rows.Close()
	var found *taskRow
	for rows.Next() {
		r, err := scanTaskRow(rows)
		if err != nil {
			return nil, err
		}
		if found != nil {
			// A second row sharing this (task_id, owner_agent) pair: genuinely
			// ambiguous with no sessionID to disambiguate.
			return nil, a2a.ErrTaskNotFound
		}
		found = r
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("taskstore: get: %w", err)
	}
	if found == nil {
		return nil, a2a.ErrTaskNotFound
	}
	return found, nil
}

// assembleTask builds the *a2a.Task by combining row's own fields with a
// full history read. historyLength (nil means "use
// defaultListTasksHistoryLength") truncates it. includeArtifacts strips
// Artifacts when the caller didn't ask for them.
func (s *sqlTaskStore) assembleTask(ctx context.Context, row *taskRow, historyLength *int, includeArtifacts bool) (*a2a.Task, error) {
	events, err := s.el.EventsBySessionID(ctx, row.sessionID)
	if err != nil {
		return nil, fmt.Errorf("taskstore: loading history for %s: %w", row.taskID, err)
	}
	var history []*a2a.Message
	var lastTask *a2a.Task
	for _, ev := range events {
		env, ok := hop.Parse(newestText(ev.Steps))
		if !ok {
			continue
		}
		if isCheckpointdInputStep(ev) {
			// A checkpointd-input event (isCheckpointdInputStep -- Role: "user",
			// mirroring relay.go's own visitedActorsFromHistory)
			if env.From != checkpointdIdentity {
				// driveRelayLoop's own forwarding of an already-logged
				// reply on to its next recipient
				continue
			}
			// the original bootstrap, or a client's own continuation message
			var msg *a2a.Message
			switch {
			case env.Data.Task != nil:
				msg = env.Data.Task.Status.Message
			case env.Data.Message != nil:
				msg = env.Data.Message
			default:
				continue
			}
			history = append(history, msg)
		} else if env.From == row.ownerAgent && (isTerminalReply(env) || env.From == env.To) {
			// row.ownerAgent's own claim (a self-continuation or a terminal reply)
			// row.ownerAgent's own reply can only ever be Task-shaped by the
			// time this function is even reachable: a taskRow only ever gets
			// created on ownerAgent's first genuine Task-shaped claim and the
			// harness runtime rejects a Message from it after that.
			if env.Data.Task == nil {
				continue
			}
			if lastTask != nil {
				history = append(history, lastTask.Status.Message)
			}
			lastTask = env.Data.Task
		}
		// a private sub-call among agents
	}

	n := defaultListTasksHistoryLength
	if historyLength != nil {
		n = *historyLength
	}
	switch {
	case n <= 0:
		history = nil
	case n < len(history):
		history = history[len(history)-n:]
	}

	var task a2a.Task
	if lastTask != nil {
		task = *lastTask
		task.Status.Message = lastTask.Status.Message
	}
	task.ID = a2a.TaskID(row.taskID)
	task.ContextID = row.contextID
	task.History = history
	task.Status.State = a2a.TaskState(row.status)
	if !includeArtifacts {
		task.Artifacts = nil
	}
	metadata := make(map[string]any, len(task.Metadata)+1)
	for k, v := range task.Metadata {
		metadata[k] = v
	}
	metadata[checkpointdTaskMetaTenant] = row.sessionID
	task.Metadata = metadata

	return &task, nil
}

// UpdateStatus durably records newState as taskID's current status;
// safe without its own retry loop only because checkpointd's own
// taskRegistry already guarantees a single writer per task,
// so no concurrent Update call ever races this read.
func (s *sqlTaskStore) UpdateStatus(ctx context.Context, taskID a2a.TaskID, ownerAgent, sessionID string, newState a2a.TaskState) error {
	row, err := s.getRow(ctx, taskID, ownerAgent, sessionID)
	if err != nil {
		return err
	}
	_, err = s.Update(ctx, &taskstore.UpdateRequest{
		Task:        &a2a.Task{ID: taskID, Status: a2a.TaskStatus{State: newState}},
		PrevVersion: row.version,
	}, ownerAgent, sessionID)
	return err
}

// CancelTaskAndSession durably, atomically marks taskID Canceled and
// sessionID TERMINATING together.
func (s *sqlTaskStore) CancelTaskAndSession(ctx context.Context, taskID a2a.TaskID, ownerAgent, sessionID string, prevVersion taskstore.TaskVersion) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("taskstore: cancel: begin tx: %w", err)
	}
	defer tx.Rollback()

	newVersion := prevVersion + 1
	result, err := tx.ExecContext(ctx,
		"UPDATE checkpointd_tasks SET status = $1, last_updated = $2, version = $3 WHERE task_id = $4 AND owner_agent = $5 AND session_id = $6 AND version = $7",
		string(a2a.TaskStateCanceled), time.Now().UnixNano(), int64(newVersion), string(taskID), ownerAgent, sessionID, int64(prevVersion))
	if err != nil {
		return fmt.Errorf("taskstore: cancel: update task: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("taskstore: cancel: update task rows affected: %w", err)
	}
	if n == 0 {
		return taskstore.ErrConcurrentModification
	}
	if _, err := tx.ExecContext(ctx,
		"UPDATE checkpointd_sessions SET state = $1, last_updated = $2 WHERE id = $3 AND state = $4",
		sessionStateTerminating, time.Now().UnixNano(), sessionID, sessionStateRunning); err != nil {
		return fmt.Errorf("taskstore: cancel: mark session terminating: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("taskstore: cancel: commit: %w", err)
	}
	return nil
}

func (s *sqlTaskStore) list(ctx context.Context, ownerAgent string, req *a2a.ListTasksRequest) (*a2a.ListTasksResponse, error) {
	pageSize := req.PageSize
	switch {
	case pageSize == 0:
		pageSize = 50
	case pageSize < 1 || pageSize > 100:
		return nil, fmt.Errorf("page size must be between 1 and 100 inclusive, got %d: %w", pageSize, a2a.ErrInvalidRequest)
	}

	where := "WHERE 1=1"
	var args []any
	arg := func(v any) string {
		args = append(args, v)
		return fmt.Sprintf("$%d", len(args))
	}
	if ownerAgent != "" {
		where += " AND owner_agent = " + arg(ownerAgent)
	}
	if req.ContextID != "" {
		where += " AND context_id = " + arg(req.ContextID)
	}
	if req.Status != a2a.TaskStateUnspecified {
		where += " AND status = " + arg(string(req.Status))
	}

	var totalSize int
	countQuery := "SELECT COUNT(*) FROM checkpointd_tasks " + where
	if err := s.db.QueryRowContext(ctx, countQuery, args...).Scan(&totalSize); err != nil {
		return nil, fmt.Errorf("taskstore: list count: %w", err)
	}

	var cursorUpdated int64
	var cursorTaskID string
	if req.PageToken != "" {
		var err error
		cursorUpdated, cursorTaskID, err = decodeListTasksPageToken(req.PageToken)
		if err != nil {
			return nil, err
		}
		where += fmt.Sprintf(" AND (last_updated < %s OR (last_updated = %s AND task_id < %s))",
			arg(cursorUpdated), arg(cursorUpdated), arg(cursorTaskID))
	}

	query := "SELECT task_id, context_id, owner_agent, session_id, status, last_updated, version FROM checkpointd_tasks " +
		where + fmt.Sprintf(" ORDER BY last_updated DESC, task_id DESC LIMIT %s", arg(pageSize+1))
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("taskstore: list: %w", err)
	}
	defer rows.Close()

	var taskRows []*taskRow
	for rows.Next() {
		var r taskRow
		var version int64
		if err := rows.Scan(&r.taskID, &r.contextID, &r.ownerAgent, &r.sessionID, &r.status, &r.lastUpdated, &version); err != nil {
			return nil, fmt.Errorf("taskstore: list scan: %w", err)
		}
		r.version = taskstore.TaskVersion(version)
		taskRows = append(taskRows, &r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("taskstore: list iterate: %w", err)
	}

	var nextPageToken string
	if len(taskRows) > pageSize {
		last := taskRows[pageSize-1]
		nextPageToken = encodeListTasksPageToken(last.lastUpdated, last.taskID)
		taskRows = taskRows[:pageSize]
	}

	tasks := make([]*a2a.Task, 0, len(taskRows))
	for _, r := range taskRows {
		task, err := s.assembleTask(ctx, r, req.HistoryLength, req.IncludeArtifacts)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}

	return &a2a.ListTasksResponse{
		Tasks:         tasks,
		TotalSize:     totalSize,
		PageSize:      pageSize,
		NextPageToken: nextPageToken,
	}, nil
}

// encodeListTasksPageToken/decodeListTasksPageToken are checkpointd's own
// opaque keyset-pagination cursor -- (last_updated, task_id), matching the
// column pair ListTasks already sorts and filters by -- not required to be
// compatible with any other server's own page tokens, only checkpointd's own
// across calls.
func encodeListTasksPageToken(lastUpdated int64, taskID string) string {
	return base64.URLEncoding.EncodeToString(fmt.Appendf(nil, "%d_%s", lastUpdated, taskID))
}

func decodeListTasksPageToken(token string) (lastUpdated int64, taskID string, err error) {
	decoded, err := base64.URLEncoding.DecodeString(token)
	if err != nil {
		return 0, "", a2a.ErrParseError
	}
	if _, err := fmt.Sscanf(string(decoded), "%d_%s", &lastUpdated, &taskID); err != nil {
		return 0, "", a2a.ErrParseError
	}
	return lastUpdated, taskID, nil
}
