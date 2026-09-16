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
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/ktock/checkpointd/internal/hop"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// Session states.
const (
	// RUNNING covers a session's entire life from the moment a
	// client's SendMessage bootstraps a brand-new relay loop until
	// the moment it's decided to be done.
	sessionStateRunning     = "RUNNING"

	// Nothing further will ever be relayed for this session. Must
	// itself be durable before any of the teardown work starts, so
	// a crash mid-teardown resumes straight into finishing that teardown
	sessionStateTerminating = "TERMINATING"

	// Nothing ever transitions a session back to RUNNING or TERMINATING.
	sessionStateTerminated  = "TERMINATED"
)

// createSessionsTable creates checkpointd_sessions (if it doesn't exist yet) on
// db. checkpointd_sessions is the durable anchor for one client SendMessage call's
// whole relay loop, including the original message that started it.
func createSessionsTable(db *sql.DB, dialect string) error {
	intType := "BIGINT"
	if dialect == "sqlite" {
		intType = "INTEGER"
	}
	if _, err := db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS checkpointd_sessions (
			id TEXT PRIMARY KEY,
			state TEXT NOT NULL,
			owner_agent TEXT NOT NULL,
			bootstrap TEXT NOT NULL,
			last_updated %s NOT NULL
		)`, intType)); err != nil {
		return fmt.Errorf("sessionstore: create checkpointd_sessions table: %w", err)
	}
	return nil
}

// sessionRow is one checkpointd_sessions row, read back via GetSession.
type sessionRow struct {
	id          string
	state       string
	ownerAgent  string
	bootstrap   *hop.Envelope
	lastUpdated int64
}

// errDuplicateSessionID wraps CreateSession's own error when id already
// exists.
var errDuplicateSessionID = errors.New("sessionstore: session id already exists")

// CreateSession durably creates id's session row in RUNNING state, together
// with bootstrap.
func (s *sqlTaskStore) CreateSession(ctx context.Context, id, ownerAgent string, bootstrap *hop.Envelope) error {
	b, err := json.Marshal(bootstrap)
	if err != nil {
		return fmt.Errorf("sessionstore: encoding bootstrap: %w", err)
	}
	if _, err := s.db.ExecContext(ctx,
		"INSERT INTO checkpointd_sessions (id, state, owner_agent, bootstrap, last_updated) VALUES ($1, $2, $3, $4, $5)",
		id, sessionStateRunning, ownerAgent, string(b), time.Now().UnixNano()); err != nil {
		if isDuplicateKeyError(err) {
			return fmt.Errorf("%w: %w", errDuplicateSessionID, err)
		}
		return fmt.Errorf("sessionstore: create session: %w", err)
	}
	return nil
}

// isDuplicateKeyError reports whether err is a primary-key/unique
// constraint violation from either SQL backend checkpointd supports.
func isDuplicateKeyError(err error) bool {
	var sqliteErr *sqlite.Error
	if errors.As(err, &sqliteErr) {
		switch sqliteErr.Code() {
		case sqlite3.SQLITE_CONSTRAINT_PRIMARYKEY, sqlite3.SQLITE_CONSTRAINT_UNIQUE:
			return true
		}
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "23505" // unique_violation
	}
	return false
}

// MarkTerminating durably marks id's session TERMINATING -- called the
// moment it's decided that nothing further will ever be relayed for this
// session, before any of the teardown work that decision implies actually
// starts.
func (s *sqlTaskStore) MarkTerminating(ctx context.Context, id string) error {
	if _, err := s.db.ExecContext(ctx,
		"UPDATE checkpointd_sessions SET state = $1, last_updated = $2 WHERE id = $3 AND state = $4",
		sessionStateTerminating, time.Now().UnixNano(), id, sessionStateRunning); err != nil {
		return fmt.Errorf("sessionstore: mark session terminating: %w", err)
	}
	return nil
}

// TerminateSession durably marks id's session TERMINATED -- the final step
// of finishTermination, once every actor it ever created has been deleted.
func (s *sqlTaskStore) TerminateSession(ctx context.Context, id string) error {
	if _, err := s.db.ExecContext(ctx,
		"UPDATE checkpointd_sessions SET state = $1, last_updated = $2 WHERE id = $3",
		sessionStateTerminated, time.Now().UnixNano(), id); err != nil {
		return fmt.Errorf("sessionstore: terminate session: %w", err)
	}
	return nil
}

// GetSession reads back id's current session row, or sql.ErrNoRows if none
// exists.
func (s *sqlTaskStore) GetSession(ctx context.Context, id string) (*sessionRow, error) {
	row := s.db.QueryRowContext(ctx,
		"SELECT id, state, owner_agent, bootstrap, last_updated FROM checkpointd_sessions WHERE id = $1", id)
	var r sessionRow
	var b string
	if err := row.Scan(&r.id, &r.state, &r.ownerAgent, &b, &r.lastUpdated); err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(b), &r.bootstrap); err != nil {
		return nil, fmt.Errorf("sessionstore: decoding bootstrap for session %s: %w", id, err)
	}
	return &r, nil
}

// sessionListPageSize bounds how many sessions ListResumableSessions
// returns per call.
const sessionListPageSize = 100

// resumableSession is one row ListResumableSessions returns.
type resumableSession struct {
	id    string
	state string
}

// ListResumableSessions returns up to sessionListPageSize sessions not yet
// TERMINATED.
func (s *sqlTaskStore) ListResumableSessions(ctx context.Context, pageToken string) (sessions []resumableSession, nextPageToken string, err error) {
	where := "WHERE state IN ($1, $2)"
	args := []any{sessionStateRunning, sessionStateTerminating}
	if pageToken != "" {
		cursorUpdated, cursorID, err := decodeListTasksPageToken(pageToken)
		if err != nil {
			return nil, "", err
		}
		where += fmt.Sprintf(" AND (last_updated > $%d OR (last_updated = $%d AND id > $%d))",
			len(args)+1, len(args)+1, len(args)+2)
		args = append(args, cursorUpdated, cursorID)
	}
	query := "SELECT id, state, last_updated FROM checkpointd_sessions " + where +
		fmt.Sprintf(" ORDER BY last_updated, id LIMIT %d", sessionListPageSize+1)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", fmt.Errorf("sessionstore: list resumable sessions: %w", err)
	}
	defer rows.Close()

	type row struct {
		resumableSession
		lastUpdated int64
	}
	var all []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.state, &r.lastUpdated); err != nil {
			return nil, "", fmt.Errorf("sessionstore: list resumable sessions scan: %w", err)
		}
		all = append(all, r)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("sessionstore: list resumable sessions iterate: %w", err)
	}

	if len(all) > sessionListPageSize {
		last := all[sessionListPageSize-1]
		nextPageToken = encodeListTasksPageToken(last.lastUpdated, last.id)
		all = all[:sessionListPageSize]
	}
	sessions = make([]resumableSession, len(all))
	for i, r := range all {
		sessions[i] = r.resumableSession
	}
	return sessions, nextPageToken, nil
}

// TaskIDForSession returns the a2a.TaskID of the task created for
// sessionID's relay loop so far.
func (s *sqlTaskStore) TaskIDForSession(ctx context.Context, sessionID string) (taskID string, ok bool, err error) {
	row := s.db.QueryRowContext(ctx, "SELECT task_id FROM checkpointd_tasks WHERE session_id = $1", sessionID)
	if err := row.Scan(&taskID); err != nil {
		if err == sql.ErrNoRows {
			return "", false, nil
		}
		return "", false, fmt.Errorf("sessionstore: task id for session: %w", err)
	}
	return taskID, true, nil
}
