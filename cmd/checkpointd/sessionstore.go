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
	sessionStateRunning = "RUNNING"

	// Nothing further will ever be relayed for this session. Must
	// itself be durable before any of the teardown work starts, so
	// a crash mid-teardown resumes straight into finishing that teardown
	sessionStateTerminating = "TERMINATING"

	// Nothing ever transitions a session back to RUNNING or TERMINATING.
	sessionStateTerminated = "TERMINATED"
)

// sqlExecer is satisfied by both *sql.DB and *sql.Tx, so schema creation can
// run either directly or inside withPostgresSchemaLock's transaction.
type sqlExecer interface {
	Exec(query string, args ...any) (sql.Result, error)
}

// createSessionsTable creates checkpointd_sessions (if it doesn't exist yet) on
// exec. checkpointd_sessions is the durable anchor for one client SendMessage call's
// whole relay loop, including the original message that started it.
func createSessionsTable(exec sqlExecer, dialect string) error {
	intType := "BIGINT"
	if dialect == "sqlite" {
		intType = "INTEGER"
	}
	if _, err := exec.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS checkpointd_sessions (
			id TEXT PRIMARY KEY,
			state TEXT NOT NULL,
			owner_agent TEXT NOT NULL,
			owner_pod TEXT NOT NULL DEFAULT '',
			owner_uid TEXT NOT NULL DEFAULT '',
			bootstrap TEXT NOT NULL,
			last_updated %s NOT NULL
		)`, intType)); err != nil {
		return fmt.Errorf("sessionstore: create checkpointd_sessions table: %w", err)
	}
	return nil
}

// withPostgresSchemaLock runs fn once, inside a single Postgres transaction
// holding a transaction-scoped advisory lock, to serialize first-time schema
// creation when multiple checkpointd replicas start against the same fresh
// Postgres database at once.
func withPostgresSchemaLock(db *sql.DB, fn func(sqlExecer) error) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("sessionstore: begin schema-creation transaction: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op once committed
	// Arbitrary fixed key: any int64 works, as long as every checkpointd
	// instance uses the same one so they actually contend on it.
	const schemaLockKey = 848301
	if _, err := tx.Exec("SELECT pg_advisory_xact_lock($1)", schemaLockKey); err != nil {
		return fmt.Errorf("sessionstore: acquire schema-creation advisory lock: %w", err)
	}
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sessionstore: commit schema-creation transaction: %w", err)
	}
	return nil
}

// sessionRow is one checkpointd_sessions row, read back via GetSession.
type sessionRow struct {
	id          string
	state       string
	ownerAgent  string
	ownerPod    string
	ownerUID    string
	bootstrap   *hop.Envelope
	lastUpdated int64
}

// errDuplicateSessionID wraps CreateSession's own error when id already
// exists.
var errDuplicateSessionID = errors.New("sessionstore: session id already exists")

// CreateSession durably creates id's session row in RUNNING state, owned
// from inception by (ownerPod, ownerUID) (a brand-new session id has no
// cross-instance contention, so no separate claim step is needed), together
// with bootstrap.
func (s *sqlTaskStore) CreateSession(ctx context.Context, id, ownerAgent, ownerPod, ownerUID string, bootstrap *hop.Envelope) error {
	b, err := json.Marshal(bootstrap)
	if err != nil {
		return fmt.Errorf("sessionstore: encoding bootstrap: %w", err)
	}
	if _, err := s.db.ExecContext(ctx,
		"INSERT INTO checkpointd_sessions (id, state, owner_agent, owner_pod, owner_uid, bootstrap, last_updated) VALUES ($1, $2, $3, $4, $5, $6, $7)",
		id, sessionStateRunning, ownerAgent, ownerPod, ownerUID, string(b), time.Now().UnixNano()); err != nil {
		if isDuplicateKeyError(err) {
			return fmt.Errorf("%w: %w", errDuplicateSessionID, err)
		}
		return fmt.Errorf("sessionstore: create session: %w", err)
	}
	return nil
}

// ClaimSession atomically claims id for (ownerPod, ownerUID), succeeding
// only if nobody currently owns it (owner_pod is empty). Used by
// continueTask and CancelTask before driving or tearing down a session that
// already existed, so at most one instance ever actively drives it at a
// time.
func (s *sqlTaskStore) ClaimSession(ctx context.Context, id, ownerPod, ownerUID string) (claimed bool, err error) {
	res, err := s.db.ExecContext(ctx,
		"UPDATE checkpointd_sessions SET owner_pod = $1, owner_uid = $2, last_updated = $3 WHERE id = $4 AND owner_pod = ''",
		ownerPod, ownerUID, time.Now().UnixNano(), id)
	if err != nil {
		return false, fmt.Errorf("sessionstore: claim session: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("sessionstore: claim session: rows affected: %w", err)
	}
	return n == 1, nil
}

// ClaimOrphanedSession atomically reassigns id's ownership to
// (newOwnerPod, newOwnerUID), succeeding only if its currently recorded
// owner_uid still equals expectedOwnerUID. Two callers share this one CAS
// shape: the salvage sweep and resumeServerSessions's own restamp step.
// Whichever of two racing callers' UPDATE lands first wins; the other affects
// 0 rows and is a silent, correct no-op.
func (s *sqlTaskStore) ClaimOrphanedSession(ctx context.Context, id, newOwnerPod, newOwnerUID, expectedOwnerUID string) (claimed bool, err error) {
	res, err := s.db.ExecContext(ctx,
		"UPDATE checkpointd_sessions SET owner_pod = $1, owner_uid = $2, last_updated = $3 WHERE id = $4 AND owner_uid = $5",
		newOwnerPod, newOwnerUID, time.Now().UnixNano(), id, expectedOwnerUID)
	if err != nil {
		return false, fmt.Errorf("sessionstore: claim orphaned session: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("sessionstore: claim orphaned session: rows affected: %w", err)
	}
	return n == 1, nil
}

// ReleaseSession unconditionally clears id's owner_pod/owner_uid back to
// empty.
func (s *sqlTaskStore) ReleaseSession(ctx context.Context, id string) error {
	if _, err := s.db.ExecContext(ctx,
		"UPDATE checkpointd_sessions SET owner_pod = '', owner_uid = '', last_updated = $1 WHERE id = $2",
		time.Now().UnixNano(), id); err != nil {
		return fmt.Errorf("sessionstore: release session: %w", err)
	}
	return nil
}

// Ping reports whether the database is currently reachable.
func (s *sqlTaskStore) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
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
		"SELECT id, state, owner_agent, owner_pod, owner_uid, bootstrap, last_updated FROM checkpointd_sessions WHERE id = $1", id)
	var r sessionRow
	var b string
	if err := row.Scan(&r.id, &r.state, &r.ownerAgent, &r.ownerPod, &r.ownerUID, &b, &r.lastUpdated); err != nil {
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
	id       string
	state    string
	ownerUID string
}

// ListResumableSessions returns up to sessionListPageSize sessions not yet
// TERMINATED and currently owned by podName.
func (s *sqlTaskStore) ListResumableSessions(ctx context.Context, pageToken, podName string) (sessions []resumableSession, nextPageToken string, err error) {
	where := "WHERE state IN ($1, $2) AND owner_pod = $3"
	args := []any{sessionStateRunning, sessionStateTerminating, podName}
	if pageToken != "" {
		cursorUpdated, cursorID, err := decodeListTasksPageToken(pageToken)
		if err != nil {
			return nil, "", err
		}
		where += fmt.Sprintf(" AND (last_updated > $%d OR (last_updated = $%d AND id > $%d))",
			len(args)+1, len(args)+1, len(args)+2)
		args = append(args, cursorUpdated, cursorID)
	}
	query := "SELECT id, state, owner_uid, last_updated FROM checkpointd_sessions " + where +
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
		if err := rows.Scan(&r.id, &r.state, &r.ownerUID, &r.lastUpdated); err != nil {
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

// ownerIncarnation identifies one specific instance incarnation currently
// recorded as owning at least one active session.
type ownerIncarnation struct {
	pod string
	uid string
}

// ListDistinctActiveOwners returns every distinct (owner_pod, owner_uid)
// pair currently recorded among non-terminal, currently-owned sessions.
func (s *sqlTaskStore) ListDistinctActiveOwners(ctx context.Context) ([]ownerIncarnation, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT DISTINCT owner_pod, owner_uid FROM checkpointd_sessions WHERE state IN ($1, $2) AND owner_pod != ''",
		sessionStateRunning, sessionStateTerminating)
	if err != nil {
		return nil, fmt.Errorf("sessionstore: list distinct active owners: %w", err)
	}
	defer rows.Close()

	var owners []ownerIncarnation
	for rows.Next() {
		var o ownerIncarnation
		if err := rows.Scan(&o.pod, &o.uid); err != nil {
			return nil, fmt.Errorf("sessionstore: list distinct active owners scan: %w", err)
		}
		owners = append(owners, o)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sessionstore: list distinct active owners iterate: %w", err)
	}
	return owners, nil
}

// ListSessionsOwnedByPodUID returns up to sessionListPageSize sessions not
// yet TERMINATED and currently owned by exactly (podName, ownerUID) -- used
// by the salvage sweep once it has confirmed that specific incarnation is
// no longer alive, so that a same-named replacement's own, already-claimed
// sessions (now carrying a different uid) are never returned alongside it.
func (s *sqlTaskStore) ListSessionsOwnedByPodUID(ctx context.Context, pageToken, podName, ownerUID string) (sessions []resumableSession, nextPageToken string, err error) {
	where := "WHERE state IN ($1, $2) AND owner_pod = $3 AND owner_uid = $4"
	args := []any{sessionStateRunning, sessionStateTerminating, podName, ownerUID}
	if pageToken != "" {
		cursorUpdated, cursorID, err := decodeListTasksPageToken(pageToken)
		if err != nil {
			return nil, "", err
		}
		where += fmt.Sprintf(" AND (last_updated > $%d OR (last_updated = $%d AND id > $%d))",
			len(args)+1, len(args)+1, len(args)+2)
		args = append(args, cursorUpdated, cursorID)
	}
	query := "SELECT id, state, owner_uid, last_updated FROM checkpointd_sessions " + where +
		fmt.Sprintf(" ORDER BY last_updated, id LIMIT %d", sessionListPageSize+1)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", fmt.Errorf("sessionstore: list sessions owned by pod/uid: %w", err)
	}
	defer rows.Close()

	type row struct {
		resumableSession
		lastUpdated int64
	}
	var all []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.state, &r.ownerUID, &r.lastUpdated); err != nil {
			return nil, "", fmt.Errorf("sessionstore: list sessions owned by pod/uid scan: %w", err)
		}
		all = append(all, r)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("sessionstore: list sessions owned by pod/uid iterate: %w", err)
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

// CountOwnedRunningSessions returns how many RUNNING sessions podName
// currently owns -- used by scale-in draining to know when it's safe to
// exit.
func (s *sqlTaskStore) CountOwnedRunningSessions(ctx context.Context, podName string) (int, error) {
	var n int
	if err := s.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM checkpointd_sessions WHERE owner_pod = $1 AND state = $2",
		podName, sessionStateRunning).Scan(&n); err != nil {
		return 0, fmt.Errorf("sessionstore: count owned running sessions: %w", err)
	}
	return n, nil
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
