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
	"fmt"
	"time"
)

// retentionBatchSize bounds how many expired sessions sweepExpiredSessions
// considers per call. A large backlog is drained over multiple
// runRetentionLoop ticks instead of one huge transaction.
const retentionBatchSize = 100

// sweepExpiredSessions deletes every TERMINATED session whose last_updated
// is older than cutoff, along with its checkpointd_tasks and
// conversation_log rows, all in one transaction per session. It scans
// sessions rather than tasks, since a session with no task ever attached
// would otherwise never be found.
func sweepExpiredSessions(ctx context.Context, db *sql.DB, registry *taskRegistry, cutoff time.Time) (deleted int, err error) {
	rows, err := db.QueryContext(ctx,
		"SELECT id FROM checkpointd_sessions WHERE state = $1 AND last_updated < $2 ORDER BY last_updated LIMIT $3",
		sessionStateTerminated, cutoff.UnixNano(), retentionBatchSize)
	if err != nil {
		return 0, fmt.Errorf("retention: querying expired sessions: %w", err)
	}
	var sessionIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, fmt.Errorf("retention: scanning expired session: %w", err)
		}
		sessionIDs = append(sessionIDs, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("retention: iterating expired sessions: %w", err)
	}

	for _, id := range sessionIDs {
		if registry.isActive(id) {
			// Shouldn't happen for a TERMINATED session, but skip rather
			// than risk deleting one still in use. A later sweep will
			// reconsider it.
			log.Infof("retention: session %s matched but is still active, skipping", id)
			continue
		}
		ok, err := deleteSessionIfStillExpired(ctx, db, id, cutoff)
		if err != nil {
			log.Infof("retention: deleting session %s: %v", id, err)
			continue
		}
		if ok {
			deleted++
		}
	}
	return deleted, nil
}

// deleteSessionIfStillExpired atomically deletes sessionID's own session
// row, plus every task and conversation_log row keyed by it, after
// re-confirming inside the same transaction that it's still TERMINATED and
// older than cutoff. It reports whether it actually deleted anything;
// false (not an error) means sessionID no longer matched.
func deleteSessionIfStillExpired(ctx context.Context, db *sql.DB, sessionID string, cutoff time.Time) (deleted bool, err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	var state string
	var lastUpdated int64
	err = tx.QueryRowContext(ctx, "SELECT state, last_updated FROM checkpointd_sessions WHERE id = $1", sessionID).Scan(&state, &lastUpdated)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("re-checking session: %w", err)
	}
	if state != sessionStateTerminated || lastUpdated >= cutoff.UnixNano() {
		return false, nil
	}

	if _, err := tx.ExecContext(ctx, "DELETE FROM conversation_log WHERE session_id = $1", sessionID); err != nil {
		return false, fmt.Errorf("deleting conversation_log rows: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM checkpointd_tasks WHERE session_id = $1", sessionID); err != nil {
		return false, fmt.Errorf("deleting checkpointd_tasks rows: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM checkpointd_sessions WHERE id = $1", sessionID); err != nil {
		return false, fmt.Errorf("deleting checkpointd_sessions row: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit: %w", err)
	}
	return true, nil
}

// runRetentionLoop calls sweepExpiredSessions every interval until ctx is done.
func runRetentionLoop(ctx context.Context, db *sql.DB, registry *taskRegistry, retentionPeriod, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cutoff := time.Now().Add(-retentionPeriod)
			deleted, err := sweepExpiredSessions(ctx, db, registry, cutoff)
			if err != nil {
				log.Infof("retention: sweep failed: %v", err)
				continue
			}
			if deleted > 0 {
				log.Infof("retention: deleted %d expired session(s)", deleted)
			}
		}
	}
}
