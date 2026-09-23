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

package eventlog

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// OpenPostgresEventLog connects to the PostgreSQL database described by dsn and
// initializes the event log schema. Caller is responsible to ensure it is safe
// for concurrent use.
func OpenPostgresEventLog(dsn string) (EventLog, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres_eventlog: open: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("postgres_eventlog: ping: %w", err)
	}

	// Create tables if they don't exist, serialized across replicas via an
	// advisory lock -- see withPostgresSchemaLock's own doc comment for why
	// CREATE TABLE IF NOT EXISTS alone isn't safe here.
	if err := withPostgresSchemaLock(ctx, db, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `
			CREATE TABLE IF NOT EXISTS conversation_log (
				conversation_id TEXT NOT NULL,
				step INTEGER NOT NULL,
				payload TEXT NOT NULL,
				session_id TEXT,
				session_step BIGINT,
				PRIMARY KEY (conversation_id, step)
			)`)
		return err
	}); err != nil {
		db.Close()
		return nil, fmt.Errorf("postgres_eventlog: create conversation_log table: %w", err)
	}

	return &sqlEventLog{db: db, dialect: "postgres"}, nil
}

// withPostgresSchemaLock runs fn once, inside a single Postgres transaction
// holding a transaction-scoped advisory lock, to serialize first-time schema
// creation when multiple checkpointd replicas start against the same fresh
// Postgres database at once. Without this, CREATE TABLE IF NOT EXISTS isn't
// actually safe under concurrent first-time creation: the existence check
// and the creation aren't atomic across sessions, so two replicas can both
// see "doesn't exist" and both attempt the DDL, and the loser hits a
// duplicate-key error on pg_type's own catalog row for the table's implicit
// row type.
//
// The lock and fn's own statements run on the same connection (required for
// pg_advisory_xact_lock, which is scoped to whichever connection took it,
// not to the whole database) and release automatically on commit, rollback,
// or the connection simply closing -- so a crash mid-DDL can never leave it
// stuck held.
func withPostgresSchemaLock(ctx context.Context, db *sql.DB, fn func(*sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("postgres_eventlog: begin schema-creation transaction: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op once committed
	// Arbitrary fixed key: any int64 works, as long as every checkpointd
	// instance uses the same one so they actually contend on it. Distinct
	// from cmd/checkpointd's own equivalent lock key -- a collision would
	// just mean incidental extra serialization across unrelated locks, not
	// a correctness issue, but there's no reason to share one.
	const schemaLockKey = 848302
	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock($1)", schemaLockKey); err != nil {
		return fmt.Errorf("postgres_eventlog: acquire schema-creation advisory lock: %w", err)
	}
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("postgres_eventlog: commit schema-creation transaction: %w", err)
	}
	return nil
}
