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

	"github.com/ktock/checkpointd/proto"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// sqlEventLog is a database backed EventLog shared by the SQLite and
// PostgreSQL implementations.
type sqlEventLog struct {
	db      *sql.DB
	dialect string
}

// SQLDB returns the underlying *sql.DB and backend name ("sqlite" or
// "postgres") for an EventLog opened via OpenSQLiteEventLog or
// OpenPostgresEventLog -- for a caller (cmd/checkpointd's own task store) that
// needs its own additional tables on the exact same connection, rather than
// a second, independent connection to the same database. ok is false for
// any other EventLog implementation (e.g. eventlogtest's in-memory fake),
// which has no SQL connection to share at all.
func SQLDB(el EventLog) (db *sql.DB, dialect string, ok bool) {
	e, isSQL := el.(*sqlEventLog)
	if !isSQL {
		return nil, "", false
	}
	return e.db, e.dialect, true
}

// Append serializes the event to JSON and inserts it into the database.
func (l *sqlEventLog) Append(ctx context.Context, event *proto.StepEvent) (step int64, err error) {
	ctx, endSpan := l.startSpan(ctx, "Append", event.ConversationId)
	defer func() { endSpan(err) }()

	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("eventlog: begin tx: %w", err)
	}
	defer tx.Rollback()

	if err := tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(step), 0) + 1 FROM conversation_log WHERE conversation_id = $1", event.ConversationId).Scan(&step); err != nil {
		return 0, fmt.Errorf("eventlog: compute step: %w", err)
	}

	// session_step orders one session's events across however many
	// conversation_ids it spans.
	var sessionStep sql.NullInt64
	if event.SessionId != "" {
		var ss int64
		if err := tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(session_step), 0) + 1 FROM conversation_log WHERE session_id = $1", event.SessionId).Scan(&ss); err != nil {
			return 0, fmt.Errorf("eventlog: compute session_step: %w", err)
		}
		sessionStep = sql.NullInt64{Int64: ss, Valid: true}
	}

	payload, err := marshalOpts.Marshal(event)
	if err != nil {
		return 0, fmt.Errorf("eventlog: marshal event: %w", err)
	}

	sessionID := sql.NullString{String: event.SessionId, Valid: event.SessionId != ""}
	if _, err := tx.ExecContext(ctx,
		"INSERT INTO conversation_log (conversation_id, step, payload, session_id, session_step) VALUES ($1, $2, $3, $4, $5)",
		event.ConversationId, step, string(payload), sessionID, sessionStep); err != nil {
		return 0, fmt.Errorf("eventlog: insert conversation: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("eventlog: commit: %w", err)
	}

	return step, nil
}

// Events retrieves all events from the database for a conversation, ordered by step.
func (l *sqlEventLog) Events(ctx context.Context, conversationID string) (events []*proto.StepEvent, err error) {
	ctx, endSpan := l.startSpan(ctx, "Events", conversationID)
	defer func() { endSpan(err) }()

	rows, err := l.db.QueryContext(ctx, "SELECT payload FROM conversation_log WHERE conversation_id = $1 ORDER BY step", conversationID)
	if err != nil {
		return nil, fmt.Errorf("eventlog: query conversation: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return nil, fmt.Errorf("eventlog: scan conversation: %w", err)
		}

		ev := &proto.StepEvent{}
		if err := unmarshalOpts.Unmarshal([]byte(payload), ev); err != nil {
			return nil, fmt.Errorf("eventlog: unmarshal event: %w", err)
		}
		events = append(events, ev)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("eventlog: iterate conversation: %w", err)
	}

	return events, nil
}

// EventsBySessionID retrieves all events from the database for a session.
func (l *sqlEventLog) EventsBySessionID(ctx context.Context, sessionID string) (events []*proto.StepEvent, err error) {
	ctx, endSpan := l.startSpan(ctx, "EventsBySessionID", sessionID)
	defer func() { endSpan(err) }()

	rows, err := l.db.QueryContext(ctx, "SELECT payload FROM conversation_log WHERE session_id = $1 ORDER BY session_step", sessionID)
	if err != nil {
		return nil, fmt.Errorf("eventlog: query session: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return nil, fmt.Errorf("eventlog: scan session event: %w", err)
		}

		ev := &proto.StepEvent{}
		if err := unmarshalOpts.Unmarshal([]byte(payload), ev); err != nil {
			return nil, fmt.Errorf("eventlog: unmarshal event: %w", err)
		}
		events = append(events, ev)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("eventlog: iterate session: %w", err)
	}

	return events, nil
}

// Close releases the database connection.
func (l *sqlEventLog) Close() error {
	return l.db.Close()
}

func (l *sqlEventLog) startSpan(ctx context.Context, name string, conversationID string) (context.Context, func(err error)) {
	const tracerName = "eventlog.sql"
	tracer := otel.Tracer(tracerName)

	ctx, span := tracer.Start(ctx, tracerName+"/"+name, trace.WithAttributes(
		attribute.String("conversation_id", conversationID),
	))
	return ctx, func(err error) {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}
