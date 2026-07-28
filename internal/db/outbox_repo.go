package db

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type OutboxEvent struct {
	ID            uuid.UUID
	Topic         string
	PartitionKey  string
	EventType     string
	SchemaVersion int
	Payload       json.RawMessage
	CreatedAt     time.Time
}

type OutboxRepo struct {
	pool *pgxpool.Pool
}

func NewOutboxRepo(pool *pgxpool.Pool) *OutboxRepo {
	return &OutboxRepo{pool: pool}
}

// Insert writes an outbox event within tx, so it commits atomically with
// whatever business-data write (e.g. an order) the event describes.
func (r *OutboxRepo) Insert(ctx context.Context, tx pgx.Tx, topic, partitionKey, eventType string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("outbox_repo: marshal payload: %w", err)
	}

	id, err := uuid.NewV7()
	if err != nil {
		return fmt.Errorf("outbox_repo: generate id: %w", err)
	}

	const q = `
		INSERT INTO outbox_events (id, topic, partition_key, event_type, payload)
		VALUES ($1, $2, $3, $4, $5)`

	if _, err := tx.Exec(ctx, q, id, topic, partitionKey, eventType, body); err != nil {
		return fmt.Errorf("outbox_repo: insert: %w", err)
	}
	return nil
}

// FetchAndClaimUnpublished returns up to limit unpublished events and locks
// the underlying rows for the lifetime of tx (FOR UPDATE SKIP LOCKED). When
// multiple relays (e.g. one per service, all sharing this table) poll
// concurrently, each row is claimed by exactly one relay — the others skip
// locked rows rather than blocking or double-fetching them. Callers must
// mark returned events published (or let tx roll back) before committing.
func (r *OutboxRepo) FetchAndClaimUnpublished(ctx context.Context, tx pgx.Tx, limit int) ([]OutboxEvent, error) {
	const q = `
		SELECT id, topic, partition_key, event_type, schema_version, payload, created_at
		FROM outbox_events
		WHERE published_at IS NULL
		ORDER BY created_at
		LIMIT $1
		FOR UPDATE SKIP LOCKED`

	rows, err := tx.Query(ctx, q, limit)
	if err != nil {
		return nil, fmt.Errorf("outbox_repo: fetch_and_claim_unpublished: %w", err)
	}
	defer rows.Close()

	var events []OutboxEvent
	for rows.Next() {
		var e OutboxEvent
		if err := rows.Scan(&e.ID, &e.Topic, &e.PartitionKey, &e.EventType, &e.SchemaVersion, &e.Payload, &e.CreatedAt); err != nil {
			return nil, fmt.Errorf("outbox_repo: scan: %w", err)
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("outbox_repo: rows: %w", err)
	}
	return events, nil
}

func (r *OutboxRepo) MarkPublished(ctx context.Context, tx pgx.Tx, id uuid.UUID) error {
	const q = `UPDATE outbox_events SET published_at = now() WHERE id = $1`
	if _, err := tx.Exec(ctx, q, id); err != nil {
		return fmt.Errorf("outbox_repo: mark_published: %w", err)
	}
	return nil
}

func (r *OutboxRepo) BeginTx(ctx context.Context) (pgx.Tx, error) {
	return r.pool.Begin(ctx)
}
