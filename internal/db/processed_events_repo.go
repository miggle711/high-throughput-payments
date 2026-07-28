package db

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type ProcessedEventsRepo struct{}

func NewProcessedEventsRepo() *ProcessedEventsRepo {
	return &ProcessedEventsRepo{}
}

// MarkProcessed records that (orderID, eventType) has been handled within
// tx. Returns (false, nil) if it was already recorded — the ON CONFLICT
// makes this the idempotency check: callers use the returned bool to decide
// whether to skip reprocessing (e.g. on Kafka redelivery).
func (r *ProcessedEventsRepo) MarkProcessed(ctx context.Context, tx pgx.Tx, orderID uuid.UUID, eventType string) (bool, error) {
	const q = `
		INSERT INTO processed_events (order_id, event_type)
		VALUES ($1, $2)
		ON CONFLICT (order_id, event_type) DO NOTHING`

	tag, err := tx.Exec(ctx, q, orderID, eventType)
	if err != nil {
		return false, fmt.Errorf("processed_events_repo: mark_processed: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}
