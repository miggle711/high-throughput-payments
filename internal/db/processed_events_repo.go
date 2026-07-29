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

// MarkProcessed records that (orderID, eventType) has been handled by
// consumerGroup within tx. Returns (false, nil) if it was already recorded
// for that consumer group. This is the idempotency check: callers use the
// returned bool to decide whether to skip reprocessing, for example on
// Kafka redelivery. Scoping by consumerGroup keeps two different services
// consuming the same event from colliding on the same record, since each
// service has its own idempotency state.
func (r *ProcessedEventsRepo) MarkProcessed(ctx context.Context, tx pgx.Tx, orderID uuid.UUID, eventType, consumerGroup string) (bool, error) {
	const q = `
		INSERT INTO processed_events (order_id, event_type, consumer_group)
		VALUES ($1, $2, $3)
		ON CONFLICT (order_id, event_type, consumer_group) DO NOTHING`

	tag, err := tx.Exec(ctx, q, orderID, eventType, consumerGroup)
	if err != nil {
		return false, fmt.Errorf("processed_events_repo: mark_processed: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}
