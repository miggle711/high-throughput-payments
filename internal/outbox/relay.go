// Package outbox implements the relay half of the transactional outbox
// pattern: it polls unpublished rows written by db.OutboxRepo.Insert and
// publishes them to Kafka, decoupling the DB write (atomic, reliable) from
// the Kafka publish (network call, can fail independently).
package outbox

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/mvar0010/high-throughput-payments/internal/db"
)

type Relay struct {
	outbox       *db.OutboxRepo
	writer       *kafka.Writer
	pollInterval time.Duration
	batchSize    int
}

func NewRelay(outbox *db.OutboxRepo, kafkaBrokers []string) *Relay {
	return &Relay{
		outbox: outbox,
		writer: &kafka.Writer{
			Addr:     kafka.TCP(kafkaBrokers...),
			Balancer: &kafka.Hash{}, // route by Message.Key so a given order's events stay in order
		},
		pollInterval: time.Second,
		batchSize:    100,
	}
}

// Run polls for unpublished outbox events and publishes them until ctx is
// canceled. Intended to run in its own goroutine alongside the HTTP server.
func (r *Relay) Run(ctx context.Context) {
	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			r.writer.Close()
			return
		case <-ticker.C:
			r.publishPending(ctx)
		}
	}
}

// publishPending claims a batch of unpublished rows within a transaction
// (locking them via FOR UPDATE SKIP LOCKED) so that other relays polling
// the same shared outbox_events table — e.g. Order Service's and Inventory
// Service's relays both running against one Postgres instance — skip rows
// this one is already handling instead of racing to publish them twice.
func (r *Relay) publishPending(ctx context.Context) {
	tx, err := r.outbox.BeginTx(ctx)
	if err != nil {
		log.Printf("outbox: begin tx: %v", err)
		return
	}
	defer tx.Rollback(ctx)

	events, err := r.outbox.FetchAndClaimUnpublished(ctx, tx, r.batchSize)
	if err != nil {
		log.Printf("outbox: fetch and claim unpublished: %v", err)
		return
	}

	for _, e := range events {
		msg := kafka.Message{
			Topic: e.Topic,
			Key:   []byte(e.PartitionKey),
			Value: e.Payload,
			Headers: []kafka.Header{
				{Key: "event_type", Value: []byte(e.EventType)},
			},
		}

		if err := r.writer.WriteMessages(ctx, msg); err != nil {
			log.Printf("outbox: publish event %s: %v", e.ID, err)
			continue // leave unpublished; released back to the pool when tx rolls back
		}

		if err := r.outbox.MarkPublished(ctx, tx, e.ID); err != nil {
			log.Printf("outbox: mark published %s: %v", e.ID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		log.Printf("outbox: commit: %v", err)
	}
}
