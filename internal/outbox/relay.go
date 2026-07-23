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

func (r *Relay) publishPending(ctx context.Context) {
	events, err := r.outbox.FetchUnpublished(ctx, r.batchSize)
	if err != nil {
		log.Printf("outbox: fetch unpublished: %v", err)
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
			continue // leave unpublished, retried next tick
		}

		if err := r.outbox.MarkPublished(ctx, e.ID); err != nil {
			log.Printf("outbox: mark published %s: %v", e.ID, err)
		}
	}
}
