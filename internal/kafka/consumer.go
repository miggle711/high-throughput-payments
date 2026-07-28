// Package kafka holds generic consumer setup shared by any service that
// reads from Kafka. Producing/relaying (the outbox pattern) lives in
// internal/outbox instead — this package is purely about the consume side.
package kafka

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
)

// Handler processes one Kafka message. Returning an error leaves the
// message's offset uncommitted, so it will be redelivered — handlers must
// be safe to call more than once for the same message (idempotent).
type Handler func(ctx context.Context, msg kafka.Message) error

type Consumer struct {
	reader  *kafka.Reader
	handler Handler
}

func NewConsumer(brokers []string, topic, groupID string, handler Handler) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID, // consumer group: partitions are load-balanced across instances sharing this ID
	})
	return &Consumer{reader: reader, handler: handler}
}

// Run reads and processes messages until ctx is canceled. Offsets are
// committed only after handler succeeds (process-then-commit), so a crash
// mid-processing results in redelivery rather than silent message loss.
func (c *Consumer) Run(ctx context.Context) {
	defer c.reader.Close()

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return // shutting down
			}
			log.Printf("kafka: fetch message: %v", err)
			continue
		}

		if err := c.handler(ctx, msg); err != nil {
			log.Printf("kafka: handle message at offset %d: %v", msg.Offset, err)
			continue // offset not committed, message will be redelivered
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("kafka: commit offset %d: %v", msg.Offset, err)
		}
	}
}
