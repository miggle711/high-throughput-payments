package outbox

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

// EnsureTopic creates topic if it doesn't already exist. Kafka topic
// creation is idempotent, so calling CreateTopics on an existing topic does
// nothing and returns no error. Safe to call on every startup.
func EnsureTopic(ctx context.Context, brokers []string, topic string, numPartitions int) error {
	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("outbox: dial kafka: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("outbox: find controller: %w", err)
	}

	controllerConn, err := kafka.DialContext(ctx, "tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return fmt.Errorf("outbox: dial controller: %w", err)
	}
	defer controllerConn.Close()

	err = controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     numPartitions,
		ReplicationFactor: 1,
	})
	if err != nil {
		return fmt.Errorf("outbox: create topic %s: %w", topic, err)
	}
	return nil
}
