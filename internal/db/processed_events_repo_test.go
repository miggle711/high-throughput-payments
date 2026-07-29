package db_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/mvar0010/high-throughput-payments/internal/db"
	"github.com/mvar0010/high-throughput-payments/internal/dbtest"
)

// TestMarkProcessed_ScopedByConsumerGroup reproduces the bug found while
// testing Inventory Service and Payment Service together: both consume
// order.created and both called MarkProcessed with the same event type.
// Before scoping by consumer group, whichever service ran first silently
// caused the other to think it had already handled the order.
func TestMarkProcessed_ScopedByConsumerGroup(t *testing.T) {
	pool := dbtest.NewPool(t)
	ctx := context.Background()
	repo := db.NewProcessedEventsRepo()
	orderID := uuid.New()

	tx1, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx1: %v", err)
	}
	isNew1, err := repo.MarkProcessed(ctx, tx1, orderID, "order.created", "inventory-service")
	if err != nil {
		t.Fatalf("mark processed (inventory-service): %v", err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("commit tx1: %v", err)
	}
	if !isNew1 {
		t.Fatalf("expected isNew=true for inventory-service's first attempt, got false")
	}

	tx2, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx2: %v", err)
	}
	isNew2, err := repo.MarkProcessed(ctx, tx2, orderID, "order.created", "payment-service")
	if err != nil {
		t.Fatalf("mark processed (payment-service): %v", err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("commit tx2: %v", err)
	}
	if !isNew2 {
		t.Fatalf("payment-service was incorrectly skipped because inventory-service already processed this order; consumer groups are not scoped correctly")
	}

	tx3, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx3: %v", err)
	}
	defer tx3.Rollback(ctx)
	isNew3, err := repo.MarkProcessed(ctx, tx3, orderID, "order.created", "inventory-service")
	if err != nil {
		t.Fatalf("mark processed (inventory-service retry): %v", err)
	}
	if isNew3 {
		t.Fatalf("expected isNew=false when inventory-service processes the same order twice, got true")
	}
}
