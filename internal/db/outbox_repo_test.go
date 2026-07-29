package db_test

import (
	"context"
	"sync"
	"testing"

	"github.com/mvar0010/high-throughput-payments/internal/db"
	"github.com/mvar0010/high-throughput-payments/internal/dbtest"
)

// TestFetchAndClaimUnpublished_ConcurrentClaimsDoNotOverlap reproduces the
// bug found while testing Inventory Service and Payment Service together:
// two relays polling the same outbox_events table at the same time could
// both fetch and publish the same row, producing duplicate Kafka messages.
// FOR UPDATE SKIP LOCKED should make each row claimed by exactly one caller.
func TestFetchAndClaimUnpublished_ConcurrentClaimsDoNotOverlap(t *testing.T) {
	pool := dbtest.NewPool(t)
	ctx := context.Background()
	repo := db.NewOutboxRepo(pool)

	const numEvents = 20
	insertOutboxEvents(t, ctx, repo, numEvents)

	const numClaimers = 5
	claimed := make([][]db.OutboxEvent, numClaimers)
	var wg sync.WaitGroup

	for i := 0; i < numClaimers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			tx, err := repo.BeginTx(ctx)
			if err != nil {
				t.Errorf("begin tx: %v", err)
				return
			}
			defer tx.Rollback(ctx)

			events, err := repo.FetchAndClaimUnpublished(ctx, tx, numEvents)
			if err != nil {
				t.Errorf("fetch and claim: %v", err)
				return
			}
			claimed[i] = events

			for _, e := range events {
				if err := repo.MarkPublished(ctx, tx, e.ID); err != nil {
					t.Errorf("mark published: %v", err)
					return
				}
			}
			if err := tx.Commit(ctx); err != nil {
				t.Errorf("commit: %v", err)
			}
		}()
	}
	wg.Wait()

	seen := map[string]int{}
	for _, events := range claimed {
		for _, e := range events {
			seen[e.ID.String()]++
		}
	}

	if len(seen) != numEvents {
		t.Fatalf("expected %d distinct events claimed across all callers, got %d", numEvents, len(seen))
	}
	for id, count := range seen {
		if count != 1 {
			t.Errorf("event %s was claimed %d times, want exactly 1", id, count)
		}
	}
}

func insertOutboxEvents(t *testing.T, ctx context.Context, repo *db.OutboxRepo, n int) {
	t.Helper()
	tx, err := repo.BeginTx(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback(ctx)

	for i := 0; i < n; i++ {
		if err := repo.Insert(ctx, tx, "order.created", "order-1", "order.created", map[string]int{"i": i}); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
}
