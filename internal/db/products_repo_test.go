package db_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mvar0010/high-throughput-payments/internal/db"
	"github.com/mvar0010/high-throughput-payments/internal/dbtest"
)

func TestDeductStock_SucceedsWhileStockAvailable(t *testing.T) {
	pool := dbtest.NewPool(t)
	ctx := context.Background()
	repo := db.NewProductsRepo()
	seedProduct(t, ctx, pool, "widget", 1)

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback(ctx)

	if err := repo.DeductStock(ctx, tx, "widget"); err != nil {
		t.Fatalf("deduct stock: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	if got := stockFor(t, ctx, pool, "widget"); got != 0 {
		t.Errorf("stock after deduction = %d, want 0", got)
	}
}

func TestDeductStock_FailsWhenStockExhausted(t *testing.T) {
	pool := dbtest.NewPool(t)
	ctx := context.Background()
	repo := db.NewProductsRepo()
	seedProduct(t, ctx, pool, "widget", 0)

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback(ctx)

	err = repo.DeductStock(ctx, tx, "widget")
	if !errors.Is(err, db.ErrInsufficientStock) {
		t.Fatalf("deduct stock with 0 in stock: got err %v, want ErrInsufficientStock", err)
	}
}

// TestDeductStock_ConcurrentDeductionsNeverOversell fires more concurrent
// deductions than there is stock and checks that exactly as many succeed
// as there was stock available, never more. Postgres row level locking on
// the UPDATE should serialize the concurrent writers.
func TestDeductStock_ConcurrentDeductionsNeverOversell(t *testing.T) {
	pool := dbtest.NewPool(t)
	ctx := context.Background()
	repo := db.NewProductsRepo()

	const startingStock = 5
	const attempts = 20
	seedProduct(t, ctx, pool, "widget", startingStock)

	var succeeded, failed int64
	var wg sync.WaitGroup
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Errorf("begin tx: %v", err)
				return
			}
			defer tx.Rollback(ctx)

			err = repo.DeductStock(ctx, tx, "widget")
			if errors.Is(err, db.ErrInsufficientStock) {
				atomic.AddInt64(&failed, 1)
				return
			}
			if err != nil {
				t.Errorf("deduct stock: %v", err)
				return
			}
			if err := tx.Commit(ctx); err != nil {
				t.Errorf("commit: %v", err)
				return
			}
			atomic.AddInt64(&succeeded, 1)
		}()
	}
	wg.Wait()

	if succeeded != startingStock {
		t.Errorf("succeeded = %d, want %d (starting stock)", succeeded, startingStock)
	}
	if failed != attempts-startingStock {
		t.Errorf("failed = %d, want %d", failed, attempts-startingStock)
	}
	if got := stockFor(t, ctx, pool, "widget"); got != 0 {
		t.Errorf("final stock = %d, want 0 (no overselling)", got)
	}
}

func seedProduct(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id string, stock int) {
	t.Helper()
	_, err := pool.Exec(ctx, `INSERT INTO products (id, stock) VALUES ($1, $2)`, id, stock)
	if err != nil {
		t.Fatalf("seed product %s: %v", id, err)
	}
}

func stockFor(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id string) int {
	t.Helper()
	var stock int
	if err := pool.QueryRow(ctx, `SELECT stock FROM products WHERE id = $1`, id).Scan(&stock); err != nil {
		t.Fatalf("query stock for %s: %v", id, err)
	}
	return stock
}
