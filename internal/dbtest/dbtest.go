// Package dbtest starts a throwaway Postgres container with the project's
// real migrations applied, so repo layer tests run against actual Postgres
// behavior (locking, constraints, ON CONFLICT) instead of a mock.
package dbtest

import (
	"context"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// NewPool starts a Postgres container, applies every migration in
// internal/db/migrations, and returns a connected pool. The container and
// pool are torn down automatically when the test ends.
func NewPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("payments"),
		tcpostgres.WithUsername("payments"),
		tcpostgres.WithPassword("payments"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("5432/tcp"),
		),
	)
	if err != nil {
		t.Fatalf("dbtest: start postgres container: %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("dbtest: terminate container: %v", err)
		}
	})

	connURL, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("dbtest: connection string: %v", err)
	}

	if err := applyMigrations(connURL); err != nil {
		t.Fatalf("dbtest: apply migrations: %v", err)
	}

	pool, err := pgxpool.New(ctx, connURL)
	if err != nil {
		t.Fatalf("dbtest: connect pool: %v", err)
	}
	t.Cleanup(pool.Close)

	return pool
}

func applyMigrations(connURL string) error {
	_, thisFile, _, _ := runtime.Caller(0)
	migrationsPath := filepath.Join(filepath.Dir(thisFile), "..", "db", "migrations")

	m, err := migrate.New("file://"+migrationsPath, connURL)
	if err != nil {
		return err
	}
	defer m.Close()

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return err
	}
	return nil
}
