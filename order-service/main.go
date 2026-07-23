package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/mvar0010/high-throughput-payments/internal/db"
	"github.com/mvar0010/high-throughput-payments/internal/outbox"
)

const shutdownTimeout = 10 * time.Second

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://payments:payments@localhost:5434/payments"
	}

	pool, err := db.NewPool(ctx, databaseURL)
	if err != nil {
		log.Fatalf("order-service: %v", err)
	}
	defer pool.Close()

	ordersRepo := db.NewOrdersRepo(pool)
	outboxRepo := db.NewOutboxRepo(pool)
	orderHandler := NewOrderHandler(ordersRepo, outboxRepo)

	kafkaBrokers := strings.Split(envOr("KAFKA_BROKERS", "localhost:9092"), ",")
	if err := outbox.EnsureTopic(ctx, kafkaBrokers, orderCreatedTopic, 3); err != nil {
		log.Fatalf("order-service: %v", err)
	}

	relay := outbox.NewRelay(outboxRepo, kafkaBrokers)
	go relay.Run(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", healthHandler)
	mux.HandleFunc("POST /orders", orderHandler.CreateOrder)
	mux.HandleFunc("GET /orders/{id}", orderHandler.GetOrder)

	// TODO: mux.HandleFunc("POST /orders/{id}/cancel", orderHandler.CancelOrder)
	// Cancel triggers the saga/compensation flow (restock inventory, refund if
	// already charged) — needs the saga pattern decided first.

	addr := ":" + envOr("PORT", "8080")
	server := &http.Server{Addr: addr, Handler: mux}

	go func() {
		log.Printf("order-service listening on %s", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	<-ctx.Done()
	log.Println("order-service shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("order-service: shutdown: %v", err)
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ready"}`))
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
