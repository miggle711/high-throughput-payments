package main

import (
	"context"
	"log"
	"net/http"
	"os"

	"github.com/mvar0010/high-throughput-payments/internal/db"
)

func main() {
	ctx := context.Background()

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
	orderHandler := NewOrderHandler(ordersRepo)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", healthHandler)
	mux.HandleFunc("POST /orders", orderHandler.CreateOrder)
	mux.HandleFunc("GET /orders/{id}", orderHandler.GetOrder)

	// TODO: mux.HandleFunc("POST /orders/{id}/cancel", orderHandler.CancelOrder)
	// Cancel triggers the saga/compensation flow (restock inventory, refund if
	// already charged) — needs the saga pattern decided first.

	addr := ":" + envOr("PORT", "8080")
	log.Printf("order-service listening on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
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
