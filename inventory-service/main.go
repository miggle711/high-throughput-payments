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
	appkafka "github.com/mvar0010/high-throughput-payments/internal/kafka"
	"github.com/mvar0010/high-throughput-payments/internal/outbox"
)

const (
	shutdownTimeout    = 10 * time.Second
	consumerGroupID    = "inventory-service"
	deductedTopicParts = 3
	failedTopicParts   = 3
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://payments:payments@localhost:5434/payments"
	}

	pool, err := db.NewPool(ctx, databaseURL)
	if err != nil {
		log.Fatalf("inventory-service: %v", err)
	}
	defer pool.Close()

	productsRepo := db.NewProductsRepo()
	processedEventsRepo := db.NewProcessedEventsRepo()
	outboxRepo := db.NewOutboxRepo(pool)
	inventoryHandler := NewInventoryHandler(pool, productsRepo, processedEventsRepo, outboxRepo)

	kafkaBrokers := strings.Split(envOr("KAFKA_BROKERS", "localhost:9092"), ",")

	if err := outbox.EnsureTopic(ctx, kafkaBrokers, inventoryDeductedType, deductedTopicParts); err != nil {
		log.Fatalf("inventory-service: %v", err)
	}
	if err := outbox.EnsureTopic(ctx, kafkaBrokers, inventoryFailedType, failedTopicParts); err != nil {
		log.Fatalf("inventory-service: %v", err)
	}

	relay := outbox.NewRelay(outboxRepo, kafkaBrokers)
	go relay.Run(ctx)

	consumer := appkafka.NewConsumer(kafkaBrokers, orderCreatedTopic, consumerGroupID, inventoryHandler.HandleOrderCreated)
	go consumer.Run(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", healthHandler)

	addr := ":" + envOr("PORT", "8081")
	server := &http.Server{Addr: addr, Handler: mux}

	go func() {
		log.Printf("inventory-service listening on %s", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	<-ctx.Done()
	log.Println("inventory-service shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("inventory-service: shutdown: %v", err)
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
