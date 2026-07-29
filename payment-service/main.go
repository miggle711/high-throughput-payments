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

	"github.com/joho/godotenv"

	"github.com/mvar0010/high-throughput-payments/internal/db"
	appkafka "github.com/mvar0010/high-throughput-payments/internal/kafka"
	"github.com/mvar0010/high-throughput-payments/internal/outbox"
	"github.com/mvar0010/high-throughput-payments/internal/stripeclient"
)

const (
	shutdownTimeout = 10 * time.Second
	consumerGroupID = "payment-service"
	topicPartitions = 3
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	_ = godotenv.Load()

	stripeSecretKey := os.Getenv("STRIPE_SECRET_KEY")
	if stripeSecretKey == "" {
		log.Fatal("payment-service: STRIPE_SECRET_KEY is required")
	}
	stripeWebhookSecret := os.Getenv("STRIPE_WEBHOOK_SECRET")
	if stripeWebhookSecret == "" {
		log.Fatal("payment-service: STRIPE_WEBHOOK_SECRET is required")
	}

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://payments:payments@localhost:5434/payments"
	}

	pool, err := db.NewPool(ctx, databaseURL)
	if err != nil {
		log.Fatalf("payment-service: %v", err)
	}
	defer pool.Close()

	paymentsRepo := db.NewPaymentsRepo(pool)
	processedEventsRepo := db.NewProcessedEventsRepo()
	outboxRepo := db.NewOutboxRepo(pool)
	stripeClient := stripeclient.New(stripeSecretKey)

	paymentHandler := NewPaymentHandler(pool, paymentsRepo, processedEventsRepo, stripeClient, consumerGroupID)
	webhookHandler := NewWebhookHandler(paymentsRepo, processedEventsRepo, outboxRepo, stripeWebhookSecret)

	kafkaBrokers := strings.Split(envOr("KAFKA_BROKERS", "localhost:9092"), ",")

	if err := outbox.EnsureTopic(ctx, kafkaBrokers, paymentCompletedType, topicPartitions); err != nil {
		log.Fatalf("payment-service: %v", err)
	}
	if err := outbox.EnsureTopic(ctx, kafkaBrokers, paymentFailedType, topicPartitions); err != nil {
		log.Fatalf("payment-service: %v", err)
	}

	relay := outbox.NewRelay(outboxRepo, kafkaBrokers)
	go relay.Run(ctx)

	consumer := appkafka.NewConsumer(kafkaBrokers, orderCreatedTopic, consumerGroupID, paymentHandler.HandleOrderCreated)
	go consumer.Run(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", healthHandler)
	mux.HandleFunc("POST /webhooks/stripe", webhookHandler.HandleStripeWebhook)

	addr := ":" + envOr("PORT", "8082")
	server := &http.Server{Addr: addr, Handler: mux}

	go func() {
		log.Printf("payment-service listening on %s", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	<-ctx.Done()
	log.Println("payment-service shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("payment-service: shutdown: %v", err)
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
