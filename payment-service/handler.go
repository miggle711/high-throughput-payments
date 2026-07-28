package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"

	"github.com/google/uuid"

	"github.com/mvar0010/high-throughput-payments/internal/db"
	"github.com/mvar0010/high-throughput-payments/internal/stripeclient"
)

const orderCreatedTopic = "order.created"

type orderCreatedEvent struct {
	OrderID   string `json:"order_id"`
	UserID    int64  `json:"user_id"`
	ProductID string `json:"product_id"`
	Amount    int64  `json:"amount"`
}

type PaymentHandler struct {
	pool            *pgxpool.Pool
	payments        *db.PaymentsRepo
	processedEvents *db.ProcessedEventsRepo
	stripe          *stripeclient.Client
	consumerGroupID string
}

func NewPaymentHandler(pool *pgxpool.Pool, payments *db.PaymentsRepo, processedEvents *db.ProcessedEventsRepo, stripe *stripeclient.Client, consumerGroupID string) *PaymentHandler {
	return &PaymentHandler{pool: pool, payments: payments, processedEvents: processedEvents, stripe: stripe, consumerGroupID: consumerGroupID}
}

// HandleOrderCreated creates and confirms a Stripe PaymentIntent for the
// order, then records it locally as pending. The actual outcome (succeeded
// or failed) is only known once Stripe calls the webhook, so no
// payment.completed event is published from here. Safe to call more than
// once for the same order since processedEvents.MarkProcessed skips any
// order already handled.
func (h *PaymentHandler) HandleOrderCreated(ctx context.Context, msg kafka.Message) error {
	var event orderCreatedEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return fmt.Errorf("payment_handler: unmarshal order.created: %w", err)
	}

	orderID, err := uuid.Parse(event.OrderID)
	if err != nil {
		return fmt.Errorf("payment_handler: invalid order_id: %w", err)
	}

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("payment_handler: begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	isNew, err := h.processedEvents.MarkProcessed(ctx, tx, orderID, orderCreatedTopic, h.consumerGroupID)
	if err != nil {
		return fmt.Errorf("payment_handler: mark processed: %w", err)
	}
	if !isNew {
		log.Printf("payment: order %s already processed, skipping", event.OrderID)
		return nil
	}

	pi, err := h.stripe.CreateAndConfirmPaymentIntent(ctx, event.Amount, event.OrderID)
	if err != nil {
		return fmt.Errorf("payment_handler: create payment intent: %w", err)
	}

	payment := &db.Payment{
		OrderID:               orderID,
		StripePaymentIntentID: pi.ID,
		Amount:                event.Amount,
		Status:                "pending",
	}
	if err := h.payments.Create(ctx, tx, payment); err != nil {
		return fmt.Errorf("payment_handler: record payment: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("payment_handler: commit: %w", err)
	}

	log.Printf("payment: created payment intent %s for order %s", pi.ID, event.OrderID)
	return nil
}
