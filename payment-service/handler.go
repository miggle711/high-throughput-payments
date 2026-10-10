package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"github.com/sony/gobreaker/v2"

	"github.com/google/uuid"

	"github.com/mvar0010/high-throughput-payments/internal/db"
	"github.com/mvar0010/high-throughput-payments/internal/resilientstripe"
)

const (
	orderCreatedTopic   = "order.created"
	orderCancelledTopic = "order.cancelled"
)

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
	outbox          *db.OutboxRepo
	stripe          *resilientstripe.Client
	refunder        *refunder
	consumerGroupID string
}

func NewPaymentHandler(pool *pgxpool.Pool, payments *db.PaymentsRepo, processedEvents *db.ProcessedEventsRepo, outbox *db.OutboxRepo, stripe *resilientstripe.Client, consumerGroupID string) *PaymentHandler {
	return &PaymentHandler{pool: pool, payments: payments, processedEvents: processedEvents, outbox: outbox, stripe: stripe, refunder: &refunder{stripe: stripe, payments: payments, outbox: outbox}, consumerGroupID: consumerGroupID}
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

	// Serialize against HandleOrderCancelled so a cancel cannot slip in
	// between this check and the payment row insert below.
	if err := h.payments.LockOrder(ctx, tx, orderID); err != nil {
		return fmt.Errorf("payment_handler: %w", err)
	}
	cancelled, err := h.payments.IsCancelled(ctx, tx, orderID)
	if err != nil {
		return fmt.Errorf("payment_handler: %w", err)
	}
	if cancelled {
		// Cancelled before we charged: skip the charge entirely. Committing
		// keeps the processed_events mark so redelivery stays a no-op.
		log.Printf("payment: order %s already cancelled, not charging", event.OrderID)
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("payment_handler: commit: %w", err)
		}
		return nil
	}

	pi, stripeErr := h.stripe.CreateAndConfirmPaymentIntent(ctx, event.Amount, event.OrderID)
	if errors.Is(stripeErr, gobreaker.ErrOpenState) {
		// The circuit is open, so this charge was never actually attempted,
		// Stripe is presumed down, not this specific card declined. Return
		// an error rather than commit: the transaction (including
		// MarkProcessed above) rolls back, the offset is not committed, and
		// Kafka redelivers this order once the consumer catches up, by
		// which point the breaker may have closed again.
		return fmt.Errorf("payment_handler: circuit open for order %s: %w", event.OrderID, stripeErr)
	}
	if stripeErr != nil {
		// Retries have been exhausted inside resilientstripe and the
		// circuit is still closed, so Stripe was actually reachable and
		// this is a final outcome (e.g. a real decline), not a transient
		// error. Record it and publish payment.failed so the rest of the
		// system reacts consistently, the same as an explicit decline.
		log.Printf("payment: stripe call failed for order %s after retries: %v", event.OrderID, stripeErr)

		if err := h.outbox.Insert(ctx, tx, paymentFailedType, event.OrderID, paymentFailedType, paymentEvent{OrderID: event.OrderID}); err != nil {
			return fmt.Errorf("payment_handler: insert payment.failed outbox event: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("payment_handler: commit: %w", err)
		}
		return nil
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

type orderCancelledEvent struct {
	OrderID string `json:"order_id"`
}

// HandleOrderCancelled records the cancel and refunds if the order was
// already charged. It covers the ordering where the charge succeeded before
// the cancel arrived. When the charge is still pending (webhook not yet
// received), recording the cancel is enough: the webhook handler checks it
// and refunds. When no payment exists yet, HandleOrderCreated sees the
// cancel and skips the charge. The advisory lock and payment row lock make
// these three cases mutually exclusive.
func (h *PaymentHandler) HandleOrderCancelled(ctx context.Context, msg kafka.Message) error {
	var event orderCancelledEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return fmt.Errorf("payment_handler: unmarshal order.cancelled: %w", err)
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

	isNew, err := h.processedEvents.MarkProcessed(ctx, tx, orderID, orderCancelledTopic, h.consumerGroupID)
	if err != nil {
		return fmt.Errorf("payment_handler: mark processed: %w", err)
	}
	if !isNew {
		return nil
	}

	if err := h.payments.LockOrder(ctx, tx, orderID); err != nil {
		return fmt.Errorf("payment_handler: %w", err)
	}
	if err := h.payments.MarkCancelled(ctx, tx, orderID); err != nil {
		return fmt.Errorf("payment_handler: %w", err)
	}

	payment, err := h.payments.GetByOrderIDForUpdate(ctx, tx, orderID)
	if err != nil && !errors.Is(err, db.ErrPaymentNotFound) {
		return fmt.Errorf("payment_handler: %w", err)
	}
	if err == nil && payment.Status == "succeeded" {
		if err := h.refunder.refund(ctx, tx, orderID, payment.StripePaymentIntentID); err != nil {
			return fmt.Errorf("payment_handler: %w", err)
		}
		log.Printf("payment: refunded order %s cancelled after charge", event.OrderID)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("payment_handler: commit: %w", err)
	}
	return nil
}
