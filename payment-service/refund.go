package main

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stripe/stripe-go/v83"

	"github.com/mvar0010/high-throughput-payments/internal/db"
)

const paymentRefundedType = "payment.refunded"

type refundIssuer interface {
	RefundPaymentIntent(ctx context.Context, paymentIntentID string) (*stripe.Refund, error)
}

// refunder is shared by the two paths that can discover a charged order was
// cancelled: the order.cancelled consumer (charge landed first) and the
// Stripe webhook (cancel landed first).
type refunder struct {
	stripe   refundIssuer
	payments *db.PaymentsRepo
	outbox   *db.OutboxRepo
}

// refund calls Stripe, marks the payment refunded, and queues
// payment.refunded, all inside tx. If the Stripe call fails the caller
// returns an error, tx rolls back, and the message or webhook is retried.
// A retry after a successful Stripe call but failed commit is safe because
// the refund uses an idempotency key.
func (r *refunder) refund(ctx context.Context, tx pgx.Tx, orderID uuid.UUID, paymentIntentID string) error {
	if _, err := r.stripe.RefundPaymentIntent(ctx, paymentIntentID); err != nil {
		return fmt.Errorf("refund: order %s: %w", orderID, err)
	}
	if err := r.payments.UpdateStatusByOrderID(ctx, tx, orderID, "refunded"); err != nil {
		return err
	}
	return r.outbox.Insert(ctx, tx, paymentRefundedType, orderID.String(), paymentRefundedType, paymentEvent{OrderID: orderID.String()})
}
