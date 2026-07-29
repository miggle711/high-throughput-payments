package db

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrPaymentNotFound = errors.New("payment not found")

type Payment struct {
	OrderID               uuid.UUID
	StripePaymentIntentID string
	Amount                int64
	Status                string
}

type PaymentsRepo struct {
	pool *pgxpool.Pool
}

func NewPaymentsRepo(pool *pgxpool.Pool) *PaymentsRepo {
	return &PaymentsRepo{pool: pool}
}

func (r *PaymentsRepo) Create(ctx context.Context, tx pgx.Tx, p *Payment) error {
	const q = `
		INSERT INTO payments (order_id, stripe_payment_intent_id, amount, status)
		VALUES ($1, $2, $3, $4)`

	if _, err := tx.Exec(ctx, q, p.OrderID, p.StripePaymentIntentID, p.Amount, p.Status); err != nil {
		return fmt.Errorf("payments_repo: create: %w", err)
	}
	return nil
}

// UpdateStatusByIntentID sets status for the payment matching a Stripe
// PaymentIntent ID, returning the order ID it belongs to. Used by the
// webhook handler, which only knows the PaymentIntent ID from Stripe, not
// the order ID directly.
func (r *PaymentsRepo) UpdateStatusByIntentID(ctx context.Context, tx pgx.Tx, intentID, status string) (uuid.UUID, error) {
	const q = `
		UPDATE payments
		SET status = $1, updated_at = now()
		WHERE stripe_payment_intent_id = $2
		RETURNING order_id`

	var orderID uuid.UUID
	err := tx.QueryRow(ctx, q, status, intentID).Scan(&orderID)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, ErrPaymentNotFound
	}
	if err != nil {
		return uuid.Nil, fmt.Errorf("payments_repo: update_status_by_intent_id: %w", err)
	}
	return orderID, nil
}

func (r *PaymentsRepo) BeginTx(ctx context.Context) (pgx.Tx, error) {
	return r.pool.Begin(ctx)
}
