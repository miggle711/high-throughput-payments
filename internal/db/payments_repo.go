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

// LockOrder takes a transaction scoped advisory lock keyed on orderID, held
// until tx ends. Payment Service uses it to serialize the order.created
// handler (which inserts the payment row after calling Stripe) against the
// order.cancelled handler (which records the cancel). Without it, both could
// miss each other when no payment row exists yet: one charges while the
// other tombstones, and nobody refunds.
func (r *PaymentsRepo) LockOrder(ctx context.Context, tx pgx.Tx, orderID uuid.UUID) error {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1::text, 0))`, orderID.String()); err != nil {
		return fmt.Errorf("payments_repo: lock_order: %w", err)
	}
	return nil
}

// MarkCancelled records that orderID was cancelled. Kept apart from the
// payments table because a cancel can arrive before any payment row exists.
func (r *PaymentsRepo) MarkCancelled(ctx context.Context, tx pgx.Tx, orderID uuid.UUID) error {
	const q = `INSERT INTO cancelled_orders (order_id) VALUES ($1) ON CONFLICT (order_id) DO NOTHING`
	if _, err := tx.Exec(ctx, q, orderID); err != nil {
		return fmt.Errorf("payments_repo: mark_cancelled: %w", err)
	}
	return nil
}

func (r *PaymentsRepo) IsCancelled(ctx context.Context, tx pgx.Tx, orderID uuid.UUID) (bool, error) {
	const q = `SELECT EXISTS (SELECT 1 FROM cancelled_orders WHERE order_id = $1)`
	var cancelled bool
	if err := tx.QueryRow(ctx, q, orderID).Scan(&cancelled); err != nil {
		return false, fmt.Errorf("payments_repo: is_cancelled: %w", err)
	}
	return cancelled, nil
}

// GetByOrderIDForUpdate returns the payment for orderID and row locks it
// until tx ends, so a concurrent webhook update waits for this transaction.
func (r *PaymentsRepo) GetByOrderIDForUpdate(ctx context.Context, tx pgx.Tx, orderID uuid.UUID) (*Payment, error) {
	const q = `
		SELECT order_id, stripe_payment_intent_id, amount, status
		FROM payments
		WHERE order_id = $1
		FOR UPDATE`

	var p Payment
	err := tx.QueryRow(ctx, q, orderID).Scan(&p.OrderID, &p.StripePaymentIntentID, &p.Amount, &p.Status)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrPaymentNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("payments_repo: get_by_order_id_for_update: %w", err)
	}
	return &p, nil
}

func (r *PaymentsRepo) UpdateStatusByOrderID(ctx context.Context, tx pgx.Tx, orderID uuid.UUID, status string) error {
	const q = `UPDATE payments SET status = $1, updated_at = now() WHERE order_id = $2`
	if _, err := tx.Exec(ctx, q, status, orderID); err != nil {
		return fmt.Errorf("payments_repo: update_status_by_order_id: %w", err)
	}
	return nil
}
