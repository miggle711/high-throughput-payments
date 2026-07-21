package db

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mvar0010/high-throughput-payments/internal/models"
)

var ErrOrderNotFound = errors.New("order not found")

type OrdersRepo struct {
	pool *pgxpool.Pool
}

func NewOrdersRepo(pool *pgxpool.Pool) *OrdersRepo {
	return &OrdersRepo{pool: pool}
}

func (r *OrdersRepo) Create(ctx context.Context, o *models.Order) error {
	const q = `
		INSERT INTO orders (user_id, product_id, amount, status)
		VALUES ($1, $2, $3, $4)
		RETURNING id, created_at`

	err := r.pool.QueryRow(ctx, q, o.UserID, o.ProductID, o.Amount, o.Status).
		Scan(&o.ID, &o.CreatedAt)
	if err != nil {
		return fmt.Errorf("orders_repo: create: %w", err)
	}
	return nil
}

func (r *OrdersRepo) GetByID(ctx context.Context, id int64) (*models.Order, error) {
	const q = `
		SELECT id, user_id, product_id, amount, status, created_at
		FROM orders
		WHERE id = $1`

	var o models.Order
	err := r.pool.QueryRow(ctx, q, id).
		Scan(&o.ID, &o.UserID, &o.ProductID, &o.Amount, &o.Status, &o.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrOrderNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("orders_repo: get_by_id: %w", err)
	}
	return &o, nil
}
