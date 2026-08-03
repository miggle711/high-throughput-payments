package db

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

var ErrInsufficientStock = errors.New("insufficient stock")

type ProductsRepo struct{}

func NewProductsRepo() *ProductsRepo {
	return &ProductsRepo{}
}

// DeductStock decrements stock by 1 within tx, failing with
// ErrInsufficientStock if none is available. The WHERE stock > 0 guard
// makes this safe under concurrent deductions for the same product,
// since Postgres row level locking serializes concurrent updates to the
// same row.
func (r *ProductsRepo) DeductStock(ctx context.Context, tx pgx.Tx, productID string) error {
	const q = `
		UPDATE products
		SET stock = stock - 1
		WHERE id = $1 AND stock > 0`

	tag, err := tx.Exec(ctx, q, productID)
	if err != nil {
		return fmt.Errorf("products_repo: deduct_stock: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrInsufficientStock
	}
	return nil
}

// RestockStock increments stock by 1 within tx. Used for compensation when
// a deducted order does not go on to succeed, for example a failed
// payment or an explicit cancellation.
func (r *ProductsRepo) RestockStock(ctx context.Context, tx pgx.Tx, productID string) error {
	const q = `
		UPDATE products
		SET stock = stock + 1
		WHERE id = $1`

	if _, err := tx.Exec(ctx, q, productID); err != nil {
		return fmt.Errorf("products_repo: restock_stock: %w", err)
	}
	return nil
}
