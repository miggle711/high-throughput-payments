package models

import "time"

type OrderStatus string

const (
	OrderStatusPending OrderStatus = "pending"
	OrderStatusPaid    OrderStatus = "paid"
	OrderStatusFailed  OrderStatus = "failed"
)

type Order struct {
	ID        int64       `json:"order_id"`
	UserID    int64       `json:"user_id"`
	ProductID string      `json:"product_id"`
	Amount    int64       `json:"amount"`
	Status    OrderStatus `json:"status"`
	CreatedAt time.Time   `json:"created_at"`
}
