package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"

	"github.com/mvar0010/high-throughput-payments/internal/db"
)

const (
	orderCreatedTopic     = "order.created"
	inventoryDeductedType = "inventory.deducted"
	inventoryFailedType   = "inventory.failed"
)

type orderCreatedEvent struct {
	OrderID   string `json:"order_id"`
	UserID    int64  `json:"user_id"`
	ProductID string `json:"product_id"`
	Amount    int64  `json:"amount"`
}

type inventoryEvent struct {
	OrderID   string `json:"order_id"`
	ProductID string `json:"product_id"`
}

type InventoryHandler struct {
	pool            *pgxpool.Pool
	products        *db.ProductsRepo
	processedEvents *db.ProcessedEventsRepo
	outbox          *db.OutboxRepo
}

func NewInventoryHandler(pool *pgxpool.Pool, products *db.ProductsRepo, processedEvents *db.ProcessedEventsRepo, outbox *db.OutboxRepo) *InventoryHandler {
	return &InventoryHandler{pool: pool, products: products, processedEvents: processedEvents, outbox: outbox}
}

// HandleOrderCreated deducts stock for the order's product and records an
// inventory.deducted (or inventory.failed) outbox event. Safe to call more
// than once for the same order since processedEvents.MarkProcessed skips
// any order already handled.
func (h *InventoryHandler) HandleOrderCreated(ctx context.Context, msg kafka.Message) error {
	var event orderCreatedEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return fmt.Errorf("inventory_handler: unmarshal order.created: %w", err)
	}

	orderID, err := uuid.Parse(event.OrderID)
	if err != nil {
		return fmt.Errorf("inventory_handler: invalid order_id: %w", err)
	}

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("inventory_handler: begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	isNew, err := h.processedEvents.MarkProcessed(ctx, tx, orderID, orderCreatedTopic)
	if err != nil {
		return fmt.Errorf("inventory_handler: mark processed: %w", err)
	}
	if !isNew {
		log.Printf("inventory: order %s already processed, skipping", event.OrderID)
		return nil
	}

	outEvent := inventoryEvent{OrderID: event.OrderID, ProductID: event.ProductID}
	outboxType := inventoryDeductedType

	deductErr := h.products.DeductStock(ctx, tx, event.ProductID)
	if errors.Is(deductErr, db.ErrInsufficientStock) {
		outboxType = inventoryFailedType
	} else if deductErr != nil {
		return fmt.Errorf("inventory_handler: deduct stock: %w", deductErr)
	}

	if err := h.outbox.Insert(ctx, tx, outboxType, event.OrderID, outboxType, outEvent); err != nil {
		return fmt.Errorf("inventory_handler: insert outbox event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("inventory_handler: commit: %w", err)
	}

	log.Printf("inventory: processed order %s (%s)", event.OrderID, outboxType)
	return nil
}
