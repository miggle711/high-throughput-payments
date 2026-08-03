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
	orderCreatedTopic      = "order.created"
	orderCancelledTopic    = "order.cancelled"
	paymentFailedTopic     = "payment.failed"
	inventoryDeductedType  = "inventory.deducted"
	inventoryFailedType    = "inventory.failed"
	inventoryRestockedType = "inventory.restocked"
)

type orderCreatedEvent struct {
	OrderID   string `json:"order_id"`
	UserID    int64  `json:"user_id"`
	ProductID string `json:"product_id"`
	Amount    int64  `json:"amount"`
}

// compensationEvent is the shape of both order.cancelled and
// payment.failed as seen by Inventory Service: both only carry enough to
// restock, order_id and product_id. Event carried state transfer, so
// Inventory Service never needs to query another service's tables.
type compensationEvent struct {
	OrderID   string `json:"order_id"`
	ProductID string `json:"product_id"`
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
	consumerGroupID string
}

func NewInventoryHandler(pool *pgxpool.Pool, products *db.ProductsRepo, processedEvents *db.ProcessedEventsRepo, outbox *db.OutboxRepo, consumerGroupID string) *InventoryHandler {
	return &InventoryHandler{pool: pool, products: products, processedEvents: processedEvents, outbox: outbox, consumerGroupID: consumerGroupID}
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

	isNew, err := h.processedEvents.MarkProcessed(ctx, tx, orderID, orderCreatedTopic, h.consumerGroupID)
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

// HandleCompensation restocks the product for an order whose payment
// failed or was cancelled, and records an inventory.restocked outbox
// event. eventType identifies which topic triggered this so the
// idempotency key (order_id, event_type, consumer_group) is distinct per
// topic, even though the handling logic is identical either way.
func (h *InventoryHandler) HandleCompensation(eventType string) func(ctx context.Context, msg kafka.Message) error {
	return func(ctx context.Context, msg kafka.Message) error {
		var event compensationEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			return fmt.Errorf("inventory_handler: unmarshal %s: %w", eventType, err)
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

		isNew, err := h.processedEvents.MarkProcessed(ctx, tx, orderID, eventType, h.consumerGroupID)
		if err != nil {
			return fmt.Errorf("inventory_handler: mark processed: %w", err)
		}
		if !isNew {
			log.Printf("inventory: %s for order %s already processed, skipping", eventType, event.OrderID)
			return nil
		}

		if err := h.products.RestockStock(ctx, tx, event.ProductID); err != nil {
			return fmt.Errorf("inventory_handler: restock stock: %w", err)
		}

		outEvent := inventoryEvent{OrderID: event.OrderID, ProductID: event.ProductID}
		if err := h.outbox.Insert(ctx, tx, inventoryRestockedType, event.OrderID, inventoryRestockedType, outEvent); err != nil {
			return fmt.Errorf("inventory_handler: insert outbox event: %w", err)
		}

		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("inventory_handler: commit: %w", err)
		}

		log.Printf("inventory: restocked order %s (%s)", event.OrderID, eventType)
		return nil
	}
}
