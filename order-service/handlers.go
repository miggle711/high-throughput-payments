package main

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/google/uuid"

	"github.com/mvar0010/high-throughput-payments/internal/db"
	"github.com/mvar0010/high-throughput-payments/internal/models"
)

const (
	orderCreatedTopic   = "order.created"
	orderCancelledTopic = "order.cancelled"
)

type OrderHandler struct {
	repo   *db.OrdersRepo
	outbox *db.OutboxRepo
}

func NewOrderHandler(repo *db.OrdersRepo, outbox *db.OutboxRepo) *OrderHandler {
	return &OrderHandler{repo: repo, outbox: outbox}
}

type createOrderRequest struct {
	UserID    int64  `json:"user_id"`
	ProductID string `json:"product_id"`
	Amount    int64  `json:"amount"`
}

type orderCreatedEvent struct {
	OrderID   uuid.UUID `json:"order_id"`
	UserID    int64     `json:"user_id"`
	ProductID string    `json:"product_id"`
	Amount    int64     `json:"amount"`
}

type orderCancelledEvent struct {
	OrderID   uuid.UUID `json:"order_id"`
	ProductID string    `json:"product_id"`
}

func (h *OrderHandler) CreateOrder(w http.ResponseWriter, r *http.Request) {
	var req createOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.UserID <= 0 || req.ProductID == "" || req.Amount <= 0 {
		writeError(w, http.StatusBadRequest, "user_id, product_id, and amount are required")
		return
	}

	order := &models.Order{
		UserID:    req.UserID,
		ProductID: req.ProductID,
		Amount:    req.Amount,
		Status:    models.OrderStatusPending,
	}

	ctx := r.Context()

	tx, err := h.repo.BeginTx(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create order")
		return
	}
	defer tx.Rollback(ctx) // does nothing if Commit already succeeded

	if err := h.repo.Create(ctx, tx, order); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create order")
		return
	}

	event := orderCreatedEvent{
		OrderID:   order.ID,
		UserID:    order.UserID,
		ProductID: order.ProductID,
		Amount:    order.Amount,
	}
	// partition key is order_id: keeps all events for one order ordered,
	// per the doc's Kafka partitioning guidance.
	if err := h.outbox.Insert(ctx, tx, orderCreatedTopic, order.ID.String(), orderCreatedTopic, event); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create order")
		return
	}

	if err := tx.Commit(ctx); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create order")
		return
	}

	writeJSON(w, http.StatusCreated, order)
}

func (h *OrderHandler) GetOrder(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid order id")
		return
	}

	order, err := h.repo.GetByID(r.Context(), id)
	if errors.Is(err, db.ErrOrderNotFound) {
		writeError(w, http.StatusNotFound, "order not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to fetch order")
		return
	}

	writeJSON(w, http.StatusOK, order)
}

func (h *OrderHandler) CancelOrder(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid order id")
		return
	}

	ctx := r.Context()

	existing, err := h.repo.GetByID(ctx, id)
	if errors.Is(err, db.ErrOrderNotFound) {
		writeError(w, http.StatusNotFound, "order not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to cancel order")
		return
	}
	if existing.Status != models.OrderStatusPending {
		writeError(w, http.StatusConflict, "order is not pending and cannot be cancelled")
		return
	}

	tx, err := h.repo.BeginTx(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to cancel order")
		return
	}
	defer tx.Rollback(ctx)

	order, err := h.repo.CancelPending(ctx, tx, id)
	if errors.Is(err, db.ErrOrderNotCancellable) {
		// Lost a race with another writer (e.g. Payment Service just marked
		// this order paid) between the check above and this update.
		writeError(w, http.StatusConflict, "order is not pending and cannot be cancelled")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to cancel order")
		return
	}

	event := orderCancelledEvent{OrderID: order.ID, ProductID: order.ProductID}
	if err := h.outbox.Insert(ctx, tx, orderCancelledTopic, order.ID.String(), orderCancelledTopic, event); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to cancel order")
		return
	}

	if err := tx.Commit(ctx); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to cancel order")
		return
	}

	writeJSON(w, http.StatusOK, order)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
