package main

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/google/uuid"

	"github.com/mvar0010/high-throughput-payments/internal/db"
	"github.com/mvar0010/high-throughput-payments/internal/models"
)

type OrderHandler struct {
	repo *db.OrdersRepo
}

func NewOrderHandler(repo *db.OrdersRepo) *OrderHandler {
	return &OrderHandler{repo: repo}
}

type createOrderRequest struct {
	UserID    int64  `json:"user_id"`
	ProductID string `json:"product_id"`
	Amount    int64  `json:"amount"`
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

	if err := h.repo.Create(r.Context(), order); err != nil {
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

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
