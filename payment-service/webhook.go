package main

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"

	"github.com/stripe/stripe-go/v83"
	"github.com/stripe/stripe-go/v83/webhook"

	"github.com/mvar0010/high-throughput-payments/internal/db"
)

const (
	paymentCompletedType = "payment.completed"
	paymentFailedType    = "payment.failed"
	webhookConsumerGroup = "payment-service-webhook"
)

type paymentEvent struct {
	OrderID string `json:"order_id"`
}

type WebhookHandler struct {
	payments        *db.PaymentsRepo
	processedEvents *db.ProcessedEventsRepo
	outbox          *db.OutboxRepo
	signingSecret   string
}

func NewWebhookHandler(payments *db.PaymentsRepo, processedEvents *db.ProcessedEventsRepo, outbox *db.OutboxRepo, signingSecret string) *WebhookHandler {
	return &WebhookHandler{payments: payments, processedEvents: processedEvents, outbox: outbox, signingSecret: signingSecret}
}

// HandleStripeWebhook verifies the Stripe Signature header before trusting
// the payload, then records the payment outcome and publishes
// payment.completed or payment.failed via the outbox. This is the source
// of truth for a payment's result, not the synchronous response from
// creating the PaymentIntent.
func (h *WebhookHandler) HandleStripeWebhook(w http.ResponseWriter, r *http.Request) {
	const maxBodyBytes = int64(65536)
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		writeError(w, http.StatusBadRequest, "request body too large")
		return
	}

	// IgnoreAPIVersionMismatch only skips comparing the event's API version
	// against the SDK's expected version. The HMAC signature itself is still
	// fully verified below, so this does not weaken the security check.
	options := webhook.ConstructEventOptions{IgnoreAPIVersionMismatch: true}
	event, err := webhook.ConstructEventWithOptions(body, r.Header.Get("Stripe-Signature"), h.signingSecret, options)
	if err != nil {
		log.Printf("payment: webhook signature verification failed: %v", err)
		writeError(w, http.StatusBadRequest, "signature verification failed")
		return
	}

	var status, outboxType string
	switch event.Type {
	case "payment_intent.succeeded":
		status, outboxType = "succeeded", paymentCompletedType
	case "payment_intent.payment_failed":
		status, outboxType = "failed", paymentFailedType
	default:
		w.WriteHeader(http.StatusOK) // event type we do not act on, acknowledge and move on
		return
	}

	var pi stripe.PaymentIntent
	if err := json.Unmarshal(event.Data.Raw, &pi); err != nil {
		writeError(w, http.StatusBadRequest, "invalid payment intent payload")
		return
	}

	ctx := r.Context()

	tx, err := h.payments.BeginTx(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to process webhook")
		return
	}
	defer tx.Rollback(ctx)

	orderID, err := h.payments.UpdateStatusByIntentID(ctx, tx, pi.ID, status)
	if errors.Is(err, db.ErrPaymentNotFound) {
		log.Printf("payment: webhook for unknown payment intent %s, ignoring", pi.ID)
		w.WriteHeader(http.StatusOK)
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to process webhook")
		return
	}

	isNew, err := h.processedEvents.MarkProcessed(ctx, tx, orderID, string(event.Type), webhookConsumerGroup)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to process webhook")
		return
	}
	if !isNew {
		w.WriteHeader(http.StatusOK) // already handled this Stripe event, acknowledge without republishing
		return
	}

	if err := h.outbox.Insert(ctx, tx, outboxType, orderID.String(), outboxType, paymentEvent{OrderID: orderID.String()}); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to process webhook")
		return
	}

	if err := tx.Commit(ctx); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to process webhook")
		return
	}

	log.Printf("payment: order %s payment %s", orderID, status)
	w.WriteHeader(http.StatusOK)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
