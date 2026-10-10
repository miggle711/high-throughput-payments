package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"github.com/stripe/stripe-go/v83"
	"github.com/stripe/stripe-go/v83/webhook"

	"github.com/mvar0010/high-throughput-payments/internal/db"
	"github.com/mvar0010/high-throughput-payments/internal/dbtest"
)

const testWebhookSecret = "whsec_test"

type fakeRefunder struct{ calls int64 }

func (f *fakeRefunder) RefundPaymentIntent(ctx context.Context, id string) (*stripe.Refund, error) {
	atomic.AddInt64(&f.calls, 1)
	return &stripe.Refund{ID: "re_fake"}, nil
}

type fixture struct {
	pool     *pgxpool.Pool
	payments *db.PaymentsRepo
	fake     *fakeRefunder
	payment  *PaymentHandler
	webhook  *WebhookHandler
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	pool := dbtest.NewPool(t)
	payments := db.NewPaymentsRepo(pool)
	processed := db.NewProcessedEventsRepo()
	outbox := db.NewOutboxRepo(pool)
	fake := &fakeRefunder{}
	return &fixture{
		pool:     pool,
		payments: payments,
		fake:     fake,
		// stripe is nil on purpose: no test here may reach the charge call.
		payment: &PaymentHandler{pool: pool, payments: payments, processedEvents: processed, outbox: outbox,
			refunder: &refunder{stripe: fake, payments: payments, outbox: outbox}, consumerGroupID: "payment-service"},
		webhook: NewWebhookHandler(payments, processed, outbox, fake, testWebhookSecret),
	}
}

func (f *fixture) insertPayment(t *testing.T, orderID uuid.UUID, intentID, status string) {
	t.Helper()
	ctx := context.Background()
	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	p := &db.Payment{OrderID: orderID, StripePaymentIntentID: intentID, Amount: 1000, Status: status}
	if err := f.payments.Create(ctx, tx, p); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func (f *fixture) cancel(t *testing.T, orderID uuid.UUID) {
	t.Helper()
	body, _ := json.Marshal(orderCancelledEvent{OrderID: orderID.String()})
	if err := f.payment.HandleOrderCancelled(context.Background(), kafka.Message{Value: body}); err != nil {
		t.Fatalf("handle order.cancelled: %v", err)
	}
}

func (f *fixture) sendSucceededWebhook(t *testing.T, intentID string) {
	t.Helper()
	payload, _ := json.Marshal(map[string]any{
		"id": "evt_" + uuid.NewString(), "object": "event", "type": "payment_intent.succeeded",
		"data": map[string]any{"object": map[string]any{"id": intentID, "object": "payment_intent"}},
	})
	signed := webhook.GenerateTestSignedPayload(&webhook.UnsignedPayload{Payload: payload, Secret: testWebhookSecret, Timestamp: time.Now()})
	req := httptest.NewRequest(http.MethodPost, "/webhooks/stripe", bytes.NewReader(signed.Payload))
	req.Header.Set("Stripe-Signature", signed.Header)
	rec := httptest.NewRecorder()
	f.webhook.HandleStripeWebhook(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("webhook status = %d, body %s", rec.Code, rec.Body.String())
	}
}

func (f *fixture) status(t *testing.T, orderID uuid.UUID) string {
	t.Helper()
	var s string
	if err := f.pool.QueryRow(context.Background(), `SELECT status FROM payments WHERE order_id = $1`, orderID).Scan(&s); err != nil {
		t.Fatal(err)
	}
	return s
}

func (f *fixture) outboxCount(t *testing.T, eventType string) int {
	t.Helper()
	var n int
	if err := f.pool.QueryRow(context.Background(), `SELECT count(*) FROM outbox_events WHERE event_type = $1`, eventType).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// Ordering 1: the charge succeeded, then the cancel arrives.
func TestCancelAfterCharge_Refunds(t *testing.T) {
	f := newFixture(t)
	orderID := uuid.New()
	f.insertPayment(t, orderID, "pi_1", "succeeded")

	f.cancel(t, orderID)

	if got := f.status(t, orderID); got != "refunded" {
		t.Errorf("status = %q, want refunded", got)
	}
	if got := atomic.LoadInt64(&f.fake.calls); got != 1 {
		t.Errorf("refund calls = %d, want 1", got)
	}
	if got := f.outboxCount(t, paymentRefundedType); got != 1 {
		t.Errorf("payment.refunded events = %d, want 1", got)
	}
}

// Ordering 2: the cancel arrives while the charge is still pending, then the
// webhook confirms it. The webhook must refund, not announce payment.completed.
func TestCancelBeforeWebhook_WebhookRefunds(t *testing.T) {
	f := newFixture(t)
	orderID := uuid.New()
	f.insertPayment(t, orderID, "pi_2", "pending")

	f.cancel(t, orderID)
	if got := atomic.LoadInt64(&f.fake.calls); got != 0 {
		t.Fatalf("refund called before charge confirmed: %d", got)
	}

	f.sendSucceededWebhook(t, "pi_2")

	if got := f.status(t, orderID); got != "refunded" {
		t.Errorf("status = %q, want refunded", got)
	}
	if got := atomic.LoadInt64(&f.fake.calls); got != 1 {
		t.Errorf("refund calls = %d, want 1", got)
	}
	if got := f.outboxCount(t, paymentCompletedType); got != 0 {
		t.Errorf("payment.completed events = %d, want 0", got)
	}
	if got := f.outboxCount(t, paymentRefundedType); got != 1 {
		t.Errorf("payment.refunded events = %d, want 1", got)
	}
}

// Ordering 3: the cancel arrives before any charge exists, so the later
// order.created must not charge. stripe is nil in the fixture, so reaching
// the charge call would panic.
func TestCancelBeforeCharge_SkipsCharge(t *testing.T) {
	f := newFixture(t)
	orderID := uuid.New()

	f.cancel(t, orderID)

	body, _ := json.Marshal(orderCreatedEvent{OrderID: orderID.String(), UserID: 1, ProductID: "p", Amount: 1000})
	if err := f.payment.HandleOrderCreated(context.Background(), kafka.Message{Value: body}); err != nil {
		t.Fatalf("handle order.created: %v", err)
	}

	var n int
	if err := f.pool.QueryRow(context.Background(), `SELECT count(*) FROM payments WHERE order_id = $1`, orderID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("payment rows = %d, want 0", n)
	}
}

// Kafka redelivers: a second order.cancelled must not refund again.
func TestCancelRedelivery_RefundsOnce(t *testing.T) {
	f := newFixture(t)
	orderID := uuid.New()
	f.insertPayment(t, orderID, "pi_3", "succeeded")

	f.cancel(t, orderID)
	f.cancel(t, orderID)

	if got := atomic.LoadInt64(&f.fake.calls); got != 1 {
		t.Errorf("refund calls = %d, want 1", got)
	}
}
