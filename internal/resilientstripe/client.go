// Package resilientstripe wraps stripeclient.Client with retry and circuit
// breaker behavior, so Payment Service does not hammer Stripe during an
// outage and recovers automatically once Stripe is healthy again.
package resilientstripe

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/sony/gobreaker/v2"
	"github.com/stripe/stripe-go/v83"
)

// retryDelays is the exact backoff schedule from the project design doc:
// immediate retry, then 1s, 5s, 30s. After the final attempt fails, the
// caller gives up rather than retrying forever. A package variable so
// tests can shorten it instead of waiting out the real 36 second sequence.
var retryDelays = []time.Duration{0, time.Second, 5 * time.Second, 30 * time.Second}

// stripeClient is the subset of stripeclient.Client this package depends
// on. Defined here, rather than depended on concretely, so tests can
// substitute a fake that fails on command instead of calling real Stripe.
type stripeClient interface {
	CreateAndConfirmPaymentIntent(ctx context.Context, amountCents int64, orderID string) (*stripe.PaymentIntent, error)
	RefundPaymentIntent(ctx context.Context, paymentIntentID string) (*stripe.Refund, error)
}

type Client struct {
	inner         stripeClient
	breaker       *gobreaker.CircuitBreaker[*stripe.PaymentIntent]
	refundBreaker *gobreaker.CircuitBreaker[*stripe.Refund]
}

// breakerTimeout is how long the circuit stays open before allowing a
// single trial call through. A package variable, rather than a New
// parameter, keeps the exported constructor simple; tests override it to
// avoid waiting out a real 30 seconds.
var breakerTimeout = 30 * time.Second

// New wraps client with a circuit breaker. The breaker opens after 5
// consecutive failed calls (each call already being a full retry sequence,
// not a single attempt) and stays open for breakerTimeout before allowing a
// single trial call through to test recovery.
func New(client stripeClient) *Client {
	settings := gobreaker.Settings{
		Name:        "stripe",
		MaxRequests: 1,
		Timeout:     breakerTimeout,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= 5
		},
	}
	refundSettings := settings
	refundSettings.Name = "stripe-refund"
	return &Client{
		inner:         client,
		breaker:       gobreaker.NewCircuitBreaker[*stripe.PaymentIntent](settings),
		refundBreaker: gobreaker.NewCircuitBreaker[*stripe.Refund](refundSettings),
	}
}

// CreateAndConfirmPaymentIntent runs the same call as stripeclient.Client,
// but the whole retry sequence below is counted by the circuit breaker as
// a single success or failure. This keeps transient blips, which retry
// already absorbs, from tripping the breaker on their own; the breaker
// only reacts to sustained failure across full retry sequences.
func (c *Client) CreateAndConfirmPaymentIntent(ctx context.Context, amountCents int64, orderID string) (*stripe.PaymentIntent, error) {
	pi, err := c.breaker.Execute(func() (*stripe.PaymentIntent, error) {
		return retry.DoWithData(
			func() (*stripe.PaymentIntent, error) {
				return c.inner.CreateAndConfirmPaymentIntent(ctx, amountCents, orderID)
			},
			retry.Context(ctx),
			retry.Attempts(uint(len(retryDelays))),
			retry.DelayType(func(n uint, _ error, _ *retry.Config) time.Duration {
				return retryDelays[n]
			}),
		)
	})
	if err != nil {
		if errors.Is(err, gobreaker.ErrOpenState) {
			return nil, fmt.Errorf("resilientstripe: circuit open, not calling stripe: %w", err)
		}
		return nil, fmt.Errorf("resilientstripe: create payment intent: %w", err)
	}
	return pi, nil
}

// RefundPaymentIntent applies the same retry schedule and circuit breaker as
// charges, with its own breaker since gobreaker is typed per result. Safe to
// retry because the underlying call uses a Stripe idempotency key.
func (c *Client) RefundPaymentIntent(ctx context.Context, paymentIntentID string) (*stripe.Refund, error) {
	r, err := c.refundBreaker.Execute(func() (*stripe.Refund, error) {
		return retry.DoWithData(
			func() (*stripe.Refund, error) {
				return c.inner.RefundPaymentIntent(ctx, paymentIntentID)
			},
			retry.Context(ctx),
			retry.Attempts(uint(len(retryDelays))),
			retry.DelayType(func(n uint, _ error, _ *retry.Config) time.Duration {
				return retryDelays[n]
			}),
		)
	})
	if err != nil {
		if errors.Is(err, gobreaker.ErrOpenState) {
			return nil, fmt.Errorf("resilientstripe: circuit open, not calling stripe: %w", err)
		}
		return nil, fmt.Errorf("resilientstripe: refund payment intent: %w", err)
	}
	return r, nil
}
