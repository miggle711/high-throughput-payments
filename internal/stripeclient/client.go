// Package stripeclient wraps the parts of the Stripe SDK Payment Service
// needs, so the rest of the codebase depends on this small interface
// instead of the SDK directly.
package stripeclient

import (
	"context"
	"fmt"

	"github.com/stripe/stripe-go/v83"
	"github.com/stripe/stripe-go/v83/paymentintent"
)

// TestPaymentMethod always succeeds in Stripe test mode. Used since this
// project has no real checkout flow to source a payment method from.
const TestPaymentMethod = "pm_card_visa"

type Client struct{}

func New(secretKey string) *Client {
	stripe.Key = secretKey
	return &Client{}
}

// CreateAndConfirmPaymentIntent charges amountCents (in the currency's
// smallest unit) and confirms it immediately using TestPaymentMethod. In
// test mode this resolves synchronously, but Payment Service still treats
// the webhook, not this return value, as the source of truth for whether
// the charge succeeded.
func (c *Client) CreateAndConfirmPaymentIntent(ctx context.Context, amountCents int64, orderID string) (*stripe.PaymentIntent, error) {
	params := &stripe.PaymentIntentParams{
		Amount:        stripe.Int64(amountCents),
		Currency:      stripe.String(string(stripe.CurrencyUSD)),
		PaymentMethod: stripe.String(TestPaymentMethod),
		Confirm:       stripe.Bool(true),
		// TestPaymentMethod never redirects, so redirect based methods are
		// switched off rather than providing a return_url we do not need.
		AutomaticPaymentMethods: &stripe.PaymentIntentAutomaticPaymentMethodsParams{
			Enabled:        stripe.Bool(true),
			AllowRedirects: stripe.String("never"),
		},
	}
	params.AddMetadata("order_id", orderID)
	params.Context = ctx

	pi, err := paymentintent.New(params)
	if err != nil {
		return nil, fmt.Errorf("stripeclient: create payment intent: %w", err)
	}
	return pi, nil
}
