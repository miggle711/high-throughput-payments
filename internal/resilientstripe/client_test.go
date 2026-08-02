package resilientstripe

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sony/gobreaker/v2"
	"github.com/stripe/stripe-go/v83"
)

var errFakeStripeDown = errors.New("fake stripe: down")

// fakeStripeClient lets tests force failures on command instead of
// depending on real network calls to Stripe. failUntilCall makes the first
// N calls fail, so tests can assert exact attempt counts; failing is a
// direct on/off toggle for tests that want to flip Stripe's health at a
// precise moment (for example, after the breaker has already tripped)
// rather than counting individual retry attempts.
type fakeStripeClient struct {
	calls         int64
	failUntilCall int64
	failing       atomic.Bool
}

func (f *fakeStripeClient) CreateAndConfirmPaymentIntent(ctx context.Context, amountCents int64, orderID string) (*stripe.PaymentIntent, error) {
	n := atomic.AddInt64(&f.calls, 1)
	if n <= f.failUntilCall || f.failing.Load() {
		return nil, errFakeStripeDown
	}
	return &stripe.PaymentIntent{ID: "pi_fake"}, nil
}

func (f *fakeStripeClient) callCount() int64 {
	return atomic.LoadInt64(&f.calls)
}

// TestCreateAndConfirmPaymentIntent_RetriesExactSchedule verifies the retry
// count matches the project design doc's schedule length (immediate, 1s,
// 5s, 30s: 4 attempts total), not retry-go's default of 10. The delays
// themselves are shortened for the test; only the count matters here.
func TestCreateAndConfirmPaymentIntent_RetriesExactSchedule(t *testing.T) {
	withShortBreakerTimeout(t, time.Hour) // keep the breaker out of this test
	wantAttempts := len(retryDelays)
	withFastRetryDelays(t)

	fake := &fakeStripeClient{failUntilCall: 999} // always fails
	client := New(fake)

	_, err := client.CreateAndConfirmPaymentIntent(context.Background(), 1000, "order-1")
	if err == nil {
		t.Fatal("expected an error, got nil")
	}

	if got := fake.callCount(); got != int64(wantAttempts) {
		t.Errorf("call count = %d, want %d", got, wantAttempts)
	}
}

// TestCreateAndConfirmPaymentIntent_SucceedsAfterTransientFailure verifies
// that retry recovers a call that fails a couple of times before Stripe
// starts succeeding again, without needing the circuit breaker at all.
func TestCreateAndConfirmPaymentIntent_SucceedsAfterTransientFailure(t *testing.T) {
	withShortBreakerTimeout(t, time.Hour)
	withFastRetryDelays(t)

	fake := &fakeStripeClient{failUntilCall: 2} // first 2 calls fail, 3rd succeeds
	client := New(fake)

	pi, err := client.CreateAndConfirmPaymentIntent(context.Background(), 1000, "order-1")
	if err != nil {
		t.Fatalf("expected success after retry, got error: %v", err)
	}
	if pi.ID != "pi_fake" {
		t.Errorf("payment intent ID = %q, want %q", pi.ID, "pi_fake")
	}
}

// TestCreateAndConfirmPaymentIntent_BreakerOpensAfterConsecutiveFailures
// verifies the breaker trips after 5 consecutive failed calls (each call
// being a full retry sequence, not a single attempt) and then rejects
// further calls immediately without going through retry again.
func TestCreateAndConfirmPaymentIntent_BreakerOpensAfterConsecutiveFailures(t *testing.T) {
	withShortBreakerTimeout(t, time.Hour) // stay open for the rest of this test
	withFastRetryDelays(t)

	fake := &fakeStripeClient{failUntilCall: 999} // always fails
	client := New(fake)

	const failuresToTripBreaker = 5
	for i := 0; i < failuresToTripBreaker; i++ {
		_, err := client.CreateAndConfirmPaymentIntent(context.Background(), 1000, "order-1")
		if err == nil {
			t.Fatalf("call %d: expected an error, got nil", i+1)
		}
	}

	callsBeforeOpenCheck := fake.callCount()

	_, err := client.CreateAndConfirmPaymentIntent(context.Background(), 1000, "order-1")
	if !errors.Is(err, gobreaker.ErrOpenState) {
		t.Fatalf("expected gobreaker.ErrOpenState once breaker is open, got: %v", err)
	}

	if got := fake.callCount(); got != callsBeforeOpenCheck {
		t.Errorf("call count grew from %d to %d, breaker should reject without calling stripe at all", callsBeforeOpenCheck, got)
	}
}

// TestCreateAndConfirmPaymentIntent_BreakerRecoversAfterTimeout verifies
// that once the breaker's timeout elapses, a single trial call is let
// through, and a success closes the breaker again.
func TestCreateAndConfirmPaymentIntent_BreakerRecoversAfterTimeout(t *testing.T) {
	withShortBreakerTimeout(t, 50*time.Millisecond)
	withFastRetryDelays(t)

	fake := &fakeStripeClient{}
	fake.failing.Store(true)
	client := New(fake)

	const failuresToTripBreaker = 5
	for i := 0; i < failuresToTripBreaker; i++ {
		client.CreateAndConfirmPaymentIntent(context.Background(), 1000, "order-1")
	}

	_, err := client.CreateAndConfirmPaymentIntent(context.Background(), 1000, "order-1")
	if !errors.Is(err, gobreaker.ErrOpenState) {
		t.Fatalf("expected breaker to be open immediately after tripping, got: %v", err)
	}

	fake.failing.Store(false)         // stripe recovers
	time.Sleep(60 * time.Millisecond) // wait out the short breaker timeout

	pi, err := client.CreateAndConfirmPaymentIntent(context.Background(), 1000, "order-1")
	if err != nil {
		t.Fatalf("expected the trial call after timeout to succeed and close the breaker, got: %v", err)
	}
	if pi.ID != "pi_fake" {
		t.Errorf("payment intent ID = %q, want %q", pi.ID, "pi_fake")
	}
}

// withShortBreakerTimeout overrides the package level breakerTimeout for
// the duration of a test, restoring it afterward, so tests do not need to
// wait out the real 30 second production timeout.
func withShortBreakerTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	original := breakerTimeout
	breakerTimeout = d
	t.Cleanup(func() { breakerTimeout = original })
}

// withFastRetryDelays overrides the package level retryDelays for the
// duration of a test, keeping the same attempt count as production but
// replacing the real 0/1s/5s/30s schedule with near instant delays, so
// tests do not wait out the real 36 second sequence.
func withFastRetryDelays(t *testing.T) {
	t.Helper()
	original := retryDelays
	fast := make([]time.Duration, len(original))
	for i := range fast {
		fast[i] = time.Millisecond
	}
	retryDelays = fast
	t.Cleanup(func() { retryDelays = original })
}
