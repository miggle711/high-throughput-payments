CREATE TABLE payments (
    order_id UUID PRIMARY KEY,
    stripe_payment_intent_id TEXT NOT NULL UNIQUE,
    amount BIGINT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_payments_stripe_payment_intent_id ON payments (stripe_payment_intent_id);
