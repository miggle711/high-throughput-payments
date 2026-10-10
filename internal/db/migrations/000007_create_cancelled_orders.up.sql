CREATE TABLE cancelled_orders (
    order_id UUID PRIMARY KEY,
    cancelled_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
