CREATE TABLE products (
    id TEXT PRIMARY KEY,
    stock INT NOT NULL CHECK (stock >= 0)
);

INSERT INTO products (id, stock) VALUES
    ('iphone', 500),
    ('airpods', 1000),
    ('macbook', 100);

CREATE TABLE processed_events (
    order_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (order_id, event_type)
);
