DROP TABLE processed_events;

CREATE TABLE processed_events (
    order_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    consumer_group TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (order_id, event_type, consumer_group)
);
