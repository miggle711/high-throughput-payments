# High Throughput Payments

A payment processing system built as three Go microservices talking over Kafka, with Postgres as the source of truth for each service and real Stripe integration for charging cards.

I built this to get hands on with the async messaging, the transactional outbox pattern, idempotent consumers, circuit breakers, and saga style compensation. I made this an actual attempt at the failure modes a real payments system has to deal with.

## Architecture

```mermaid
flowchart TD
    Client([POST /orders]) --> OrderSvc[Order Service]
    OrderSvc --> OrderDB[(Postgres:<br/>orders, outbox_events)]
    OrderSvc -->|publishes order.created| Kafka{{Kafka}}

    Kafka --> InventorySvc[Inventory Service]
    Kafka --> PaymentSvc[Payment Service]

    InventorySvc -->|deducts stock| InventoryDB[(Postgres:<br/>products)]
    InventorySvc -->|publishes| InvEvents[inventory.deducted<br/>or inventory.failed]

    PaymentSvc -->|charges the card| Stripe([Stripe])
    Stripe -->|webhook confirms outcome| PaymentSvc
    PaymentSvc -->|publishes| PayEvents[payment.completed<br/>or payment.failed]

    PayEvents -.->|consumed by| InventorySvc
    Cancel([POST /orders/id/cancel]) -->|publishes order.cancelled| Kafka
    Kafka -.->|order.cancelled| InventorySvc

    InventorySvc -->|restocks on failure or cancel| InventoryDB
```

Order Service never waits on Kafka, Stripe, or anything downstream, it writes to Postgres and returns. Every write that also needs to tell another service about it goes through the transactional outbox pattern instead, so a service never talks to Kafka directly inside a request handler. Inventory Service listening for `payment.failed` and `order.cancelled` to restock automatically is the saga compensation piece, no central coordinator, each service just reacts to events on its own.

Each service has its own set of tables, all living in one shared Postgres instance for now (see Design Decisions below for why).

## Quick start

You'll need Go, Docker, and a free Stripe account (test mode only, no card required).

```bash
git clone <this repo>
cd high-throughput-payments

# start Postgres and Kafka
docker compose up -d

# apply migrations
migrate -path internal/db/migrations -database "postgres://payments:payments@localhost:5434/payments?sslmode=disable" up

# set your Stripe test keys in .env (see .env.example if present, or just):
echo "STRIPE_SECRET_KEY=sk_test_..." >> .env
echo "STRIPE_WEBHOOK_SECRET=whsec_..." >> .env   # from `stripe listen`

# run each service in its own terminal
go run ./order-service      # :8080
go run ./inventory-service  # :8081
go run ./payment-service    # :8082

# in another terminal, forward Stripe webhooks locally
stripe listen --forward-to localhost:8082/webhooks/stripe
```

Try it:

```bash
curl -X POST localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"user_id": 1, "product_id": "iphone", "amount": 999}'

curl localhost:8080/orders/<order_id>
curl -X POST localhost:8080/orders/<order_id>/cancel
```

Watch the logs across all three services and you'll see the order get created, inventory get deducted, Stripe get charged, and the webhook come back to confirm it, all within a second or two.

## What each service does

**Order Service** takes the order, writes it to Postgres, and publishes `order.created`. It never waits on Kafka, Stripe, or anything else, the request returns as soon as Postgres commits.

**Inventory Service** consumes `order.created` and deducts stock. It also consumes `payment.failed` and `order.cancelled` to put stock back, that's the saga compensation piece, no central coordinator, each service just reacts to events on its own.

**Payment Service** consumes `order.created`, creates a Stripe PaymentIntent, and waits for Stripe's webhook to actually confirm whether it worked. The webhook, not the synchronous API response, is the source of truth, and it's signature verified so nobody can fake a payment confirmation by hitting the endpoint directly.

## Design decisions I made

**The outbox pattern, everywhere.** Any time a service needs to write to its own database and also tell Kafka about it, those two things can't be one atomic operation over the network. So instead, the write and an "event to publish" row go into the same Postgres transaction, and a background relay goroutine picks up unpublished rows and sends them to Kafka, retrying forever if it needs to. Found and fixed a real bug here: multiple services' relays polling the same shared table raced and could publish the same event twice. Fixed with `SELECT ... FOR UPDATE SKIP LOCKED` so concurrent relays split the work instead of colliding.

**UUIDv7 for order IDs**, not sequential integers. Sequential IDs leak your order volume to anyone who's curious. UUIDv7 is unguessable but still roughly sorted by creation time, so you don't have to worry about the index fragmentation cost a fully random UUIDv4 would cost us.

**Idempotent consumers, scoped correctly.** Kafka guarantees at least once delivery, so every consumer checks a `processed_events` table before doing real work. Also found a real bug here: two different services consuming the same event both wrote to that table using only `(order_id, event_type)` as the key, so whichever service ran first silently made the other one skip its own work. Fixed by scoping the key per consumer group.

**Circuit breaker plus retry around Stripe** Retry alone handles a blip. If Stripe is actually down, retrying every single order just piles load onto an already struggling service. The breaker trips after 5 consecutive failed sequences and stops trying for 30 seconds, then tests recovery with one call. Verified this live against real Stripe by using an invalid API key: watched the exact retry schedule happen, watched the breaker trip after 5 failures, and watched a stuck order get picked back up and successfully charged once I restored the real key.

**Events carry their own state.** When Inventory Service needs to restock after a cancellation, it doesn't query Order Service's table for the product ID, that event already carries everything it needs. Keeps the services honestly decoupled even though they happen to share one Postgres instance today.

**One shared Postgres instance for now**, not a database per service. In a real production setup each service would likely own a separate database. I kept them in one instance since I'm the only contributor and it's not worth the hassle of managing separate schemas and connection strings for a solo project, but the code doesn't assume this, nothing queries across another service's tables directly.

## Load testing

Ran Locust against Order Service two ways: baseline with nothing consuming its events, and full stack with Inventory and Payment Service both running and Payment Service making real Stripe calls.

Order Service's latency barely moved between the two runs (P50 4ms vs 5ms), which is the whole point of the outbox pattern, its write path doesn't care what's happening downstream.

The load test also revealed something interesting... the Payment Service is the actual bottleneck, sustaining around 58 payments a minute because every payment makes a real synchronous call to Stripe. This is just Stripe's own latency showing. Full writeup, HTML reports, and the fix I'd apply next (scale Payment Service horizontally across more Kafka consumer instances) are in [`loadtest/results/summary.md`](loadtest/results/summary.md).

## Failure scenarios that I've Tested so far

- **Stripe goes down.** Forced this with an invalid API key. Watched 4 retry attempts happen on the exact schedule (immediate, 1s, 5s, 30s), watched the breaker trip after 5 consecutive failures, watched it correctly leave a stuck order in Kafka instead of falsely marking it failed, and watched it get picked up and charged successfully the moment I restored a working key. Details in [PR #22](../../pull/22).
- **A service crashes mid-processing.** Kafka's offset is only committed after the handler actually succeeds, so a crash mid-message just means it gets redelivered on restart. Proven for real: restarting Inventory Service after adding the compensation consumer immediately caught up on a backlog of real `payment.failed` events from earlier testing that had never been consumed. Details in [PR #24](../../pull/24).
- **Cancel an order while it's being charged.**  a customer could cancel while Payment Service was mid flight charging them, and the order ended up cancelled while the charge still went through. Fixed in [#23](../../issues/23): Payment Service now consumes `order.cancelled`, refunds through Stripe if the charge already landed (or when the webhook confirms it), skips the charge if the cancel came first, and publishes `payment.refunded`. Refunds use a Stripe idempotency key so redelivery never refunds twice. Covered by tests for all three orderings.

## What's not done yet

- Known bug: `payment.failed` events carry no product id, so Inventory doesn't actually restock after a failed payment ([#28](../../issues/28))
- Scaling Payment Service horizontally to close the Stripe latency bottleneck the load test found ([#25](../../issues/25))
- Prometheus and Grafana dashboards
- Jaeger distributed tracing across the three services
- A live deployed version you can hit without cloning the repo

## What I'd do differently with more time

Probably split the shared Postgres instance into one database per service properly, if only to force myself to deal with the operational complexity of that (connection management, migrations across multiple databases, no more "just join across tables" temptation even accidentally).

## Tech stack

Go, Postgres, Kafka (KRaft mode, no Zookeeper), Stripe, Docker Compose, Locust, GitHub Actions CI with testcontainers backed repo layer tests.
