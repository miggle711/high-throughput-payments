# Load Test Results

Two 60 second runs at 50 concurrent users (spawn rate 10/s) against Order Service, using the traffic mix in `loadtest/locustfile.py`: 70% order creation, 25% status checks, 5% cancellations.

## Baseline: Order Service only, nothing consuming its events

No Inventory Service or Payment Service running. Orders are accepted and written to Postgres, but never processed further; `order.created` events accumulate unconsumed in the outbox.

| Endpoint | Requests | Req/s | P50 | P95 | P99 |
|---|---|---|---|---|---|
| POST /orders | 3322 | 55.6 | 5ms | 320ms | 1200ms |
| GET /orders/{id} | 1142 | 19.1 | 3ms | 230ms | 730ms |
| POST /orders/{id}/cancel | 236 | 4.0 | 6ms | 730ms | 1600ms |
| **Aggregate** | 4700 | 78.7 | 5ms | 300ms | 1200ms |

Full report: `baseline.html`

## Full stack: Order Service, Inventory Service, and Payment Service all running

Same traffic mix and load, but Inventory Service is deducting stock and Payment Service is making real Stripe test mode API calls for every order.

| Endpoint | Requests | Req/s | P50 | P95 | P99 |
|---|---|---|---|---|---|
| POST /orders | 3433 | 57.4 | 4ms | 300ms | 520ms |
| GET /orders/{id} | 1212 | 20.3 | 2ms | 270ms | 620ms |
| POST /orders/{id}/cancel | 235 | 3.9 | 5ms | 440ms | 780ms |
| **Aggregate** | 4880 | 81.6 | 4ms | 300ms | 580ms |

Full report: `full-stack.html`

## What this shows

**Order Service's own latency does not depend on what Inventory Service or Payment Service are doing.** P50 latency for `POST /orders` was actually marginally better with the full stack running (4ms vs 5ms), and aggregate throughput was comparable (81.6 vs 78.7 req/s). This is the core architectural claim of the outbox pattern: the customer facing write path is a single Postgres transaction, decoupled entirely from downstream processing and any external API calls. Payment Service being slow, or briefly falling behind, has no effect on how fast an order is accepted.

**Payment Service, not Order Service or Inventory Service, is the system's real throughput ceiling.** During the full stack run:

- Inventory Service kept up with load entirely; consumer lag returned to zero within seconds of the load test ending.
- Payment Service fell behind during the run and stayed behind afterward. Measured directly: it sustained roughly **58 payments/minute** (about 1/second across 3 partitions) once the load test stopped generating new orders, while 3433 orders had been created. At that rate, working through the full backlog would take roughly an hour.
- This is not a bug: every payment involves a real, synchronous Stripe API call (create and confirm a PaymentIntent), and Stripe's own round trip latency (network plus their processing time) is the bottleneck, not our code. This is exactly the kind of dependency the circuit breaker and retry logic (see the resilience work in PR #22) exist to protect against under a real outage, and exactly why Payment Service's slowness is isolated from Order Service instead of blocking it.

**Where this points next:** Payment Service's real ceiling here is bound by calling Stripe synchronously, once per order, on a single consumer per partition. Scaling Payment Service horizontally (more consumer instances sharing the consumer group, so more partitions are read in parallel) is the direct lever for closing this gap, since Kafka already partitions `order.created` for exactly this reason.

## How to reproduce

```
docker compose up -d
# apply migrations, see internal/db/migrations

# baseline: only order-service running
go run ./order-service

# in another terminal
python3 -m locust -f loadtest/locustfile.py --host=http://localhost:8090 \
  --headless --users 50 --spawn-rate 10 --run-time 60s \
  --html loadtest/results/baseline.html --csv loadtest/results/baseline

# stop order-service, reset data (see below), then start all three services
go run ./order-service &
go run ./inventory-service &
go run ./payment-service &

python3 -m locust -f loadtest/locustfile.py --host=http://localhost:8090 \
  --headless --users 50 --spawn-rate 10 --run-time 60s \
  --html loadtest/results/full-stack.html --csv loadtest/results/full-stack
```

Reset test data between runs so results are comparable:

```sql
DELETE FROM outbox_events;
DELETE FROM processed_events;
DELETE FROM payments;
DELETE FROM orders;
UPDATE products SET stock = 100000;
```
