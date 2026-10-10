#!/bin/bash
# Measures how fast K Payment Service instances drain a backlog of orders.
# Usage: loadtest/drain.sh <instances> <orders>
# Needs docker compose up, migrations applied, order-service built to
# $BIN/order and payment-service to $BIN/payment, and Stripe keys in .env.
# Orders are queued with no Payment Service running, then the instances start
# and the script times how long until every order has a payment row.
set -u
K=$1
N=$2
DB="postgres://payments:payments@localhost:5434/payments?sslmode=disable"
BIN=${BIN:-/tmp/payments-bin}
LOGS=${LOGS:-/tmp/payments-logs}
mkdir -p "$LOGS"
cd "$(dirname "$0")/.."
set -a; . ./.env; set +a

psql -q "$DB" -c "TRUNCATE payments, processed_events, outbox_events, orders, cancelled_orders" -c "UPDATE products SET stock=100000"

if ! curl -sf localhost:8080/health >/dev/null; then
  PORT=8080 "$BIN/order" > "$LOGS/order.log" 2>&1 &
  sleep 4
fi

seq "$N" | xargs -P 20 -I{} curl -s -o /dev/null -X POST localhost:8080/orders \
  -H 'Content-Type: application/json' -d '{"user_id":1,"product_id":"iphone","amount":999}'
while [ "$(psql -tA "$DB" -c "select count(*) from outbox_events where published_at is null")" != "0" ]; do sleep 1; done

PIDS=()
START=$(date +%s)
for i in $(seq "$K"); do
  PORT=$((8081 + i)) "$BIN/payment" > "$LOGS/payment$i.log" 2>&1 &
  PIDS+=($!)
done

while true; do
  C=$(psql -tA "$DB" -c "select count(*) from payments")
  ELAPSED=$(( $(date +%s) - START ))
  [ "$C" -ge "$N" ] && break
  [ "$ELAPSED" -gt 900 ] && { echo "timeout at $C payments"; break; }
  sleep 1
done
echo "instances=$K payments=$C seconds=$ELAPSED per_minute=$(( C * 60 / (ELAPSED + 1) ))"
kill "${PIDS[@]}" 2>/dev/null
wait "${PIDS[@]}" 2>/dev/null
