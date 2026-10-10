#!/bin/bash
# Same measurement as drain.sh, but scales the payment-service Deployment
# on the kind cluster instead of starting local processes.
# Usage: loadtest/drain-k8s.sh <replicas> <orders>
# Needs the cluster from the README deployed and order-service reachable on
# localhost:18080 (kubectl -n payments port-forward svc/order-service 18080:8080).
set -u
K=$1
N=$2
PSQL="kubectl -n payments exec postgres-0 -- psql -U payments -tA"

kubectl -n payments scale deploy/payment-service --replicas=0 >/dev/null
kubectl -n payments wait --for=delete pod -l app=payment-service --timeout=60s >/dev/null 2>&1
$PSQL -c "TRUNCATE payments, processed_events, outbox_events, orders, cancelled_orders" -c "UPDATE products SET stock=100000" >/dev/null

seq "$N" | xargs -P 20 -I{} curl -s -o /dev/null -X POST localhost:18080/orders \
  -H 'Content-Type: application/json' -d '{"user_id":1,"product_id":"iphone","amount":999}'
while [ "$($PSQL -c "select count(*) from outbox_events where published_at is null")" != "0" ]; do sleep 1; done

START=$(date +%s)
kubectl -n payments scale deploy/payment-service --replicas="$K" >/dev/null
while true; do
  C=$($PSQL -c "select count(*) from payments")
  ELAPSED=$(( $(date +%s) - START ))
  [ "$C" -ge "$N" ] && break
  [ "$ELAPSED" -gt 900 ] && { echo "timeout at $C payments"; break; }
  sleep 1
done
echo "replicas=$K payments=$C seconds=$ELAPSED per_minute=$(( C * 60 / (ELAPSED + 1) ))"
