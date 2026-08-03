"""
Load test for Order Service, simulating realistic traffic rather than
hammering POST /orders alone: most traffic is order creation, some is
customers checking status after buying, and a small fraction is
cancellation, matching the project design doc's guidance.

Run against Order Service directly:
    locust -f loadtest/locustfile.py --host=http://localhost:8090

Two comparison runs are used to produce before/after numbers:
  baseline:   only Order Service running, nothing consuming its events
  full stack: Order Service, Inventory Service, and Payment Service all
              running, so orders are actually processed end to end

Both runs hit the same Order Service endpoints. The comparison shows
whether Order Service's own latency depends on downstream processing,
which is the actual architectural claim: async processing decouples the
customer facing path from slow external calls like Stripe.
"""

import random

from locust import HttpUser, between, task

PRODUCTS = ["iphone", "airpods", "macbook"]


class OrderUser(HttpUser):
    wait_time = between(0.1, 1)

    def on_start(self):
        self.created_order_ids = []

    @task(70)
    def create_order(self):
        user_id = random.randint(1, 10000)
        product_id = random.choice(PRODUCTS)
        amount = random.randint(100, 200000)

        with self.client.post(
            "/orders",
            json={"user_id": user_id, "product_id": product_id, "amount": amount},
            catch_response=True,
        ) as response:
            if response.status_code != 201:
                response.failure(f"expected 201, got {response.status_code}")
                return
            order_id = response.json().get("order_id")
            if order_id:
                self.created_order_ids.append(order_id)
                # bound memory, this is a long running load test, not a
                # correctness check on every order ever created
                if len(self.created_order_ids) > 200:
                    self.created_order_ids.pop(0)

    @task(25)
    def check_order_status(self):
        if not self.created_order_ids:
            return
        order_id = random.choice(self.created_order_ids)
        self.client.get(f"/orders/{order_id}", name="/orders/[id]")

    @task(5)
    def cancel_order(self):
        if not self.created_order_ids:
            return
        order_id = self.created_order_ids.pop()
        self.client.post(f"/orders/{order_id}/cancel", name="/orders/[id]/cancel")
