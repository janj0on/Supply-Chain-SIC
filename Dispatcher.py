"""
Dispatcher.py

Turns an order into a shipment.   topic new-orders  ->  topic shipments

For every order it picks a vehicle that is
    - available (not on a trip, turnaround finished)
    - the right cargo type (refrigerated product -> refrigerated truck/van; dry goods can go in ANY
      truck, so the numbering never skips)
    - big enough (capacity_kg >= quantity_kg)
  Among the vehicles that fit, the LOWEST NUMBER wins (TRUCK-1 first, then TRUCK-2, ...), so the
  same few trucks are reused and you can follow them by eye.
and writes one shipment: order_id -> vehicle_id -> shipment_id (the trip).

All trucks start at the hub, so the corridor is always hub -> destination warehouse.

Why this is a service and not Flink SQL: giving a vehicle to an order USES UP the vehicle.
Two orders must never get the same truck. That needs shared mutable state, which streaming SQL
cannot do safely. Flink creates the orders; this service allocates the fleet.

Restart safe: it first rebuilds the state from the topics (vehicles, shipments, vehicle-status,
products), then reads the orders. Shipment ids come from the order id, so an order that was
already dispatched is never dispatched twice.
"""

import json
import os
import re
import time
import uuid

from confluent_kafka import Consumer, KafkaError, Producer
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP = os.getenv("BOOTSTRAP")
HUB_WAREHOUSE_ID = os.getenv("START_WAREHOUSE_ID", "WH-11")
CLIENT_ID = os.getenv("CLIENT_ID_DISPATCHER", "supply-chain-dispatcher")
FLEET_SIZE = int(os.getenv("FLEET_SIZE", "30"))   # vehicles numbered above this (stale topic data) are ignored


def vehicle_number(vid):
    """TRUCK-12 -> 12 (so TRUCK-2 sorts before TRUCK-10)."""
    m = re.search(r"(\d+)$", str(vid))
    return int(m.group(1)) if m else 10**9

CARGO_BY_VEHICLE_TYPE = {
    "refrigerated_truck": "refrigerated",
    "reefer_van": "refrigerated",
    "heavy_truck": "dry",
}


class DispatchState:
    """All dispatch logic. No Kafka in here, so it can be tested alone."""

    def __init__(self, hub):
        self.hub = hub
        self.vehicles = {}          # vehicle_id -> {"cargo", "capacity", "available"}
        self.product_cargo = {}     # product_id -> "refrigerated" | "dry"
        self.dispatched_orders = set()
        self.pending = []           # orders waiting for a vehicle
        self.rejected = set()

    # ---- state from topics ------------------------------------------------
    def on_product(self, p):
        try:
            self.product_cargo[p["product_id"]] = "refrigerated" if float(p["max_c"]) < 15 else "dry"
        except (KeyError, ValueError, TypeError):
            pass

    def on_vehicle(self, v):
        vid = v.get("vehicle_id")
        cargo = CARGO_BY_VEHICLE_TYPE.get(v.get("vehicle_type"))
        # only the current fleet: exactly "TRUCK-<n>" with n in 1..FLEET_SIZE.
        # (old runs left "TRUCK-00006"-style ids in Kafka; those trucks do not exist in the generator)
        n = vehicle_number(vid) if vid else 0
        if not vid or cargo is None or vid != f"TRUCK-{n}" or not 1 <= n <= FLEET_SIZE:
            return
        old = self.vehicles.get(vid)
        self.vehicles[vid] = {
            "cargo": cargo,
            "capacity": float(v.get("capacity_kg") or 0),
            "available": old["available"] if old else True,
        }

    def on_vehicle_status(self, s):
        v = self.vehicles.get(s.get("vehicle_id"))
        if v is not None and s.get("status") == "available":
            v["available"] = True

    def on_shipment(self, s):
        """Shipments written by us (or by an earlier run) tell which vehicles are busy."""
        if s.get("status") == "dispatched":
            self.dispatched_orders.add(s.get("order_id"))
            v = self.vehicles.get(s.get("vehicle_id"))
            if v is not None:
                v["available"] = False
        # status "delivered": the vehicle comes back with a vehicle-status event after turnaround

    # ---- orders -------------------------------------------------------------
    def on_order(self, o):
        oid = o.get("order_id")
        if not oid or oid in self.dispatched_orders or oid in self.rejected:
            return
        if any(p["order_id"] == oid for p in self.pending):
            return
        self.pending.append(o)

    def _best_vehicle(self, cargo, quantity):
        best = None
        for vid, v in self.vehicles.items():
            if not v["available"] or (cargo == "refrigerated" and v["cargo"] != "refrigerated") or v["capacity"] < quantity:
                continue
            key = (vehicle_number(vid), vid)
            if best is None or key < best[0]:
                best = (key, vid)
        return best[1] if best else None

    def assign(self):
        """Try to give a vehicle to every waiting order. Returns the new shipments."""
        shipments = []
        # URGENT first, then oldest first (sort is stable)
        self.pending.sort(key=lambda o: 0 if o.get("priority") == "URGENT" else 1)
        still_waiting = []
        for o in self.pending:
            product = o.get("product_id")
            cargo = self.product_cargo.get(product)
            try:
                quantity = float(o["quantity_kg"])
                destination = o["destination_warehouse_id"]
            except (KeyError, TypeError, ValueError):
                self.rejected.add(o.get("order_id"))
                print(f"[REJECT] {o.get('order_id')}: bad order {o}")
                continue
            if cargo is None:
                still_waiting.append(o)          # products topic not read yet
                continue
            biggest = max((v["capacity"] for v in self.vehicles.values() if cargo == "dry" or v["cargo"] == cargo), default=0)
            if self.vehicles and quantity > biggest:
                self.rejected.add(o["order_id"])
                print(f"[REJECT] {o['order_id']}: {quantity} kg is more than the biggest {cargo} vehicle ({biggest} kg)")
                continue
            vid = self._best_vehicle(cargo, quantity)
            if vid is None:
                still_waiting.append(o)
                continue
            self.vehicles[vid]["available"] = False
            self.dispatched_orders.add(o["order_id"])
            oid = o["order_id"]
            shipments.append({
                "shipment_id": "SHP-" + (oid[4:] if oid.startswith("ORD-") else oid),
                "order_id": oid,
                "product_id": product,
                "vehicle_id": vid,
                "origin_warehouse_id": self.hub,
                "destination_warehouse_id": destination,
                "quantity_kg": quantity,
                "status": "dispatched",
            })
        self.pending = still_waiting
        return shipments


# =============================================================================
# Kafka wiring
# =============================================================================

def make_consumer(topics, tag):
    c = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": f"dispatcher-{tag}-{uuid.uuid4().hex[:8]}",   # new group each run: always replay
        "client.id": f"{CLIENT_ID}-{tag}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    c.subscribe(topics)
    return c


def read_json(msg):
    try:
        value = json.loads(msg.value())
        return value if isinstance(value, dict) else None
    except (TypeError, ValueError):
        return None


def drain(consumer, state, handlers, max_messages=2000):
    count = 0
    for _ in range(max_messages):
        msg = consumer.poll(0)
        if msg is None:
            break
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"[CONSUMER ERROR] {msg.error()}")
            continue
        record = read_json(msg)
        if record is not None:
            handlers[msg.topic()](record)
            count += 1
    return count


def main():
    if not BOOTSTRAP:
        raise ValueError("BOOTSTRAP is missing from your .env file")

    state = DispatchState(HUB_WAREHOUSE_ID)
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "client.id": CLIENT_ID,
                         "acks": "all", "enable.idempotence": True})

    state_handlers = {
        "products": state.on_product,
        "vehicles": state.on_vehicle,
        "vehicle-status": state.on_vehicle_status,
        "shipments": state.on_shipment,
    }
    state_consumer = make_consumer(list(state_handlers), "state")

    print(f"Dispatcher starting. Hub = {HUB_WAREHOUSE_ID}. Rebuilding state from Kafka...")
    quiet_since = time.monotonic()
    while time.monotonic() - quiet_since < 4.0:     # stop when nothing new arrived for 4 s
        if drain(state_consumer, state, state_handlers):
            quiet_since = time.monotonic()
        else:
            time.sleep(0.2)
    busy = sum(1 for v in state.vehicles.values() if not v["available"])
    print(f"State ready: {len(state.vehicles)} vehicles ({busy} busy), "
          f"{len(state.product_cargo)} products, {len(state.dispatched_orders)} orders already dispatched.")

    order_consumer = make_consumer(["new-orders"], "orders")
    order_handlers = {"new-orders": state.on_order}
    sent = 0
    last_report = time.monotonic()

    try:
        while True:
            drain(state_consumer, state, state_handlers)
            got = drain(order_consumer, state, order_handlers)
            for s in state.assign():
                producer.produce("shipments", key=s["shipment_id"], value=json.dumps(s, separators=(",", ":")))
                sent += 1
                print(f"[DISPATCH] {s['order_id']} -> {s['vehicle_id']} ({s['quantity_kg']:.0f} kg to {s['destination_warehouse_id']})")
            producer.poll(0)

            if time.monotonic() - last_report > 30:
                free = sum(1 for v in state.vehicles.values() if v["available"])
                print(f"[STATUS] shipments sent={sent} | waiting orders={len(state.pending)} | free vehicles={free}")
                last_report = time.monotonic()
            if not got:
                time.sleep(0.2)
    except KeyboardInterrupt:
        print("\nStopping dispatcher...")
    finally:
        producer.flush(15)
        state_consumer.close()
        order_consumer.close()


if __name__ == "__main__":
    main()
