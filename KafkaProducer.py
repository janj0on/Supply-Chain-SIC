"""
KafkaProducer.py  (the bridge between the generator files and Kafka)

Files -> Kafka
    products.csv, warehouses.csv, vehicles.csv      master data topics
    telemetry.jsonl        -> vehicle-telemetry     key = vehicle_id
    inventory_stock.jsonl  -> inventory-stock       key = site_id|product_id
    demand_events.jsonl    -> demand-events         key = site_id|product_id
    vehicle_status.jsonl   -> vehicle-status        key = vehicle_id
    shipment_updates.jsonl -> shipments             key = shipment_id

Kafka -> file  (so the generator can start the trucks the Dispatcher chose)
    topic shipments (status = dispatched) -> dispatch_inbox.jsonl

The original safety features are kept: idempotent producer, DLQ topic,
local backup file if even the DLQ fails.
"""

import csv
import hashlib
import io
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP = os.getenv("BOOTSTRAP")
CLIENT_ID = os.getenv("CLIENT_ID_FILE", "supply-chain-producer")

if not BOOTSTRAP:
    raise ValueError("BOOTSTRAP is missing from your .env file")

DATA_DIR = Path(os.getenv("DATA_DIR", Path(__file__).resolve().parent / "data"))

DLQ_TOPIC = os.getenv("DLQ_TOPIC", "producer-dlq")
TOPIC_PARTITIONS = max(1, int(os.getenv("TOPIC_PARTITIONS", "3")))
REPLICATION_FACTOR = max(1, int(os.getenv("REPLICATION_FACTOR", "1")))
CSV_REFRESH_SECONDS = max(0.5, float(os.getenv("CSV_REFRESH_SECONDS", "2")))
TELEMETRY_POLL_SECONDS = max(0.1, float(os.getenv("TELEMETRY_POLL_SECONDS", "0.2")))

# ---------------------------------------------------------------- topics
CSV_TOPICS = {
    "products.csv": "products",
    "warehouses.csv": "warehouses",
    "vehicles.csv": "vehicles",
}
STATIC_FILES = {"products.csv", "warehouses.csv"}
DYNAMIC_FILES = {"vehicles.csv"}


def site_product_key(r):
    return f"{r.get('site_id')}|{r.get('product_id')}"


# file name -> (topic, function that builds the Kafka key, field that must exist)
JSONL_TOPICS = {
    "telemetry.jsonl":        ("vehicle-telemetry", lambda r: r.get("vehicle_id"), "event_id"),
    "inventory_stock.jsonl":  ("inventory-stock",   site_product_key,              "event_id"),
    "demand_events.jsonl":    ("demand-events",     site_product_key,              "event_id"),
    "vehicle_status.jsonl":   ("vehicle-status",    lambda r: r.get("vehicle_id"), "event_id"),
    "shipment_updates.jsonl": ("shipments",         lambda r: r.get("shipment_id"), "shipment_id"),
}

# Topics that Flink or the Dispatcher write to. Created here so they have 3 partitions
# and exist before the Flink jobs start.
OTHER_TOPICS = ["shipments", "alerts", "replenishment-triggered", "new-orders"]

ALL_TOPICS = sorted(set(
    list(CSV_TOPICS.values())
    + [v[0] for v in JSONL_TOPICS.values()]
    + OTHER_TOPICS
    + [DLQ_TOPIC]
))

SHIPMENTS_TOPIC = "shipments"
INBOX_FILE = DATA_DIR / "dispatch_inbox.jsonl"
LOCAL_FAILURE_FILE = DATA_DIR / "producer_failed_records.jsonl"

# ---------------------------------------------------------------- clients
producer = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "client.id": CLIENT_ID,
    "acks": "all",
    "enable.idempotence": True,
    "retries": 10,
    "max.in.flight.requests.per.connection": 5,
    "request.timeout.ms": 30000,
    "delivery.timeout.ms": 120000,
    "queue.buffering.max.messages": 100000,
})

admin = AdminClient({
    "bootstrap.servers": BOOTSTRAP,
    "client.id": f"{CLIENT_ID}-admin",
})

stats = {
    "delivered": 0,
    "delivery_failed": 0,
    "dlq_delivered": 0,
    "dlq_failed": 0,
    "local_backups": 0,
    "inbox_written": 0,
}

KEY_FIELDS = ["product_id", "vehicle_id", "warehouse_id", "shipment_id", "event_id", "id"]

warned_missing = set()
static_files_loaded = set()
dynamic_fingerprints = {}


# ---------------------------------------------------------------- topics
def ensure_topics():
    print("Checking Kafka topics...")
    try:
        metadata = admin.list_topics(timeout=15)
        existing_topics = set(metadata.topics.keys())
        missing_topics = [t for t in ALL_TOPICS if t not in existing_topics]

        if not missing_topics:
            print("All required Kafka topics already exist.")
            return

        print("Creating missing topics:", ", ".join(missing_topics))
        new_topics = [
            NewTopic(t, num_partitions=TOPIC_PARTITIONS, replication_factor=REPLICATION_FACTOR)
            for t in missing_topics
        ]
        futures = admin.create_topics(new_topics, request_timeout=15)

        errors = []
        for topic, future in futures.items():
            try:
                future.result()
                print(f"[TOPIC CREATED] {topic}")
            except KafkaException as exc:
                error = exc.args[0]
                if error.code() == KafkaError.TOPIC_ALREADY_EXISTS:
                    print(f"[TOPIC EXISTS] {topic}")
                else:
                    errors.append(f"{topic}: {error}")

        if errors:
            raise RuntimeError("Could not create required topics:\n" + "\n".join(errors))

        metadata = admin.list_topics(timeout=15)
        still_missing = [t for t in ALL_TOPICS if t not in metadata.topics]
        if still_missing:
            raise RuntimeError("Topics are still missing: " + ", ".join(still_missing))

        print("Topic verification passed.")

    except Exception as exc:
        raise RuntimeError(
            "Kafka topic initialization failed. "
            "Check broker availability, topic permissions, and replication factor."
        ) from exc


# ---------------------------------------------------------------- sending
def record_key(record):
    for field in KEY_FIELDS:
        value = record.get(field)
        if value is not None and str(value) != "":
            return str(value)
    return None


def save_local_failure(envelope, reason):
    """Last-resort local backup. This file is NOT automatically replayed."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    backup = {
        **envelope,
        "local_backup_reason": str(reason),
        "backup_time_utc": datetime.now(timezone.utc).isoformat(),
    }
    try:
        with LOCAL_FAILURE_FILE.open("a", encoding="utf-8") as file:
            file.write(json.dumps(backup, default=str) + "\n")
            file.flush()
            os.fsync(file.fileno())
        stats["local_backups"] += 1
        print(f"[LOCAL BACKUP] {LOCAL_FAILURE_FILE} | reason={reason}")
    except Exception as exc:
        print(f"[CRITICAL] Could not save failed record: {exc}")
        print(f"[CRITICAL] Unsaved record: {backup}")


def send_to_dlq(original_topic, key, original_value, error):
    """Send a failed record to the DLQ. A failed DLQ record never goes back into the DLQ."""
    envelope = {
        "original_topic": original_topic,
        "original_key": key,
        "original_value": original_value,
        "error": str(error),
        "failed_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    try:
        producer.produce(
            topic=DLQ_TOPIC,
            key=key,
            value=json.dumps(envelope, default=str),
            callback=lambda err, msg: on_dlq_delivery(err, msg, envelope),
        )
        producer.poll(0)
    except Exception as exc:
        stats["dlq_failed"] += 1
        save_local_failure(envelope, f"Could not enqueue DLQ message: {exc}")


def on_dlq_delivery(error, message, envelope):
    if error is not None:
        stats["dlq_failed"] += 1
        print(f"[DLQ DELIVERY FAILED] {error}")
        save_local_failure(envelope, f"DLQ delivery failed: {error}")
        return
    stats["dlq_delivered"] += 1
    print(f"[DLQ OK] original_topic={envelope['original_topic']} key={envelope['original_key']}")


def on_delivery(error, message, original_topic, key, value):
    if error is not None:
        stats["delivery_failed"] += 1
        print(f"[DELIVERY FAILED] topic={original_topic} key={key} error={error}")
        send_to_dlq(original_topic, key, value, error)
        return
    stats["delivered"] += 1


def send_record(topic, record, key=None):
    try:
        value = json.dumps(record, ensure_ascii=False, default=str)
    except (TypeError, ValueError) as exc:
        stats["delivery_failed"] += 1
        send_to_dlq(topic, key, repr(record), f"JSON serialization error: {exc}")
        return False

    if key is None:
        key = record_key(record)

    try:
        producer.produce(
            topic=topic,
            key=key,
            value=value,
            callback=lambda err, msg: on_delivery(err, msg, topic, key, value),
        )
        producer.poll(0)
        return True
    except BufferError as exc:
        stats["delivery_failed"] += 1
        send_to_dlq(topic, key, value, f"Producer queue full: {exc}")
        return False
    except Exception as exc:
        stats["delivery_failed"] += 1
        send_to_dlq(topic, key, value, f"Produce exception: {exc}")
        return False


# ---------------------------------------------------------------- CSV master data
def read_csv_snapshot(path):
    """Read a complete CSV snapshot. If the generator is writing at the same time, try again later."""
    try:
        before = path.stat()
        raw = path.read_bytes()
        after = path.stat()
        if (before.st_size != after.st_size
                or before.st_mtime_ns != after.st_mtime_ns
                or len(raw) != after.st_size):
            return None
        text = raw.decode("utf-8-sig")
        reader = csv.DictReader(io.StringIO(text))
        if not reader.fieldnames:
            return []
        return list(reader)
    except (OSError, UnicodeDecodeError, csv.Error):
        return None


def poll_csv_files():
    """Static files are sent once. vehicles.csv is a snapshot: only changed rows are sent."""
    for filename, topic in CSV_TOPICS.items():
        path = DATA_DIR / filename

        if not path.exists():
            if filename not in warned_missing:
                print(f"[WAITING FOR FILE] {path}")
                warned_missing.add(filename)
            continue

        if filename in STATIC_FILES and filename in static_files_loaded:
            continue

        rows = read_csv_snapshot(path)
        if rows is None:
            continue

        if filename in STATIC_FILES:
            for row in rows:
                send_record(topic, row)
            static_files_loaded.add(filename)
            print(f"[CSV LOADED] {filename}: {len(rows)} records -> {topic}")

        elif filename in DYNAMIC_FILES:
            changed_count = 0
            for row in rows:
                key = record_key(row)
                if key is None:
                    send_record(topic, row)
                    changed_count += 1
                    continue

                fingerprint = hashlib.sha256(
                    json.dumps(row, sort_keys=True, ensure_ascii=False, default=str).encode("utf-8")
                ).hexdigest()
                fingerprint_key = (topic, key)
                if dynamic_fingerprints.get(fingerprint_key) == fingerprint:
                    continue
                if send_record(topic, row, key=key):
                    dynamic_fingerprints[fingerprint_key] = fingerprint
                    changed_count += 1

            if changed_count:
                print(f"[CSV UPDATE] {filename}: {changed_count} changed records -> {topic}")


# ---------------------------------------------------------------- JSONL files
class JsonlTail:
    """Follow one growing .jsonl file and send each complete line to its topic."""

    def __init__(self, filename, topic, key_fn, required_field):
        self.path = DATA_DIR / filename
        self.topic = topic
        self.key_fn = key_fn
        self.required = required_field
        self.position = 0
        self.pending = b""
        self.sent = 0

    def poll(self):
        if not self.path.exists():
            return
        try:
            size = self.path.stat().st_size
            if size < self.position:
                self.position, self.pending = 0, b""
                print(f"[RESET] {self.path.name} was truncated.")
            with self.path.open("rb") as file:
                file.seek(self.position)
                chunk = file.read()
                self.position = file.tell()
            if not chunk:
                return
            self.pending += chunk
            lines = self.pending.split(b"\n")
            self.pending = lines.pop()
            for line in lines:
                self.process(line)
        except OSError as exc:
            print(f"[FILE READ ERROR] {self.path.name}: {exc}")

    def process(self, raw_line):
        text = raw_line.decode("utf-8", errors="replace").strip()
        if not text:
            return
        try:
            record = json.loads(text)
        except json.JSONDecodeError as exc:
            send_to_dlq(self.topic, None, text, f"Invalid JSON: {exc}")
            return
        if not isinstance(record, dict):
            send_to_dlq(self.topic, None, text, "JSON must be an object")
            return
        if not record.get(self.required):
            send_to_dlq(self.topic, record_key(record), text, f"Missing {self.required}")
            return
        if send_record(self.topic, record, key=self.key_fn(record)):
            self.sent += 1


# ---------------------------------------------------------------- Kafka -> generator
def open_dispatch_consumer():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "generator-bridge",
        "client.id": f"{CLIENT_ID}-dispatch",
        "auto.offset.reset": "latest",     # only react to shipments created from now on
        "enable.auto.commit": True,
    })
    consumer.subscribe([SHIPMENTS_TOPIC])
    return consumer


def forward_dispatched_shipments(consumer):
    """Copy shipments with status=dispatched into dispatch_inbox.jsonl for the generator."""
    lines = []
    for _ in range(500):
        msg = consumer.poll(0)
        if msg is None:
            break
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"[CONSUMER ERROR] {msg.error()}")
            continue
        try:
            shipment = json.loads(msg.value())
        except (TypeError, ValueError):
            continue
        if isinstance(shipment, dict) and shipment.get("status") == "dispatched":
            lines.append(json.dumps(shipment, separators=(",", ":")))
    if lines:
        INBOX_FILE.parent.mkdir(parents=True, exist_ok=True)   # data/ may have been deleted while running
        with INBOX_FILE.open("a", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
        stats["inbox_written"] += len(lines)
        print(f"[DISPATCH] {len(lines)} shipment(s) -> {INBOX_FILE.name}")


# ---------------------------------------------------------------- main loop
def follow_files():
    tails = [JsonlTail(name, topic, key_fn, req) for name, (topic, key_fn, req) in JSONL_TOPICS.items()]
    consumer = open_dispatch_consumer()
    last_csv_refresh = 0.0
    last_report = time.monotonic()

    print("[FOLLOWING]", ", ".join(t.path.name for t in tails))
    print("Waiting for events... Press Ctrl+C to stop.")

    try:
        while True:
            now = time.monotonic()

            if now - last_csv_refresh >= CSV_REFRESH_SECONDS:
                poll_csv_files()
                last_csv_refresh = now

            for tail in tails:
                tail.poll()

            forward_dispatched_shipments(consumer)
            producer.poll(0)

            if now - last_report >= 30:
                print("[SENT] " + " | ".join(f"{t.topic}={t.sent}" for t in tails)
                      + f" | delivered={stats['delivered']} failed={stats['delivery_failed']}")
                last_report = now

            time.sleep(TELEMETRY_POLL_SECONDS)
    finally:
        consumer.close()


def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    print("Starting Supply Chain Kafka Producer (bridge)...")
    print(f"Kafka brokers: {BOOTSTRAP}")
    print(f"DLQ topic: {DLQ_TOPIC}")

    ensure_topics()
    poll_csv_files()

    try:
        follow_files()
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        remaining = producer.flush(30)
        print("\n========== PRODUCER SUMMARY ==========")
        print(f"Delivered successfully: {stats['delivered']}")
        print(f"Delivery failures:      {stats['delivery_failed']}")
        print(f"DLQ deliveries:         {stats['dlq_delivered']}")
        print(f"DLQ failures:           {stats['dlq_failed']}")
        print(f"Local backups:          {stats['local_backups']}")
        print(f"Shipments to inbox:     {stats['inbox_written']}")
        print(f"Messages still queued:  {remaining}")
        if remaining:
            print("[WARNING] Some messages remained queued after the flush timeout.")
        if stats["local_backups"]:
            print("Review and replay local failures from:", LOCAL_FAILURE_FILE)
        print("Producer stopped.")


if __name__ == "__main__":
    main()
