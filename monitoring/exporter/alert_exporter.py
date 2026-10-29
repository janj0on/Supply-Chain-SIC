#!/usr/bin/env python3
"""
alert_exporter.py - reads the Kafka topic `alerts` (written by flink-sql/03_detection.sql)
and exposes Prometheus metrics on :8000/metrics.

Prometheus cannot read Kafka, so this small program sits in the middle:
    Kafka `alerts`  ->  this exporter  ->  Prometheus  ->  Grafana
"""
import json
import os
import signal
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError
from prometheus_client import Counter, Gauge, Histogram, start_http_server

BOOTSTRAP = os.getenv("BOOTSTRAP", "kafka:29092")
TOPIC = os.getenv("ALERTS_TOPIC", "alerts")
GROUP_ID = os.getenv("GROUP_ID", "alert-exporter")
PORT = int(os.getenv("EXPORTER_PORT", "8000"))
# Known values, used to create every counter at 0 before the first alert (see init_series).
ALERT_TYPES = [t.strip() for t in os.getenv(
    "ALERT_TYPES", "temperature_breach,excessive_idling,route_deviation,overspeed").split(",") if t.strip()]
FLEET_SIZE = int(os.getenv("FLEET_SIZE", "30"))

ALERTS = Counter("supply_alerts_total", "Alerts read from Kafka, by anomaly type", ["anomaly_type"])
ALERTS_BY_VEHICLE = Counter("supply_alerts_by_vehicle_total", "Alerts by vehicle and type", ["vehicle_id", "anomaly_type"])
DURATION = Histogram(
    "supply_alert_duration_seconds", "Length of the anomaly episode (duration_s)", ["anomaly_type"],
    buckets=(30, 45, 60, 90, 120, 180, 300, 600),
)
DELAY = Histogram(
    "supply_alert_detection_delay_seconds",
    "Kafka write time of the alert minus the end of the episode (how late Flink reports it)",
    ["anomaly_type"], buckets=(5, 10, 20, 30, 45, 60, 90, 120, 300),
)
LAST_ALERT_TS = Gauge("supply_alert_last_timestamp_seconds", "Unix time when the last alert was read")
BAD_MESSAGES = Counter("supply_alert_bad_messages_total", "Messages that could not be parsed")
KAFKA_ERRORS = Counter("supply_alert_kafka_errors_total", "Kafka consumer errors")

running = True


def stop(*_):
    global running
    running = False


def init_series():
    """Create every counter at 0 at start-up.

    A Prometheus counter that first appears with value 1 looks like "nothing happened" to increase()
    and rate(): they need a 0 before the 1, so the first alert of every type / vehicle would be lost.
    Starting at 0 fixes that. New labels that are not in these lists still work, they just lose their first alert.
    """
    for kind in ALERT_TYPES:
        ALERTS.labels(kind)
        DURATION.labels(kind)
        DELAY.labels(kind)
        for n in range(1, FLEET_SIZE + 1):
            ALERTS_BY_VEHICLE.labels(f"TRUCK-{n}", kind)


def to_epoch(text):
    """Flink writes yyyy-MM-dd HH:mm:ss.SSS (or with a T). The values are UTC."""
    t = datetime.fromisoformat(str(text).replace("Z", "").strip())
    return t.replace(tzinfo=timezone.utc).timestamp()


def handle(msg):
    try:
        a = json.loads(msg.value())
        kind = a["anomaly_type"]
        vehicle = a["vehicle_id"]
    except (ValueError, KeyError, TypeError):
        BAD_MESSAGES.inc()
        return

    ALERTS.labels(kind).inc()
    ALERTS_BY_VEHICLE.labels(vehicle, kind).inc()
    LAST_ALERT_TS.set(time.time())

    if a.get("duration_s") is not None:
        DURATION.labels(kind).observe(max(float(a["duration_s"]), 0))

    # msg.timestamp() = (type, milliseconds). Only use it if it is a real timestamp.
    ts_type, ts_ms = msg.timestamp()
    if ts_type != 0 and a.get("end_time"):
        try:
            DELAY.labels(kind).observe(max(ts_ms / 1000.0 - to_epoch(a["end_time"]), 0))
        except ValueError:
            pass


def main():
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    init_series()
    start_http_server(PORT)
    print(f"metrics on :{PORT}/metrics, reading {TOPIC} from {BOOTSTRAP}", flush=True)

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        # The Flink sink is exactly-once (Kafka transactions): only read committed alerts.
        "isolation.level": "read_committed",
        "enable.auto.commit": True,
    })
    consumer.subscribe([TOPIC])
    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                # a missing topic is normal until Flink writes the first alert
                if msg.error().code() != KafkaError.UNKNOWN_TOPIC_OR_PART:
                    KAFKA_ERRORS.inc()
                    print("kafka error:", msg.error(), flush=True)
                continue
            handle(msg)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()