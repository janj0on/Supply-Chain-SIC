#!/usr/bin/env python3
"""
score_alerts.py - compare the Flink `alerts` topic with data/ground_truth.jsonl.

    python score_alerts.py --truth data/ground_truth.jsonl            # reads Kafka
    python score_alerts.py --truth data/ground_truth.jsonl \
        --telemetry-file data/telemetry.jsonl --alerts-file alerts.jsonl   # reads files

Anomalies are now EPISODES (several readings in a row), so scoring is per episode:

  * Only readings that really reached Kafka count (the generator may lose some in offline gaps).
  * Readings of the same shipment and type, less than SESSION_GAP seconds apart, form one episode.
  * An episode MUST raise an alert when it lasts at least the detector threshold
    (temperature_breach and excessive_idling: 30 s; route_deviation: any length).
    Short harmless blips are labelled "not an anomaly" by the generator, so an alert for
    one of them is a false positive.
  * An alert is correct when it overlaps a true episode of the same shipment and type.
  * overspeed is a single reading, so it is still matched by event_id.
  * Episodes that end in the last --grace-seconds of the stream are ignored: the alert is
    written only after the watermark has passed the end of the episode.

Missed episodes are often events that arrived later than the 10 s watermark.
Those are dropped on purpose (they stay in Bronze).
"""
import argparse
import glob
import json
import os
from collections import defaultdict
from datetime import datetime, timezone

SESSION_GAP = 12.0
MIN_DURATION = {"temperature_breach": 30.0, "excessive_idling": 30.0, "route_deviation": 0.0}


def parse_time(text):
    t = datetime.fromisoformat(str(text).replace("Z", "+00:00"))
    if t.tzinfo is None:
        t = t.replace(tzinfo=timezone.utc)
    return t.timestamp()


def read_lines_from_files(path):
    files = sorted(f for f in glob.glob(os.path.join(path, "**", "*"), recursive=True)
                   if os.path.isfile(f) and not os.path.basename(f).startswith(".")) if os.path.isdir(path) else [path]
    for name in files:
        with open(name, encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    yield line


def read_topic(bootstrap, topic):
    from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": f"scorer-{os.getpid()}",
                         "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    meta = consumer.list_topics(topic, timeout=15)
    if topic not in meta.topics or meta.topics[topic].error:
        raise SystemExit(f"topic '{topic}' not found on {bootstrap}")
    partitions = list(meta.topics[topic].partitions)
    consumer.assign([TopicPartition(topic, p, OFFSET_BEGINNING) for p in partitions])
    end = {p: consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=15)[1] for p in partitions}
    done = {p for p in partitions if end[p] == 0}
    while len(done) < len(partitions):
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        yield msg.value()
        if msg.offset() >= end[msg.partition()] - 1:
            done.add(msg.partition())
    consumer.close()


def build_episodes(events):
    """events: list of (time, event_id) of one shipment and one type -> list of (start, end)."""
    events.sort()
    episodes, start, last = [], None, None
    for ts, _ in events:
        if start is None or ts - last > SESSION_GAP:
            if start is not None:
                episodes.append((start, last))
            start = ts
        last = ts
    if start is not None:
        episodes.append((start, last))
    return episodes


def overlaps(a_start, a_end, b_start, b_end):
    return a_start <= b_end + SESSION_GAP and b_start <= a_end + SESSION_GAP


def score(truth, telemetry, alerts, grace):
    """
    truth:     {event_id: anomaly_type} for labelled anomaly readings
    telemetry: {event_id: (time, shipment_id)} for readings that reached Kafka
    alerts:    list of dicts
    """
    newest = max((t for t, _ in telemetry.values()), default=0.0)
    cutoff = newest - grace

    seen = defaultdict(list)                       # (shipment, type) -> [(time, event_id)]
    for event_id, anomaly_type in truth.items():
        if event_id in telemetry:
            ts, shipment = telemetry[event_id]
            seen[(shipment, anomaly_type)].append((ts, event_id))

    expected = defaultdict(list)                   # type -> [(shipment, start, end)]
    for (shipment, anomaly_type), events in seen.items():
        if anomaly_type == "overspeed":
            for ts, event_id in events:
                if ts <= cutoff:
                    expected["overspeed"].append((shipment, ts, ts, event_id))
            continue
        for start, end in build_episodes(events):
            if end <= cutoff and end - start >= MIN_DURATION.get(anomaly_type, 0.0):
                expected[anomaly_type].append((shipment, start, end, None))

    true_events = defaultdict(list)                # (shipment, type) -> [(start, end)] of every labelled episode
    for (shipment, anomaly_type), events in seen.items():
        true_events[(shipment, anomaly_type)] = build_episodes(list(events))

    by_type = defaultdict(list)
    for a in alerts:
        by_type[a["anomaly_type"]].append(a)

    rows, misses = [], []
    for anomaly_type in sorted(set(expected) | set(by_type)):
        exp = expected.get(anomaly_type, [])
        got = by_type.get(anomaly_type, [])
        caught, missed = 0, []
        for shipment, start, end, event_id in exp:
            if anomaly_type == "overspeed":
                hit = any(a["event_id"] == event_id for a in got)
            else:
                hit = any(a["shipment_id"] == shipment and
                          overlaps(start, end, parse_time(a["start_time"]), parse_time(a["end_time"])) for a in got)
            if hit:
                caught += 1
            else:
                missed.append((anomaly_type, shipment, start, end))
        false_pos = 0
        for a in got:
            if parse_time(a["start_time"]) > cutoff:
                continue
            if anomaly_type == "overspeed":
                ok = truth.get(a["event_id"]) == "overspeed"
            else:
                ok = any(overlaps(s, e, parse_time(a["start_time"]), parse_time(a["end_time"]))
                         for s, e in true_events.get((a["shipment_id"], anomaly_type), []))
            if not ok:
                false_pos += 1
        precision = caught / (caught + false_pos) if caught + false_pos else 1.0
        recall = caught / len(exp) if exp else 1.0
        rows.append((anomaly_type, len(exp), caught, false_pos, len(exp) - caught, precision, recall))
        misses.extend(missed)
    return rows, misses


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--truth", default="data/ground_truth.jsonl")
    p.add_argument("--telemetry-topic", default="vehicle-telemetry")
    p.add_argument("--alerts-topic", default="alerts")
    p.add_argument("--telemetry-file", help="read telemetry from a file instead of Kafka")
    p.add_argument("--alerts-file", help="read alerts from a file/folder instead of Kafka")
    p.add_argument("--grace-seconds", type=float, default=30.0)
    p.add_argument("--show-misses", type=int, default=0, help="print the first N missed episodes")
    a = p.parse_args()

    truth = {}
    with open(a.truth, encoding="utf-8") as f:
        for line in f:
            r = json.loads(line)
            if r.get("is_anomaly"):
                truth[r["event_id"]] = r["anomaly_type"]

    tel_lines = read_lines_from_files(a.telemetry_file) if a.telemetry_file else read_topic(a.bootstrap, a.telemetry_topic)
    telemetry = {}
    for raw in tel_lines:
        e = json.loads(raw)
        telemetry[e["event_id"]] = (parse_time(e["event_time_utc"]), e["shipment_id"])

    alert_lines = read_lines_from_files(a.alerts_file) if a.alerts_file else read_topic(a.bootstrap, a.alerts_topic)
    alerts = {}
    for raw in alert_lines:
        r = json.loads(raw)
        alerts[r["alert_id"]] = r                  # same alert twice (restart) counts once

    rows, misses = score(truth, telemetry, list(alerts.values()), a.grace_seconds)
    print(f"telemetry events seen: {len(telemetry):,} | labelled anomaly readings: {len(truth):,} | alerts: {len(alerts):,}\n")
    print(f"{'anomaly_type':<22}{'expected':>9}{'caught':>8}{'false+':>8}{'missed':>8}{'precision':>11}{'recall':>8}")
    for t, exp, tp, fp, fn, pr, rc in rows:
        print(f"{t:<22}{exp:>9}{tp:>8}{fp:>8}{fn:>8}{pr:>11.3f}{rc:>8.3f}")
    for kind, shipment, start, end in misses[:a.show_misses]:
        print(f"  missed {kind} {shipment} {datetime.fromtimestamp(start, timezone.utc):%H:%M:%S} -> "
              f"{datetime.fromtimestamp(end, timezone.utc):%H:%M:%S} ({end - start:.0f} s)")


if __name__ == "__main__":
    main()
