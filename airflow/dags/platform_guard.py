"""
DAG A - platform_guard (every 5 minutes)

It does NOT run the streaming pipeline. Docker Compose runs it (generator, bridge, dispatcher, Kafka,
HDFS, Flink, exporter, Prometheus, Grafana). This DAG watches it, and submits a Flink job when
that job is missing.

    check_infra -> check_topics -> ensure_flink_jobs -> check_apps -> check_monitoring
"""
from __future__ import annotations

import time
from datetime import timedelta

import docker
import pendulum
from airflow.decorators import dag, task

from sc_common import (EXPECTED_TOPICS, FLINK_JOBS, INFRA_CONTAINERS, STREAMING_APPS,
                       flink_jobs, http_get, run_in)


@dag(
    dag_id="platform_guard",
    description="DAG A: watches Kafka, Flink, the streaming apps and the exporter",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=30)},
    tags=["supply-chain", "streaming", "guard"],
)
def platform_guard():

    @task
    def check_infra():
        client = docker.from_env()
        bad = []
        for name in INFRA_CONTAINERS:
            status = client.containers.get(name).status
            print(f"{name}: {status}")
            if status != "running":
                bad.append(f"{name} ({status})")
        if bad:
            raise RuntimeError("containers not running: " + ", ".join(bad))
        run_in("kafka", ["/opt/kafka/bin/kafka-broker-api-versions.sh", "--bootstrap-server", "kafka:29092"])

    @task(retries=6, retry_delay=timedelta(seconds=30))   # on the first start the bridge needs a moment to create them
    def check_topics():
        _, out = run_in("kafka", ["/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "kafka:29092", "--list"])
        existing = set(out.split())
        missing = [t for t in EXPECTED_TOPICS if t not in existing]
        if missing:
            raise RuntimeError("missing Kafka topics: " + ", ".join(missing))

    @task(execution_timeout=timedelta(minutes=10))
    def ensure_flink_jobs():
        running = [j["name"] for j in flink_jobs() if j["state"] == "RUNNING"]
        print("RUNNING Flink jobs:", running)
        missing = [name for name in FLINK_JOBS if not any(name in r.lower() for r in running)]
        if not missing:
            print("All 4 Flink jobs are running.")
            return
        if len(running) >= len(FLINK_JOBS):
            # Safety stop: 4 jobs run but the names did not match. Never start duplicates.
            raise RuntimeError(f"{len(running)} jobs run but {missing} were not found by name. "
                               f"Check the names at http://localhost:8082 (FLINK_JOBS in sc_common.py).")
        for name in missing:
            print(f"Submitting {name} ({FLINK_JOBS[name]}) ...")
            run_in("flink-jobmanager", ["./bin/sql-client.sh", "-f", f"/opt/flink/sql/{FLINK_JOBS[name]}"],
                   workdir="/opt/flink")
            for _ in range(30):
                if any(name in j["name"].lower() and j["state"] == "RUNNING" for j in flink_jobs()):
                    break
                time.sleep(2)
            else:
                raise RuntimeError(f"Flink job {name} was submitted but is not RUNNING after 60 s")

    @task
    def check_apps():
        client = docker.from_env()
        bad = []
        for name in STREAMING_APPS:
            c = client.containers.get(name)
            if c.status != "running":
                c.start()
                time.sleep(5)
                c.reload()
            if c.status != "running":
                bad.append(f"{name} ({c.status})")
        if bad:
            raise RuntimeError("streaming apps not running: " + ", ".join(bad))

    @task
    def check_monitoring():
        targets = {
            "alert-exporter": "http://alert-exporter:8000/metrics",
            "prometheus": "http://prometheus:9090/-/ready",
            "grafana": "http://grafana:3000/api/health",
        }
        bad = []
        for name, url in targets.items():
            try:
                status, _ = http_get(url)
                if status != 200:
                    bad.append(f"{name} ({status})")
            except Exception as exc:
                bad.append(f"{name} ({exc})")
        if bad:
            raise RuntimeError("monitoring down: " + ", ".join(bad))

    check_infra() >> check_topics() >> ensure_flink_jobs() >> check_apps() >> check_monitoring()


platform_guard()
