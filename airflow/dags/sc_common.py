"""Helpers shared by both DAGs. Airflow reaches the other containers through the Docker socket."""
import json
import urllib.request

import docker

FLINK_REST = "http://jobmanager:8081"
SPARK_CONTAINER = "supply-chain-spark"

# Flink job name (= pipeline.name inside the SQL file) -> SQL file
FLINK_JOBS = {
    "bronze": "01_bronze.sql",
    "silver": "02_silver.sql",
    "detection": "03_detection.sql",
    "replenishment": "04_replenishment.sql",
}

# Same list as ALL_TOPICS in KafkaProducer.py
EXPECTED_TOPICS = sorted([
    "alerts", "demand-events", "inventory-stock", "new-orders", "producer-dlq", "products",
    "replenishment-triggered", "shipments", "vehicle-status", "vehicle-telemetry",
    "vehicles", "warehouses",
])

INFRA_CONTAINERS = ["kafka", "namenode", "datanode", "flink-jobmanager", "flink-taskmanager", SPARK_CONTAINER]
STREAMING_APPS = ["generator", "bridge", "dispatcher"]


def run_in(container, cmd, workdir=None, check=True):
    """docker exec <container> <cmd>. The output goes into the Airflow task log."""
    c = docker.from_env().containers.get(container)
    code, out = c.exec_run(cmd, workdir=workdir)
    text = out.decode("utf-8", "replace") if out else ""
    print(f"$ [{container}] {' '.join(cmd)}\n{text}")
    if check and code != 0:
        raise RuntimeError(f"command failed in {container} (exit {code}): {' '.join(cmd)}")
    return code, text


def http_get(url, timeout=5):
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return resp.status, resp.read().decode("utf-8", "replace")


def flink_jobs():
    _, body = http_get(f"{FLINK_REST}/jobs/overview")
    return json.loads(body)["jobs"]


def spark_submit(py_file):
    """Same command the old DAG used: spark-submit --master local[*] /opt/spark-jobs/<file>."""
    return run_in(SPARK_CONTAINER, ["/opt/spark/bin/spark-submit", "--master", "local[*]",
                                    f"/opt/spark-jobs/{py_file}"])


def spark_sql(sql_file):
    return run_in(SPARK_CONTAINER, ["/opt/spark/bin/spark-sql", "-f", f"/opt/spark-jobs/{sql_file}"])
