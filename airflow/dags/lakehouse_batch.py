"""
DAG B - lakehouse_batch (every 15 minutes)

Flink wrote Bronze and Silver. This DAG runs your Spark jobs from Silver to Gold.
The Spark files in spark/ are the same ones as before, only the trigger changed.

    wait_for_silver -> init_gold_namespace -> silver_quality_gate -> build_gold_kpis
        -> build_daily_shipment_performance -> build_quality_tables
        -> create_dimensional_model -> model_all_gold_tables

gold_data_quality.py must run before model_all_gold_tables.sql, which reads its tables.
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.sensors.python import PythonSensor

from sc_common import run_in, spark_sql, spark_submit


def silver_ready():
    """True when Silver telemetry already has Parquet files on HDFS."""
    _, out = run_in(
        "namenode",
        [
            "hdfs",
            "dfs",
            "-find",
            "/warehouse/silver/telemetry/data",
            "-name",
            "*.parquet",
        ],
        check=False,
    )

    ready = ".parquet" in out

    print(
        "Silver telemetry parquet files found"
        if ready
        else "Silver telemetry has no parquet files yet"
    )

    return ready


@dag(
    dag_id="lakehouse_batch",
    description="DAG B: Silver -> Gold (Spark / Iceberg on HDFS)",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=30),
    },
    tags=["supply-chain", "gold", "spark"],
)
def lakehouse_batch():

    wait_for_silver = PythonSensor(
        task_id="wait_for_silver",
        python_callable=silver_ready,
        poke_interval=30,
        timeout=15 * 60,
        mode="reschedule",
    )

    @task
    def init_gold_namespace():
        spark_sql("00_init_gold.sql")

    @task
    def silver_quality_gate():
        spark_submit("silver_quality_gate.py")

    @task
    def build_gold_kpis():
        spark_submit("gold_kpi_suite.py")

    @task
    def build_daily_shipment_performance():
        spark_submit("gold_shipment_performance.py")

    @task
    def build_quality_tables():
        spark_submit("gold_data_quality.py")

    @task
    def create_dimensional_model():
        spark_sql("create_dimensional_model.sql")

    @task
    def model_all_gold_tables():
        spark_sql("model_all_gold_tables.sql")

    (
        wait_for_silver
        >> init_gold_namespace()
        >> silver_quality_gate()
        >> build_gold_kpis()
        >> build_daily_shipment_performance()
        >> build_quality_tables()
        >> create_dimensional_model()
        >> model_all_gold_tables()
    )


lakehouse_batch()
