from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="supply_chain_gold_daily",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["supply-chain", "gold", "spark"],
) as dag:

    build_gold_kpis = BashOperator(
        task_id="build_gold_kpis",
        bash_command=(
            "docker exec supply-chain-spark "
            "/opt/spark/bin/spark-submit "
            "--master 'local[*]' "
            "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
            "--conf spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog "
            "--conf spark.sql.catalog.lake.type=hadoop "
            "--conf spark.sql.catalog.lake.warehouse=hdfs://namenode:8020/warehouse "
            "/opt/spark-jobs/gold_kpi_suite.py"
        ),
    )