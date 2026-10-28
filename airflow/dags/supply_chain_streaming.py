from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="supply_chain_streaming",
    description="Start and check the persistent Kafka and Flink streaming pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["supply-chain", "kafka", "flink", "ingestion"],
) as dag:

    start_streaming = BashOperator(
        task_id="start_streaming",
        bash_command=r"""
set -e

if docker inspect supply-chain-ingestion >/dev/null 2>&1; then
    docker start supply-chain-ingestion
else
    docker run -d \
        --name supply-chain-ingestion \
        --restart unless-stopped \
        --network supply-chain-sic_default \
        -v /run/host-services/docker.proxy.sock:/var/run/docker.sock \
        -v /run/desktop/mnt/host/c/Users/Salma/Supply-Chain-SIC:/app \
        -w /app \
        -e BOOTSTRAP=kafka:29092 \
        supply-chain-streaming:latest \
        python run_all.py --keep-flink
fi

echo "Streaming container start requested."
""",
    )

    check_streaming = BashOperator(
        task_id="check_streaming",
        bash_command="""
set -e
docker inspect supply-chain-ingestion >/dev/null
docker ps --filter name=supply-chain-ingestion --format 'Container: {{.Names}} | Status: {{.Status}}'
""",
    )

    start_streaming >> check_streaming
