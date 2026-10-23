import csv
import json
import time
import os
from pathlib import Path
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()


producer = Producer({
    "bootstrap.servers": os.getenv("BOOTSTRAP"),
    "client.id": os.getenv("CLIENT_ID_FILE"),
})


DATA_DIR = Path(__file__).resolve().parent / "data"

CSV_TOPICS = {
    "products.csv": "products",
    "vehicles.csv": "vehicles",
    "warehouses.csv": "warehouses",
    "shipments.csv": "shipments",
}

KEY_FIELDS = [
    "product_id",
    "vehicle_id",
    "warehouse_id",
    "shipment_id",
    "id",
]


def send_record(topic, record):
    key = next(
        (record[field] for field in KEY_FIELDS
         if record.get(field)),
        None
    )

    producer.produce(
        topic=topic,
        key=str(key) if key is not None else None,
        value=json.dumps(record, default=str),
    )
    producer.poll(0)


def ingest_csv(filename, topic):
    path = DATA_DIR / filename

    if not path.exists():
        print(f"File not found: {path}")
        return

    count = 0

    with path.open("r", encoding="utf-8-sig",
                   newline="") as file:
        reader = csv.DictReader(file)

        for row in reader:
            send_record(topic, row)
            count += 1

    producer.flush()
    print(f"{filename}: sent {count} records -> {topic}")


def ingest_telemetry():
    path = DATA_DIR / "telemetry.jsonl"

    if not path.exists():
        print(f"File not found: {path}")
        return

    count = 0

    with path.open("r", encoding="utf-8") as file:
        for line in file:
            if not line.strip():
                continue

            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                print("Skipping invalid JSON line")
                continue

            send_record("vehicle-telemetry", event)
            count += 1

            
            time.sleep(0.1)

    producer.flush()
    print(
        f"Telemetry: sent {count} events "
        "-> vehicle-telemetry"
    )


if __name__ == "__main__":
    print("Starting file ingestion...")

    for filename, topic in CSV_TOPICS.items():
        ingest_csv(filename, topic)

    ingest_telemetry()

    print("File ingestion finished.")