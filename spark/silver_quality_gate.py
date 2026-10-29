from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

spark = (
    SparkSession.builder
    .appName("SilverQualityGate")
    .getOrCreate()
)

try:
    telemetry = spark.table("lake.silver.telemetry")

    metrics = telemetry.agg(
        count("*").alias("total_rows"),
        count(when(col("event_id").isNull(), 1)).alias("null_event_id"),
        count(when(col("event_ts").isNull(), 1)).alias("null_event_ts"),
        count(when(col("shipment_id").isNull(), 1)).alias("null_shipment_id"),
        count(when(col("vehicle_id").isNull(), 1)).alias("null_vehicle_id"),
    ).first()

    print("Silver quality-gate results:")
    print(f"Total rows: {metrics['total_rows']}")
    print(f"Null event_id: {metrics['null_event_id']}")
    print(f"Null event_ts: {metrics['null_event_ts']}")
    print(f"Null shipment_id: {metrics['null_shipment_id']}")
    print(f"Null vehicle_id: {metrics['null_vehicle_id']}")

    failures = []

    if metrics["total_rows"] == 0:
        failures.append("Silver telemetry is empty")

    for field in (
        "null_event_id",
        "null_event_ts",
        "null_shipment_id",
        "null_vehicle_id",
    ):
        if metrics[field] > 0:
            failures.append(f"{field} must be zero")

    if failures:
        raise RuntimeError("Silver quality gate FAILED: " + "; ".join(failures))

    print("Silver quality gate PASSED.")

finally:
    spark.stop()