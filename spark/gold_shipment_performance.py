from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    spark = (
        SparkSession.builder
        .appName("SupplyChain-Gold-Shipment-Performance")
        .getOrCreate()
    )

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:
        # Read the existing cleaned Silver Iceberg table.
        telemetry = spark.table("lake.silver.telemetry")

        # Build daily shipment-performance metrics.
        gold = (
            telemetry
            .groupBy(
                "dt",
                "origin_warehouse_id",
                "destination_warehouse_id",
                "product_id",
                "shipment_status",
            )
            .agg(
                F.count("*").alias("telemetry_event_count"),
                F.countDistinct("shipment_id").alias("shipment_count"),
                F.countDistinct("vehicle_id").alias("vehicle_count"),
                F.round(F.avg("route_progress_pct"), 2)
                    .alias("avg_route_progress_pct"),
                F.round(F.avg("speed_kmh"), 2).alias("avg_speed_kmh"),
                F.round(F.avg("fuel_level_pct"), 2)
                    .alias("avg_fuel_level_pct"),
                F.round(F.avg("route_distance_m"), 2)
                    .alias("avg_route_distance_m"),
                F.sum(
                    F.when(F.col("temperature_breach_injected") == True, 1)
                     .otherwise(0)
                ).alias("temperature_breach_event_count"),
                F.max("event_ts").alias("last_event_ts"),
            )
            .withColumn("gold_updated_at", F.current_timestamp())
        )

        # Write the Gold dataset as an Iceberg table.
        # Re-running this job replaces the Gold table contents, avoiding
        # duplicate daily aggregates on scheduled runs.
        (
            gold.writeTo("lake.gold.daily_shipment_performance")
            .using("iceberg")
            .partitionedBy("dt")
            .createOrReplace()
        )

        print("Gold table created successfully: lake.gold.daily_shipment_performance")
        spark.table("lake.gold.daily_shipment_performance").show(20, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
