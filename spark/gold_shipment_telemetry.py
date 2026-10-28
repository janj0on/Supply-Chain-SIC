from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def main():
    spark = (
        SparkSession.builder
        .appName("SupplyChain-Gold-Shipment-Telemetry")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:
        raw = spark.table("lake.silver.telemetry")

        event_window = Window.partitionBy("event_id").orderBy(
            F.col("ingest_ts").desc_nulls_last(),
            F.col("kafka_offset").desc_nulls_last()
        )

        telemetry = (
            raw.withColumn("_rn", F.row_number().over(event_window))
               .filter(F.col("_rn") == 1)
               .drop("_rn")
        )

        gold = (
            telemetry
            .groupBy(
                "dt",
                "shipment_id",
                "order_id",
                "product_id",
                "origin_warehouse_id",
                "destination_warehouse_id",
                "shipment_status"
            )
            .agg(
                F.count("*").alias("unique_event_count"),
                F.countDistinct("vehicle_id").alias("vehicle_count"),
                F.round(F.avg("route_progress_pct"), 2).alias("avg_route_progress_pct"),
                F.round(F.max("route_progress_pct"), 2).alias("max_route_progress_pct"),
                F.round(F.avg("speed_kmh"), 2).alias("avg_speed_kmh"),
                F.round(F.max("speed_kmh"), 2).alias("max_speed_kmh"),
                F.round(F.avg("fuel_level_pct"), 2).alias("avg_fuel_level_pct"),
                F.round(F.min("fuel_level_pct"), 2).alias("min_fuel_level_pct"),
                F.round(F.avg("fuel_consumption_l_per_100km"), 2).alias("avg_fuel_consumption_l_per_100km"),
                F.round(F.avg("obd_engine_load_pct"), 2).alias("avg_engine_load_pct"),
                F.round(F.avg("obd_coolant_temp_c"), 2).alias("avg_coolant_temp_c"),
                F.round(F.avg("reefer_cargo_temp_c"), 2).alias("avg_cargo_temp_c"),
                F.round(F.avg("humidity_pct"), 2).alias("avg_humidity_pct"),
                F.sum(F.when(F.col("door_open") == True, 1).otherwise(0)).alias("door_open_event_count"),
                F.sum(F.when(F.col("reefer_on") == True, 1).otherwise(0)).alias("reefer_on_event_count"),
                F.sum(F.when(F.col("temperature_breach_injected") == True, 1).otherwise(0)).alias("temperature_breach_event_count"),
                F.sum(F.when(F.col("obd_fault_code").isNotNull(), 1).otherwise(0)).alias("engine_fault_event_count"),
                F.max("event_ts").alias("last_event_ts")
            )
            .withColumn("gold_updated_at", F.current_timestamp())
        )

        (
            gold.writeTo("lake.gold.shipment_telemetry_performance")
                .using("iceberg")
                .partitionedBy("dt")
                .createOrReplace()
        )

        print("Created lake.gold.shipment_telemetry_performance")
        spark.table("lake.gold.shipment_telemetry_performance").show(20, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
