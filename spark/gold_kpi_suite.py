from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def write_gold(df, table_name, partition_col="dt"):
    writer = df.writeTo(f"lake.gold.{table_name}").using("iceberg")
    if partition_col and partition_col in df.columns:
        writer = writer.partitionedBy(partition_col)
    writer.createOrReplace()
    print(f"Created/updated lake.gold.{table_name}")


def main():
    spark = (
        SparkSession.builder
        .appName("SupplyChain-Gold-KPI-Suite")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:
        telemetry_raw = spark.table("lake.silver.telemetry")
        alerts_raw = spark.table("lake.silver.alerts")
        quarantine = spark.table("lake.silver.quarantine")

        # Keep the latest-ingested row for each event_id.
        # Silver remains unchanged.
        event_window = Window.partitionBy("event_id").orderBy(
            F.col("ingest_ts").desc_nulls_last(),
            F.col("kafka_offset").desc_nulls_last()
        )
        telemetry = (
            telemetry_raw
            .withColumn("_rn", F.row_number().over(event_window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )

        alerts = alerts_raw.dropDuplicates(["alert_id"])

        # 1. Shipment-level telemetry KPIs
        shipment = (
            telemetry
            .groupBy("shipment_id", "order_id", "product_id",
                     "origin_warehouse_id", "destination_warehouse_id",
                     "shipment_status", "dt")
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
        write_gold(shipment, "shipment_telemetry_performance")

        # 2. Vehicle-level operational KPIs
        vehicle = (
            telemetry
            .groupBy("vehicle_id", "dt")
            .agg(
                F.count("*").alias("unique_event_count"),
                F.countDistinct("shipment_id").alias("shipment_count"),
                F.round(F.avg("speed_kmh"), 2).alias("avg_speed_kmh"),
                F.round(F.max("speed_kmh"), 2).alias("max_speed_kmh"),
                F.round(F.avg("engine_rpm"), 2).alias("avg_engine_rpm"),
                F.round(F.avg("obd_engine_load_pct"), 2).alias("avg_engine_load_pct"),
                F.round(F.avg("obd_coolant_temp_c"), 2).alias("avg_coolant_temp_c"),
                F.round(F.avg("fuel_level_pct"), 2).alias("avg_fuel_level_pct"),
                F.round(F.avg("fuel_consumption_l_per_100km"), 2).alias("avg_fuel_consumption_l_per_100km"),
                F.sum(F.when(F.col("obd_fault_code").isNotNull(), 1).otherwise(0)).alias("engine_fault_event_count"),
                F.sum(F.when(F.col("door_open") == True, 1).otherwise(0)).alias("door_open_event_count"),
                F.sum(F.when(F.col("temperature_breach_injected") == True, 1).otherwise(0)).alias("temperature_breach_event_count"),
                F.max("event_ts").alias("last_event_ts")
            )
            .withColumn("gold_updated_at", F.current_timestamp())
        )
        write_gold(vehicle, "vehicle_performance")

        # 3. Daily anomaly KPIs
        anomaly = (
            alerts
            .groupBy("dt", "anomaly_type")
            .agg(
                F.count("*").alias("alert_count"),
                F.countDistinct("shipment_id").alias("affected_shipment_count"),
                F.countDistinct("vehicle_id").alias("affected_vehicle_count"),
                F.round(F.avg("duration_s"), 2).alias("avg_duration_s"),
                F.max("duration_s").alias("max_duration_s"),
                F.round(F.avg("observed_value"), 2).alias("avg_observed_value"),
                F.round(F.avg("threshold_value"), 2).alias("avg_threshold_value"),
                F.min("event_time").alias("first_alert_ts"),
                F.max("event_time").alias("last_alert_ts")
            )
            .withColumn("gold_updated_at", F.current_timestamp())
        )
        write_gold(anomaly, "daily_anomaly_performance")

        # 4. Route-level KPIs
        route = (
            telemetry
            .groupBy("dt", "origin_warehouse_id", "destination_warehouse_id")
            .agg(
                F.count("*").alias("unique_event_count"),
                F.countDistinct("shipment_id").alias("shipment_count"),
                F.countDistinct("vehicle_id").alias("vehicle_count"),
                F.round(F.avg("route_progress_pct"), 2).alias("avg_route_progress_pct"),
                F.round(F.max("route_progress_pct"), 2).alias("max_route_progress_pct"),
                F.round(F.avg("speed_kmh"), 2).alias("avg_speed_kmh"),
                F.round(F.max("speed_kmh"), 2).alias("max_speed_kmh"),
                F.round(F.avg("route_distance_m"), 2).alias("avg_route_distance_m"),
                F.round(F.avg("route_estimated_duration_s"), 2).alias("avg_estimated_duration_s"),
                F.sum(F.when(F.col("temperature_breach_injected") == True, 1).otherwise(0)).alias("temperature_breach_event_count")
            )
            .withColumn("gold_updated_at", F.current_timestamp())
        )
        write_gold(route, "route_performance")

        # 5. Product-level shipment KPIs
        product = (
            telemetry
            .groupBy("dt", "product_id")
            .agg(
                F.count("*").alias("unique_event_count"),
                F.countDistinct("shipment_id").alias("shipment_count"),
                F.countDistinct("vehicle_id").alias("vehicle_count"),
                F.round(F.avg("route_progress_pct"), 2).alias("avg_route_progress_pct"),
                F.round(F.avg("speed_kmh"), 2).alias("avg_speed_kmh"),
                F.round(F.avg("reefer_cargo_temp_c"), 2).alias("avg_cargo_temp_c"),
                F.round(F.min("reefer_cargo_temp_c"), 2).alias("min_cargo_temp_c"),
                F.round(F.max("reefer_cargo_temp_c"), 2).alias("max_cargo_temp_c"),
                F.round(F.avg("humidity_pct"), 2).alias("avg_humidity_pct"),
                F.sum(F.when(F.col("temperature_breach_injected") == True, 1).otherwise(0)).alias("temperature_breach_event_count"),
                F.sum(F.when(F.col("door_open") == True, 1).otherwise(0)).alias("door_open_event_count")
            )
            .withColumn("gold_updated_at", F.current_timestamp())
        )
        write_gold(product, "product_shipment_performance")

        # 6. Data quality metrics: telemetry duplicates, nulls and quarantine.
        telemetry_count = telemetry_raw.count()
        distinct_events = telemetry_raw.select("event_id").distinct().count()
        duplicate_rows = telemetry_count - distinct_events

        def metric_row(name, dimension, value, dt):
            return (name, dimension, int(value), dt)

        dates = [
            r["dt"] for r in telemetry_raw.select("dt").distinct().collect()
        ]
        dt_value = max(dates) if dates else None

        quality_rows = [
            metric_row("telemetry_rows", "all", telemetry_count, dt_value),
            metric_row("distinct_event_ids", "all", distinct_events, dt_value),
            metric_row("repeated_event_rows", "all", duplicate_rows, dt_value),
            metric_row("null_event_id_rows", "all",
                       telemetry_raw.filter(F.col("event_id").isNull()).count(), dt_value),
            metric_row("null_event_timestamp_rows", "all",
                       telemetry_raw.filter(F.col("event_ts").isNull()).count(), dt_value),
            metric_row("null_shipment_id_rows", "all",
                       telemetry_raw.filter(F.col("shipment_id").isNull()).count(), dt_value),
            metric_row("null_vehicle_id_rows", "all",
                       telemetry_raw.filter(F.col("vehicle_id").isNull()).count(), dt_value),
        ]

        quality_schema = ["metric_name", "dimension", "metric_value", "dt"]
        quality = spark.createDataFrame(quality_rows, quality_schema)

        quarantine_metrics = (
            quarantine
            .groupBy("dt", "reason", "source_topic")
            .agg(F.count("*").alias("metric_value"))
            .select(
                F.lit("quarantined_records").alias("metric_name"),
                F.concat_ws(" | ", F.col("reason"), F.col("source_topic")).alias("dimension"),
                F.col("metric_value").cast("long"),
                F.col("dt")
            )
        )
        quality = quality.unionByName(quarantine_metrics, allowMissingColumns=True)
        quality = quality.withColumn("gold_updated_at", F.current_timestamp())
        write_gold(quality, "data_quality_metrics")

        print("All six additional Gold KPI tables were created successfully.")
        for table in [
            "shipment_telemetry_performance",
            "vehicle_performance",
            "daily_anomaly_performance",
            "route_performance",
            "product_shipment_performance",
            "data_quality_metrics",
        ]:
            print(f"--- lake.gold.{table} ---")
            spark.table(f"lake.gold.{table}").show(10, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
