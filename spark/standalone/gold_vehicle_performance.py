from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

def main():
    spark = SparkSession.builder.appName("SupplyChain-Gold-Vehicle-Performance").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    try:
        raw = spark.table("lake.silver.telemetry")
        w = Window.partitionBy("event_id").orderBy(
            F.col("ingest_ts").desc_nulls_last(),
            F.col("kafka_offset").desc_nulls_last()
        )
        telemetry = raw.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

        vehicle = telemetry.groupBy("vehicle_id", "dt").agg(
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
        ).withColumn("gold_updated_at", F.current_timestamp())

        vehicle.writeTo("lake.gold.vehicle_performance").using("iceberg").partitionedBy("dt").createOrReplace()
        print("Created lake.gold.vehicle_performance")
        spark.table("lake.gold.vehicle_performance").show(20, truncate=False)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
