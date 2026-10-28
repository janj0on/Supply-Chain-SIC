from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

def main():
    spark = SparkSession.builder.appName("SupplyChain-Gold-Route-Performance").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    try:
        raw = spark.table("lake.silver.telemetry")
        w = Window.partitionBy("event_id").orderBy(
            F.col("ingest_ts").desc_nulls_last(),
            F.col("kafka_offset").desc_nulls_last()
        )
        telemetry = raw.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

        route = telemetry.groupBy("dt", "origin_warehouse_id", "destination_warehouse_id").agg(
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
        ).withColumn("gold_updated_at", F.current_timestamp())

        route.writeTo("lake.gold.route_performance").using("iceberg").partitionedBy("dt").createOrReplace()
        print("Created lake.gold.route_performance")
        spark.table("lake.gold.route_performance").show(20, truncate=False)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
