from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

def main():
    spark = SparkSession.builder.appName("SupplyChain-Gold-Product-Performance").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    try:
        raw = spark.table("lake.silver.telemetry")
        w = Window.partitionBy("event_id").orderBy(
            F.col("ingest_ts").desc_nulls_last(),
            F.col("kafka_offset").desc_nulls_last()
        )
        telemetry = raw.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

        product = telemetry.groupBy("dt", "product_id").agg(
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
        ).withColumn("gold_updated_at", F.current_timestamp())

        product.writeTo("lake.gold.product_shipment_performance").using("iceberg").partitionedBy("dt").createOrReplace()
        print("Created lake.gold.product_shipment_performance")
        spark.table("lake.gold.product_shipment_performance").show(20, truncate=False)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
