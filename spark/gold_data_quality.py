from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = SparkSession.builder.appName("SupplyChain-Gold-Data-Quality").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    try:
        telemetry = spark.table("lake.silver.telemetry")

        quality = telemetry.groupBy("dt").agg(
            F.count("*").alias("raw_telemetry_rows"),
            F.countDistinct("event_id").alias("distinct_event_ids"),
            (F.count("*") - F.countDistinct("event_id")).alias("repeated_event_rows"),
            F.sum(F.when(F.col("event_id").isNull(), 1).otherwise(0)).alias("null_event_id_rows"),
            F.sum(F.when(F.col("event_ts").isNull(), 1).otherwise(0)).alias("null_event_timestamp_rows"),
            F.sum(F.when(F.col("shipment_id").isNull(), 1).otherwise(0)).alias("null_shipment_id_rows"),
            F.sum(F.when(F.col("vehicle_id").isNull(), 1).otherwise(0)).alias("null_vehicle_id_rows")
        ).withColumn("gold_updated_at", F.current_timestamp())

        quality.writeTo("lake.gold.data_quality_metrics").using("iceberg").partitionedBy("dt").createOrReplace()
        print("Created lake.gold.data_quality_metrics")
        spark.table("lake.gold.data_quality_metrics").show(20, truncate=False)

        quarantine = spark.table("lake.silver.quarantine").groupBy("dt", "reason", "source_topic").agg(
            F.count("*").alias("quarantined_record_count")
        ).withColumn("gold_updated_at", F.current_timestamp())

        quarantine.writeTo("lake.gold.quarantine_quality_metrics").using("iceberg").partitionedBy("dt").createOrReplace()
        print("Created lake.gold.quarantine_quality_metrics")
        spark.table("lake.gold.quarantine_quality_metrics").show(20, truncate=False)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
