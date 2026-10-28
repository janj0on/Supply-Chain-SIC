from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = SparkSession.builder.appName("SupplyChain-Gold-Anomaly-Performance").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    try:
        alerts = spark.table("lake.silver.alerts").dropDuplicates(["alert_id"])

        anomaly = alerts.groupBy("dt", "anomaly_type").agg(
            F.count("*").alias("alert_count"),
            F.countDistinct("shipment_id").alias("affected_shipment_count"),
            F.countDistinct("vehicle_id").alias("affected_vehicle_count"),
            F.round(F.avg("duration_s"), 2).alias("avg_duration_s"),
            F.max("duration_s").alias("max_duration_s"),
            F.round(F.avg("observed_value"), 2).alias("avg_observed_value"),
            F.round(F.avg("threshold_value"), 2).alias("avg_threshold_value"),
            F.min("event_time").alias("first_alert_ts"),
            F.max("event_time").alias("last_alert_ts")
        ).withColumn("gold_updated_at", F.current_timestamp())

        anomaly.writeTo("lake.gold.daily_anomaly_performance").using("iceberg").partitionedBy("dt").createOrReplace()
        print("Created lake.gold.daily_anomaly_performance")
        spark.table("lake.gold.daily_anomaly_performance").show(20, truncate=False)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
