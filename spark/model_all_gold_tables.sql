-- Complete dimensional model for all 8 existing Gold KPI tables.
-- Safe approach: creates/replaces NEW model tables only.
-- Existing KPI tables and existing fact_telemetry/dim_vehicle/dim_shipment/dim_product/dim_route remain untouched.
-- Run with the Iceberg Spark SQL command documented in the project notes.

CREATE OR REPLACE TABLE lake.gold.dim_date_model USING iceberg AS
WITH all_dates AS (
    SELECT to_date(dt) AS full_date FROM lake.gold.daily_shipment_performance WHERE dt IS NOT NULL
    UNION
    SELECT to_date(dt) FROM lake.gold.shipment_telemetry_performance WHERE dt IS NOT NULL
    UNION
    SELECT to_date(dt) FROM lake.gold.vehicle_performance WHERE dt IS NOT NULL
    UNION
    SELECT to_date(dt) FROM lake.gold.daily_anomaly_performance WHERE dt IS NOT NULL
    UNION
    SELECT to_date(dt) FROM lake.gold.route_performance WHERE dt IS NOT NULL
    UNION
    SELECT to_date(dt) FROM lake.gold.product_shipment_performance WHERE dt IS NOT NULL
    UNION
    SELECT to_date(dt) FROM lake.gold.data_quality_metrics WHERE dt IS NOT NULL
    UNION
    SELECT to_date(dt) FROM lake.gold.quarantine_quality_metrics WHERE dt IS NOT NULL
    UNION
    SELECT to_date(event_ts) FROM lake.silver.telemetry WHERE event_ts IS NOT NULL
)
SELECT
    CAST(date_format(full_date, 'yyyyMMdd') AS INT) AS date_key,
    full_date,
    year(full_date) AS year,
    quarter(full_date) AS quarter,
    month(full_date) AS month,
    date_format(full_date, 'MMMM') AS month_name,
    dayofmonth(full_date) AS day_of_month,
    dayofweek(full_date) AS day_of_week
FROM all_dates
WHERE full_date IS NOT NULL;

CREATE OR REPLACE TABLE lake.gold.dim_anomaly_model USING iceberg AS
SELECT DISTINCT anomaly_type
FROM lake.gold.daily_anomaly_performance
WHERE anomaly_type IS NOT NULL;

CREATE OR REPLACE TABLE lake.gold.dim_quarantine_reason_model USING iceberg AS
SELECT DISTINCT reason, source_topic
FROM lake.gold.quarantine_quality_metrics
WHERE reason IS NOT NULL OR source_topic IS NOT NULL;

CREATE OR REPLACE TABLE lake.gold.fact_daily_shipment_performance USING iceberg AS
SELECT
    CAST(date_format(to_date(dt), 'yyyyMMdd') AS INT) AS date_key,
    sha2(concat_ws('||', coalesce(origin_warehouse_id, ''), coalesce(destination_warehouse_id, '')), 256) AS route_key,
    origin_warehouse_id, destination_warehouse_id, product_id, shipment_status,
    telemetry_event_count, shipment_count, vehicle_count,
    avg_route_progress_pct, avg_speed_kmh, avg_fuel_level_pct,
    avg_route_distance_m, temperature_breach_event_count,
    last_event_ts, gold_updated_at
FROM lake.gold.daily_shipment_performance;

CREATE OR REPLACE TABLE lake.gold.fact_shipment_telemetry_performance USING iceberg AS
SELECT
    CAST(date_format(to_date(dt), 'yyyyMMdd') AS INT) AS date_key,
    shipment_id, order_id, product_id,
    sha2(concat_ws('||', coalesce(origin_warehouse_id, ''), coalesce(destination_warehouse_id, '')), 256) AS route_key,
    origin_warehouse_id, destination_warehouse_id, shipment_status,
    unique_event_count, vehicle_count, avg_route_progress_pct, max_route_progress_pct,
    avg_speed_kmh, max_speed_kmh, avg_fuel_level_pct, min_fuel_level_pct,
    avg_fuel_consumption_l_per_100km, avg_engine_load_pct, avg_coolant_temp_c,
    avg_cargo_temp_c, avg_humidity_pct, door_open_event_count, reefer_on_event_count,
    temperature_breach_event_count, engine_fault_event_count, last_event_ts, gold_updated_at
FROM lake.gold.shipment_telemetry_performance;

CREATE OR REPLACE TABLE lake.gold.fact_vehicle_performance USING iceberg AS
SELECT
    vehicle_id,
    CAST(date_format(to_date(dt), 'yyyyMMdd') AS INT) AS date_key,
    unique_event_count, shipment_count, avg_speed_kmh, max_speed_kmh,
    avg_engine_rpm, avg_engine_load_pct, avg_coolant_temp_c, avg_fuel_level_pct,
    avg_fuel_consumption_l_per_100km, engine_fault_event_count, door_open_event_count,
    temperature_breach_event_count, last_event_ts, gold_updated_at
FROM lake.gold.vehicle_performance;

CREATE OR REPLACE TABLE lake.gold.fact_daily_anomaly_performance USING iceberg AS
SELECT
    CAST(date_format(to_date(dt), 'yyyyMMdd') AS INT) AS date_key,
    anomaly_type,
    alert_count, affected_shipment_count, affected_vehicle_count,
    avg_duration_s, max_duration_s, avg_observed_value, avg_threshold_value,
    first_alert_ts, last_alert_ts, gold_updated_at
FROM lake.gold.daily_anomaly_performance;

CREATE OR REPLACE TABLE lake.gold.fact_route_performance USING iceberg AS
SELECT
    CAST(date_format(to_date(dt), 'yyyyMMdd') AS INT) AS date_key,
    sha2(concat_ws('||', coalesce(origin_warehouse_id, ''), coalesce(destination_warehouse_id, '')), 256) AS route_key,
    origin_warehouse_id, destination_warehouse_id,
    unique_event_count, shipment_count, vehicle_count,
    avg_route_progress_pct, max_route_progress_pct, avg_speed_kmh, max_speed_kmh,
    avg_route_distance_m, avg_estimated_duration_s,
    temperature_breach_event_count, gold_updated_at
FROM lake.gold.route_performance;

CREATE OR REPLACE TABLE lake.gold.fact_product_shipment_performance USING iceberg AS
SELECT
    CAST(date_format(to_date(dt), 'yyyyMMdd') AS INT) AS date_key,
    product_id, unique_event_count, shipment_count, vehicle_count,
    avg_route_progress_pct, avg_speed_kmh, avg_cargo_temp_c, min_cargo_temp_c,
    max_cargo_temp_c, avg_humidity_pct, temperature_breach_event_count,
    door_open_event_count, gold_updated_at
FROM lake.gold.product_shipment_performance;

CREATE OR REPLACE TABLE lake.gold.fact_data_quality_metrics USING iceberg AS
SELECT
    CAST(date_format(to_date(dt), 'yyyyMMdd') AS INT) AS date_key,
    raw_telemetry_rows, distinct_event_ids, repeated_event_rows,
    null_event_id_rows, null_event_timestamp_rows, null_shipment_id_rows,
    null_vehicle_id_rows, gold_updated_at
FROM lake.gold.data_quality_metrics;

CREATE OR REPLACE TABLE lake.gold.fact_quarantine_quality_metrics USING iceberg AS
SELECT
    CAST(date_format(to_date(dt), 'yyyyMMdd') AS INT) AS date_key,
    reason, source_topic, quarantined_record_count, gold_updated_at
FROM lake.gold.quarantine_quality_metrics;
