-- =====================================================================
-- 02_silver.sql   Flink job: Kafka -> Silver (Iceberg on HDFS)
--
-- Silver = clean data. Flink does the light cleaning:
--   telemetry : parse to typed columns, drop duplicates (event_id), keep only events
--               that are not later than the watermark, write them in event-time order.
--   alerts    : drop duplicates (alert_id).
--   quarantine: bad rows go here with a reason; the raw record stays in Bronze
--               (kafka_partition + kafka_offset point to it).
--
-- Reasons: unparseable, bad_speed, bad_gps, late_event.
-- late_event = older than the 10 s watermark. Late events are NOT in silver.telemetry.
-- Spark can decide later what to do with them (they are in Bronze and in quarantine).
--
-- NOTE: ORDER BY on event time makes this part run with parallelism 1.
-- That is fine for thousands of events/s. If you outgrow it, remove ORDER BY.
--
-- Run:  docker exec -it flink-jobmanager ./bin/sql-client.sh -f /opt/flink/sql/02_silver.sql
-- =====================================================================

SET 'pipeline.name' = 'silver';
SET 'execution.checkpointing.interval' = '30 s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.min-pause' = '10 s';
SET 'execution.checkpointing.timeout' = '10 min';
SET 'table.exec.source.idle-timeout' = '10 s';
SET 'table.exec.state.ttl' = '1 h';
SET 'table.local-time-zone' = 'UTC';

-- Hadoop catalog: the tables are plain folders on HDFS, no catalog service needed.
--   lake.bronze.raw_events -> hdfs://namenode:8020/warehouse/bronze/raw_events
--   lake.silver.*          -> hdfs://namenode:8020/warehouse/silver/*
CREATE CATALOG lake WITH (
    'type' = 'iceberg',
    'catalog-type' = 'hadoop',
    'warehouse' = 'hdfs://namenode:8020/warehouse',
    'property-version' = '1'
);

CREATE DATABASE IF NOT EXISTS lake.silver;

-- ---------------------------------------------------------------------
-- Silver tables (Iceberg). Partitioned by event date and hour.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lake.silver.telemetry (
    event_id STRING,
    event_ts TIMESTAMP(3),
    vehicle_id STRING,
    device_id STRING,
    shipment_id STRING,
    order_id STRING,
    product_id STRING,
    origin_warehouse_id STRING,
    destination_warehouse_id STRING,
    gps_latitude DOUBLE,
    gps_longitude DOUBLE,
    gps_method STRING,
    speed_kmh DOUBLE,
    heading_degrees INT,
    engine_rpm INT,
    fuel_level_pct DOUBLE,
    fuel_consumption_l_per_100km DOUBLE,
    obd_coolant_temp_c DOUBLE,
    obd_engine_load_pct DOUBLE,
    obd_fault_code STRING,
    reefer_cargo_temp_c DOUBLE,
    reefer_setpoint_c DOUBLE,
    cargo_temp_min_c DOUBLE,
    cargo_temp_max_c DOUBLE,
    humidity_pct DOUBLE,
    reefer_on BOOLEAN,
    door_open BOOLEAN,
    temperature_breach_injected BOOLEAN,
    shipment_status STRING,
    route_progress_pct DOUBLE,
    route_distance_m DOUBLE,
    route_estimated_duration_s DOUBLE,
    kafka_partition INT,
    kafka_offset BIGINT,
    ingest_ts TIMESTAMP(3),
    dt STRING,
    hr STRING
) PARTITIONED BY (dt, hr) WITH (
    'format-version' = '2'
);

CREATE TABLE IF NOT EXISTS lake.silver.alerts (
    alert_id STRING,
    event_id STRING,
    event_time TIMESTAMP(3),
    vehicle_id STRING,
    shipment_id STRING,
    product_id STRING,
    anomaly_type STRING,
    observed_value DOUBLE,
    threshold_value DOUBLE,
    detail STRING,
    start_time TIMESTAMP(3),
    end_time TIMESTAMP(3),
    duration_s INT,
    kafka_partition INT,
    kafka_offset BIGINT,
    ingest_ts TIMESTAMP(3),
    dt STRING
) PARTITIONED BY (dt) WITH (
    'format-version' = '2'
);

CREATE TABLE IF NOT EXISTS lake.silver.quarantine (
    source_topic STRING,
    reason STRING,
    event_id STRING,
    vehicle_id STRING,
    event_time_utc STRING,
    kafka_partition INT,
    kafka_offset BIGINT,
    quarantined_at TIMESTAMP(3),
    dt STRING
) PARTITIONED BY (dt) WITH (
    'format-version' = '2'
);

-- ---------------------------------------------------------------------
-- Kafka sources
-- ---------------------------------------------------------------------
CREATE TEMPORARY TABLE telemetry_src (
    event_id STRING,
    event_time_utc STRING,
    vehicle_id STRING,
    device_id STRING,
    shipment_id STRING,
    order_id STRING,
    product_id STRING,
    origin_warehouse_id STRING,
    destination_warehouse_id STRING,
    gps_latitude DOUBLE,
    gps_longitude DOUBLE,
    gps_method STRING,
    speed_kmh DOUBLE,
    heading_degrees INT,
    engine_rpm INT,
    fuel_level_pct DOUBLE,
    fuel_consumption_l_per_100km DOUBLE,
    obd_coolant_temp_c DOUBLE,
    obd_engine_load_pct DOUBLE,
    obd_fault_code STRING,
    reefer_cargo_temp_c DOUBLE,
    reefer_setpoint_c DOUBLE,
    cargo_temp_min_c DOUBLE,
    cargo_temp_max_c DOUBLE,
    humidity_pct DOUBLE,
    reefer_on BOOLEAN,
    door_open BOOLEAN,
    temperature_breach_injected BOOLEAN,
    shipment_status STRING,
    route_progress_pct DOUBLE,
    route_distance_m DOUBLE,
    route_estimated_duration_s DOUBLE,
    kafka_partition INT METADATA FROM 'partition' VIRTUAL,
    kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL,
    kafka_ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
    event_ts AS TRY_CAST(REPLACE(REPLACE(REPLACE(event_time_utc, 'T', ' '), 'Z', ''), '+00:00', '') AS TIMESTAMP(3)),
    proc_time AS PROCTIME(),
    WATERMARK FOR event_ts AS event_ts - INTERVAL '10' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'vehicle-telemetry',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-silver-telemetry',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

CREATE TEMPORARY TABLE alerts_src (
    alert_id STRING,
    event_id STRING,
    event_time TIMESTAMP(3),
    vehicle_id STRING,
    shipment_id STRING,
    product_id STRING,
    anomaly_type STRING,
    observed_value DOUBLE,
    threshold_value DOUBLE,
    detail STRING,
    start_time TIMESTAMP(3),
    end_time TIMESTAMP(3),
    duration_s INT,
    kafka_partition INT METADATA FROM 'partition' VIRTUAL,
    kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL,
    kafka_ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
    proc_time AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'alerts',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-silver-alerts',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

-- ---------------------------------------------------------------------
-- Telemetry, in this order:
--   1. basic checks (unparseable, bad speed, bad GPS)  -> bad rows go to quarantine
--   2. drop duplicates (keep the first copy of every event_id)
--   3. late check: CURRENT_WATERMARK is the watermark at this point of the stream.
--      Duplicates are removed BEFORE this check, so a late copy of an event that was
--      already written is just a duplicate, not a "late event".
--   4. good rows are written in event-time order
-- ---------------------------------------------------------------------
CREATE TEMPORARY VIEW telemetry_checked AS
SELECT
    *,
    CASE
        WHEN event_id IS NULL OR vehicle_id IS NULL OR shipment_id IS NULL OR event_ts IS NULL
            THEN 'unparseable'
        WHEN speed_kmh IS NULL OR speed_kmh < 0 OR speed_kmh > 250
            THEN 'bad_speed'
        WHEN gps_latitude IS NULL OR gps_longitude IS NULL
             OR gps_latitude NOT BETWEEN -90 AND 90 OR gps_longitude NOT BETWEEN -180 AND 180
            THEN 'bad_gps'
        ELSE CAST(NULL AS STRING)
    END AS bad_reason
FROM telemetry_src;

CREATE TEMPORARY VIEW telemetry_dedup AS
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY proc_time ASC) AS rn
    FROM telemetry_checked
    WHERE bad_reason IS NULL
) WHERE rn = 1;

CREATE TEMPORARY VIEW telemetry_timed AS
SELECT
    *,
    CASE WHEN event_ts <= CURRENT_WATERMARK(event_ts) THEN 'late_event' END AS late_reason
FROM telemetry_dedup;

CREATE TEMPORARY VIEW alerts_dedup AS
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY alert_id ORDER BY proc_time ASC) AS rn
    FROM alerts_src
    WHERE alert_id IS NOT NULL
) WHERE rn = 1;

-- ---------------------------------------------------------------------
-- One job, three outputs
-- ---------------------------------------------------------------------
EXECUTE STATEMENT SET
BEGIN

INSERT INTO lake.silver.telemetry
SELECT
    event_id,
    CAST(event_ts AS TIMESTAMP(3)),
    vehicle_id, device_id, shipment_id, order_id, product_id,
    origin_warehouse_id, destination_warehouse_id,
    gps_latitude, gps_longitude, gps_method, speed_kmh, heading_degrees, engine_rpm,
    fuel_level_pct, fuel_consumption_l_per_100km, obd_coolant_temp_c, obd_engine_load_pct, obd_fault_code,
    reefer_cargo_temp_c, reefer_setpoint_c, cargo_temp_min_c, cargo_temp_max_c, humidity_pct,
    reefer_on, door_open, temperature_breach_injected, shipment_status,
    route_progress_pct, route_distance_m, route_estimated_duration_s,
    kafka_partition, kafka_offset,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)),
    DATE_FORMAT(event_ts, 'yyyy-MM-dd'),
    DATE_FORMAT(event_ts, 'HH')
FROM telemetry_timed
WHERE late_reason IS NULL
ORDER BY event_ts;

INSERT INTO lake.silver.quarantine
SELECT
    CAST('vehicle-telemetry' AS STRING),
    bad_reason,
    event_id,
    vehicle_id,
    event_time_utc,
    kafka_partition,
    kafka_offset,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)),
    DATE_FORMAT(CAST(kafka_ts AS TIMESTAMP(3)), 'yyyy-MM-dd')
FROM telemetry_checked
WHERE bad_reason IS NOT NULL;

INSERT INTO lake.silver.quarantine
SELECT
    CAST('vehicle-telemetry' AS STRING),
    late_reason,
    event_id,
    vehicle_id,
    event_time_utc,
    kafka_partition,
    kafka_offset,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)),
    DATE_FORMAT(CAST(kafka_ts AS TIMESTAMP(3)), 'yyyy-MM-dd')
FROM telemetry_timed
WHERE late_reason IS NOT NULL;

INSERT INTO lake.silver.alerts
SELECT
    alert_id, event_id, event_time, vehicle_id, shipment_id, product_id, anomaly_type,
    observed_value, threshold_value, detail, start_time, end_time, duration_s,
    kafka_partition, kafka_offset,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)),
    DATE_FORMAT(CAST(kafka_ts AS TIMESTAMP(3)), 'yyyy-MM-dd')
FROM alerts_dedup;

END;
