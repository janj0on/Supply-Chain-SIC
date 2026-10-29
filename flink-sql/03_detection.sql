-- =====================================================================
-- 03_detection.sql   Flink job: anomaly detection  ->  topic `alerts`
--
-- Reads vehicle-telemetry, writes one alert per anomaly EPISODE.
-- Rules (demo scale; change the numbers here):
--   temperature_breach : fridge outside [min,max] for >= 30 s
--   excessive_idling   : engine on and speed ~0 for >= 30 s
--   route_deviation    : more than 5 km from the corridor line, for >= 3 readings
--   overspeed          : speed above 120 km/h, ONE alert per episode
-- Readings less than 20 s apart belong to the same episode (SESSION gap).
-- An alert is written when the episode is over (watermark passed its end).
--
-- Changes compared with the previous version:
--   * Watermark delay 10 s -> 30 s      (late telemetry is tolerated longer)
--   * Session gap 12 s -> 20 s          (small data gaps no longer split an episode)
--   * route_deviation needs >= 3 bad readings (one GPS glitch no longer alerts)
--   * Readings with impossible GPS coordinates are dropped
--   * overspeed is now one alert per episode (session window), not per reading
--   * Kafka sink is exactly-once (transactions), so restarts do not duplicate alerts
--
-- Trade-offs:
--   * Alerts appear later: about watermark delay + session gap + one checkpoint.
--   * Consumers of `alerts` must read with isolation.level=read_committed,
--     otherwise they can see uncommitted (and later aborted) alerts.
--
-- The warehouse coordinates are a constant lookup (MAP) generated from
-- warehouses.csv. No join, so no second watermark to wait for.
-- If warehouses change, regenerate the MAPs and restart the job.
-- Run:  docker exec -it flink-jobmanager ./bin/sql-client.sh -f /opt/flink/sql/03_detection.sql
--
-- RESTART TIP: restart from a savepoint, not from scratch:
--   ./bin/flink stop --savepointPath hdfs://namenode:8020/savepoints <jobId>
--   then SET 'execution.savepoint.path' = '<savepoint path>'; before the INSERT.
-- =====================================================================

SET 'pipeline.name' = 'detection';
SET 'execution.checkpointing.interval' = '30 s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.min-pause' = '10 s';
SET 'execution.checkpointing.timeout' = '10 min';
SET 'table.exec.source.idle-timeout' = '10 s';
SET 'table.exec.state.ttl' = '6 h';
SET 'table.local-time-zone' = 'UTC';

CREATE TEMPORARY TABLE telemetry (
    event_id STRING,
    event_time_utc STRING,
    vehicle_id STRING,
    shipment_id STRING,
    product_id STRING,
    origin_warehouse_id STRING,
    destination_warehouse_id STRING,
    gps_latitude DOUBLE,
    gps_longitude DOUBLE,
    speed_kmh DOUBLE,
    engine_rpm INT,
    reefer_cargo_temp_c DOUBLE,
    cargo_temp_min_c DOUBLE,
    cargo_temp_max_c DOUBLE,
    event_ts AS TRY_CAST(REPLACE(REPLACE(REPLACE(event_time_utc, 'T', ' '), 'Z', ''), '+00:00', '') AS TIMESTAMP(3)),
    WATERMARK FOR event_ts AS event_ts - INTERVAL '30' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'vehicle-telemetry',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-detection',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

-- Exactly-once Kafka sink: alerts are written inside Kafka transactions that are
-- committed on each checkpoint. The transaction timeout must be larger than the
-- checkpoint interval plus the longest restart time, and not above the broker's
-- transaction.max.timeout.ms (15 min by default).
CREATE TEMPORARY TABLE alerts (
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
    duration_s INT
) WITH (
    'connector' = 'kafka',
    'topic' = 'alerts',
    'properties.bootstrap.servers' = 'kafka:29092',
    'key.format' = 'raw',
    'key.fields' = 'vehicle_id',
    'format' = 'json',
    'sink.delivery-guarantee' = 'exactly-once',
    'sink.transactional-id-prefix' = 'detection-alerts',
    'properties.transaction.timeout.ms' = '600000'
);

-- ---------------------------------------------------------------------
-- Clean input: drop rows that could not be parsed or have impossible GPS
-- ---------------------------------------------------------------------
CREATE TEMPORARY VIEW t AS
SELECT * FROM telemetry
WHERE event_id IS NOT NULL AND vehicle_id IS NOT NULL AND shipment_id IS NOT NULL
  AND event_ts IS NOT NULL
  AND gps_latitude BETWEEN -90 AND 90
  AND gps_longitude BETWEEN -180 AND 180;

-- ---------------------------------------------------------------------
-- Static lookup: warehouse id -> coordinates (28 warehouses)
-- ---------------------------------------------------------------------
CREATE TEMPORARY VIEW t_geo AS
SELECT
    t.*,
    CAST(lat_map[origin_warehouse_id] AS DOUBLE)      AS o_lat,
    CAST(lon_map[origin_warehouse_id] AS DOUBLE)      AS o_lon,
    CAST(lat_map[destination_warehouse_id] AS DOUBLE) AS d_lat,
    CAST(lon_map[destination_warehouse_id] AS DOUBLE) AS d_lon
FROM (
    SELECT
        *,
        MAP['WH-01', 30.0444, 'WH-02', 31.2001, 'WH-03', 30.0131, 'WH-04', 30.4667, 'WH-05', 30.8754, 'WH-06', 31.0364, 'WH-07', 30.5877, 'WH-08', 30.8481, 'WH-09', 31.1107, 'WH-10', 30.523, 'WH-11', 30.5965, 'WH-12', 31.2653, 'WH-13', 31.4175, 'WH-14', 29.9668, 'WH-15', 29.3084, 'WH-16', 29.0661, 'WH-17', 28.1099, 'WH-18', 27.1809, 'WH-19', 26.5591, 'WH-20', 26.1551, 'WH-21', 25.6872, 'WH-22', 24.0889, 'WH-23', 27.2579, 'WH-24', 31.3543, 'WH-25', 30.2824, 'WH-26', 28.1099, 'WH-27', 25.439, 'WH-28', 30.228] AS lat_map,
        MAP['WH-01', 31.2357, 'WH-02', 29.9187, 'WH-03', 31.2089, 'WH-04', 31.1833, 'WH-05', 31.0335, 'WH-06', 31.3807, 'WH-07', 31.502, 'WH-08', 30.3435, 'WH-09', 30.94, 'WH-10', 30.9876, 'WH-11', 32.2715, 'WH-12', 32.3019, 'WH-13', 31.8144, 'WH-14', 32.5498, 'WH-15', 30.8428, 'WH-16', 31.0994, 'WH-17', 30.7503, 'WH-18', 31.1837, 'WH-19', 31.6957, 'WH-20', 32.716, 'WH-21', 32.6396, 'WH-22', 32.8998, 'WH-23', 33.8116, 'WH-24', 27.2373, 'WH-25', 33.6176, 'WH-26', 33.6116, 'WH-27', 30.5586, 'WH-28', 31.481] AS lon_map
    FROM t
) AS t;

-- Distance from the corridor line, in km: the perpendicular distance from the GPS point
-- to the straight line hub -> destination. Flat-earth approximation around the origin,
-- fine for corridors of a few hundred km. (The trucks only drive between the two ends, so
-- the line and the segment give the same answer.)
-- Kept short on purpose: Flink copies every expression into the places that use it,
-- so a long formula makes the query planner very slow.
CREATE TEMPORARY VIEW t_vec AS
SELECT
    *,
    (d_lon - o_lon) * 111.320 * COS(RADIANS(o_lat)) AS vx,
    (d_lat - o_lat) * 110.574                       AS vy,
    (gps_longitude - o_lon) * 111.320 * COS(RADIANS(o_lat)) AS px,
    (gps_latitude - o_lat) * 110.574                        AS py
FROM t_geo;

CREATE TEMPORARY VIEW t_dist AS
SELECT
    *,
    ABS(vx * py - vy * px) / NULLIF(SQRT(vx * vx + vy * vy), 0) AS corridor_km
FROM t_vec;

-- ---------------------------------------------------------------------
-- Bad readings (one row per reading that breaks a rule)
-- ---------------------------------------------------------------------
CREATE TEMPORARY VIEW bad_temp AS
SELECT * FROM t
WHERE reefer_cargo_temp_c < cargo_temp_min_c OR reefer_cargo_temp_c > cargo_temp_max_c;

CREATE TEMPORARY VIEW bad_idle AS
SELECT * FROM t
WHERE engine_rpm > 0 AND speed_kmh < 1.0;

CREATE TEMPORARY VIEW bad_route AS
SELECT * FROM t_dist
WHERE corridor_km > 5.0;

CREATE TEMPORARY VIEW bad_speed AS
SELECT * FROM t
WHERE speed_kmh > 120.0;

-- ---------------------------------------------------------------------
-- Episodes: SESSION window per shipment. One row per episode.
-- The event_id is the id of the FIRST reading of the episode
-- (smallest "time|id" string; the time text has a fixed length of 23).
-- ---------------------------------------------------------------------
CREATE TEMPORARY VIEW alert_temperature AS
SELECT
    CONCAT(shipment_id, ':temperature_breach:', DATE_FORMAT(MIN(event_ts), 'yyyyMMddHHmmssSSS')) AS alert_id,
    SUBSTRING(MIN(CONCAT(CAST(event_ts AS STRING), '|', event_id)) FROM 25) AS event_id,
    CAST(MIN(event_ts) AS TIMESTAMP(3)) AS event_time,
    vehicle_id,
    shipment_id,
    product_id,
    CAST('temperature_breach' AS STRING) AS anomaly_type,
    CAST(MAX(reefer_cargo_temp_c) AS DOUBLE) AS observed_value,
    CAST(MAX(cargo_temp_max_c) AS DOUBLE) AS threshold_value,
    CONCAT('fridge out of range for ', CAST(TIMESTAMPDIFF(SECOND, MIN(event_ts), MAX(event_ts)) AS STRING), ' s') AS detail,
    CAST(MIN(event_ts) AS TIMESTAMP(3)) AS start_time,
    CAST(MAX(event_ts) AS TIMESTAMP(3)) AS end_time,
    CAST(TIMESTAMPDIFF(SECOND, MIN(event_ts), MAX(event_ts)) AS INT) AS duration_s
FROM bad_temp
GROUP BY shipment_id, vehicle_id, product_id, SESSION(event_ts, INTERVAL '20' SECOND)
HAVING TIMESTAMPDIFF(SECOND, MIN(event_ts), MAX(event_ts)) >= 30;

CREATE TEMPORARY VIEW alert_idling AS
SELECT
    CONCAT(shipment_id, ':excessive_idling:', DATE_FORMAT(MIN(event_ts), 'yyyyMMddHHmmssSSS')) AS alert_id,
    SUBSTRING(MIN(CONCAT(CAST(event_ts AS STRING), '|', event_id)) FROM 25) AS event_id,
    CAST(MIN(event_ts) AS TIMESTAMP(3)) AS event_time,
    vehicle_id,
    shipment_id,
    product_id,
    CAST('excessive_idling' AS STRING) AS anomaly_type,
    CAST(TIMESTAMPDIFF(SECOND, MIN(event_ts), MAX(event_ts)) AS DOUBLE) AS observed_value,
    CAST(30 AS DOUBLE) AS threshold_value,
    CONCAT('engine on, speed 0 for ', CAST(TIMESTAMPDIFF(SECOND, MIN(event_ts), MAX(event_ts)) AS STRING), ' s') AS detail,
    CAST(MIN(event_ts) AS TIMESTAMP(3)) AS start_time,
    CAST(MAX(event_ts) AS TIMESTAMP(3)) AS end_time,
    CAST(TIMESTAMPDIFF(SECOND, MIN(event_ts), MAX(event_ts)) AS INT) AS duration_s
FROM bad_idle
GROUP BY shipment_id, vehicle_id, product_id, SESSION(event_ts, INTERVAL '20' SECOND)
HAVING TIMESTAMPDIFF(SECOND, MIN(event_ts), MAX(event_ts)) >= 30;

-- route_deviation: needs at least 3 bad readings, so a single GPS glitch is ignored.
CREATE TEMPORARY VIEW alert_route AS
SELECT
    CONCAT(shipment_id, ':route_deviation:', DATE_FORMAT(MIN(event_ts), 'yyyyMMddHHmmssSSS')) AS alert_id,
    SUBSTRING(MIN(CONCAT(CAST(event_ts AS STRING), '|', event_id)) FROM 25) AS event_id,
    CAST(MIN(event_ts) AS TIMESTAMP(3)) AS event_time,
    vehicle_id,
    shipment_id,
    product_id,
    CAST('route_deviation' AS STRING) AS anomaly_type,
    CAST(MAX(corridor_km) AS DOUBLE) AS observed_value,
    CAST(5 AS DOUBLE) AS threshold_value,
    CONCAT('up to ', CAST(CAST(MAX(corridor_km) AS DECIMAL(8, 1)) AS STRING), ' km from the corridor') AS detail,
    CAST(MIN(event_ts) AS TIMESTAMP(3)) AS start_time,
    CAST(MAX(event_ts) AS TIMESTAMP(3)) AS end_time,
    CAST(TIMESTAMPDIFF(SECOND, MIN(event_ts), MAX(event_ts)) AS INT) AS duration_s
FROM bad_route
GROUP BY shipment_id, vehicle_id, product_id, SESSION(event_ts, INTERVAL '20' SECOND)
HAVING COUNT(*) >= 3;

-- overspeed: one alert per episode (was one per reading).
CREATE TEMPORARY VIEW alert_overspeed AS
SELECT
    CONCAT(shipment_id, ':overspeed:', DATE_FORMAT(MIN(event_ts), 'yyyyMMddHHmmssSSS')) AS alert_id,
    SUBSTRING(MIN(CONCAT(CAST(event_ts AS STRING), '|', event_id)) FROM 25) AS event_id,
    CAST(MIN(event_ts) AS TIMESTAMP(3)) AS event_time,
    vehicle_id,
    shipment_id,
    product_id,
    CAST('overspeed' AS STRING) AS anomaly_type,
    CAST(MAX(speed_kmh) AS DOUBLE) AS observed_value,
    CAST(120 AS DOUBLE) AS threshold_value,
    CONCAT('speed above 120 km/h, peak ', CAST(CAST(MAX(speed_kmh) AS DECIMAL(8, 1)) AS STRING), ' km/h') AS detail,
    CAST(MIN(event_ts) AS TIMESTAMP(3)) AS start_time,
    CAST(MAX(event_ts) AS TIMESTAMP(3)) AS end_time,
    CAST(TIMESTAMPDIFF(SECOND, MIN(event_ts), MAX(event_ts)) AS INT) AS duration_s
FROM bad_speed
GROUP BY shipment_id, vehicle_id, product_id, SESSION(event_ts, INTERVAL '20' SECOND);

INSERT INTO alerts
SELECT * FROM alert_temperature
UNION ALL SELECT * FROM alert_idling
UNION ALL SELECT * FROM alert_route
UNION ALL SELECT * FROM alert_overspeed;