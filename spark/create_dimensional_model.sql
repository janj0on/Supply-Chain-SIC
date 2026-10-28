
-- Build dimensions and fact table from cleaned Silver telemetry.
-- These are new tables; existing Gold KPI tables are not modified.

CREATE OR REPLACE TABLE lake.gold.dim_date
USING iceberg AS
SELECT DISTINCT
    CAST(date_format(to_date(event_ts), 'yyyyMMdd') AS INT) AS date_key,
    to_date(event_ts) AS full_date,
    year(event_ts) AS year,
    quarter(event_ts) AS quarter,
    month(event_ts) AS month,
    dayofmonth(event_ts) AS day_of_month,
    dayofweek(event_ts) AS day_of_week
FROM lake.silver.telemetry
WHERE event_ts IS NOT NULL;

CREATE OR REPLACE TABLE lake.gold.dim_vehicle
USING iceberg AS
SELECT
    vehicle_id,
    MAX(device_id) AS device_id
FROM lake.silver.telemetry
WHERE vehicle_id IS NOT NULL
GROUP BY vehicle_id;

CREATE OR REPLACE TABLE lake.gold.dim_product
USING iceberg AS
SELECT DISTINCT product_id
FROM lake.silver.telemetry
WHERE product_id IS NOT NULL;

-- Keep one current descriptive row per shipment.
CREATE OR REPLACE TABLE lake.gold.dim_shipment
USING iceberg AS
WITH ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY shipment_id
            ORDER BY event_ts DESC, kafka_partition DESC, kafka_offset DESC
        ) AS rn
    FROM lake.silver.telemetry
    WHERE shipment_id IS NOT NULL
)
SELECT
    shipment_id,
    order_id,
    product_id,
    origin_warehouse_id,
    destination_warehouse_id,
    shipment_status
FROM ranked
WHERE rn = 1;

CREATE OR REPLACE TABLE lake.gold.dim_route
USING iceberg AS
SELECT DISTINCT
    sha2(
        concat_ws(
            '||',
            coalesce(origin_warehouse_id, ''),
            coalesce(destination_warehouse_id, '')
        ),
        256
    ) AS route_key,
    origin_warehouse_id,
    destination_warehouse_id
FROM lake.silver.telemetry;

-- One row per unique telemetry event.
CREATE OR REPLACE TABLE lake.gold.fact_telemetry
USING iceberg AS
SELECT
    s.event_id,
    CAST(date_format(to_date(s.event_ts), 'yyyyMMdd') AS INT) AS date_key,
    s.vehicle_id,
    s.shipment_id,
    s.product_id,
    sha2(
        concat_ws(
            '||',
            coalesce(s.origin_warehouse_id, ''),
            coalesce(s.destination_warehouse_id, '')
        ),
        256
    ) AS route_key,
    s.event_ts,
    s.speed_kmh,
    s.heading_degrees,
    s.engine_rpm,
    s.fuel_level_pct,
    s.fuel_consumption_l_per_100km,
    s.obd_engine_load_pct,
    s.obd_coolant_temp_c,
    s.reefer_cargo_temp_c,
    s.reefer_setpoint_c,
    s.humidity_pct,
    s.route_progress_pct,
    s.route_distance_m,
    s.route_estimated_duration_s,
    s.temperature_breach_injected,
    s.door_open,
    s.reefer_on,
    s.obd_fault_code,
    s.kafka_partition,
    s.kafka_offset,
    s.dt
FROM lake.silver.telemetry s;
