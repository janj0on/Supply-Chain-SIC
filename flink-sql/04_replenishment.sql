-- =====================================================================
-- 04_replenishment.sql   Flink job: shortage trigger -> orders (no customer action)
--
-- Reads   inventory-stock   (stock level after every consumption / receipt)
--         demand-events     (every consumption)
-- Writes  replenishment-triggered   why an order was triggered
--         new-orders                the order itself (the Dispatcher picks it up)
--
-- A shortage is when, for one (site, product):
--   stock < reorder point                                  -> below_reorder_point
--   or stock / demand rate < supplier lead time            -> projected_stockout
-- demand rate = kg consumed in the last 60 s / 60.
--
-- ONE OPEN ORDER AT A TIME. The order is "open" from the first trigger until the
-- delivery arrives (event_type = 'receipt'). So the order id contains the number of
-- receipts seen so far. Every trigger inside the same open period has the same
-- (site, product, receipts) key, and only the FIRST one is kept.
-- When a delivery arrives the number goes up, so a new shortage can create a new order.
-- The order id is always the same for the same shortage, so a replay after a restart
-- cannot create a different order (the Dispatcher ignores an order id it has seen).
--
-- Run:  docker exec -it flink-jobmanager ./bin/sql-client.sh -f /opt/flink/sql/04_replenishment.sql
-- =====================================================================

SET 'pipeline.name' = 'replenishment';
SET 'execution.checkpointing.interval' = '30 s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.min-pause' = '10 s';
SET 'execution.checkpointing.timeout' = '10 min';
SET 'table.exec.source.idle-timeout' = '10 s';
SET 'table.exec.state.ttl' = '1 d';
SET 'table.local-time-zone' = 'UTC';

CREATE TEMPORARY TABLE inventory_stock (
    event_id STRING,
    event_time_utc STRING,
    site_id STRING,
    product_id STRING,
    event_type STRING,
    stock_kg DOUBLE,
    reorder_point_kg DOUBLE,
    order_up_to_kg DOUBLE,
    lead_time_s INT,
    shipment_id STRING,
    event_ts AS TRY_CAST(REPLACE(REPLACE(REPLACE(event_time_utc, 'T', ' '), 'Z', ''), '+00:00', '') AS TIMESTAMP(3)),
    WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'inventory-stock',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-replenishment-stock',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

CREATE TEMPORARY TABLE demand_events (
    event_id STRING,
    event_time_utc STRING,
    site_id STRING,
    product_id STRING,
    quantity_kg DOUBLE,
    event_ts AS TRY_CAST(REPLACE(REPLACE(REPLACE(event_time_utc, 'T', ' '), 'Z', ''), '+00:00', '') AS TIMESTAMP(3)),
    WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'demand-events',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-replenishment-demand',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

CREATE TEMPORARY TABLE replenishment_triggered (
    order_id STRING,
    site_id STRING,
    product_id STRING,
    trigger_reason STRING,
    stock_kg DOUBLE,
    reorder_point_kg DOUBLE,
    demand_rate_kg_per_s DOUBLE,
    lead_time_s INT,
    quantity_kg DOUBLE,
    triggered_at TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'replenishment-triggered',
    'properties.bootstrap.servers' = 'kafka:29092',
    'key.format' = 'raw',
    'key.fields' = 'order_id',
    'format' = 'json'
);

CREATE TEMPORARY TABLE new_orders (
    order_id STRING,
    product_id STRING,
    destination_warehouse_id STRING,
    quantity_kg DOUBLE,
    priority STRING,
    created_at TIMESTAMP(3),
    delivery_deadline TIMESTAMP(3),
    status STRING,
    trigger_reason STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'new-orders',
    'properties.bootstrap.servers' = 'kafka:29092',
    'key.format' = 'raw',
    'key.fields' = 'order_id',
    'format' = 'json'
);

-- ---------------------------------------------------------------------
-- 1. One stream with stock rows and demand rows
-- ---------------------------------------------------------------------
CREATE TEMPORARY VIEW stock_and_demand AS
SELECT
    site_id, product_id, event_ts, event_type,
    stock_kg, reorder_point_kg, order_up_to_kg, lead_time_s,
    CAST(0 AS DOUBLE) AS demand_kg
FROM inventory_stock
WHERE site_id IS NOT NULL AND product_id IS NOT NULL AND event_ts IS NOT NULL
UNION ALL
SELECT
    site_id, product_id, event_ts, CAST('demand' AS STRING),
    CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS INT),
    quantity_kg
FROM demand_events
WHERE site_id IS NOT NULL AND product_id IS NOT NULL AND event_ts IS NOT NULL;

-- ---------------------------------------------------------------------
-- 2. Running state per (site, product): receipts so far, demand in the last 60 s
--    (two views: Flink needs the same window definition inside one SELECT)
-- ---------------------------------------------------------------------
CREATE TEMPORARY VIEW with_receipts AS
SELECT
    *,
    SUM(CASE WHEN event_type = 'receipt' THEN 1 ELSE 0 END) OVER (
        PARTITION BY site_id, product_id ORDER BY event_ts
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS receipts_so_far
FROM stock_and_demand;

CREATE TEMPORARY VIEW with_rate AS
SELECT
    *,
    SUM(demand_kg) OVER (
        PARTITION BY site_id, product_id ORDER BY event_ts
        RANGE BETWEEN INTERVAL '60' SECOND PRECEDING AND CURRENT ROW) AS demand_60s
FROM with_receipts;

-- ---------------------------------------------------------------------
-- 3. Shortage check on every stock row
-- ---------------------------------------------------------------------
CREATE TEMPORARY VIEW shortages AS
SELECT
    site_id,
    product_id,
    event_ts,
    stock_kg,
    reorder_point_kg,
    order_up_to_kg,
    lead_time_s,
    receipts_so_far,
    demand_60s / 60.0 AS rate_kg_per_s,
    CASE WHEN stock_kg < reorder_point_kg THEN 'below_reorder_point' ELSE 'projected_stockout' END AS trigger_reason,
    PROCTIME() AS proc_time
FROM with_rate
WHERE stock_kg IS NOT NULL
  AND (stock_kg < reorder_point_kg
       OR (demand_60s > 0 AND stock_kg * 60.0 / demand_60s < lead_time_s));

-- ---------------------------------------------------------------------
-- 4. Open-order check: keep only the first shortage per (site, product, receipts)
-- ---------------------------------------------------------------------
CREATE TEMPORARY VIEW new_shortages AS
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY site_id, product_id, receipts_so_far ORDER BY proc_time ASC) AS rn
    FROM shortages
) WHERE rn = 1;

-- ---------------------------------------------------------------------
-- 5. The order: refill up to the "order up to" level. Never less than half of that
--    level (when the stock-out is projected the stock can still be high), and never
--    more than the biggest vehicle (20,000 kg) can carry.
-- ---------------------------------------------------------------------
CREATE TEMPORARY VIEW orders_wanted AS
SELECT
    *,
    CASE WHEN order_up_to_kg - stock_kg < 0.5 * order_up_to_kg
         THEN 0.5 * order_up_to_kg
         ELSE order_up_to_kg - stock_kg END AS want_kg
FROM new_shortages;

CREATE TEMPORARY VIEW orders_to_create AS
SELECT
    CONCAT('ORD-', site_id, '-', product_id, '-', CAST(receipts_so_far AS STRING)) AS order_id,
    site_id,
    product_id,
    trigger_reason,
    stock_kg,
    reorder_point_kg,
    rate_kg_per_s,
    lead_time_s,
    CASE WHEN want_kg > 20000 THEN CAST(20000 AS DOUBLE) ELSE ROUND(want_kg, 1) END AS quantity_kg,
    CASE WHEN stock_kg < 0.5 * reorder_point_kg
              OR (rate_kg_per_s > 0 AND stock_kg / rate_kg_per_s < 0.5 * lead_time_s)
         THEN 'URGENT' ELSE 'NORMAL' END AS priority,
    CAST(event_ts AS TIMESTAMP(3)) AS triggered_at
FROM orders_wanted;

EXECUTE STATEMENT SET
BEGIN

INSERT INTO replenishment_triggered
SELECT order_id, site_id, product_id, trigger_reason, stock_kg, reorder_point_kg,
       rate_kg_per_s, lead_time_s, quantity_kg, triggered_at
FROM orders_to_create;

INSERT INTO new_orders
SELECT order_id, product_id, site_id, quantity_kg, priority, triggered_at,
       triggered_at + INTERVAL '2' HOUR, CAST('NEW' AS STRING), trigger_reason
FROM orders_to_create;

END;
