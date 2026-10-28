-- =====================================================================
-- 01_bronze.sql   Flink job: Kafka -> Bronze (Iceberg on HDFS)
--
-- Bronze = raw, append-only, nothing cleaned.
-- ONE table (lake.bronze.raw_events) for ALL topics. Each row is one Kafka record:
--     kafka_topic, kafka_partition, kafka_offset   where it came from
--     kafka_key, kafka_ts                          key and Kafka timestamp
--     payload                                      the message exactly as received (JSON text)
--     ingest_ts                                    when Flink wrote it
--     dt, hr                                       date and hour of kafka_ts (partitions)
-- Partitioned by topic, date, hour. Duplicates and late events are kept on purpose.
-- Spark reads one topic with:  WHERE kafka_topic = 'vehicle-telemetry'
--
-- The Iceberg sink commits on every checkpoint (30 s), so a new file appears every 30 s.
-- Many small files are normal here; compact them later with Airflow + Spark.
--
-- Run:  docker exec -it flink-jobmanager ./bin/sql-client.sh -f /opt/flink/sql/01_bronze.sql
-- =====================================================================

SET 'pipeline.name' = 'bronze';
SET 'execution.checkpointing.interval' = '30 s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.min-pause' = '10 s';
SET 'execution.checkpointing.timeout' = '10 min';
SET 'table.exec.source.idle-timeout' = '10 s';
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

CREATE DATABASE IF NOT EXISTS lake.bronze;

CREATE TABLE IF NOT EXISTS lake.bronze.raw_events (
    kafka_topic STRING,
    kafka_partition INT,
    kafka_offset BIGINT,
    kafka_key STRING,
    kafka_ts TIMESTAMP(3),
    payload STRING,
    ingest_ts TIMESTAMP(3),
    dt STRING,
    hr STRING
) PARTITIONED BY (kafka_topic, dt, hr) WITH (
    'format-version' = '2'
);

-- Every topic in one source. Key and value are read as plain text (no parsing).
CREATE TEMPORARY TABLE raw_src (
    kafka_key STRING,
    payload STRING,
    kafka_topic STRING METADATA FROM 'topic' VIRTUAL,
    kafka_partition INT METADATA FROM 'partition' VIRTUAL,
    kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL,
    kafka_ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL
) WITH (
    'connector' = 'kafka',
    'topic-pattern' = 'vehicle-telemetry|inventory-stock|demand-events|vehicle-status|shipments|vehicles|warehouses|products|alerts|replenishment-triggered|new-orders|producer-dlq',
    'scan.topic-partition-discovery.interval' = '1 min',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-bronze',
    'scan.startup.mode' = 'earliest-offset',
    'key.format' = 'raw',
    'key.fields' = 'kafka_key',
    'value.format' = 'raw',
    'value.fields-include' = 'EXCEPT_KEY'
);

INSERT INTO lake.bronze.raw_events
SELECT
    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_key,
    CAST(kafka_ts AS TIMESTAMP(3)),
    payload,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)),
    DATE_FORMAT(CAST(kafka_ts AS TIMESTAMP(3)), 'yyyy-MM-dd'),
    DATE_FORMAT(CAST(kafka_ts AS TIMESTAMP(3)), 'HH')
FROM raw_src;
