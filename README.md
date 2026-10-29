# Supply-Chain-SIC

A self-contained, Dockerised **real-time supply-chain analytics platform** for a refrigerated (cold-chain) logistics network. It simulates a fleet of reefer trucks moving food products across 28 Egyptian governorate warehouses, streams the resulting telemetry through Kafka, cleans and enriches it with Flink, stores it in an Iceberg lakehouse on HDFS (Bronze → Silver → Gold), detects anomalies in real time, and exposes live alert dashboards in Grafana. Airflow schedules the batch (Spark) work and watches the health of the platform.

> **Stack:** Python · Apache Kafka 4.3 · Apache Flink 1.20 (SQL) · Apache Iceberg 1.9 · HDFS (Hadoop 3.4) · Apache Spark 3.5 · Apache Airflow 2.10 · Prometheus · Grafana 11 · Docker Compose

---

## Table of contents

1. [What this project demonstrates](#1-what-this-project-demonstrates)
2. [Architecture](#2-architecture)
3. [Quick start](#3-quick-start)
4. [Repository layout](#4-repository-layout)
5. [The simulated world (data generator)](#5-the-simulated-world-data-generator)
6. [Ingestion: the Kafka bridge and topics](#6-ingestion-the-kafka-bridge-and-topics)
7. [Lakehouse design: Bronze → Silver → Gold](#7-lakehouse-design-bronze--silver--gold)
8. [Real-time anomaly detection](#8-real-time-anomaly-detection)
9. [Gold layer: KPIs and dimensional model](#9-gold-layer-kpis-and-dimensional-model)
10. [Orchestration with Airflow](#10-orchestration-with-airflow)
11. [Monitoring: Prometheus and Grafana](#11-monitoring-prometheus-and-grafana)
12. [Evaluating detection quality](#12-evaluating-detection-quality)
13. [The closed loop (brief)](#13-the-closed-loop-brief)
14. [Configuration reference](#14-configuration-reference)
15. [Operations cheat-sheet](#15-operations-cheat-sheet)
16. [Design decisions and known limitations](#16-design-decisions-and-known-limitations)

---

## 1. What this project demonstrates

| Concern | How it is covered |
|---|---|
| **Streaming ingestion** | Python producer with idempotence, `acks=all`, DLQ topic and a local backup file |
| **Stream processing** | Four Flink SQL jobs: event-time watermarks, de-duplication, session windows, exactly-once sinks |
| **Lakehouse** | Iceberg v2 tables on HDFS using the Hadoop catalog (no catalog service needed) |
| **Medallion architecture** | Bronze (raw, append-only) → Silver (typed, deduped, ordered, quarantined) → Gold (KPIs + star schema) |
| **Data quality** | Quarantine table with reasons, Silver quality gate, per-day quality metrics in Gold |
| **Anomaly detection** | Four rules evaluated as *episodes* (start, end, duration), not single readings |
| **Observability** | Alerts → Prometheus exporter → Grafana dashboard that is itself generated from Python code |
| **Orchestration** | Airflow: a platform health guard (every 5 min) and the Silver → Gold batch (every 15 min) |
| **Ground-truth evaluation** | The generator records which readings are truly anomalous, so precision/recall of the detector can be measured |
| **Realistically messy data** | Jitter, duplicates, out-of-order events, offline gaps, all injected on purpose |

---

## 2. Architecture

```
 STREAMING (always on, Docker Compose)                                 BATCH (scheduled by Airflow)

 ┌───────────┐  files   ┌────────┐       ┌───────┐
 │ generator │ ───────▶ │ bridge │ ────▶ │ Kafka │
 └───────────┘          └────────┘       └───┬───┘
   simulates trucks,                         │
   stock, demand,                            ├──▶ 01_bronze  ─▶ lake.bronze.raw_events ──┐
   messy data                                │                                           │
                                             ├──▶ 02_silver  ─▶ lake.silver.telemetry    │
                                             │                  lake.silver.alerts       ├──▶ Spark ─▶ lake.gold.*
                                             │                  lake.silver.quarantine ──┘     (DAG B, every 15 min)
                                             │
                                             ├──▶ 03_detection ─▶ topic `alerts`
                                             │                         │
                                             │                         ▼
                                             │                  alert-exporter ─▶ Prometheus ─▶ Grafana
                                             │
                                             └──▶ 04_replenishment ─▶ orders ─▶ dispatcher ─▶ (back to generator)

 DAG A `platform_guard` (every 5 min) watches Kafka, HDFS, Flink jobs, the apps and the monitoring stack.
```

**Data flow in one paragraph.** The *generator* writes JSON-lines files that mimic IoT devices and inventory systems. The *bridge* tails those files and publishes them to Kafka. Four Flink SQL jobs read Kafka in parallel: one archives everything raw (Bronze), one cleans telemetry and alerts (Silver), one detects anomalies and publishes alerts back to Kafka, and one handles replenishment. Spark then reads Silver on a schedule and builds the Gold KPI tables and a dimensional model. Prometheus and Grafana visualise the alert stream.

### Containers

| Container | Image | Role |
|---|---|---|
| `kafka` | apache/kafka 4.3.1 | Single-node KRaft broker (internal `:29092`, host `:9092`) |
| `namenode`, `datanode`, `hdfs-init` | apache/hadoop 3.4.1 | HDFS storage; `hdfs-init` creates `/warehouse/{bronze,silver,gold}` and exits |
| `flink-jobmanager`, `flink-taskmanager` | flink 1.20.5 + Iceberg, Hadoop, Kafka jars | Runs the four SQL jobs (8 task slots, 30 s checkpoints) |
| `supply-chain-spark` | apache/spark 3.5.6 + Iceberg runtime | Idle container; Airflow `docker exec`s Spark jobs into it |
| `generator`, `bridge`, `dispatcher` | one shared Python 3.12 image | The streaming apps |
| `alert-exporter`, `prometheus`, `grafana` | custom / prom 2.55 / grafana 11.3 | Monitoring |
| `airflow-postgres`, `airflow-init`, `airflow-scheduler`, `airflow-webserver` | Airflow 2.10.5 | Orchestration (LocalExecutor on Postgres) |

---

## 3. Quick start

**Requirements:** Docker with Compose v2 and roughly **8 GB of RAM** available to Docker.

```bash
# Build and start everything
docker compose -f docker-compose.yml up -d --build

# Or, for a guaranteed clean first run (wipes all volumes, then starts empty)
python wipe_all.py --yes --up
```

**Linux only:** Airflow needs the Docker socket. Add these to `.env`:

```bash
DOCKER_SOCK=/var/run/docker.sock
DOCKER_GID=$(stat -c %g /var/run/docker.sock)   # put the resulting number in .env
```

### What happens after start-up

1. Kafka becomes healthy, the generator starts, and the bridge creates all topics.
2. Within about **5 minutes** Airflow's `platform_guard` DAG submits the four Flink jobs (it waits for the Kafka topics on its first run).
3. After the first Flink checkpoint (30 s) Parquet files appear in HDFS.
4. DAG `lakehouse_batch` waits until Silver has Parquet files, then builds Gold, and repeats every 15 minutes.

### URLs

| URL | What you see | Login |
|---|---|---|
| http://localhost:8080 | Airflow: DAGs `platform_guard` and `lakehouse_batch` | admin / admin |
| http://localhost:3000/d/supply-chain-alerts | Grafana dashboard *Supply Chain – Flink Alerts* | admin / admin |
| http://localhost:8082 | Flink UI: 4 jobs should be `RUNNING`, checkpoints every 30 s | n/a |
| http://localhost:9870 | HDFS browser (`/warehouse/bronze`, `silver`, `gold`) | n/a |
| http://localhost:9090/targets | Prometheus targets (`alert-exporter` must be UP) | n/a |
| http://localhost:8000/metrics | Raw exporter metrics | n/a |

### Verify it works

```bash
# Streaming apps
docker logs -f generator            # also: bridge, dispatcher

# Files landing in the lake
docker exec namenode hdfs dfs -ls -R /warehouse | head -30
docker exec namenode hdfs dfs -du -h /warehouse

# Gold tables (after the first DAG B run)
docker exec supply-chain-spark /opt/spark/bin/spark-sql -e "SHOW TABLES IN lake.gold"

# Peek at a topic
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 --topic alerts --from-beginning --max-messages 3
```

---

## 4. Repository layout

```
Supply-Chain-SIC/
├── docker-compose.yml        # the whole platform (single project: supply-chain-sic)
├── .env                      # simulation + producer settings (see §14)
├── Dockerfile.app            # one image for generator, bridge, dispatcher
├── requirements.txt          # confluent-kafka, python-dotenv
├── wipe_all.py               # reset helper: cancels jobs, removes containers and volumes
│
├── DataGenerator.py          # simulated physical world (trucks, stock, demand, messy data)
├── KafkaProducer.py          # "bridge": generator files -> Kafka (+ topic creation, DLQ)
├── Dispatcher.py             # order -> vehicle allocation (see §13)
├── score_alerts.py           # precision / recall of the detector vs ground truth
│
├── flink/Dockerfile          # Flink image + Iceberg / Hadoop / Kafka connector jars
├── flink-sql/
│   ├── 01_bronze.sql         # Kafka -> lake.bronze.raw_events
│   ├── 02_silver.sql         # Kafka -> silver telemetry / alerts / quarantine
│   ├── 03_detection.sql      # telemetry -> alerts topic (4 anomaly rules)
│   └── 04_replenishment.sql  # stock shortage -> orders (see §13)
│
├── spark/
│   ├── Dockerfile, spark-defaults.conf     # Iceberg catalog "lake" baked into the image
│   ├── silver_quality_gate.py              # fails the DAG if Silver is empty / has nulls
│   ├── gold_kpi_suite.py                   # six Gold KPI tables
│   ├── gold_shipment_performance.py        # daily shipment performance table
│   ├── gold_data_quality.py                # per-day quality + quarantine metrics
│   ├── create_dimensional_model.sql        # dim_* and fact_telemetry
│   ├── model_all_gold_tables.sql           # fact_* / dim_*_model over every KPI table
│   ├── 00_init_gold.sql                    # creates the gold namespace
│   └── standalone/                         # single-table versions of the KPI jobs (not scheduled)
│
├── airflow/
│   ├── Dockerfile
│   └── dags/  platform_guard.py, lakehouse_batch.py, sc_common.py
│
└── monitoring/
    ├── exporter/alert_exporter.py          # Kafka alerts -> Prometheus metrics
    ├── prometheus/prometheus.yml
    ├── grafana/{provisioning,dashboards}/  # auto-provisioned datasource + dashboard JSON
    └── dashboard-as-code/build_dashboard.py  # generates the dashboard JSON with grafanalib
```

---

## 5. The simulated world (data generator)

`DataGenerator.py` simulates the **physical** world only. It never makes decisions; it reacts to shipments it is told to run.

### Network and products

* **28 warehouses**: the 27 Egyptian governorates plus Obour. One of them is the **hub** (`START_WAREHOUSE_ID`, default `WH-11` Ismailia). All trucks leave the hub; the other 27 are consuming sites.
* **4 products**, each with a required temperature range:

| Product | Range (°C) |
|---|---|
| Dairy | 2 to 6 |
| Fresh Produce | 4 to 10 |
| Frozen Food | −18 to −12 |
| Dry Goods | 5 to 35 |

* **Fleet:** `FLEET_SIZE` trucks (default 30) named `TRUCK-1 … TRUCK-N`, alternating refrigerated trucks and reefer vans (10 t / 20 t capacity). Every truck is refrigerated and large enough for any order, so any truck can serve any order.

### Telemetry

A truck on a trip emits one reading every `SEND_INTERVAL_SECONDS` (default 5 s). Each event carries about 30 fields:

* **Identity:** `event_id` (UUID), `event_time_utc`, `vehicle_id`, `device_id`, `shipment_id`, `order_id`, `product_id`, origin and destination warehouse
* **Position:** `gps_latitude/longitude`, `heading_degrees`, `route_progress_pct`, `route_distance_m`, `route_estimated_duration_s`
* **Engine / OBD:** `speed_kmh`, `engine_rpm`, `fuel_level_pct`, `fuel_consumption_l_per_100km`, `obd_coolant_temp_c`, `obd_engine_load_pct`, `obd_fault_code`
* **Reefer / cargo:** `reefer_cargo_temp_c`, `reefer_setpoint_c`, `cargo_temp_min_c/max_c`, `humidity_pct`, `reefer_on`, `door_open`
* **Status:** `shipment_status` (`in_transit` → `delivered`)

Trucks follow a straight-line corridor from hub to destination; trip length is `TRIP_DURATION_SECONDS` (default 200 s) scaled by `SIMULATION_SPEED`.

### Injected anomalies (episodes)

Anomalies are *episodes* that last several readings, as in real life:

| Anomaly | What the generator does | Duration |
|---|---|---|
| `temperature_breach` | Cargo temp pushed 2 to 6 °C above the product's maximum | 35 to 65 s |
| `excessive_idling` | Engine on, speed 0, position frozen | 35 to 65 s |
| `route_deviation` | Position offset 8 to 20 km sideways from the corridor | 15 to 40 s |
| `overspeed` | A single reading at 140 km/h | one event |

It also injects **harmless blips** (a short temperature spike or short idle, under 10 s) that a duration-aware detector must *ignore*. They are labelled "not an anomaly" in the ground truth.

### Messy data (schema never changes)

Applied when a line is written to the file:

| Defect | Setting | Behaviour |
|---|---|---|
| Timestamp jitter | `JITTER_SECONDS` | ± 0.3 s on `event_time_utc` |
| Duplicates | `DUPLICATE_PROBABILITY` | Same line emitted again (0 to 3 s or 10 to 40 s later) |
| Late events | `LATE_PROBABILITY` | 70 % arrive 1 to 8 s late (inside the watermark); the rest 15 to 45 s late (outside it) |
| Offline gaps | `OFFLINE_PROBABILITY` | Device goes dark for 10 to 25 s; buffered events burst out afterwards, 30 % are lost for good |

### Output files (in the `app-data` volume, mounted at `/data`)

`products.csv`, `warehouses.csv`, `vehicles.csv` (master data) · `telemetry.jsonl` · `inventory_stock.jsonl` · `demand_events.jsonl` · `vehicle_status.jsonl` · `shipment_updates.jsonl` · `ground_truth.jsonl` (never sent to Kafka; used for scoring).

---

## 6. Ingestion: the Kafka bridge and topics

`KafkaProducer.py` is the **bridge** between the generator's files and Kafka.

* Creates every required topic up front (3 partitions, replication 1) so they exist before the Flink jobs start.
* **Reliable producer:** `acks=all`, `enable.idempotence=true`, retries, delivery timeout.
* **Dead-letter queue:** records that cannot be sent go to `producer-dlq`; if even that fails they are appended to `producer_failed_records.jsonl`.
* Master-data CSVs are re-checked every `CSV_REFRESH_SECONDS` and re-published when their content changes.
* Messages are keyed (`vehicle_id`, `site_id|product_id`, or `shipment_id`) so related events stay in one partition and stay ordered.

### Topics

| Topic | Written by | Content |
|---|---|---|
| `vehicle-telemetry` | bridge | Truck sensor readings (key `vehicle_id`) |
| `inventory-stock` | bridge | Stock level after every consumption/receipt |
| `demand-events` | bridge | Every consumption at a site |
| `vehicle-status` | bridge | Vehicle available again after turnaround |
| `vehicles`, `warehouses`, `products` | bridge | Master data |
| `alerts` | Flink `03_detection` | One alert per anomaly episode |
| `replenishment-triggered`, `new-orders` | Flink `04_replenishment` | See §13 |
| `shipments` | dispatcher / bridge | Order → vehicle → trip, then delivery |
| `producer-dlq` | bridge | Records that could not be sent |

---

## 7. Lakehouse design: Bronze → Silver → Gold

All tables are **Apache Iceberg (format v2)** stored as Parquet on HDFS. The Iceberg **Hadoop catalog** is used, so tables are just folders under `hdfs://namenode:8020/warehouse`, and there is no metastore to run. Flink and Spark use the same catalog name, `lake`.

```
hdfs://namenode:8020/warehouse/
├── bronze/raw_events/                 # 1 table for every topic
├── silver/{telemetry, alerts, quarantine}/
└── gold/…                             # Spark-built KPI + model tables
```

### Bronze: raw and append-only (`01_bronze.sql`)

A single table, **`lake.bronze.raw_events`**, holds every Kafka record exactly as received:

| Column | Meaning |
|---|---|
| `kafka_topic`, `kafka_partition`, `kafka_offset`, `kafka_key`, `kafka_ts` | Provenance |
| `payload` | The message as raw JSON text |
| `ingest_ts` | When Flink wrote it |
| `dt`, `hr` | Date / hour of `kafka_ts` |

Partitioned by `(kafka_topic, dt, hr)`. Duplicates and late events are **kept on purpose**, so Bronze is the replayable source of truth. The Iceberg sink commits on every checkpoint (30 s), so expect many small files; compaction is a future improvement.

### Silver: clean and typed (`02_silver.sql`)

One Flink job, three outputs:

| Table | Content |
|---|---|
| **`lake.silver.telemetry`** | Fully typed telemetry, de-duplicated by `event_id`, written in event-time order, partitioned by `(dt, hr)` |
| **`lake.silver.alerts`** | Alerts de-duplicated by `alert_id` |
| **`lake.silver.quarantine`** | Rejected rows with a `reason` and a pointer (`kafka_partition`, `kafka_offset`) back to the raw record in Bronze |

Telemetry goes through four steps, in this order:

1. **Validity checks** → quarantine with reason `unparseable`, `bad_speed` (null, < 0 or > 250) or `bad_gps` (null or out of range).
2. **De-duplication** on `event_id` (first copy wins).
3. **Lateness check** against a **10 s watermark**. Anything older goes to quarantine as `late_event`. Duplicates are removed *before* this check, so a late copy of an already-written event is simply a duplicate, not a "late event".
4. **Ordered write** by `event_ts`. (This makes that part of the job run at parallelism 1, which is fine for thousands of events per second.)

### Gold: business-ready (Spark)

Built on a schedule from Silver; see §9.

---

## 8. Real-time anomaly detection

`03_detection.sql` reads `vehicle-telemetry` and writes **one alert per anomaly episode** to the `alerts` topic.

### Rules

| Anomaly | Rule | Episode condition |
|---|---|---|
| `temperature_breach` | Cargo temp outside `[cargo_temp_min_c, cargo_temp_max_c]` | Lasts **≥ 30 s** |
| `excessive_idling` | `engine_rpm > 0` and `speed_kmh < 1` | Lasts **≥ 30 s** |
| `route_deviation` | More than **5 km** from the hub → destination corridor | **≥ 3** readings |
| `overspeed` | `speed_kmh > 120` | One alert per episode |

### How episodes are built

* **Event-time watermark: 30 s** (tolerates late telemetry).
* A **SESSION window with a 20 s gap**, grouped per `(shipment_id, vehicle_id, product_id)`, joins nearby bad readings into one episode.
* `HAVING` clauses enforce the minimum duration / reading count, so short blips and single GPS glitches are ignored.
* **Corridor distance** is the perpendicular distance from the GPS point to the straight line hub → destination, using a flat-earth approximation around the origin. Warehouse coordinates are a constant `MAP` lookup (no join, so no second watermark to wait for).
* `alert_id` = `shipment_id:anomaly_type:start_time`, so the same episode always gets the same id.

### Alert schema

`alert_id, event_id (first reading of the episode), event_time, vehicle_id, shipment_id, product_id, anomaly_type, observed_value, threshold_value, detail, start_time, end_time, duration_s`

### Delivery guarantees and trade-offs

* The Kafka sink is **exactly-once** (transactions, committed each checkpoint), so restarts do not duplicate alerts. Consumers **must** read with `isolation.level=read_committed`; the exporter does.
* An alert is emitted only **after the episode is over** (watermark passes its end), so latency is roughly *watermark + session gap + one checkpoint*. The Grafana "detection delay" panel makes this visible.
* If the job is restarted, prefer restoring from a **savepoint**; starting from scratch re-reads Kafka from the earliest offset.

---

## 9. Gold layer: KPIs and dimensional model

Gold is rebuilt by Spark with `createOrReplace()`, so every run is idempotent: re-running never double-counts. Before building, telemetry is de-duplicated again by `event_id` (latest `ingest_ts`/offset wins), which protects the KPIs if a Flink job was resubmitted and replayed Bronze/Silver.

### KPI tables

| Table | Grain | Highlights |
|---|---|---|
| `shipment_telemetry_performance` | shipment × day | Avg/max speed, fuel, engine load, coolant, cargo temp, humidity, door-open / fault / breach counts |
| `vehicle_performance` | vehicle × day | Shipments handled, speed, RPM, engine load, fuel, faults |
| `daily_anomaly_performance` | day × anomaly type | Alert count, affected shipments/vehicles, avg/max duration |
| `route_performance` | day × origin × destination | Distance, estimated duration, progress, speed, breaches |
| `product_shipment_performance` | day × product | Cargo temperature min/avg/max, humidity, breaches, door-open |
| `daily_shipment_performance` | day × route × product × status | Event/shipment/vehicle counts, progress, speed, fuel |
| `data_quality_kpis` | metric (long format) | Row counts, distinct IDs, duplicate rows, null counts, quarantine counts by reason |
| `data_quality_metrics` | day | Per-day raw rows, distinct IDs, repeats, null counts |
| `quarantine_quality_metrics` | day × reason × topic | Quarantined record counts |

> The five single-table scripts in `spark/standalone/` build the same tables as `gold_kpi_suite.py`; they are handy for debugging and are **not scheduled**.

### Dimensional (star-schema) model

Two SQL scripts turn the KPI tables into a BI-friendly model.

**`create_dimensional_model.sql`**: dimensions and a telemetry fact built straight from Silver

* `dim_date`, `dim_vehicle`, `dim_product`, `dim_shipment` (latest row per shipment), `dim_route` (SHA-256 `route_key` over origin‖destination)
* `fact_telemetry`: one row per unique telemetry event, keyed by `date_key`, `vehicle_id`, `shipment_id`, `product_id`, `route_key`

**`model_all_gold_tables.sql`**: a fact table for every KPI table, keyed on `date_key` / `route_key`

* `dim_date_model`, `dim_anomaly_model`, `dim_quarantine_reason_model`
* `fact_daily_shipment_performance`, `fact_shipment_telemetry_performance`, `fact_vehicle_performance`, `fact_daily_anomaly_performance`, `fact_route_performance`, `fact_product_shipment_performance`, `fact_data_quality_metrics`, `fact_quarantine_quality_metrics`

Surrogate keys are deterministic (`yyyyMMdd` ints for dates, SHA-256 for routes), so facts from different jobs always join cleanly.

### Querying Gold

```bash
docker exec -it supply-chain-spark /opt/spark/bin/spark-sql
spark-sql> SELECT anomaly_type, SUM(alert_count) FROM lake.gold.daily_anomaly_performance GROUP BY 1;
spark-sql> SELECT * FROM lake.gold.fact_route_performance ORDER BY shipment_count DESC LIMIT 10;
```

---

## 10. Orchestration with Airflow

Airflow only does **scheduled work**; Docker Compose runs the streaming pipeline. Airflow reaches other containers (`docker exec`) through the Docker socket. It runs on **Postgres + LocalExecutor**, so the health guard keeps ticking even while a long Gold run is busy.

### DAG A: `platform_guard` (every 5 min)

```
check_infra → check_topics → ensure_flink_jobs → check_apps → check_monitoring
```

| Task | Purpose |
|---|---|
| `check_infra` | Kafka, NameNode, DataNode, Flink and Spark containers are running; Kafka answers API calls |
| `check_topics` | All 12 expected topics exist (retries while the bridge creates them on first start) |
| `ensure_flink_jobs` | Submits any of `bronze`, `silver`, `detection`, `replenishment` that is not `RUNNING`; refuses to submit if names don't match, to avoid duplicates |
| `check_apps` | Restarts generator / bridge / dispatcher if stopped |
| `check_monitoring` | Exporter, Prometheus and Grafana health endpoints respond |

> A resubmitted Flink job restarts from the **earliest Kafka offset**, so Bronze can receive duplicates. Normal crashes are handled by Flink's own restart strategy (20 attempts, 10 s delay); this path only triggers when a job is gone entirely.

### DAG B: `lakehouse_batch` (every 15 min)

```
wait_for_silver → init_gold_namespace → silver_quality_gate → build_gold_kpis
  → build_daily_shipment_performance → build_quality_tables
  → create_dimensional_model → model_all_gold_tables
```

* `wait_for_silver` is a rescheduling sensor that checks for Parquet files in `/warehouse/silver/telemetry/data`.
* `silver_quality_gate` **fails the run** if Silver telemetry is empty or has null `event_id`, `event_ts`, `shipment_id` or `vehicle_id`, so bad data never reaches Gold.
* `max_active_runs=1`, 1 retry, 30 min task timeout.
* Spark runs in local mode inside `supply-chain-spark`; the Iceberg catalog settings come from `spark/spark-defaults.conf` baked into the image.

---

## 11. Monitoring: Prometheus and Grafana

```
Flink 03_detection → Kafka `alerts` → alert-exporter → Prometheus (5 s scrape) → Grafana
```

Prometheus cannot read Kafka, so `alert_exporter.py` sits in the middle: it consumes `alerts` (with `read_committed`) and exposes metrics on `:8000/metrics`.

| Metric | Type | Description |
|---|---|---|
| `supply_alerts_total{anomaly_type}` | counter | Alerts by type |
| `supply_alerts_by_vehicle_total{vehicle_id,anomaly_type}` | counter | Alerts by vehicle and type |
| `supply_alert_duration_seconds` | histogram | Episode length |
| `supply_alert_detection_delay_seconds` | histogram | Kafka write time minus episode end (how late Flink reports) |
| `supply_alert_last_timestamp_seconds` | gauge | Time of the most recent alert |
| `supply_alert_bad_messages_total`, `supply_alert_kafka_errors_total` | counters | Pipeline-health signals |

All counters are initialised at 0 for every known type and vehicle, because Prometheus `increase()`/`rate()` would otherwise miss the very first alert of each series.

### Dashboard "Supply Chain – Flink Alerts"

Panels: alerts in the selected window · seconds since last alert · exporter up · unreadable messages · alerts per 5 min by type · alerts by type · top 10 vehicles · top 10 vehicles by type · episode length p95 · detection delay p95.

The dashboard is **dashboard-as-code**: edit and regenerate it instead of clicking in the UI.

```bash
pip install grafanalib
python monitoring/dashboard-as-code/build_dashboard.py   # rewrites monitoring/grafana/dashboards/supply-chain-alerts.json
```

Grafana reloads the JSON within ~30 s. Counters reset to 0 if the exporter container restarts.

---

## 12. Evaluating detection quality

Because the generator knows which readings are real anomalies, you can **measure the detector**:

```bash
docker exec generator python score_alerts.py \
  --bootstrap kafka:29092 --truth /data/ground_truth.jsonl
```

`score_alerts.py` reports **precision and recall per anomaly type**, scored per episode:

* Only readings that actually reached Kafka count (offline-gap losses are excluded).
* An episode that meets the minimum duration **must** raise an alert (a miss lowers recall).
* An alert that overlaps no true episode of the same shipment and type (for example one triggered by a harmless blip) is a false positive.
* Episodes ending in the last `--grace-seconds` of the stream are ignored, since their alert is not yet due.
* Typical misses come from events that arrived after the watermark. This is a deliberate trade-off: they stay in Bronze.

---

## 13. The closed loop (brief)

The platform is *closed-loop*: nothing is ordered by hand.

```
sites consume stock → Flink sees a shortage and creates an order → Dispatcher assigns a free truck
→ truck drives (telemetry) → delivery arrives, stock rises, order closes
```

* **Replenishment** (`04_replenishment.sql`) raises an order when stock drops below the reorder point or projected stock-out is sooner than the supplier lead time. At most one order is open per site and product, and order IDs are deterministic, so replays can't create duplicates.
* **Dispatcher** (`Dispatcher.py`) allocates the lowest-numbered free, suitable truck to each order. It is a service rather than Flink SQL because allocating a truck consumes it, which needs shared mutable state. It rebuilds its state from Kafka on restart.
* Why it matters for analytics: it makes shipment and telemetry volume *emerge* from simulated demand instead of being scripted. Raise `DEMAND_SCALE` to get more trucks on the road.

---

## 14. Configuration reference

All simulation settings live in `.env` (inside Docker, `BOOTSTRAP` and `DATA_DIR` are overridden by Compose).

| Variable | Default in `.env` | Meaning |
|---|---|---|
| `BOOTSTRAP` | `localhost:9092` | Kafka address for scripts run on the host |
| `TOPIC_PARTITIONS`, `REPLICATION_FACTOR` | 3, 1 | Topic creation |
| `START_WAREHOUSE_ID` | `WH-11` | The hub (Ismailia) |
| `FLEET_SIZE` | 30 | Trucks `TRUCK-1 … TRUCK-N` |
| `SEND_INTERVAL_SECONDS` | 5 | Telemetry cadence per truck |
| `TRIP_DURATION_SECONDS` | 200 | Trip length at speed 1.0 |
| `SIMULATION_SPEED` | 1.0 | Trip speed multiplier |
| `TURNAROUND_SECONDS` | 2 | Time before an arrived truck can be reused |
| `DEMAND_SCALE` | 0.2 | Demand intensity (1.0 ≈ one order / 4 s, ~58 trucks on road; 0.2 ≈ 12 trucks) |
| `RANDOM_SEED` | 42 | Repeatable simulations |
| `ENABLE_ANOMALIES` | true | Master switch |
| `ANOMALY_PROBABILITY` | 0.06 | Chance per reading that a new anomaly starts |
| `BLIP_PROBABILITY` | 0.01 | Chance per reading of a harmless blip |
| `ENABLE_TEMPERATURE_BREACH`, `ENABLE_IDLING`, `ENABLE_ROUTE_DEVIATION`, `ENABLE_OVERSPEED` | true | Per-anomaly switches |
| `ENABLE_MESSY_DATA` | true | Master switch for the defects below |
| `JITTER_SECONDS`, `DUPLICATE_PROBABILITY`, `LATE_PROBABILITY`, `LATE_WITHIN_WATERMARK_RATIO`, `MAX_LATE_SECONDS`, `OFFLINE_PROBABILITY`, `OFFLINE_DROP_RATIO` | 0.3, 0.02, 0.03, 0.7, 45, 0.004, 0.3 | Messy-data tuning |
| `DOCKER_SOCK`, `DOCKER_GID` | n/a | Linux only, for Airflow |

Detection thresholds are not environment variables: change them directly in `flink-sql/03_detection.sql` and resubmit the job.

---

## 15. Operations cheat-sheet

```bash
# Start / stop
docker compose up -d --build
docker compose down                 # keeps data
docker compose down -v              # DELETES volumes (Bronze, Silver, Gold, Kafka, metrics)

# Full reset
python wipe_all.py --yes --up       # wipe everything, restart empty
python wipe_all.py --hdfs-only      # keep Kafka/containers, empty only HDFS

# Submit a Flink job by hand
docker exec flink-jobmanager ./bin/sql-client.sh -f /opt/flink/sql/03_detection.sql

# Run a Spark job by hand
docker exec supply-chain-spark /opt/spark/bin/spark-submit --master 'local[*]' /opt/spark-jobs/gold_kpi_suite.py

# Run the debugging mode for one app
docker compose stop dispatcher && python Dispatcher.py     # host .env uses localhost:9092

# Inspect Kafka and HDFS
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list
docker exec namenode hdfs dfs -ls /warehouse/silver
```

**Troubleshooting**

| Symptom | Check |
|---|---|
| No Flink jobs after 5+ min | Airflow `platform_guard` logs; on Linux, `DOCKER_SOCK` / `DOCKER_GID` in `.env` |
| `lakehouse_batch` stuck on `wait_for_silver` | Flink UI: is `silver` RUNNING and completing checkpoints? Then look for `.parquet` under `/warehouse/silver/telemetry/data` |
| Silver quality gate failed | Read the task log; it prints the null counts per column |
| Empty Grafana panels | Prometheus `/targets`; `docker logs alert-exporter`; are there alerts in the topic? |
| Out-of-memory / sluggish | Give Docker ≥ 8 GB or lower `DEMAND_SCALE` / `FLEET_SIZE` |

---

## 16. Design decisions and known limitations

**Design decisions**

* **Bronze = one table for all topics.** Simple to ingest and replay; Spark filters by `kafka_topic`.
* **Hadoop catalog** keeps the stack small (no Hive Metastore / REST catalog), at the cost of weaker concurrency control than a catalog service.
* **Flink does light cleaning, Spark does heavy aggregation.** Streaming handles dedup, ordering and quarantine; batch handles KPIs.
* **Episodes instead of single readings** make alerts actionable and keep dashboards from flooding.
* **Exactly-once alert sink** plus deterministic `alert_id` make alerting idempotent.
* **Dashboard-as-code** keeps Grafana versionable and reviewable.

**Known limitations / things to know**

* **Label leakage:** `silver.telemetry` still carries `temperature_breach_injected`, a generator-only label that a real sensor would not have. The Gold KPIs use it for `temperature_breach_event_count`; for realism, derive it from `reefer_cargo_temp_c` vs the product range instead.
* **Late events** (older than the 10 s Silver watermark) are quarantined, not merged into Silver. They remain in Bronze for reprocessing.
* **Small files:** the 30 s checkpoint commit produces many small Iceberg files. Add a compaction job (Spark `rewrite_data_files`) before running this at scale.
* **Resubmitted Flink jobs replay from the earliest offset**, which can duplicate Bronze rows. Silver and Gold de-duplicate, Bronze does not.
* **Flink checkpoints** live on a local Docker volume, not on HDFS. A savepoint on HDFS is recommended before planned restarts.
* **Single-node everything:** one Kafka broker, one DataNode (replication 1), Flink with one TaskManager, Spark in `local[*]`. This is a development / demo topology, not an HA deployment.
* **Credentials** (Airflow `admin/admin`, Grafana `admin/admin`, a fixed Airflow secret key) are demo defaults. Change them before exposing anything.
* **Counter resets:** exporter metrics restart from 0 whenever the exporter container restarts.
* `down -v` and `wipe_all.py` permanently delete Bronze, Silver and Gold.