# Supply-Chain-SIC

A closed-loop supply-chain simulation: **Kafka -> Flink -> Iceberg (Bronze, Silver) on HDFS**.
Spark starts at Silver and builds Gold.

Sites use up stock -> Flink sees a shortage and creates an order (nobody orders by hand) -> the
Dispatcher gives the order a free truck -> the truck drives and sends telemetry every 5 s ->
it arrives, stock goes up, the order is closed, and the truck can be used again.

```
DataGenerator.py  (the physical world: stock, demand, trucks, messy data)
   | files in data/
KafkaProducer.py  (bridge)  <----------------- shipments (status = dispatched)
   |                                                   ^
   v                                                   |
 Kafka topics                                    Dispatcher.py
   |--> 01_bronze.sql        raw copy of every topic -> lake.bronze.raw_events
   |--> 02_silver.sql        telemetry + alerts, deduped, ordered; bad/late -> quarantine
   |--> 03_detection.sql     alerts (episodes with start, end, duration)
   '--> 04_replenishment.sql shortage -> replenishment-triggered + new-orders ---> Dispatcher
```

## The trucks

* The fleet is `TRUCK-1`, `TRUCK-2`, ... `TRUCK-N` (`FLEET_SIZE`, default 30). The same list every run.
* For every new order the Dispatcher picks the **lowest free number**. With 12 trucks on the road you
  see `TRUCK-1` ... `TRUCK-12`. When `TRUCK-1` arrives it is free again (after `TURNAROUND_SECONDS`)
  and takes the next order.
* Every truck is refrigerated and big enough for any order, so dry goods can ride in any truck and
  the numbering never has gaps.
* An active truck sends one telemetry reading every `SEND_INTERVAL_SECONDS` (5 s) from the moment it
  starts until it arrives. The last reading has `shipment_status = delivered`.
* A trip takes `TRIP_DURATION_SECONDS` (200 s) whatever the distance.
* Sensor values are plausible: fuel only goes down during a trip, heading follows the route,
  RPM and engine load follow speed, `route_distance_m` is the straight-line distance in metres.

## Quick start

```bash
pip install -r requirements.txt
python run_all.py --clean      # wipe old state, start the containers, start everything
```

Press **Ctrl+C** to stop. `run_all.py` stops the scripts and cancels the Flink jobs.
(You may see a harmless `KeyboardInterrupt` traceback from a script while it stops.)

| Command | What it does |
|---|---|
| `python run_all.py --clean` | First time, or when you want a fresh start: `down -v`, delete `data/`, start containers, start everything |
| `python run_all.py` | Containers are already up: start generator, bridge, 4 Flink jobs, dispatcher |
| `python run_all.py --down` | Same, and run `docker compose down` when you stop (volumes are kept) |
| `python run_all.py --keep-flink` | Same, but leave the Flink jobs running when you stop |
| `python wipe_all.py` | Delete **everything** (asks you to type YES). See below |

Every script's output is shown with a prefix (`[gen]`, `[bridge]`, `[disp]`, `[flink]`) and also
saved in `logs/`.

### What you should see

1. `[disp] [DISPATCH] ORD-... -> TRUCK-1` : the Dispatcher gave an order a truck
2. `[bridge] [DISPATCH] n shipment(s) -> dispatch_inbox.jsonl` : the bridge passed it to the generator
3. `[gen] [START] TRUCK-1 ...` : the truck is driving, telemetry starts
4. `[gen] [ARRIVED] TRUCK-1 ...` : delivered, stock goes up, the truck is free again

The first order appears within a minute (the sites start close to their reorder point).
Check http://localhost:8082: 4 Flink jobs must be RUNNING.

## Run by hand (one terminal each)

```bash
docker compose -f Flink-Kafka-docker-compose.yml up -d --build
docker ps        # kafka, namenode, datanode, flink-jobmanager, flink-taskmanager = Up
                 # hdfs-init is a one-shot container: it creates /warehouse/bronze and /warehouse/silver, then exits

python DataGenerator.py                 # 1. the world
python KafkaProducer.py                 # 2. the bridge: wait for "[FOLLOWING]" (it creates the topics)

docker exec flink-jobmanager ./bin/sql-client.sh -f /opt/flink/sql/01_bronze.sql        # 3. Flink jobs,
docker exec flink-jobmanager ./bin/sql-client.sh -f /opt/flink/sql/02_silver.sql        #    each one stays
docker exec flink-jobmanager ./bin/sql-client.sh -f /opt/flink/sql/03_detection.sql     #    running after the
docker exec flink-jobmanager ./bin/sql-client.sh -f /opt/flink/sql/04_replenishment.sql #    command ends

python Dispatcher.py                    # 4. the dispatcher
```

Rules: start the bridge **before** the Flink jobs (they need the topics), and **keep the bridge
running**. It forwards a shipment to the generator only while it is running; a shipment created
while it was stopped never starts, and its truck stays "busy" forever. If that happens, do a clean start.

## Delete everything

```bash
python wipe_all.py              # asks for YES, then wipes everything
python wipe_all.py --yes        # no question
python wipe_all.py --up         # wipe, then start empty containers
python wipe_all.py --hdfs-only  # only empty HDFS (Bronze + Silver); Kafka stays
```

A full wipe cancels the Flink jobs, runs `docker compose down -v` (Kafka topics and offsets, HDFS
volumes = Bronze and Silver, Flink checkpoints), removes leftover volumes, and deletes `data/` and
`logs/`. It keeps your code, `.env` and the Docker images. Stop the scripts first.
Do a clean start when you change the design or the fleet: old topics, old trucks and old shipments
in Kafka would otherwise be replayed.

## Settings (`.env`)

| Setting | Default | Meaning |
|---|---|---|
| `FLEET_SIZE` | 30 | number of trucks, `TRUCK-1` ... `TRUCK-N` (the maximum on the road at once) |
| `SEND_INTERVAL_SECONDS` | 5 | telemetry every N seconds per active truck |
| `TRIP_DURATION_SECONDS` | 200 | length of a trip |
| `TURNAROUND_SECONDS` | 2 | time before an arrived truck can be used again |
| `DEMAND_SCALE` | 0.2 | how fast sites use stock. 1.0 = one order every ~4 s (~58 trucks on the road), 0.2 = about 12 trucks. Lower = fewer trucks |
| `ENABLE_MESSY_DATA` | true | jitter, duplicates, late events, offline gaps. Set `false` for a clean 5 s stream |
| `ANOMALY_PROBABILITY` | 0.01 | chance per reading that a new anomaly starts |
| `ENABLE_ANOMALIES` and the four `ENABLE_*` switches | true | which anomaly types are generated |

If orders wait in the queue (`[STATUS] ... waiting orders=N | free vehicles=0`), raise `FLEET_SIZE`
or lower `DEMAND_SCALE`.

## What to look at

| Where | What you should see |
|---|---|
| http://localhost:8082 | 4 jobs RUNNING. Open a job -> Checkpoints: a new completed checkpoint every 30 s |
| http://localhost:9870 -> Utilities -> Browse the file system | `/warehouse/bronze/raw_events/...` and `/warehouse/silver/...` get Parquet files after the first checkpoint |
| `python score_alerts.py --truth data/ground_truth.jsonl` | precision / recall per anomaly type |

```bash
docker exec namenode hdfs dfs -ls -R /warehouse | head -30      # the Bronze and Silver files
docker exec namenode hdfs dfs -du -h /warehouse                 # size of each layer
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic new-orders --from-beginning --max-messages 3
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic alerts --from-beginning --max-messages 3
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic shipments --from-beginning --max-messages 3
```

## Topics

| Topic | Written by | Content |
|---|---|---|
| vehicle-telemetry | bridge | truck readings, one per truck every 5 s |
| inventory-stock | bridge | stock after every consumption / receipt, with reorder point and lead time |
| demand-events | bridge | every consumption at a site |
| vehicle-status | bridge | vehicle available again (after turnaround) |
| vehicles, warehouses, products | bridge | master data |
| alerts | 03_detection | one alert per anomaly episode (start_time, end_time, duration_s) |
| replenishment-triggered | 04_replenishment | why an order was created |
| new-orders | 04_replenishment | the automatic order |
| shipments | Dispatcher (dispatched), bridge (delivered) | order -> vehicle -> trip |
| producer-dlq | bridge | records that could not be sent |

## Detection rules (change the numbers in `03_detection.sql`)

| Rule | Threshold (demo scale) |
|---|---|
| temperature_breach | fridge outside its range for 30 s or more |
| excessive_idling | engine on, speed under 1 km/h, for 30 s or more |
| route_deviation | more than 5 km from the corridor line (hub -> destination) |
| overspeed | above 120 km/h (one reading) |

Readings less than 12 s apart are one episode. The alert is written when the episode is over.

## Files

| File | Role |
|---|---|
| `DataGenerator.py` | the physical world: stock, demand, trucks, telemetry, messy data |
| `KafkaProducer.py` | bridge: files -> Kafka, and dispatched shipments -> `data/dispatch_inbox.jsonl` |
| `Dispatcher.py` | order -> lowest free truck -> shipment (needs shared state, so it is not Flink SQL) |
| `run_all.py` | start everything in the right order, stop with Ctrl+C |
| `wipe_all.py` | delete everything (Kafka, HDFS, Flink state, local files) |
| `score_alerts.py` | compare the `alerts` topic with `data/ground_truth.jsonl` |
| `flink-sql/01..04_*.sql` | Bronze, Silver, detection, replenishment jobs |
| `Flink-Kafka-docker-compose.yml`, `Dockerfile` | Kafka, HDFS, Flink containers |

## Things to know

* The generator makes messy data on purpose when it writes telemetry (schema unchanged): jitter on
  the event time, duplicates, out-of-order (late) events, offline gaps where some readings are lost.
* Bronze keeps everything, including duplicates and late events. Silver drops duplicates and moves
  events that are older than the 10 s watermark to `silver.quarantine` (reason `late_event`).
* `silver.telemetry` still has the column `temperature_breach_injected`. It is a label from the
  generator. Drop it in Gold, a real sensor would not have it.
* Master and state topics (vehicles, shipments, orders, ...) are in Bronze only. Spark builds their
  Silver version (latest row per key).
* Storage is HDFS (one NameNode, one DataNode, replication 1) with the Iceberg **Hadoop catalog**:
  the tables are folders under `hdfs://namenode:8020/warehouse` and there is no catalog service.
  Spark reads the same tables with a Hadoop catalog whose `warehouse` is that path.
* `down -v` deletes the HDFS volumes, so Bronze and Silver are wiped. A plain `down` keeps them.
* Flink checkpoints go to a local Docker volume, not HDFS.
* The trip time is the same for every destination, so the speed field does not match the distance.
* Old files you can delete: `flink-sql/anomaly_detection.sql`, `OrdersOnDemand.py`, `alerts.jsonl`, `flink-lib/`.