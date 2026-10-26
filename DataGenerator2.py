"""
DataGenerator.py  (closed-loop version)

This script simulates the PHYSICAL world. It does not decide anything.

    Sites consume stock  ->  Flink notices a shortage and creates an order
    ->  Dispatcher picks a vehicle  ->  the shipment comes back to this script
    ->  a truck drives (telemetry)  ->  it arrives and the stock goes up again.

Files written to data/ (KafkaProducer.py sends them to Kafka):
    products.csv  warehouses.csv  vehicles.csv      master data (batch)
    telemetry.jsonl          -> topic vehicle-telemetry   (schema unchanged)
    ground_truth.jsonl       -> not sent, used by score_alerts.py (schema unchanged)
    inventory_stock.jsonl    -> topic inventory-stock
    demand_events.jsonl      -> topic demand-events
    vehicle_status.jsonl     -> topic vehicle-status
    shipment_updates.jsonl   -> topic shipments (status = delivered)

File read from data/ (KafkaProducer.py fills it from the topic `shipments`):
    dispatch_inbox.jsonl     one line per shipment the Dispatcher created

Anomalies now last several events (episodes):
    temperature_breach  fridge out of range for ~35-65 s
    excessive_idling    engine on, speed 0, for ~35-65 s
    route_deviation     truck drifts away from the corridor line for ~15-40 s
    overspeed           one event
  plus short harmless blips (not anomalies) that a duration rule must ignore.

Messy data is added when events are WRITTEN (the telemetry schema is untouched):
    jitter, duplicates, out-of-order (late) events, offline gaps.
"""

import argparse
import csv
import heapq
import json
import math
import os
import random
import time
import uuid
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# ============================================================
# CONFIGURATION
# ============================================================

def env_bool(name, default):
    return os.getenv(name, str(default)).strip().lower() in ("1", "true", "yes")


OUTPUT_DIR = Path(os.getenv("DATA_DIR", Path(__file__).resolve().parent / "data"))

HUB_WAREHOUSE_ID = os.getenv("START_WAREHOUSE_ID", "WH-11")
FLEET_SIZE = max(1, int(os.getenv("FLEET_SIZE", "5000")))

SEND_INTERVAL_SECONDS = max(0.1, float(os.getenv("SEND_INTERVAL_SECONDS", "5")))
TRIP_DURATION_SECONDS = max(1.0, float(os.getenv("TRIP_DURATION_SECONDS", "200")))
SIMULATION_SPEED = max(0.01, float(os.getenv("SIMULATION_SPEED", "1.0")))
TURNAROUND_SECONDS = max(0.0, float(os.getenv("TURNAROUND_SECONDS", "30")))

# Inventory and demand. DEMAND_SCALE=1 gives about 1 order every 4 s for 27 sites x 4 products.
DEMAND_SCALE = max(0.01, float(os.getenv("DEMAND_SCALE", "1.0")))
DEMAND_MEAN_INTERVAL_SECONDS = 20.0   # per (site, product) before scaling
DEMAND_KG_RANGE = (50.0, 250.0)

# Anomalies. ANOMALY_PROBABILITY = chance, per telemetry event, that a new anomaly starts.
ENABLE_ANOMALIES = env_bool("ENABLE_ANOMALIES", True)
ANOMALY_PROBABILITY = min(1.0, max(0.0, float(os.getenv("ANOMALY_PROBABILITY", "0.01"))))
BLIP_PROBABILITY = min(1.0, max(0.0, float(os.getenv("BLIP_PROBABILITY", "0.01"))))
ENABLE_TEMPERATURE_BREACH = env_bool("ENABLE_TEMPERATURE_BREACH", True)
ENABLE_IDLING = env_bool("ENABLE_IDLING", True)
# Old name kept so your .env keeps working: a GPS jump is now a route deviation.
ENABLE_ROUTE_DEVIATION = env_bool("ENABLE_ROUTE_DEVIATION", env_bool("ENABLE_GPS_JUMP", True))
ENABLE_OVERSPEED = env_bool("ENABLE_OVERSPEED", True)

# Episode length in seconds (events = seconds / SEND_INTERVAL + 1).
EPISODE_SECONDS = {
    "temperature_breach": (35.0, 65.0),   # detector threshold is 30 s
    "excessive_idling":   (35.0, 65.0),   # detector threshold is 30 s
    "route_deviation":    (15.0, 40.0),
    "temperature_blip":   (0.0, 10.0),    # NOT an anomaly: shorter than the threshold
    "idle_blip":          (0.0, 10.0),    # NOT an anomaly
}
DEVIATION_KM = (8.0, 20.0)               # detector limit is 5 km

# Messy data (applied when a telemetry line is written)
ENABLE_MESSY_DATA = env_bool("ENABLE_MESSY_DATA", True)
JITTER_SECONDS = float(os.getenv("JITTER_SECONDS", "0.3"))                 # +/- on event_time_utc
DUPLICATE_PROBABILITY = float(os.getenv("DUPLICATE_PROBABILITY", "0.02"))
LATE_PROBABILITY = float(os.getenv("LATE_PROBABILITY", "0.03"))
LATE_WITHIN_WATERMARK_RATIO = float(os.getenv("LATE_WITHIN_WATERMARK_RATIO", "0.7"))  # 1-8 s late; rest 15-45 s
MAX_LATE_SECONDS = float(os.getenv("MAX_LATE_SECONDS", "45"))
OFFLINE_PROBABILITY = float(os.getenv("OFFLINE_PROBABILITY", "0.004"))     # per event, starts a gap
OFFLINE_SECONDS = (10.0, 25.0)
OFFLINE_DROP_RATIO = float(os.getenv("OFFLINE_DROP_RATIO", "0.3"))          # gap events lost for good

RUN_SECONDS = float(os.getenv("RUN_SECONDS", "0"))  # 0 = run until Ctrl+C (used by tests)

random.seed(int(os.getenv("RANDOM_SEED", "42")))


# ============================================================
# MASTER DATA
# ============================================================

PRODUCTS = [
    {"product_id": "PROD-DAIRY",   "product_name": "Dairy",         "min_c": 2,   "max_c": 6},
    {"product_id": "PROD-PRODUCE", "product_name": "Fresh Produce", "min_c": 4,   "max_c": 10},
    {"product_id": "PROD-FROZEN",  "product_name": "Frozen Food",   "min_c": -18, "max_c": -12},
    {"product_id": "PROD-DRY",     "product_name": "Dry Goods",     "min_c": 5,   "max_c": 35},
]

GOVERNORATES = [
    ("Cairo", 30.0444, 31.2357), ("Alexandria", 31.2001, 29.9187), ("Giza", 30.0131, 31.2089),
    ("Qalyubia", 30.4667, 31.1833), ("Gharbia", 30.8754, 31.0335), ("Dakahlia", 31.0364, 31.3807),
    ("Sharqia", 30.5877, 31.5020), ("Beheira", 30.8481, 30.3435), ("Kafr El Sheikh", 31.1107, 30.9400),
    ("Monufia", 30.5230, 30.9876), ("Ismailia", 30.5965, 32.2715), ("Port Said", 31.2653, 32.3019),
    ("Damietta", 31.4175, 31.8144), ("Suez", 29.9668, 32.5498), ("Faiyum", 29.3084, 30.8428),
    ("Beni Suef", 29.0661, 31.0994), ("Minya", 28.1099, 30.7503), ("Asyut", 27.1809, 31.1837),
    ("Sohag", 26.5591, 31.6957), ("Qena", 26.1551, 32.7160), ("Luxor", 25.6872, 32.6396),
    ("Aswan", 24.0889, 32.8998), ("Red Sea", 27.2579, 33.8116), ("Matrouh", 31.3543, 27.2373),
    ("North Sinai", 30.2824, 33.6176), ("South Sinai", 28.1099, 33.6116), ("New Valley", 25.4390, 30.5586),
]
WAREHOUSE_DEFINITIONS = list(GOVERNORATES) + [("Obour", 30.2280, 31.4810)]

VEHICLE_FIELDS = ["vehicle_id", "device_id", "vehicle_type", "capacity_kg", "fuel_type",
                  "has_obd_device", "has_reefer_sensor"]
SHIPMENT_FIELDS = ["shipment_id", "order_id", "product_id", "vehicle_id", "origin_warehouse_id",
                   "destination_warehouse_id", "quantity_kg", "status"]


# ============================================================
# SMALL HELPERS
# ============================================================

def utc_now(offset_seconds=0.0):
    t = datetime.now(timezone.utc) + timedelta(seconds=offset_seconds)
    return t.isoformat().replace("+00:00", "Z")


def dumps(obj):
    return json.dumps(obj, separators=(",", ":"))


def write_csv(path, rows):
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def read_csv_rows(path):
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def ensure_fleet(path):
    """Keep the vehicles you already have, add new ones until FLEET_SIZE."""
    rows = read_csv_rows(path)
    largest = 0
    for r in rows:
        v = r.get("vehicle_id", "")
        if v.startswith("TRUCK-"):
            try:
                largest = max(largest, int(v[6:]))
            except ValueError:
                pass
    new_rows = []
    n = largest + 1
    while len(rows) + len(new_rows) < FLEET_SIZE:
        new_rows.append({
            "vehicle_id": f"TRUCK-{n:05d}",
            "device_id": f"TELEM-{n:05d}",
            "vehicle_type": random.choice(["refrigerated_truck", "reefer_van", "heavy_truck"]),
            "capacity_kg": random.choice([1500, 3000, 5000, 10000, 20000]),
            "fuel_type": "diesel",
            "has_obd_device": True,
            "has_reefer_sensor": True,
        })
        n += 1
    if not path.exists() or path.stat().st_size == 0:
        write_csv(path, new_rows)
    elif new_rows:
        with path.open("a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=VEHICLE_FIELDS, extrasaction="ignore")
            w.writerows(new_rows)
    return rows + new_rows


# ============================================================
# OPTIONAL EXTERNAL API INTEGRATION
# ============================================================

def get_json(url, timeout=20):
    """Fetch JSON from a public API using only the Python standard library."""
    request = Request(url, headers={"User-Agent": "SupplyChainTelemetryDemo/1.0"})
    with urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def haversine_km(lat1, lon1, lat2, lon2):
    radius = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dlat, dlon = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlon / 2) ** 2
    return 2 * radius * math.asin(min(1.0, math.sqrt(a)))


class RouteProvider:
    """Use real driving routes from the public OSRM demo API, with a safe fallback."""
    def __init__(self, enabled=False, max_pairs=20):
        self.enabled = enabled
        self.max_pairs = max_pairs
        self.cache = {}
        self.api_attempts = 0

    def get_route(self, origin, destination):
        key = (origin["warehouse_id"], destination["warehouse_id"])
        if key in self.cache:
            return self.cache[key]

        straight = haversine_km(origin["latitude"], origin["longitude"],
                                destination["latitude"], destination["longitude"])
        route = {
            "distance_km": round(max(1.0, straight * 1.3), 3),
            "duration_seconds": max(60.0, straight * 1.3 / 55 * 3600),
            "geometry": [[origin["longitude"], origin["latitude"]],
                         [destination["longitude"], destination["latitude"]]],
            "route_source": "simulated",
        }
        if not self.enabled or self.api_attempts >= self.max_pairs:
            self.cache[key] = route
            return route

        coords = (f'{origin["longitude"]},{origin["latitude"]};'
                  f'{destination["longitude"]},{destination["latitude"]}')
        url = f"https://router.project-osrm.org/route/v1/driving/{coords}?overview=full&geometries=geojson&steps=false"
        self.api_attempts += 1
        try:
            payload = get_json(url)
            if payload.get("code") == "Ok" and payload.get("routes"):
                result = payload["routes"][0]
                geometry = result.get("geometry", {}).get("coordinates", [])
                route = {
                    "distance_km": round(result["distance"] / 1000, 3),
                    "duration_seconds": float(result["duration"]),
                    "geometry": geometry or route["geometry"],
                    "route_source": "OSRM",
                }
            else:
                print(f"[OSRM] No route for {key}; using simulated fallback.")
        except (HTTPError, URLError, TimeoutError, ValueError, KeyError, OSError) as exc:
            print(f"[OSRM] {exc}; using simulated fallback.")
        self.cache[key] = route
        return route


class WeatherProvider:
    """Fetch historical hourly weather from Open-Meteo; cache calls and fall back to simulation."""
    def __init__(self, enabled=False, max_locations=20):
        self.enabled = enabled
        self.max_locations = max_locations
        self.cache = {}
        self.locations = set()

    @staticmethod
    def fallback():
        return {
            "weather_source": "simulated",
            "weather_temperature_2m_c": round(random.uniform(15, 35), 1),
            "weather_relative_humidity_2m_pct": random.randint(20, 85),
            "weather_precipitation_mm": round(random.choice([0, 0, 0, 0.1, 0.5, 1.0]), 1),
            "weather_wind_speed_10m_kmh": round(random.uniform(0, 30), 1),
            "weather_code": 0,
        }

    def get_weather(self, latitude, longitude, event_time):
        fallback = self.fallback()
        if not self.enabled:
            return fallback
        lat, lon = round(latitude, 2), round(longitude, 2)
        day = event_time.strftime("%Y-%m-%d")
        key = (lat, lon, day)
        if key in self.cache:
            return self.cache[key]
        location_key = (lat, lon)
        if location_key not in self.locations and len(self.locations) >= self.max_locations:
            self.cache[key] = fallback
            return fallback
        self.locations.add(location_key)
        params = urlencode({
            "latitude": lat, "longitude": lon,
            "start_date": day, "end_date": day,
            "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,weather_code",
            "timezone": "GMT",
        })
        url = "https://archive-api.open-meteo.com/v1/archive?" + params
        try:
            payload = get_json(url)
            hourly = payload.get("hourly", {})
            times = hourly.get("time", [])
            if not times:
                self.cache[key] = fallback
                return fallback
            index = min(range(len(times)), key=lambda i: abs(int(times[i][11:13]) - event_time.hour))
            def value(name):
                values = hourly.get(name, [])
                return values[index] if index < len(values) else None
            temperature = value("temperature_2m")
            if temperature is None:
                self.cache[key] = fallback
                return fallback
            result = {
                "weather_source": "Open-Meteo Archive API",
                "weather_temperature_2m_c": temperature,
                "weather_relative_humidity_2m_pct": value("relative_humidity_2m"),
                "weather_precipitation_mm": value("precipitation"),
                "weather_wind_speed_10m_kmh": value("wind_speed_10m"),
                "weather_code": value("weather_code"),
            }
            self.cache[key] = result
            return result
        except (HTTPError, URLError, TimeoutError, ValueError, KeyError, OSError) as exc:
            print(f"[Open-Meteo] {exc}; using simulated weather.")
            self.cache[key] = fallback
            return fallback


def interpolate_route_geometry(geometry, progress):
    """Interpolate along OSRM [longitude, latitude] geometry, or fallback line."""
    if not geometry:
        return 0.0, 0.0
    if len(geometry) == 1:
        return geometry[0][1], geometry[0][0]
    progress = max(0.0, min(1.0, progress))
    position = progress * (len(geometry) - 1)
    left = min(int(position), len(geometry) - 2)
    fraction = position - left
    lon1, lat1 = geometry[left]
    lon2, lat2 = geometry[left + 1]
    return lat1 + (lat2 - lat1) * fraction, lon1 + (lon2 - lon1) * fraction


# ============================================================
# GEOMETRY (corridor = straight line hub -> destination)
# ============================================================

def corridor_position(origin, destination, progress):
    p = max(0.0, min(1.0, progress))
    return (origin["latitude"] + (destination["latitude"] - origin["latitude"]) * p,
            origin["longitude"] + (destination["longitude"] - origin["longitude"]) * p)


def perpendicular_offset_deg(origin, destination, km):
    """(dlat, dlon) that moves a point `km` kilometres sideways from the corridor line."""
    lat0 = math.radians(origin["latitude"])
    east = (destination["longitude"] - origin["longitude"]) * 111.320 * math.cos(lat0)
    north = (destination["latitude"] - origin["latitude"]) * 110.574
    length = math.hypot(east, north) or 1.0
    pe, pn = -north / length, east / length          # unit vector, 90 degrees to the line
    return (pn * km / 110.574, pe * km / (111.320 * math.cos(lat0)))


# ============================================================
# TRIPS AND TELEMETRY
# ============================================================

def episode_events(kind):
    low, high = EPISODE_SECONDS[kind]
    return max(1, int(random.uniform(low, high) / SEND_INTERVAL_SECONDS) + 1)


def enabled_anomalies():
    kinds = []
    if ENABLE_TEMPERATURE_BREACH:
        kinds.append("temperature_breach")
    if ENABLE_IDLING:
        kinds.append("excessive_idling")
    if ENABLE_ROUTE_DEVIATION:
        kinds.append("route_deviation")
    if ENABLE_OVERSPEED:
        kinds.append("overspeed")
    return kinds


def maybe_start_episode(trip, position):
    """Called once per event when no episode is running. Returns the anomaly type or None."""
    if not ENABLE_ANOMALIES or trip["episode"]:
        return None
    r = random.random()
    kinds = enabled_anomalies()
    kind = None
    if kinds and r < ANOMALY_PROBABILITY:
        kind = random.choice(kinds)
    elif r < ANOMALY_PROBABILITY + BLIP_PROBABILITY:
        kind = random.choice(["temperature_blip", "idle_blip"])
    if kind is None or kind == "overspeed":
        return kind
    ep = {"kind": kind, "left": episode_events(kind) - 1}
    if kind in ("excessive_idling", "idle_blip"):
        ep["frozen"] = position
    if kind == "route_deviation":
        km = random.uniform(*DEVIATION_KM) * random.choice([-1, 1])
        ep["offset"] = perpendicular_offset_deg(trip["origin"], trip["destination"], km)
    trip["episode"] = ep
    return kind


def create_event(trip, now, weather):
    """Build one telemetry event. Same fields as before, nothing added or removed."""
    vehicle, shipment, product = trip["vehicle"], trip["shipment"], trip["product"]
    origin, destination = trip["origin"], trip["destination"]

    # Progress only moves while the truck is driving (idling pauses the trip).
    dt = now - trip["last_tick"]
    trip["last_tick"] = now
    ep = trip["episode"]
    idling_now = ep is not None and ep["kind"] in ("excessive_idling", "idle_blip")
    if not idling_now:
        trip["progress"] = min(1.0, trip["progress"] + dt * SIMULATION_SPEED / TRIP_DURATION_SECONDS)
    progress = trip["progress"]

    lat, lon = interpolate_route_geometry(trip["route"]["geometry"], progress)
    if 0.0 < progress < 1.0:
        lat += random.uniform(-0.0003, 0.0003)
        lon += random.uniform(-0.0003, 0.0003)

    speed = round(max(0, min(120, random.gauss(52, 12))), 1)
    temperature = round(random.uniform(product["min_c"], product["max_c"]), 2)

    kind = None
    label = None            # what ground_truth says (None = normal or harmless blip)
    if ep is None:
        kind = maybe_start_episode(trip, (lat, lon))
        ep = trip["episode"]
        if ep is not None:
            ep["left"] += 1     # the event we are building now is the first of the episode
    else:
        kind = ep["kind"]

    if ep is not None:
        kind = ep["kind"]
        ep["left"] -= 1
        if ep["left"] <= 0:
            trip["episode"] = None

    if kind in ("temperature_breach", "temperature_blip"):
        temperature = round(product["max_c"] + random.uniform(2.0, 6.0), 2)
    elif kind in ("excessive_idling", "idle_blip"):
        speed = 0.0
        lat = ep["frozen"][0] + random.uniform(-0.0001, 0.0001)
        lon = ep["frozen"][1] + random.uniform(-0.0001, 0.0001)
    elif kind == "route_deviation":
        lat += ep["offset"][0]
        lon += ep["offset"][1]
    elif kind == "overspeed":
        speed = 140.0

    if kind in ("temperature_breach", "excessive_idling", "route_deviation", "overspeed"):
        label = kind

    if progress >= 1.0:
        lat, lon = destination["latitude"], destination["longitude"]

    event_id = str(uuid.uuid4())
    jitter = random.uniform(-JITTER_SECONDS, JITTER_SECONDS) if ENABLE_MESSY_DATA else 0.0

    event = {
        "event_id": event_id,
        "event_time_utc": utc_now(jitter),
        "vehicle_id": vehicle["vehicle_id"],
        "device_id": vehicle["device_id"],
        "shipment_id": shipment["shipment_id"],
        "order_id": shipment["order_id"],
        "product_id": product["product_id"],
        "origin_warehouse_id": origin["warehouse_id"],
        "destination_warehouse_id": destination["warehouse_id"],
        "gps_latitude": round(lat, 6),
        "gps_longitude": round(lon, 6),
        "gps_method": "osrm_route_simulation" if trip["route"]["route_source"] == "OSRM" else "straight_line_simulation",
        "route_source": trip["route"]["route_source"],
        "speed_kmh": speed,
        "heading_degrees": random.randint(0, 359),
        "engine_rpm": random.randint(900, 2400) if speed > 1 else 750,
        "fuel_level_pct": round(random.uniform(5, 100), 1),
        "fuel_consumption_l_per_100km": round(random.uniform(8, 38), 2),
        "obd_coolant_temp_c": round(random.uniform(78, 105), 1),
        "obd_engine_load_pct": round(random.uniform(10, 95), 1),
        "obd_fault_code": random.choice(["P0300", "P0420", "P0171"]) if random.random() < 0.01 else None,
        "reefer_cargo_temp_c": temperature,
        "reefer_setpoint_c": round((product["min_c"] + product["max_c"]) / 2, 1),
        "cargo_temp_min_c": product["min_c"],
        "cargo_temp_max_c": product["max_c"],
        "humidity_pct": round(random.uniform(35, 95), 1),
        "reefer_on": True,
        "door_open": random.random() < 0.04,
        "temperature_breach_injected": kind in ("temperature_breach", "temperature_blip"),
        "shipment_status": "delivered" if progress >= 1.0 else "in_transit",
        "route_progress_pct": round(progress * 100, 1),
        "route_distance_m": round(trip["route"]["distance_km"] * 1000, 1),
        "route_estimated_duration_s": round(trip["route"]["duration_seconds"], 2),
        **weather,
    }
    truth = {"event_id": event_id, "is_anomaly": label is not None, "anomaly_type": label}
    return event, truth


# ============================================================
# MESSY DATA: decide WHEN a line reaches the file
# ============================================================

class Outbox:
    """Holds telemetry lines until their release time, so lines can arrive late or twice."""

    def __init__(self, file):
        self.file = file
        self.heap = []
        self.counter = 0
        self.stats = {"events": 0, "late": 0, "duplicates": 0, "gap_buffered": 0, "gap_lost": 0}

    def schedule(self, release_at, line):
        self.counter += 1
        heapq.heappush(self.heap, (release_at, self.counter, line))

    def send(self, trip, line, now):
        self.stats["events"] += 1
        if not ENABLE_MESSY_DATA:
            self.schedule(now, line)
            return

        # offline gap: the device cannot send. It keeps the data (or loses it) and sends later.
        if trip["offline_until"] is None and random.random() < OFFLINE_PROBABILITY:
            trip["offline_until"] = now + random.uniform(*OFFLINE_SECONDS)
            trip["offline_drops"] = random.random() < OFFLINE_DROP_RATIO
        if trip["offline_until"] is not None:
            if trip["offline_drops"]:
                self.stats["gap_lost"] += 1
            else:
                trip["buffer"].append(line)
                self.stats["gap_buffered"] += 1
            return

        delay = 0.0
        if random.random() < LATE_PROBABILITY:
            self.stats["late"] += 1
            if random.random() < LATE_WITHIN_WATERMARK_RATIO:
                delay = random.uniform(1.0, 8.0)                  # inside the 10 s watermark
            else:
                delay = random.uniform(15.0, MAX_LATE_SECONDS)    # after the watermark
        self.schedule(now + delay, line)

        if random.random() < DUPLICATE_PROBABILITY:
            self.stats["duplicates"] += 1
            copy_delay = random.uniform(0.0, 3.0) if random.random() < 0.7 else random.uniform(10.0, 40.0)
            self.schedule(now + delay + copy_delay, line)

    def reconnect_if_due(self, trip, now, force=False):
        until = trip["offline_until"]
        if until is None or (now < until and not force):
            return
        for line in trip["buffer"]:           # burst of old events
            self.schedule(now, line)
        trip["buffer"] = []
        trip["offline_until"] = None

    def release_due(self, now):
        while self.heap and self.heap[0][0] <= now:
            _, _, line = heapq.heappop(self.heap)
            self.file.write(line + "\n")

    def drain(self):
        while self.heap:
            self.file.write(heapq.heappop(self.heap)[2] + "\n")


# ============================================================
# INVENTORY AND DEMAND
# ============================================================

class Inventory:
    def __init__(self, sites, products, stock_file, demand_file, now):
        self.stock_file, self.demand_file = stock_file, demand_file
        lead = int(TRIP_DURATION_SECONDS / SIMULATION_SPEED + TURNAROUND_SECONDS + 30)
        self.items = {}
        for s in sites:
            for p in products:
                up_to = random.choice([4000.0, 6000.0, 8000.0])
                self.items[(s["warehouse_id"], p["product_id"])] = {
                    "stock": round(random.uniform(0.6, 1.0) * up_to, 1),
                    "reorder_point": round(0.35 * up_to, 1),
                    "order_up_to": up_to,
                    "lead_time_s": lead,
                    "next_demand": now + random.uniform(0, DEMAND_MEAN_INTERVAL_SECONDS),
                }
        for key in self.items:
            self.snapshot(key, "initial", None, 0.0)

    def snapshot(self, key, event_type, shipment_id, offset):
        it = self.items[key]
        self.stock_file.write(dumps({
            "event_id": str(uuid.uuid4()),
            "event_time_utc": utc_now(offset),
            "site_id": key[0],
            "product_id": key[1],
            "event_type": event_type,
            "stock_kg": round(it["stock"], 1),
            "reorder_point_kg": it["reorder_point"],
            "order_up_to_kg": it["order_up_to"],
            "lead_time_s": it["lead_time_s"],
            "shipment_id": shipment_id,
        }) + "\n")

    def step(self, now):
        for key, it in self.items.items():
            if now < it["next_demand"]:
                continue
            wanted = round(random.uniform(*DEMAND_KG_RANGE), 1)
            taken = min(wanted, it["stock"])
            it["stock"] -= taken
            it["next_demand"] = now + random.expovariate(DEMAND_SCALE / DEMAND_MEAN_INTERVAL_SECONDS)
            self.demand_file.write(dumps({
                "event_id": str(uuid.uuid4()),
                "event_time_utc": utc_now(),
                "site_id": key[0],
                "product_id": key[1],
                "quantity_kg": wanted,
            }) + "\n")
            self.snapshot(key, "consumption", None, 0.001)   # 1 ms after the demand event

    def receive(self, site_id, product_id, quantity_kg, shipment_id):
        key = (site_id, product_id)
        if key in self.items:
            self.items[key]["stock"] += float(quantity_kg)
            self.snapshot(key, "receipt", shipment_id, 0.0)


# ============================================================
# MAIN LOOP
# ============================================================

class Tail:
    """Read new complete lines from a growing file."""

    def __init__(self, path, skip_existing=True):
        self.path = path
        self.pos = path.stat().st_size if (skip_existing and path.exists()) else 0
        self.pending = b""

    def lines(self):
        if not self.path.exists():
            return []
        size = self.path.stat().st_size
        if size < self.pos:
            self.pos, self.pending = 0, b""
        with self.path.open("rb") as f:
            f.seek(self.pos)
            chunk = f.read()
            self.pos = f.tell()
        if not chunk:
            return []
        parts = (self.pending + chunk).split(b"\n")
        self.pending = parts.pop()
        return [p for p in parts if p.strip()]


def main():
    parser = argparse.ArgumentParser(description="Generate supply-chain telemetry with optional external APIs.")
    parser.add_argument("--use-osrm", action="store_true", help="Fetch real driving routes from OSRM (limited to 20 route pairs).")
    parser.add_argument("--use-weather", action="store_true", help="Fetch historical hourly weather from Open-Meteo where available.")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    warehouses = [{
        "warehouse_id": f"WH-{i:02d}",
        "warehouse_name": f"{name} Distribution Center",
        "governorate": name,
        "latitude": lat,
        "longitude": lon,
    } for i, (name, lat, lon) in enumerate(WAREHOUSE_DEFINITIONS, start=1)]
    wh = {w["warehouse_id"]: w for w in warehouses}
    if HUB_WAREHOUSE_ID not in wh:
        raise ValueError(f"Invalid START_WAREHOUSE_ID={HUB_WAREHOUSE_ID}. Available: {', '.join(wh)}")
    hub = wh[HUB_WAREHOUSE_ID]
    sites = [w for w in warehouses if w["warehouse_id"] != HUB_WAREHOUSE_ID]
    product_by_id = {p["product_id"]: p for p in PRODUCTS}

    write_csv(OUTPUT_DIR / "products.csv", PRODUCTS)
    write_csv(OUTPUT_DIR / "warehouses.csv", warehouses)
    fleet = ensure_fleet(OUTPUT_DIR / "vehicles.csv")
    vehicle_by_id = {v["vehicle_id"]: v for v in fleet}
    busy = set()
    route_provider = RouteProvider(enabled=args.use_osrm, max_pairs=20)
    weather_provider = WeatherProvider(enabled=args.use_weather, max_locations=20)

    inbox = Tail(OUTPUT_DIR / "dispatch_inbox.jsonl", skip_existing=True)
    started_shipments = set()
    available_at = []                       # heap: (time, vehicle_id, shipment_id)
    trips = []

    names = ["telemetry", "ground_truth", "inventory_stock", "demand_events",
             "vehicle_status", "shipment_updates"]
    files = {n: (OUTPUT_DIR / f"{n}.jsonl").open("a", encoding="utf-8") for n in names}
    outbox = Outbox(files["telemetry"])
    now = time.monotonic()
    inventory = Inventory(sites, PRODUCTS, files["inventory_stock"], files["demand_events"], now)

    print(f"Output directory : {OUTPUT_DIR}")
    print(f"Hub              : {HUB_WAREHOUSE_ID} ({hub['warehouse_name']})")
    print(f"Fleet            : {len(fleet)} vehicles | sites: {len(sites)} | products: {len(PRODUCTS)}")
    print(f"Messy data       : {ENABLE_MESSY_DATA} | anomalies: {ENABLE_ANOMALIES} {enabled_anomalies()}")
    print(f"OSRM API         : {args.use_osrm}")
    print(f"Open-Meteo API   : {args.use_weather}")
    print("Waiting for shipments from the Dispatcher... Ctrl+C to stop.\n")

    t_start = time.monotonic()
    last_flush = t_start
    arrived = 0
    try:
        while True:
            now = time.monotonic()
            if RUN_SECONDS and now - t_start > RUN_SECONDS:
                break

            # 1) new shipments from the Dispatcher -> start trips
            for raw in inbox.lines():
                try:
                    s = json.loads(raw)
                    vehicle = vehicle_by_id[s["vehicle_id"]]
                    product = product_by_id[s["product_id"]]
                    destination = wh[s["destination_warehouse_id"]]
                except (ValueError, KeyError) as exc:
                    print(f"[SKIP SHIPMENT] {exc!r}: {raw[:120]!r}")
                    continue
                if s["shipment_id"] in started_shipments or s["vehicle_id"] in busy:
                    continue
                started_shipments.add(s["shipment_id"])
                busy.add(s["vehicle_id"])
                shipment = {k: s.get(k) for k in SHIPMENT_FIELDS}
                route = route_provider.get_route(hub, destination)
                trips.append({
                    "vehicle": vehicle, "shipment": shipment, "product": product,
                    "origin": hub, "destination": destination, "route": route,
                    "progress": 0.0, "last_tick": now, "next_event_at": now, "episode": None,
                    "offline_until": None, "offline_drops": False, "buffer": [],
                })
                print(f"[START] {s['vehicle_id']} {s['shipment_id']} {hub['governorate']} -> "
                      f"{destination['governorate']} {s['quantity_kg']} kg {s['product_id']}")

            # 2) demand at the sites
            inventory.step(now)

            # 3) telemetry from every truck that is due
            finished = []
            for trip in trips:
                outbox.reconnect_if_due(trip, now)
                if now < trip["next_event_at"]:
                    continue
                event_time = datetime.now(timezone.utc)
                weather = weather_provider.get_weather(
                    trip["origin"]["latitude"], trip["origin"]["longitude"], event_time
                )
                event, truth = create_event(trip, now, weather)
                files["ground_truth"].write(dumps(truth) + "\n")
                outbox.send(trip, dumps(event), now)
                if event["shipment_status"] == "delivered":
                    finished.append(trip)
                else:
                    trip["next_event_at"] = now + SEND_INTERVAL_SECONDS * random.uniform(0.9, 1.1)

            # 4) arrivals: stock goes up, shipment is delivered, truck comes back later
            for trip in finished:
                trips.remove(trip)
                outbox.reconnect_if_due(trip, now, force=True)
                shp, veh = trip["shipment"], trip["vehicle"]["vehicle_id"]
                inventory.receive(shp["destination_warehouse_id"], shp["product_id"],
                                  shp["quantity_kg"], shp["shipment_id"])
                files["shipment_updates"].write(dumps({**shp, "status": "delivered"}) + "\n")
                heapq.heappush(available_at, (now + TURNAROUND_SECONDS, veh, shp["shipment_id"]))
                arrived += 1
                print(f"[ARRIVED] {veh} {shp['shipment_id']} -> {trip['destination']['governorate']}")

            while available_at and available_at[0][0] <= now:
                _, veh, shipment_id = heapq.heappop(available_at)
                busy.discard(veh)
                files["vehicle_status"].write(dumps({
                    "event_id": str(uuid.uuid4()), "event_time_utc": utc_now(),
                    "vehicle_id": veh, "status": "available", "shipment_id": shipment_id,
                }) + "\n")

            # 5) write telemetry lines that are due (late ones wait in the outbox)
            outbox.release_due(now)

            if now - last_flush > 0.5:
                for f in files.values():
                    f.flush()
                last_flush = now

            time.sleep(0.01)

    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        outbox.drain()
        for f in files.values():
            f.flush()
            f.close()
        print("\n========== GENERATOR SUMMARY ==========")
        print(f"Trips started: {len(started_shipments)} | arrived: {arrived} | active: {len(trips)}")
        print(f"Telemetry events: {outbox.stats}")
        print(f"OSRM API attempts: {route_provider.api_attempts}")
        print(f"Open-Meteo locations queried: {len(weather_provider.locations)}")


if __name__ == "__main__":
    main()
