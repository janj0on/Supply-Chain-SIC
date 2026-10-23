import argparse, csv, json, random, time, uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from faker import Faker
fake = Faker() #to generate the fake data
random.seed(42) # makes python sequence the same when you run it
Faker.seed(42)


PRODUCTS = [
    {
        "product_id": "PROD-DAIRY",
        "product_name": "Dairy",
        "min_c": 2,
        "max_c": 6
    },
    {
        "product_id": "PROD-PRODUCE",
        "product_name": "Fresh Produce",
        "min_c": 4,
        "max_c": 10
    },
    {
        "product_id": "PROD-FROZEN",
        "product_name": "Frozen Food",
        "min_c": -18,
        "max_c": -12
    }
]



GOVERNORATES = [
    ("Cairo", 30.0444, 31.2357),
    ("Alexandria", 31.2001, 29.9187),
    ("Giza", 30.0131, 31.2089),
    ("Qalyubia", 30.3292, 31.2165),
    ("Sharqia", 30.5877, 31.5020),
    ("Dakahlia", 31.0364, 31.3807),
    ("Gharbia", 30.8754, 31.0335),
    ("Monufia", 30.5230, 30.9876),
    ("Kafr El Sheikh", 31.1107, 30.9400),
    ("Beheira", 30.8481, 30.3436),
    ("Damietta", 31.4175, 31.8144),
    ("Port Said", 31.2653, 32.3019),
    ("Ismailia", 30.5965, 32.2715),
    ("Suez", 29.9668, 32.5498),
    ("North Sinai", 30.2824, 33.6176),
    ("South Sinai", 28.1099, 33.6116),
    ("Beni Suef", 29.0661, 31.0994),
    ("Fayoum", 29.3084, 30.8428),
    ("Minya", 28.1099, 30.7503),
    ("Assiut", 27.1809, 31.1837),
    ("Sohag", 26.5591, 31.6957),
    ("Qena", 26.1551, 32.7160),
    ("Luxor", 25.6872, 32.6396),
    ("Aswan", 24.0889, 32.8998),
    ("Red Sea", 27.2579, 33.8116),
    ("New Valley", 25.4390, 30.5586),
    ("Matrouh", 31.3543, 27.2373),
]


def iso(dt):
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")



def write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)



def get_json(url):
    req = Request(
        url,
        headers={"User-Agent": "EgyptSupplyChainStudentProject/1.0"}
    )

    with urlopen(req, timeout=20) as r:
        return json.loads(r.read().decode())


    


def main():
    p = argparse.ArgumentParser()

    # Configure the number of records
    p.add_argument("--out-dir", default="data")
    p.add_argument("--vehicles", type=int, default=5000)
    p.add_argument("--shipments", type=int, default=10000)
    p.add_argument("--events", type=int, default=100000)
    p.add_argument(
        "--use-weather",
        action="store_true",
        help="Add real hourly historical weather from Open-Meteo"
    )
    p.add_argument(
        "--use-osrm",
        action="store_true",
        help="Use real road route geometry from OSRM for up to 20 route pairs"
    )

    a, _ = p.parse_known_args()

    if min(a.vehicles, a.shipments, a.events) < 1:
        p.error("counts must be positive")

    out = Path(a.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    # 1. Generate warehouses
    
    warehouses = [
    {
        "warehouse_id": f"WH-{i:02d}",
        "warehouse_name": f"{name} Distribution Center",
        "governorate": name,
        "latitude": latitude,
        "longitude": longitude
    }
    for i, (name, latitude, longitude)
    in enumerate(GOVERNORATES, 1)
 ]

    # 2. Generate vehicles
    vehicles = [
        {
            "vehicle_id": f"TRUCK-{i:05d}",
            "device_id": f"TELEM-{i:05d}",
            "vehicle_type": random.choice([
                "refrigerated_truck",
                "reefer_van",
                "heavy_truck"
            ]),
            "capacity_kg": random.choice([
                1500, 3000, 5000, 10000, 20000
            ]),
            "fuel_type": "diesel",
            "has_obd_device": True,
            "has_reefer_sensor": True
        }
        for i in range(1, a.vehicles + 1)
    ]

    # 3. Generate shipments BEFORE writing shipments.csv
    shipments = []

    for i in range(1, a.shipments + 1):
        origin = random.choice(warehouses)

        dest = random.choice([
            w for w in warehouses
            if w["warehouse_id"] != origin["warehouse_id"]
        ])

        product = random.choice(PRODUCTS)
        vehicle = random.choice(vehicles)

        shipments.append({
            "shipment_id": f"SHP-{i:06d}",
            "order_id": f"ORD-{i:06d}",
            "product_id": product["product_id"],
            "vehicle_id": vehicle["vehicle_id"],
            "origin_warehouse_id": origin["warehouse_id"],
            "destination_warehouse_id": dest["warehouse_id"],
            "quantity_kg": random.randint(
                100, max(100, vehicle["capacity_kg"])
            ),
            "status": random.choice([
                "in_transit",
                "in_transit",
                "delivered",
                "pending"
            ])
        })

    # 4. Write master data CSV files AFTER generating the data
    write_csv(out / "products.csv", PRODUCTS)
    write_csv(out / "vehicles.csv", vehicles)
    write_csv(out / "warehouses.csv", warehouses)
    write_csv(out / "shipments.csv", shipments)

    # Lookup dictionaries for telemetry generation
    wh = {w["warehouse_id"]: w for w in warehouses}
    vh = {v["vehicle_id"]: v for v in vehicles}
    ph = {x["product_id"]: x for x in PRODUCTS}

    route_cache = {}
    weather_cache = {}
    counts = {}

    base = datetime.now(timezone.utc) - timedelta(days=7)

    # 5. Generate telemetry JSONL
    with (out / "telemetry.jsonl").open(
        "w", encoding="utf-8"
    ) as f:

        for i in range(a.events):
            s = shipments[i % len(shipments)]
            v = vh[s["vehicle_id"]]
            prod = ph[s["product_id"]]
            o = wh[s["origin_warehouse_id"]]
            d = wh[s["destination_warehouse_id"]]

            count = counts.get(s["shipment_id"], 0)
            counts[s["shipment_id"]] = count + 1
            progress = (count % 100) / 99

            t = base + timedelta(seconds=i)
            routekey = (o["warehouse_id"], d["warehouse_id"])
            route = route_cache.get(routekey)

            # Optional OSRM road geometry
            if (
                a.use_osrm
                and routekey not in route_cache
                and len(route_cache) < 20
            ):
                try:
                    url = (
                        "https://router.project-osrm.org/route/v1/driving/"
                        f"{o['longitude']},{o['latitude']};"
                        f"{d['longitude']},{d['latitude']}"
                        "?overview=full&geometries=geojson"
                    )

                    data = get_json(url)
                    coords = data["routes"][0]["geometry"]["coordinates"]

                    route = (
                        [(lat, lon) for lon, lat in coords],
                        data["routes"][0]["distance"],
                        data["routes"][0]["duration"]
                    )

                    route_cache[routekey] = route
                    time.sleep(0.15)

                except Exception as e:
                    print("OSRM warning:", e)
                    route_cache[routekey] = None

            if route:
                coords, dist, duration = route
                lat, lon = coords[
                    min(len(coords) - 1, int(progress * (len(coords) - 1)))
                ]
                gps_method = "OSRM_road_route"
            else:
                lat = (
                    o["latitude"]
                    + (d["latitude"] - o["latitude"]) * progress
                    + random.uniform(-0.003, 0.003)
                )
                lon = (
                    o["longitude"]
                    + (d["longitude"] - o["longitude"]) * progress
                    + random.uniform(-0.003, 0.003)
                )
                dist = duration = None
                gps_method = "straight_line_simulation"

            # Simulate a temperature breach in approximately 3% of events
            breach = random.random() < 0.03

            if breach:
                temp = random.choice([
                    random.uniform(prod["max_c"] + 1, prod["max_c"] + 8),
                    random.uniform(prod["min_c"] - 8, prod["min_c"] - 1)
                ])
            else:
                temp = random.uniform(prod["min_c"], prod["max_c"])

            speed = max(0, min(120, random.gauss(52, 18)))
            date = t.strftime("%Y-%m-%d")

            event = {
                "event_id": str(uuid.uuid4()),
                "event_time_utc": iso(t),
                "vehicle_id": v["vehicle_id"],
                "device_id": v["device_id"],
                "shipment_id": s["shipment_id"],
                "order_id": s["order_id"],
                "product_id": prod["product_id"],
                "origin_warehouse_id": o["warehouse_id"],
                "destination_warehouse_id": d["warehouse_id"],
                "gps_latitude": round(lat, 6),
                "gps_longitude": round(lon, 6),
                "gps_method": gps_method,
                "speed_kmh": round(speed, 1),
                "heading_degrees": random.randint(0, 359),
                "engine_rpm": (
                    random.randint(650, 2600)
                    if speed > 1 else random.randint(600, 900)
                ),
                "fuel_level_pct": round(random.uniform(5, 100), 1),
                "fuel_consumption_l_per_100km": round(
                    random.uniform(8, 38), 2
                ),
                "obd_coolant_temp_c": round(
                    random.uniform(78, 105), 1
                ),
                "obd_engine_load_pct": round(
                    random.uniform(10, 95), 1
                ),
                "obd_fault_code": (
                    random.choice(["P0300", "P0420", "P0171"])
                    if random.random() < 0.01 else None
                ),
                "reefer_cargo_temp_c": round(temp, 2),
                "reefer_setpoint_c": round(
                    (prod["min_c"] + prod["max_c"]) / 2, 1
                ),
                "cargo_temp_min_c": prod["min_c"],
                "cargo_temp_max_c": prod["max_c"],
                "humidity_pct": round(random.uniform(35, 95), 1),
                "reefer_on": random.random() > 0.02,
                "door_open": random.random() < 0.04,
                "temperature_breach_injected": breach,
                "shipment_status": s["status"],
                "route_progress_pct": round(progress * 100, 1),
                "route_distance_m": round(dist, 1) if dist else None,
                "route_estimated_duration_s": (
                    round(duration, 1) if duration else None
                )
            }

            # Optional historical weather
            if a.use_weather:
                wk = (
                    round(lat * 4) / 4,
                    round(lon * 4) / 4,
                    date
                )

                if wk not in weather_cache:
                    try:
                        q = urlencode({
                            "latitude": wk[0],
                            "longitude": wk[1],
                            "start_date": date,
                            "end_date": date,
                            "hourly": (
                                "temperature_2m,relative_humidity_2m,"
                                "precipitation,wind_speed_10m,weather_code"
                            ),
                            "timezone": "UTC"
                        })

                        weather_cache[wk] = get_json(
                            "https://archive-api.open-meteo.com/v1/archive?"
                            + q
                        )
                        time.sleep(0.1)

                    except Exception as e:
                        print("Weather warning:", e)
                        weather_cache[wk] = None

                wd = weather_cache[wk]

                if wd and wd.get("hourly", {}).get("time"):
                    h = wd["hourly"]
                    target = t.strftime("%Y-%m-%dT%H:00")

                    idx = (
                        h["time"].index(target)
                        if target in h["time"]
                        else min(len(h["time"]) - 1, t.hour)
                    )

                    for field in [
                        "temperature_2m",
                        "relative_humidity_2m",
                        "precipitation",
                        "wind_speed_10m",
                        "weather_code"
                    ]:
                        event["weather_" + field] = h[field][idx]
                else:
                    for field in [
                        "temperature_2m",
                        "relative_humidity_2m",
                        "precipitation",
                        "wind_speed_10m",
                        "weather_code"
                    ]:
                        event["weather_" + field] = None

            f.write(json.dumps(event, separators=(",", ":")) + "\n")

            if (i + 1) % 10000 == 0:
                print(f"Generated {i + 1:,}/{a.events:,} events")

    print(f"Finished. Files written to {out.resolve()}")
    print(
        "Reminder: GPS points use OSRM road geometry only when OSRM succeeds; "
        "vehicle sensor readings are simulated."
    )


if __name__ == "__main__":
    main()
