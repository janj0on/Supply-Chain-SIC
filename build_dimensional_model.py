#!/usr/bin/env python3
"""
Build a local dimensional model from the Supply-Chain-SIC data folder.

Inputs are read-only. Outputs are written to ./dimensional_model/output.
Requires pandas. CSV outputs are used so pyarrow/Parquet is not required.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys
import pandas as pd

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
OUT = ROOT / "dimensional_model" / "output"


def read_jsonl(name: str) -> pd.DataFrame:
    path = DATA / name
    if not path.exists():
        print(f"WARNING: missing {path}; using an empty table")
        return pd.DataFrame()
    if path.stat().st_size == 0:
        return pd.DataFrame()
    return pd.read_json(path, lines=True, convert_dates=False)


def read_csv(name: str) -> pd.DataFrame:
    path = DATA / name
    if not path.exists():
        print(f"WARNING: missing {path}; using an empty table")
        return pd.DataFrame()
    return pd.read_csv(path)


def ensure_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    result = df.copy()
    for col in columns:
        if col not in result.columns:
            result[col] = pd.NA
    return result


def date_key(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, utc=True, errors="coerce")
    keys = parsed.dt.strftime("%Y%m%d")
    return pd.to_numeric(keys, errors="coerce").astype("Int64")


def save_table(name: str, df: pd.DataFrame) -> None:
    df.to_csv(OUT / f"{name}.csv", index=False, encoding="utf-8-sig")
    print(f"Created {name}.csv: {len(df):,} rows")


def main() -> int:
    if not DATA.exists():
        print(f"ERROR: data folder not found: {DATA}")
        print("Place this script in the project root beside the data folder.")
        return 1

    OUT.mkdir(parents=True, exist_ok=True)

    products = read_csv("products.csv")
    vehicles = read_csv("vehicles.csv")
    warehouses = read_csv("warehouses.csv")
    demand = read_jsonl("demand_events.jsonl")
    inventory = read_jsonl("inventory_stock.jsonl")
    dispatch = read_jsonl("dispatch_inbox.jsonl")
    shipment_updates = read_jsonl("shipment_updates.jsonl")
    telemetry = read_jsonl("telemetry.jsonl")
    vehicle_status = read_jsonl("vehicle_status.jsonl")
    ground_truth = read_jsonl("ground_truth.jsonl")

    # ---------------- Dimensions ----------------
    products = ensure_columns(products, ["product_id", "product_name", "min_c", "max_c"])
    products = products.dropna(subset=["product_id"]).drop_duplicates("product_id").copy()
    products = products.sort_values("product_id")
    products.insert(0, "product_key", range(1, len(products) + 1))

    vehicles = ensure_columns(
        vehicles,
        ["vehicle_id", "device_id", "vehicle_type", "capacity_kg", "fuel_type",
         "has_obd_device", "has_reefer_sensor"],
    )
    vehicles = vehicles.dropna(subset=["vehicle_id"]).drop_duplicates("vehicle_id").copy()
    vehicles = vehicles.sort_values("vehicle_id")
    vehicles.insert(0, "vehicle_key", range(1, len(vehicles) + 1))

    warehouses = ensure_columns(
        warehouses, ["warehouse_id", "warehouse_name", "governorate", "latitude", "longitude"]
    )
    warehouses = warehouses.dropna(subset=["warehouse_id"]).drop_duplicates("warehouse_id").copy()
    warehouses = warehouses.sort_values("warehouse_id")
    warehouses.insert(0, "warehouse_key", range(1, len(warehouses) + 1))

    # Build route dimension from every available origin/destination pair.
    route_sources = []
    for df in [dispatch, shipment_updates, telemetry]:
        df = ensure_columns(df, ["origin_warehouse_id", "destination_warehouse_id"])
        route_sources.append(df[["origin_warehouse_id", "destination_warehouse_id"]])
    routes = pd.concat(route_sources, ignore_index=True) if route_sources else pd.DataFrame()
    routes = ensure_columns(routes, ["origin_warehouse_id", "destination_warehouse_id"])
    routes = routes.dropna(subset=["origin_warehouse_id", "destination_warehouse_id"])
    routes = routes.drop_duplicates().sort_values(
        ["origin_warehouse_id", "destination_warehouse_id"]
    ).reset_index(drop=True)
    routes.insert(0, "route_key", range(1, len(routes) + 1))

    # Date dimension from observed event timestamps (plus a continuous date range).
    date_series = []
    for df in [demand, inventory, telemetry]:
        if "event_time_utc" in df.columns:
            date_series.append(pd.to_datetime(df["event_time_utc"], utc=True, errors="coerce").dropna())
    if date_series and sum(len(x) for x in date_series) > 0:
        all_dates = pd.concat(date_series).dt.floor("D")
        days = pd.date_range(all_dates.min(), all_dates.max(), freq="D", tz="UTC")
    else:
        days = pd.DatetimeIndex([], tz="UTC")
    dim_date = pd.DataFrame({"date": days.strftime("%Y-%m-%d")})
    parsed_dates = pd.to_datetime(dim_date["date"], utc=True, errors="coerce")
    dim_date.insert(0, "date_key", pd.to_numeric(parsed_dates.dt.strftime("%Y%m%d"), errors="coerce").astype("Int64"))
    dim_date["year"] = parsed_dates.dt.year.astype("Int64")
    dim_date["quarter"] = parsed_dates.dt.quarter.astype("Int64")
    dim_date["month"] = parsed_dates.dt.month.astype("Int64")
    dim_date["month_name"] = parsed_dates.dt.month_name()
    dim_date["day_of_month"] = parsed_dates.dt.day.astype("Int64")
    dim_date["day_of_week"] = parsed_dates.dt.dayofweek.astype("Int64")
    dim_date["day_name"] = parsed_dates.dt.day_name()
    dim_date["is_weekend"] = parsed_dates.dt.dayofweek >= 5

    # Maps from business IDs to dimension surrogate keys.
    product_map = products.set_index("product_id")["product_key"].to_dict() if len(products) else {}
    vehicle_map = vehicles.set_index("vehicle_id")["vehicle_key"].to_dict() if len(vehicles) else {}
    warehouse_map = warehouses.set_index("warehouse_id")["warehouse_key"].to_dict() if len(warehouses) else {}
    route_map = {
        (r.origin_warehouse_id, r.destination_warehouse_id): int(r.route_key)
        for r in routes.itertuples(index=False)
    } if len(routes) else {}
    date_map = dict(zip(dim_date["date"], dim_date["date_key"])) if len(dim_date) else {}

    def add_time_and_date(df: pd.DataFrame, time_col: str = "event_time_utc") -> pd.DataFrame:
        df = df.copy()
        if time_col not in df.columns:
            df[time_col] = pd.NA
        parsed = pd.to_datetime(df[time_col], utc=True, errors="coerce")
        df["event_time_utc"] = parsed.dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ").where(parsed.notna(), pd.NA)
        df["date_key"] = pd.to_numeric(parsed.dt.strftime("%Y%m%d"), errors="coerce").astype("Int64")
        return df

    def add_common_keys(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        if "product_id" in df:
            df["product_key"] = df["product_id"].map(product_map).astype("Int64")
        if "vehicle_id" in df:
            df["vehicle_key"] = df["vehicle_id"].map(vehicle_map).astype("Int64")
        if "site_id" in df:
            df["warehouse_key"] = df["site_id"].map(warehouse_map).astype("Int64")
        if "origin_warehouse_id" in df:
            df["origin_warehouse_key"] = df["origin_warehouse_id"].map(warehouse_map).astype("Int64")
        if "destination_warehouse_id" in df:
            df["destination_warehouse_key"] = df["destination_warehouse_id"].map(warehouse_map).astype("Int64")
        if {"origin_warehouse_id", "destination_warehouse_id"}.issubset(df.columns):
            df["route_key"] = [
                route_map.get((o, d), pd.NA)
                for o, d in zip(df["origin_warehouse_id"], df["destination_warehouse_id"])
            ]
            df["route_key"] = pd.array(df["route_key"], dtype="Int64")
        return df

    # ---------------- Facts ----------------
    demand = ensure_columns(demand, ["event_id", "event_time_utc", "site_id", "product_id", "quantity_kg"])
    demand = add_common_keys(add_time_and_date(demand))
    demand_fact = demand[[
        "event_id", "event_time_utc", "date_key", "site_id", "warehouse_key",
        "product_id", "product_key", "quantity_kg"
    ]].drop_duplicates("event_id")

    inventory = ensure_columns(
        inventory, ["event_id", "event_time_utc", "site_id", "product_id", "event_type",
                    "stock_kg", "reorder_point_kg", "order_up_to_kg", "lead_time_s", "shipment_id"]
    )
    inventory = add_common_keys(add_time_and_date(inventory))
    inventory_fact = inventory[[
        "event_id", "event_time_utc", "date_key", "site_id", "warehouse_key",
        "product_id", "product_key", "event_type", "stock_kg", "reorder_point_kg",
        "order_up_to_kg", "lead_time_s", "shipment_id"
    ]].drop_duplicates("event_id")

    # Preserve source events separately: dispatch and updates do not include event timestamps,
    # so choosing a "latest" status from these files would be an unsupported assumption.
    shipment_frames = []
    for source_name, source_df in [
        ("dispatch_inbox", dispatch),
        ("shipment_updates", shipment_updates),
    ]:
        source_df = ensure_columns(
            source_df, ["shipment_id", "order_id", "product_id", "vehicle_id",
                        "origin_warehouse_id", "destination_warehouse_id", "quantity_kg", "status"]
        ).copy()
        source_df["source_file"] = source_name
        source_df["source_record_key"] = source_name + ":" + source_df["shipment_id"].astype("string")
        shipment_frames.append(source_df)
    shipment_events = pd.concat(shipment_frames, ignore_index=True) if shipment_frames else pd.DataFrame()
    shipment_events = add_common_keys(shipment_events)
    shipment_fact = shipment_events[[
        "source_record_key", "source_file", "shipment_id", "order_id", "product_id",
        "product_key", "vehicle_id", "vehicle_key", "origin_warehouse_id",
        "origin_warehouse_key", "destination_warehouse_id", "destination_warehouse_key",
        "route_key", "quantity_kg", "status"
    ]].drop_duplicates(["source_file", "shipment_id"])

    telemetry = ensure_columns(
        telemetry,
        ["event_id", "event_time_utc", "vehicle_id", "device_id", "shipment_id", "order_id",
         "product_id", "origin_warehouse_id", "destination_warehouse_id", "gps_latitude",
         "gps_longitude", "gps_method", "route_source", "speed_kmh", "heading_degrees",
         "engine_rpm", "fuel_level_pct", "fuel_consumption_l_per_100km", "obd_coolant_temp_c",
         "obd_engine_load_pct", "obd_fault_code", "reefer_cargo_temp_c", "reefer_setpoint_c",
         "cargo_temp_min_c", "cargo_temp_max_c", "humidity_pct", "reefer_on", "door_open",
         "temperature_breach_injected", "shipment_status", "route_progress_pct", "route_distance_m",
         "route_estimated_duration_s", "weather_source", "weather_temperature_2m_c",
         "weather_relative_humidity_2m_pct", "weather_precipitation_mm",
         "weather_wind_speed_10m_kmh", "weather_code"]
    )
    telemetry = add_common_keys(add_time_and_date(telemetry))
    if not ground_truth.empty:
        ground_truth = ensure_columns(ground_truth, ["event_id", "is_anomaly", "anomaly_type"])
        ground_truth = ground_truth.drop_duplicates("event_id")
        telemetry = telemetry.merge(
            ground_truth[["event_id", "is_anomaly", "anomaly_type"]],
            on="event_id", how="left", validate="many_to_one"
        )
    else:
        telemetry["is_anomaly"] = pd.NA
        telemetry["anomaly_type"] = pd.NA

    telemetry_fact = telemetry.drop_duplicates("event_id").copy()
    telemetry_fact.insert(0, "telemetry_fact_key", range(1, len(telemetry_fact) + 1))

    vehicle_status = ensure_columns(
        vehicle_status, ["event_id", "event_time_utc", "vehicle_id", "status", "shipment_id"]
    )
    vehicle_status = add_common_keys(add_time_and_date(vehicle_status))
    vehicle_status_fact = vehicle_status[[
        "event_id", "event_time_utc", "date_key", "vehicle_id", "vehicle_key", "status", "shipment_id"
    ]].drop_duplicates("event_id")

    # ---------------- Write output ----------------
    for name, df in [
        ("dim_date", dim_date),
        ("dim_product", products),
        ("dim_vehicle", vehicles),
        ("dim_warehouse", warehouses),
        ("dim_route", routes),
        ("fact_demand", demand_fact),
        ("fact_inventory", inventory_fact),
        ("fact_shipment", shipment_fact),
        ("fact_telemetry", telemetry_fact),
        ("fact_vehicle_status", vehicle_status_fact),
    ]:
        save_table(name, df)

    # Ground truth remains a separate validation reference table, preserving its grain.
    if not ground_truth.empty:
        save_table("reference_ground_truth", ground_truth.drop_duplicates("event_id"))

    # Validation report: counts, duplicate source IDs, and unmatched dimension references.
    checks = []
    def check(label: str, value) -> None:
        checks.append({"check": label, "result": str(value)})

    check("input_demand_rows", len(demand))
    check("input_inventory_rows", len(inventory))
    check("input_dispatch_rows", len(dispatch))
    check("input_shipment_update_rows", len(shipment_updates))
    check("input_telemetry_rows", len(telemetry))
    check("input_vehicle_status_rows", len(vehicle_status))
    check("ground_truth_rows", len(ground_truth))
    check("dim_product_rows", len(products))
    check("dim_vehicle_rows", len(vehicles))
    check("dim_warehouse_rows", len(warehouses))
    check("dim_route_rows", len(routes))
    check("fact_telemetry_rows", len(telemetry_fact))
    check("telemetry_rows_without_ground_truth_match", int(telemetry_fact["is_anomaly"].isna().sum()))
    check("demand_rows_without_product_key", int(demand_fact["product_key"].isna().sum()))
    check("demand_rows_without_warehouse_key", int(demand_fact["warehouse_key"].isna().sum()))
    check("inventory_rows_without_product_key", int(inventory_fact["product_key"].isna().sum()))
    check("inventory_rows_without_warehouse_key", int(inventory_fact["warehouse_key"].isna().sum()))
    check("telemetry_rows_without_vehicle_key", int(telemetry_fact["vehicle_key"].isna().sum()))
    check("telemetry_rows_without_route_key", int(telemetry_fact["route_key"].isna().sum()))
    pd.DataFrame(checks).to_csv(OUT / "validation_report.csv", index=False, encoding="utf-8-sig")

    metadata = {
        "input_directory": str(DATA),
        "output_directory": str(OUT),
        "table_grains": {
            "dim_date": "one row per UTC calendar date observed across timestamped source events",
            "dim_product": "one row per product_id",
            "dim_vehicle": "one row per vehicle_id",
            "dim_warehouse": "one row per warehouse_id",
            "dim_route": "one row per unique origin_warehouse_id and destination_warehouse_id pair",
            "fact_demand": "one row per unique demand event_id",
            "fact_inventory": "one row per unique inventory event_id",
            "fact_shipment": "one row per shipment record per source file; dispatch and update records remain separate",
            "fact_telemetry": "one row per unique telemetry event_id, enriched from ground_truth by event_id when matched",
            "fact_vehicle_status": "one row per unique vehicle-status event_id",
            "reference_ground_truth": "one row per unique ground-truth event_id; reference/validation data, not an operational fact",
        },
        "note": "CSV outputs only; source files were not modified. Null foreign keys indicate missing dimension matches.",
    }
    (OUT / "model_metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    print(f"\nFinished. Model output folder: {OUT}")
    print("Review validation_report.csv before using the tables in Power BI.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
