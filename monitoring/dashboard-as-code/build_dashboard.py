#!/usr/bin/env python3
"""
build_dashboard.py - builds the Grafana dashboard from Python (library: grafanalib)
and writes the JSON file that Grafana loads automatically.

    pip install grafanalib
    python monitoring/dashboard-as-code/build_dashboard.py

Change a panel here, run the script again, Grafana reloads the file within ~30 s.

How the counts work
-------------------
The bars and the big number do NOT show "everything since the start". They show the alerts of a SLIDING WINDOW
that you choose with the "Count window" drop-down at the top of the dashboard (15m, 1h, 6h, 24h).
Nothing is reset at a fixed time: every alert counts until it is older than the window, then it leaves the count.
"""
import json
import os

from grafanalib.core import (
    BarGauge, Dashboard, GridPos, Stat, Target, Template, Templating, Threshold, Time, TimeSeries,
)
from grafanalib._gen import DashboardEncoder

DS = "Prometheus"  # name of the data source (see grafana/provisioning/datasources)
OUT = os.path.join(os.path.dirname(__file__), "..", "grafana", "dashboards", "supply-chain-alerts.json")

# ---- the "Count window" drop-down --------------------------------------------------------------
WINDOWS = ["15m", "1h", "6h", "24h"]
DEFAULT_WINDOW = "1h"
W = "$window"  # Grafana puts the chosen window here, e.g. increase(...[1h])


def q(expr, legend="", instant=False):
    return Target(expr=expr, legendFormat=legend, datasource=DS, refId="A", instant=instant)


# Stat and bar panels must show the CURRENT value (last), not the average of the time range (grafanalib default).
LAST = "lastNotNull"


def bar(full, yellow_from, red_from):
    """Bar settings: green below yellow_from, yellow from yellow_from, red from red_from; a full bar = `full` alerts."""
    return dict(
        calc=LAST, decimals=0, min=0, max=full,
        thresholds=[Threshold("green", 0, 0.0), Threshold("yellow", 1, float(yellow_from)), Threshold("red", 2, float(red_from))],
    )


# ---- change these numbers to move the colours (they count alerts inside the chosen window) ------
# green from 0, yellow from YELLOW_AT, red from RED_AT; a completely full bar = FULL alerts (FULL must be >= RED_AT)
YELLOW_AT, RED_AT, FULL = 120, 180, 240
TYPE_BAR = bar(full=FULL, yellow_from=YELLOW_AT, red_from=RED_AT)      # chart 1: alerts by type
VEHICLE_BAR = bar(full=FULL, yellow_from=YELLOW_AT, red_from=RED_AT)   # chart 2: top 10 vehicles (bar)
CELL_YELLOW, CELL_RED = YELLOW_AT, RED_AT                              # chart 3: top 10 vehicles (table cells + Total)

ALERT_TYPES = ["temperature_breach", "excessive_idling", "route_deviation", "overspeed"]  # columns of the table

# ---- the queries --------------------------------------------------------------------------------
# increase() over a sliding window; round() removes the small decimals Prometheus adds when it extrapolates.
TYPE_COUNT = f"sum by (anomaly_type) (round(increase(supply_alerts_total[{W}])))"
VEHICLE_COUNT = f"sum by (vehicle_id) (round(increase(supply_alerts_by_vehicle_total[{W}])))"
TOP10_VEHICLES = f"topk(10, {VEHICLE_COUNT} > 0)"
# every (vehicle, type) pair, but only for the vehicles that are in the top 10
TOP10_BY_TYPE = (
    f"sum by (vehicle_id, anomaly_type) (round(increase(supply_alerts_by_vehicle_total[{W}]))) > 0 "
    f"and on (vehicle_id) {TOP10_VEHICLES}"
)

panels = [
    # ---- row 1: numbers ----
    Stat(
        title="Alerts (last $window)",
        dataSource=DS,
        targets=[q(f"sum(round(increase(supply_alerts_total[{W}])))")],
        gridPos=GridPos(h=4, w=6, x=0, y=0),
        reduceCalc=LAST,
        decimals=0,
    ),
    Stat(
        title="Seconds since last alert",
        dataSource=DS,
        targets=[q("time() - supply_alert_last_timestamp_seconds > 0")],
        gridPos=GridPos(h=4, w=6, x=6, y=0),
        reduceCalc=LAST,
        format="s",
        decimals=0,
    ),
    Stat(
        title="Exporter up (1 = yes)",
        dataSource=DS,
        targets=[q("up{job=\"alert-exporter\"}")],
        gridPos=GridPos(h=4, w=6, x=12, y=0),
        reduceCalc=LAST,
    ),
    Stat(
        title="Unreadable alert messages",
        dataSource=DS,
        targets=[q("sum(supply_alert_bad_messages_total)")],
        gridPos=GridPos(h=4, w=6, x=18, y=0),
        reduceCalc=LAST,
        decimals=0,
    ),
    # ---- row 2: alerts over time and by type ----
    TimeSeries(
        title="Alerts per 5 min, by type",
        dataSource=DS,
        targets=[q("sum by (anomaly_type) (increase(supply_alerts_total[5m]))", "{{anomaly_type}}")],
        gridPos=GridPos(h=8, w=16, x=0, y=4),
        lineWidth=2,
        fillOpacity=20,
    ),
    BarGauge(
        title="Alerts by type (last $window)",
        dataSource=DS,
        targets=[q(TYPE_COUNT, "{{anomaly_type}}", instant=True)],
        gridPos=GridPos(h=8, w=8, x=16, y=4),
        **TYPE_BAR,
        orientation="horizontal",
    ),
    # ---- row 3: the top 10 vehicles (bar = total per vehicle, table = the same vehicles split by type) ----
    BarGauge(
        title="Top 10 vehicles by alerts (last $window)",
        dataSource=DS,
        targets=[q(TOP10_VEHICLES, "{{vehicle_id}}", instant=True)],
        gridPos=GridPos(h=11, w=8, x=0, y=12),
        **VEHICLE_BAR,
        orientation="horizontal",
    ),
    # (the table is added below, after the dashboard JSON is built)
    # ---- row 4: how long and how late ----
    TimeSeries(
        title="Episode length p95 (seconds)",
        dataSource=DS,
        targets=[q(
            "histogram_quantile(0.95, sum by (le, anomaly_type) (rate(supply_alert_duration_seconds_bucket[10m])))",
            "{{anomaly_type}}",
        )],
        gridPos=GridPos(h=8, w=12, x=0, y=23),
        unit="s",
    ),
    TimeSeries(
        title="Detection delay p95 (seconds)",
        dataSource=DS,
        targets=[q(
            "histogram_quantile(0.95, sum by (le, anomaly_type) (rate(supply_alert_detection_delay_seconds_bucket[10m])))",
            "{{anomaly_type}}",
        )],
        gridPos=GridPos(h=8, w=12, x=12, y=23),
        unit="s",
    ),
]

dashboard = Dashboard(
    title="Supply Chain - Flink Alerts",
    uid="supply-chain-alerts",
    description="Alerts written by flink-sql/03_detection.sql to the Kafka topic alerts",
    time=Time("now-1h", "now"),
    refresh="10s",
    timezone="utc",
    panels=panels,
    templating=Templating(list=[
        Template(name="window", label="Count window", type="interval", query=",".join(WINDOWS),
                 default=DEFAULT_WINDOW, auto=False),
    ]),
    schemaVersion=39,
).auto_panel_ids()


def steps(yellow, red):
    return {"mode": "absolute", "steps": [
        {"color": "green", "value": None}, {"color": "yellow", "value": yellow}, {"color": "red", "value": red}]}


def type_table(panel_id):
    """Rows = the top 10 vehicles, columns = alert types, cell = how many alerts of that type (coloured)."""
    ds = {"type": "prometheus", "uid": "prometheus"}
    return {
        "id": panel_id,
        "type": "table",
        "title": "Top 10 vehicles - alerts by type (last $window)",
        "datasource": ds,
        "gridPos": {"h": 11, "w": 16, "x": 8, "y": 12},
        "targets": [{"datasource": ds, "expr": TOP10_BY_TYPE, "format": "table", "instant": True, "range": False, "refId": "A"}],
        "transformations": [
            # one row per vehicle, one column per alert type
            {"id": "groupingToMatrix",
             "options": {"columnField": "anomaly_type", "rowField": "vehicle_id", "valueField": "Value"}},
            # the matrix cells arrive as TEXT ("1" + "1" = "11"): turn them into numbers before adding them up
            {"id": "convertFieldType",
             "options": {"fields": {}, "conversions": [
                 {"targetField": t, "destinationType": "number"} for t in ALERT_TYPES]}},
            # add a Total column and put the vehicle with most alerts first
            {"id": "calculateField",
             "options": {"mode": "reduceRow", "reduce": {"reducer": "sum"}, "alias": "Total"}},
            {"id": "sortBy", "options": {"fields": {}, "sort": [{"field": "Total", "desc": True}]}},
            {"id": "organize", "options": {"renameByName": {"vehicle_id\\anomaly_type": "Vehicle"}}},
        ],
        "fieldConfig": {
            "defaults": {
                "decimals": 0,
                "thresholds": steps(CELL_YELLOW, CELL_RED),
                "custom": {"align": "center", "cellOptions": {"type": "color-background"}},
            },
            "overrides": [
                # the vehicle name column is text: no colour
                {"matcher": {"id": "byType", "options": "string"},
                 "properties": [{"id": "custom.cellOptions", "value": {"type": "auto"}},
                                {"id": "custom.align", "value": "left"}]},
                # Total uses the same colours as the bar on the left
                {"matcher": {"id": "byName", "options": "Total"},
                 "properties": [{"id": "thresholds", "value": steps(VEHICLE_BAR["thresholds"][1].value,
                                                                    VEHICLE_BAR["thresholds"][2].value)},
                                {"id": "custom.cellOptions", "value": {"type": "color-background"}}]},
            ],
        },
        "options": {"showHeader": True, "cellHeight": "sm"},
    }


def build():
    data = json.loads(json.dumps(dashboard.to_json_data(), cls=DashboardEncoder))
    data["panels"].append(type_table(max(p["id"] for p in data["panels"]) + 1))
    # make sure Grafana opens with the default window selected
    for v in data["templating"]["list"]:
        if v["name"] == "window":
            v["current"] = {"selected": True, "text": DEFAULT_WINDOW, "value": DEFAULT_WINDOW}
            v["options"] = [{"selected": w == DEFAULT_WINDOW, "text": w, "value": w} for w in WINDOWS]
    return data


if __name__ == "__main__":
    path = os.path.abspath(OUT)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(build(), f, indent=2)
    print("written:", path)