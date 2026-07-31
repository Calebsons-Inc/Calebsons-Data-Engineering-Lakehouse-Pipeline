from __future__ import annotations

"""Seed five industry-specific demo datasets under data/scenarios/<slug>/."""

import re
from pathlib import Path

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCENARIOS_DIR = PROJECT_ROOT / "data" / "scenarios"

SCENARIOS: dict[str, dict] = {
    "retail-pos-reporting": {
        "industry": "Retail",
        "entity": "POS sales",
        "raw_name": "pos_sales.csv",
        "bronze_name": "pos_sales.parquet",
        "silver_name": "pos_sales.parquet",
        "gold_name": "category_summary.parquet",
        "group_col": "category",
        "value_col": "amount_usd",
        "id_col": "sale_id",
        "name_col": "store_name",
        "date_col": "sale_date",
        "csv": """Sale ID,Store Name,Category,Amount USD,Status,Sale Date
1,Market Street,Grocery,84.20,active,2026-04-01
2,Market Street,Grocery,36.50,active,2026-04-01
3,Harbor Mall,Apparel,129.00,active,2026-04-02
4,Harbor Mall,Apparel,-12.00,active,2026-04-02
5,Cedar Plaza,Electronics,499.99,inactive,2026-04-03
6,Cedar Plaza,Electronics,219.00,active,2026-04-03
7,Market Street,Home,58.75,active,2026-04-04
""",
    },
    "clinic-appointment-ops": {
        "industry": "Healthcare",
        "entity": "clinic appointments",
        "raw_name": "appointments.csv",
        "bronze_name": "appointments.parquet",
        "silver_name": "appointments.parquet",
        "gold_name": "department_summary.parquet",
        "group_col": "department",
        "value_col": "fee_usd",
        "id_col": "appointment_id",
        "name_col": "patient_name",
        "date_col": "appointment_date",
        "csv": """Appointment ID,Patient Name,Department,Fee USD,Status,Appointment Date
101,Nora Patel,Primary Care,145.00,active,2026-04-01
102,Sam Ortiz,Primary Care,145.00,cancelled,2026-04-01
103,Lee Nguyen,Cardiology,320.00,active,2026-04-02
104,Ava Brooks,Cardiology,-40.00,active,2026-04-02
105,Jonah West,Orthopedics,280.00,no_show,2026-04-03
106,Mia Chen,Orthopedics,280.00,active,2026-04-03
107,Owen Blake,Dermatology,190.50,active,2026-04-04
""",
    },
    "fleet-delivery-rollups": {
        "industry": "Logistics",
        "entity": "fleet deliveries",
        "raw_name": "deliveries.csv",
        "bronze_name": "deliveries.parquet",
        "silver_name": "deliveries.parquet",
        "gold_name": "hub_summary.parquet",
        "group_col": "hub",
        "value_col": "freight_usd",
        "id_col": "shipment_id",
        "name_col": "lane",
        "date_col": "ship_date",
        "csv": """Shipment ID,Lane,Hub,Freight USD,Status,Ship Date
DL-01,Austin→Dallas,Central,410.00,delivered,2026-04-01
DL-02,Austin→Houston,Central,365.00,delivered,2026-04-01
DL-03,Seattle→Portland,Northwest,290.00,delivered,2026-04-02
DL-04,Seattle→Boise,Northwest,-25.00,delivered,2026-04-02
DL-05,Miami→Orlando,Southeast,255.00,failed,2026-04-03
DL-06,Miami→Tampa,Southeast,240.00,delivered,2026-04-03
DL-07,Denver→Salt Lake,Mountain,380.00,delivered,2026-04-04
""",
    },
    "payments-quality-gate": {
        "industry": "Fintech",
        "entity": "card payments",
        "raw_name": "payments.csv",
        "bronze_name": "payments.parquet",
        "silver_name": "payments.parquet",
        "gold_name": "channel_summary.parquet",
        "group_col": "channel",
        "value_col": "amount_usd",
        "id_col": "txn_id",
        "name_col": "merchant",
        "date_col": "txn_date",
        "csv": """Txn ID,Merchant,Channel,Amount USD,Status,Txn Date
TX-9001,Blue Bottle,In-store,18.40,settled,2026-04-01
TX-9002,City Transit,Mobile,3.50,settled,2026-04-01
TX-9003,Northwind Soft,Online,129.00,settled,2026-04-02
TX-9004,Northwind Soft,Online,-9.99,settled,2026-04-02
TX-9005,Corner Market,In-store,62.10,reversed,2026-04-03
TX-9006,Skyline Gym,Mobile,49.00,settled,2026-04-03
TX-9007,Parcel Box,Online,27.80,settled,2026-04-04
""",
    },
    "saas-usage-onboarding": {
        "industry": "SaaS",
        "entity": "subscription usage",
        "raw_name": "usage_events.csv",
        "bronze_name": "usage_events.parquet",
        "silver_name": "usage_events.parquet",
        "gold_name": "product_summary.parquet",
        "group_col": "product",
        "value_col": "seat_value_usd",
        "id_col": "event_id",
        "name_col": "account_name",
        "date_col": "event_date",
        "csv": """Event ID,Account Name,Product,Seat Value USD,Status,Event Date
EV-1,Helio Labs,Analytics,1200.00,active,2026-04-01
EV-2,Helio Labs,Analytics,1200.00,active,2026-04-01
EV-3,North Peak,Workflow,850.00,active,2026-04-02
EV-4,North Peak,Workflow,-100.00,active,2026-04-02
EV-5,Orbit Co,Support Desk,400.00,churned,2026-04-03
EV-6,Orbit Co,Support Desk,400.00,active,2026-04-03
EV-7,Cedar AI,Analytics,1200.00,active,2026-04-04
""",
    },
}

ACTIVE_STATUSES = {
    "retail-pos-reporting": {"active"},
    "clinic-appointment-ops": {"active"},
    "fleet-delivery-rollups": {"delivered"},
    "payments-quality-gate": {"settled"},
    "saas-usage-onboarding": {"active"},
}


def clean_column_name(column_name: str) -> str:
    cleaned = column_name.strip().lower()
    cleaned = re.sub(r"[^a-z0-9]+", "_", cleaned)
    return cleaned.strip("_")


def seed_scenario(slug: str) -> dict[str, Path]:
    config = SCENARIOS[slug]
    root = SCENARIOS_DIR / slug
    raw_dir = root / "raw"
    bronze_dir = root / "bronze"
    silver_dir = root / "silver"
    gold_dir = root / "gold"
    for directory in (raw_dir, bronze_dir, silver_dir, gold_dir):
        directory.mkdir(parents=True, exist_ok=True)

    raw_path = raw_dir / config["raw_name"]
    bronze_path = bronze_dir / config["bronze_name"]
    silver_path = silver_dir / config["silver_name"]
    gold_path = gold_dir / config["gold_name"]

    raw_path.write_text(config["csv"].lstrip(), encoding="utf-8")

    bronze = pl.read_csv(raw_path)
    bronze = bronze.rename({column: clean_column_name(column) for column in bronze.columns})
    bronze.write_parquet(bronze_path)

    id_col = config["id_col"]
    name_col = config["name_col"]
    group_col = config["group_col"]
    value_col = config["value_col"]
    date_col = config["date_col"]
    allowed = ACTIVE_STATUSES[slug]

    silver = bronze.with_columns(
        pl.col(id_col).cast(pl.Utf8).alias("id"),
        pl.col(name_col).cast(pl.Utf8).alias("name"),
        pl.col(group_col).cast(pl.Utf8).str.to_titlecase().alias("group"),
        pl.col(value_col).cast(pl.Float64).alias("value"),
        pl.col("status").cast(pl.Utf8).str.to_lowercase().alias("status"),
        pl.col(date_col).cast(pl.Utf8).str.strptime(pl.Date, "%Y-%m-%d").alias("event_date"),
    ).select("id", "name", "group", "value", "status", "event_date")

    silver = silver.filter(pl.col("status").is_in(list(allowed)) & pl.col("value").gt(0))
    silver.write_parquet(silver_path)

    gold = (
        silver.group_by("group")
        .agg(
            pl.len().alias("transaction_count"),
            pl.col("value").sum().round(2).alias("total_value"),
            pl.col("value").mean().round(2).alias("average_value"),
            pl.col("event_date").max().alias("latest_event_date"),
        )
        .sort("group")
        .rename({"group": "category"})
    )
    gold.write_parquet(gold_path)

    return {
        "raw": raw_path,
        "bronze": bronze_path,
        "silver": silver_path,
        "gold": gold_path,
    }


def seed_all() -> None:
    for slug in SCENARIOS:
        paths = seed_scenario(slug)
        print(f"Seeded {slug}:")
        for layer, path in paths.items():
            print(f"  {layer}: {path.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    seed_all()
