from __future__ import annotations

import re
from pathlib import Path

import duckdb
import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
WAREHOUSE_DIR = PROJECT_ROOT / "warehouse"
WAREHOUSE_PATH = WAREHOUSE_DIR / "lakehouse.duckdb"

RAW_CSV_PATH = RAW_DIR / "sales_orders.csv"
BRONZE_PARQUET_PATH = BRONZE_DIR / "sales_orders.parquet"
SILVER_PARQUET_PATH = SILVER_DIR / "sales_orders.parquet"
GOLD_PARQUET_PATH = GOLD_DIR / "category_summary.parquet"


def ensure_directories() -> None:
    for directory in (RAW_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR, WAREHOUSE_DIR):
        directory.mkdir(parents=True, exist_ok=True)


def clean_column_name(column_name: str) -> str:
    cleaned = column_name.strip().lower()
    cleaned = re.sub(r"[^a-z0-9]+", "_", cleaned)
    return cleaned.strip("_")


def clean_column_names(frame: pl.DataFrame) -> pl.DataFrame:
    renamed_columns = {
        column_name: clean_column_name(column_name) for column_name in frame.columns
    }
    return frame.rename(renamed_columns)


def sql_string_literal(path: Path) -> str:
    return str(path).replace("'", "''")


def refresh_warehouse() -> Path:
    ensure_directories()
    connection = duckdb.connect(str(WAREHOUSE_PATH))

    if BRONZE_PARQUET_PATH.exists():
        connection.execute(
            f"""
            create or replace view bronze_sales as
            select * from read_parquet('{sql_string_literal(BRONZE_PARQUET_PATH)}')
            """
        )

    if SILVER_PARQUET_PATH.exists():
        connection.execute(
            f"""
            create or replace view silver_sales as
            select * from read_parquet('{sql_string_literal(SILVER_PARQUET_PATH)}')
            """
        )

    if GOLD_PARQUET_PATH.exists():
        connection.execute(
            f"""
            create or replace view gold_category_summary as
            select * from read_parquet('{sql_string_literal(GOLD_PARQUET_PATH)}')
            """
        )

    connection.close()
    return WAREHOUSE_PATH
