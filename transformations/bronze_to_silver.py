from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from transformations.lakehouse_utils import (  # noqa: E402
    BRONZE_PARQUET_PATH,
    SILVER_PARQUET_PATH,
    ensure_directories,
    refresh_warehouse,
)


def run_bronze_to_silver() -> Path:
    ensure_directories()

    frame = pl.read_parquet(BRONZE_PARQUET_PATH)
    frame = frame.with_columns(
        pl.col("order_id").cast(pl.Int64).alias("id"),
        pl.col("customer_name").str.to_titlecase().alias("name"),
        pl.col("category").str.to_titlecase().alias("category"),
        pl.col("value_usd").cast(pl.Float64).alias("value"),
        pl.col("status").str.to_lowercase().alias("status"),
        pl.col("order_date").str.strptime(pl.Date, "%Y-%m-%d").alias("order_date"),
    ).select("id", "name", "category", "value", "status", "order_date")

    frame = frame.filter(
        pl.col("status").eq("active")
        & pl.col("value").gt(0)
        & pl.col("category").is_not_null()
    )

    frame.write_parquet(SILVER_PARQUET_PATH)
    refresh_warehouse()
    return SILVER_PARQUET_PATH


def main() -> None:
    output_path = run_bronze_to_silver()
    print(f"Wrote silver parquet to {output_path}")


if __name__ == "__main__":
    main()
