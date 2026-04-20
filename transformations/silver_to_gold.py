from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from transformations.lakehouse_utils import (  # noqa: E402
    GOLD_PARQUET_PATH,
    SILVER_PARQUET_PATH,
    ensure_directories,
    refresh_warehouse,
)


def run_silver_to_gold() -> Path:
    ensure_directories()

    frame = pl.read_parquet(SILVER_PARQUET_PATH)
    gold_frame = (
        frame.group_by("category")
        .agg(
            pl.len().alias("transaction_count"),
            pl.col("value").sum().round(2).alias("total_value"),
            pl.col("value").mean().round(2).alias("average_value"),
            pl.col("order_date").max().alias("latest_order_date"),
        )
        .sort("category")
    )

    gold_frame.write_parquet(GOLD_PARQUET_PATH)
    refresh_warehouse()
    return GOLD_PARQUET_PATH


def main() -> None:
    output_path = run_silver_to_gold()
    print(f"Wrote gold parquet to {output_path}")


if __name__ == "__main__":
    main()
