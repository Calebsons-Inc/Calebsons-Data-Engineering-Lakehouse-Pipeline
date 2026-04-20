from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from transformations.lakehouse_utils import (  # noqa: E402
    BRONZE_PARQUET_PATH,
    RAW_CSV_PATH,
    clean_column_names,
    ensure_directories,
    refresh_warehouse,
)


def run_ingest_raw_to_bronze() -> Path:
    ensure_directories()

    frame = pl.read_csv(RAW_CSV_PATH)
    frame = clean_column_names(frame)

    string_columns = [
        column_name
        for column_name, dtype in frame.schema.items()
        if dtype == pl.String
    ]
    if string_columns:
        frame = frame.with_columns(
            [pl.col(column_name).str.strip_chars().alias(column_name) for column_name in string_columns]
        )

    frame.write_parquet(BRONZE_PARQUET_PATH)
    refresh_warehouse()
    return BRONZE_PARQUET_PATH


def main() -> None:
    output_path = run_ingest_raw_to_bronze()
    print(f"Wrote bronze parquet to {output_path}")


if __name__ == "__main__":
    main()
