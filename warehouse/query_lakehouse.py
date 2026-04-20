from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from transformations.lakehouse_utils import WAREHOUSE_PATH, refresh_warehouse  # noqa: E402


DEFAULT_SQL = "select * from gold_category_summary order by category"


def run_query(sql: str) -> None:
    refresh_warehouse()
    connection = duckdb.connect(str(WAREHOUSE_PATH))
    cursor = connection.execute(sql)
    columns = [description[0] for description in cursor.description]
    rows = cursor.fetchall()
    widths = [len(column) for column in columns]
    for row in rows:
        for index, value in enumerate(row):
            widths[index] = max(widths[index], len(str(value)))
    header = " | ".join(column.ljust(widths[index]) for index, column in enumerate(columns))
    separator = "-+-".join("-" * width for width in widths)
    print(header)
    print(separator)
    for row in rows:
        print(" | ".join(str(value).ljust(widths[index]) for index, value in enumerate(row)))
    connection.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Query the DuckDB lakehouse.")
    parser.add_argument(
        "--sql",
        default=DEFAULT_SQL,
        help="SQL statement to execute against warehouse/lakehouse.duckdb",
    )
    args = parser.parse_args()
    run_query(args.sql)


if __name__ == "__main__":
    main()
