from __future__ import annotations

import sys
import os

# Add project root to PYTHONPATH
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, PROJECT_ROOT)


import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from transformations.bronze_to_silver import run_bronze_to_silver  # noqa: E402
from transformations.ingest_raw_to_bronze import run_ingest_raw_to_bronze  # noqa: E402
from transformations.silver_to_gold import run_silver_to_gold  # noqa: E402


with DAG(
    dag_id="lakehouse_pipeline",
    description="Minimal raw-to-gold lakehouse pipeline with Polars, DuckDB, Airflow, and dbt.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lakehouse", "duckdb", "polars"],
) as dag:
    ingest_raw_to_bronze = PythonOperator(
        task_id="ingest_raw_to_bronze",
        python_callable=run_ingest_raw_to_bronze,
        do_xcom_push=False,
    )

    bronze_to_silver = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=run_bronze_to_silver,
        do_xcom_push=False,
    )

    silver_to_gold = PythonOperator(
        task_id="silver_to_gold",
        python_callable=run_silver_to_gold,
        do_xcom_push=False,
    )

    ingest_raw_to_bronze >> bronze_to_silver >> silver_to_gold
