# Calebsons Lakehouse Pipeline Walkthrough

## Overview

This project is a minimal lakehouse pipeline that moves one sample dataset through four stages:

- `data/raw/` stores the original CSV input
- `data/bronze/` stores the cleaned raw data as Parquet
- `data/silver/` stores typed and filtered records as Parquet
- `data/gold/` stores aggregated analytics as Parquet

The pipeline uses:

- `Polars` for fast dataframe work
- `DuckDB` as the local warehouse and query engine
- `Airflow` to orchestrate the three pipeline steps
- `dbt` to build analytics models on top of the DuckDB warehouse

### Bronze, Silver, and Gold

- Bronze: raw CSV loaded into Parquet with cleaned column names
- Silver: typed, renamed, and filtered business-ready rows
- Gold: aggregated category-level summary for reporting

## Setup Instructions

### Python Version

Use Python `3.11`.

### Install Dependencies

The project uses three small Python environments:

```bash
cd /Users/calebthompson/Documents/2/calebsons/calebsons_inc/calebsons_data_engineering_lakehouse_pipeline
python3.11 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

Create the Airflow environment:

```bash
python3.11 -m venv .venv-airflow
source .venv-airflow/bin/activate
AIRFLOW_VERSION=2.8.4
PYTHON_VERSION=3.11
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install --upgrade pip
pip install --constraint "${CONSTRAINT_URL}" -r requirements-airflow.txt
```

Create the dbt environment:

```bash
python3.11 -m venv .venv-dbt
source .venv-dbt/bin/activate
pip install --upgrade pip
pip install -r requirements-dbt.txt
```

### Folder Structure

```text
airflow/                 Airflow DAG definition
dbt/                     dbt project, profile, and models
data/raw/                Source CSV data
data/bronze/             Bronze Parquet output
data/silver/             Silver Parquet output
data/gold/               Gold Parquet output
transformations/         Python ETL scripts
warehouse/               DuckDB warehouse utilities and database file
requirements.txt         Core pipeline dependencies
requirements-airflow.txt Airflow dependencies
requirements-dbt.txt     dbt dependencies
walkthrough.md           This guide
```

## Running the Pipeline Manually

Run the scripts from the project root in this order:

```bash
source .venv/bin/activate
python transformations/ingest_raw_to_bronze.py
python transformations/bronze_to_silver.py
python transformations/silver_to_gold.py
```

Expected outputs:

- `data/bronze/sales_orders.parquet`
- `data/silver/sales_orders.parquet`
- `data/gold/category_summary.parquet`
- `warehouse/lakehouse.duckdb`

What each step does:

- `ingest_raw_to_bronze.py` reads `data/raw/sales_orders.csv`, standardizes column names, and writes bronze Parquet
- `bronze_to_silver.py` casts types, renames columns, filters inactive and invalid records, and writes silver Parquet
- `silver_to_gold.py` aggregates by category and writes gold Parquet

## Running Airflow

Initialize Airflow locally from the project root:

```bash
source .venv-airflow/bin/activate
export AIRFLOW_HOME="$(pwd)/airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/airflow"
airflow db migrate
```

Create an Airflow user:

```bash
source .venv-airflow/bin/activate
airflow users create \
  --username admin \
  --firstname Caleb \
  --lastname Sons \
  --role Admin \
  --email admin@example.com \
  --password admin
```

Start the webserver in one terminal:

```bash
source .venv-airflow/bin/activate
export AIRFLOW_HOME="$(pwd)/airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/airflow"
airflow webserver --port 8080
```

Start the scheduler in another terminal:

```bash
source .venv-airflow/bin/activate
export AIRFLOW_HOME="$(pwd)/airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/airflow"
airflow scheduler
```

Open `http://localhost:8080`, log in with `admin` / `admin`, enable the `lakehouse_pipeline` DAG, and trigger it.

Task behavior:

- `ingest_raw_to_bronze` writes bronze Parquet and refreshes the DuckDB warehouse
- `bronze_to_silver` writes silver Parquet and refreshes the DuckDB warehouse
- `silver_to_gold` writes gold Parquet and refreshes the DuckDB warehouse

You can also trigger it from the CLI:

```bash
source .venv-airflow/bin/activate
export AIRFLOW_HOME="$(pwd)/airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/airflow"
airflow dags trigger lakehouse_pipeline
```

## Running dbt

Run the raw-to-gold scripts first so the DuckDB warehouse and source views exist.

From the project root:

```bash
source .venv-dbt/bin/activate
export DBT_DUCKDB_PATH="$(pwd)/warehouse/lakehouse.duckdb"
dbt run --project-dir dbt --profiles-dir dbt
```

What dbt builds:

- `silver_model` reads from the DuckDB `silver_sales` view
- `gold_model` aggregates from `silver_model`

Useful dbt commands:

```bash
source .venv-dbt/bin/activate
export DBT_DUCKDB_PATH="$(pwd)/warehouse/lakehouse.duckdb"
dbt debug --project-dir dbt --profiles-dir dbt
dbt run --project-dir dbt --profiles-dir dbt
dbt test --project-dir dbt --profiles-dir dbt
```

## Querying the Warehouse

Run the provided query script:

```bash
source .venv/bin/activate
python warehouse/query_lakehouse.py
```

Run a custom query:

```bash
source .venv/bin/activate
python warehouse/query_lakehouse.py --sql "select * from silver_sales order by id"
```

Open DuckDB directly:

```bash
duckdb warehouse/lakehouse.duckdb
```

Example SQL queries:

```sql
show tables;
select * from bronze_sales;
select * from silver_sales order by id;
select * from gold_category_summary order by category;
select category, sum(value) as total_value
from silver_sales
group by category
order by category;
```

## Troubleshooting

### Airflow import errors

Make sure you installed dependencies with the Airflow constraints file and started Airflow from the project root with:

```bash
source .venv-airflow/bin/activate
export AIRFLOW_HOME="$(pwd)/airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/airflow"
```

### dbt cannot find the database

Set the DuckDB path before running dbt:

```bash
source .venv-dbt/bin/activate
export DBT_DUCKDB_PATH="$(pwd)/warehouse/lakehouse.duckdb"
```

If the database file does not exist yet, run:

```bash
source .venv/bin/activate
python transformations/ingest_raw_to_bronze.py
python transformations/bronze_to_silver.py
python transformations/silver_to_gold.py
```

### Reset the pipeline

From the project root:

```bash
source .venv/bin/activate
rm -f warehouse/lakehouse.duckdb warehouse/lakehouse.duckdb.wal
rm -f data/bronze/*.parquet data/silver/*.parquet data/gold/*.parquet
rm -rf airflow/logs airflow/airflow.db
rm -rf dbt/target dbt/logs
```

Then rerun:

```bash
source .venv/bin/activate
python transformations/ingest_raw_to_bronze.py
python transformations/bronze_to_silver.py
python transformations/silver_to_gold.py
python warehouse/query_lakehouse.py
```
