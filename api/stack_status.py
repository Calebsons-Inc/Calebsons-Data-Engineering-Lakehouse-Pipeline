from __future__ import annotations

import os
import shutil
import socket
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
WAREHOUSE_PATH = PROJECT_ROOT / "warehouse" / "lakehouse.duckdb"
AIRFLOW_DAG_PATH = PROJECT_ROOT / "airflow" / "dags" / "lakehouse_pipeline.py"
DBT_PROJECT_PATH = PROJECT_ROOT / "dbt" / "dbt_project.yml"
DBT_MANIFEST_PATH = PROJECT_ROOT / "dbt" / "target" / "manifest.json"
DBT_VENV_BIN = PROJECT_ROOT / ".venv-dbt" / "bin" / "dbt"
AIRFLOW_VENV_BIN = PROJECT_ROOT / ".venv-airflow" / "bin" / "airflow"

AIRFLOW_HOST = os.getenv("AIRFLOW_HEALTH_HOST", "127.0.0.1")
AIRFLOW_PORT = int(os.getenv("AIRFLOW_HEALTH_PORT", "8080"))


def _probe_tcp(host: str, port: int, timeout: float = 0.6) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _probe_http(url: str, timeout: float = 1.2) -> tuple[bool, str]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            body = response.read().decode("utf-8", errors="replace")
            if 200 <= response.status < 300:
                return True, body[:240]
            return False, f"HTTP {response.status}"
    except urllib.error.HTTPError as exc:
        # Airflow may redirect unauthenticated users; port is still up.
        if exc.code in {301, 302, 303, 307, 308, 401, 403}:
            return True, f"HTTP {exc.code} (reachable)"
        return False, f"HTTP {exc.code}"
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)


def _check_api() -> dict[str, Any]:
    return {
        "id": "api",
        "label": "FastAPI backend",
        "ok": True,
        "required_for_demos": True,
        "detail": "API process is responding.",
        "hint": "python -m uvicorn api.server:app --reload --port 8000",
    }


def _check_layers() -> dict[str, Any]:
    required = {
        "raw": DATA_DIR / "raw" / "sales_orders.csv",
        "bronze": DATA_DIR / "bronze" / "sales_orders.parquet",
        "silver": DATA_DIR / "silver" / "sales_orders.parquet",
        "gold": DATA_DIR / "gold" / "category_summary.parquet",
    }
    missing = [name for name, path in required.items() if not path.exists()]
    ok = not missing
    return {
        "id": "layers",
        "label": "Lakehouse layers",
        "ok": ok,
        "required_for_demos": True,
        "detail": (
            "Raw, bronze, silver, and gold outputs are present."
            if ok
            else f"Missing layers: {', '.join(missing)}."
        ),
        "hint": (
            "python transformations/ingest_raw_to_bronze.py && "
            "python transformations/bronze_to_silver.py && "
            "python transformations/silver_to_gold.py"
        ),
        "missing": missing,
    }


def _check_warehouse() -> dict[str, Any]:
    ok = WAREHOUSE_PATH.exists()
    return {
        "id": "warehouse",
        "label": "DuckDB warehouse",
        "ok": ok,
        "required_for_demos": True,
        "detail": (
            f"Found {WAREHOUSE_PATH.name}."
            if ok
            else "warehouse/lakehouse.duckdb is missing."
        ),
        "hint": "Run the pipeline transforms so refresh_warehouse() creates the DuckDB file.",
    }


def _check_airflow() -> dict[str, Any]:
    dag_present = AIRFLOW_DAG_PATH.exists()
    port_open = _probe_tcp(AIRFLOW_HOST, AIRFLOW_PORT)
    health_ok = False
    detail_parts: list[str] = []

    if not dag_present:
        detail_parts.append("DAG file missing.")
    else:
        detail_parts.append("DAG file found.")

    if port_open:
        healthy, message = _probe_http(f"http://{AIRFLOW_HOST}:{AIRFLOW_PORT}/health")
        health_ok = healthy
        detail_parts.append(
            f"Webserver on :{AIRFLOW_PORT} reachable."
            if healthy
            else f"Port {AIRFLOW_PORT} open but /health failed ({message})."
        )
    else:
        detail_parts.append(f"Webserver not reachable on {AIRFLOW_HOST}:{AIRFLOW_PORT}.")

    venv_ready = AIRFLOW_VENV_BIN.exists()
    if not venv_ready:
        detail_parts.append(".venv-airflow not installed.")

    ok = dag_present and port_open and health_ok
    return {
        "id": "airflow",
        "label": "Airflow",
        "ok": ok,
        "required_for_demos": True,
        "detail": " ".join(detail_parts),
        "hint": (
            "source .venv-airflow/bin/activate && "
            "export AIRFLOW_HOME=\"$(pwd)/airflow\" && "
            "export AIRFLOW__CORE__DAGS_FOLDER=\"$(pwd)/airflow\" && "
            "airflow webserver --port 8080"
        ),
        "url": f"http://{AIRFLOW_HOST}:{AIRFLOW_PORT}",
    }


def _resolve_dbt_binary() -> str | None:
    if DBT_VENV_BIN.exists():
        return str(DBT_VENV_BIN)
    return shutil.which("dbt")


def _check_dbt() -> dict[str, Any]:
    project_ok = DBT_PROJECT_PATH.exists()
    binary = _resolve_dbt_binary()
    warehouse_ok = WAREHOUSE_PATH.exists()
    manifest_ok = DBT_MANIFEST_PATH.exists()
    detail_parts: list[str] = []

    if not project_ok:
        detail_parts.append("dbt/dbt_project.yml missing.")
    else:
        detail_parts.append("dbt project found.")

    if binary is None:
        detail_parts.append("dbt CLI not found (.venv-dbt or PATH).")
    else:
        detail_parts.append("dbt CLI available.")

    if warehouse_ok:
        detail_parts.append("DuckDB target warehouse present.")
    else:
        detail_parts.append("DuckDB warehouse missing (dbt needs it).")

    if manifest_ok:
        detail_parts.append("Compiled models found (target/manifest.json).")
    else:
        detail_parts.append("No dbt run artifact yet — run dbt once.")

    ok = project_ok and binary is not None and warehouse_ok and manifest_ok
    return {
        "id": "dbt",
        "label": "dbt",
        "ok": ok,
        "required_for_demos": True,
        "detail": " ".join(detail_parts),
        "hint": (
            "source .venv-dbt/bin/activate && "
            "export DBT_DUCKDB_PATH=\"$(pwd)/warehouse/lakehouse.duckdb\" && "
            "dbt run --project-dir dbt --profiles-dir dbt"
        ),
        "manifest_present": manifest_ok,
    }


def _check_scenarios() -> dict[str, Any]:
    from transformations.seed_scenarios import SCENARIOS, SCENARIOS_DIR

    missing: list[str] = []
    for slug, config in SCENARIOS.items():
        gold = SCENARIOS_DIR / slug / "gold" / config["gold_name"]
        if not gold.exists():
            missing.append(slug)

    ok = not missing
    return {
        "id": "scenarios",
        "label": "Industry scenarios",
        "ok": ok,
        "required_for_demos": True,
        "detail": (
            f"All {len(SCENARIOS)} industry datasets are seeded."
            if ok
            else f"Missing scenario data: {', '.join(missing)}."
        ),
        "hint": "python transformations/seed_scenarios.py",
        "missing": missing,
    }


def build_stack_status() -> dict[str, Any]:
    services = [
        _check_api(),
        _check_layers(),
        _check_warehouse(),
        _check_scenarios(),
        _check_airflow(),
        _check_dbt(),
    ]
    required = [service for service in services if service["required_for_demos"]]
    demos_ready = all(service["ok"] for service in required)
    return {
        "checked_at": datetime.now().isoformat() + "Z",
        "demos_ready": demos_ready,
        "services": services,
        "summary": (
            "Stack is ready — open a real-life industry demo."
            if demos_ready
            else "Start Airflow, dbt, seed industry scenarios, and pipeline layers before unlocking demos."
        ),
    }
