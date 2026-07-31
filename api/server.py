from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from api.scenarios import get_scenario_config, list_scenario_meta, scenario_layer_paths
from api.stack_status import build_stack_status
from config.sources import get_source, summarize_contracts
from transformations.seed_scenarios import SCENARIOS

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"

_PRIMARY = get_source("sales_orders")
RAW_CSV_PATH = _PRIMARY.raw_path
BRONZE_PARQUET_PATH = _PRIMARY.bronze_path
SILVER_PARQUET_PATH = DATA_DIR / "silver" / "sales_orders.parquet"
GOLD_PARQUET_PATH = DATA_DIR / "gold" / "category_summary.parquet"

app = FastAPI(title="Calebsons Datalake API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

LAYERS = {
    "raw": {"path": RAW_CSV_PATH, "kind": "csv", "label": "Raw"},
    "bronze": {"path": BRONZE_PARQUET_PATH, "kind": "parquet", "label": "Bronze"},
    "silver": {"path": SILVER_PARQUET_PATH, "kind": "parquet", "label": "Silver"},
    "gold": {"path": GOLD_PARQUET_PATH, "kind": "parquet", "label": "Gold"},
}


def _json_value(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if value is None:
        return None
    return value


def _read_layer(layer: str) -> pl.DataFrame:
    meta = LAYERS.get(layer)
    if meta is None:
        raise HTTPException(status_code=404, detail=f"Unknown layer: {layer}")

    path: Path = meta["path"]
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"{layer} data not found at {path.name}")

    if meta["kind"] == "csv":
        return pl.read_csv(path)

    return pl.read_parquet(path)


def _layer_stats(layer: str) -> dict[str, Any]:
    meta = LAYERS[layer]
    path: Path = meta["path"]
    exists = path.exists()
    rows = 0
    columns: list[str] = []
    updated_at = None

    if exists:
        frame = _read_layer(layer)
        rows = frame.height
        columns = list(frame.columns)
        updated_at = datetime.fromtimestamp(path.stat().st_mtime).isoformat()

    return {
        "id": layer,
        "label": meta["label"],
        "exists": exists,
        "rows": rows,
        "columns": columns,
        "updated_at": updated_at,
        "format": meta["kind"],
    }


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/sources")
def sources() -> dict[str, Any]:
    """Raw source contracts: paths, schema names, and freshness SLAs."""
    from config.sources import check_all_sources_freshness

    freshness = [
        {
            "source": result.source,
            "ok": result.ok,
            "warnings": list(result.warnings),
            "errors": list(result.errors),
            "file_age_hours": result.file_age_hours,
            "data_age_hours": result.data_age_hours,
            "checked_at": result.checked_at.isoformat(),
            "details": result.details,
        }
        for result in check_all_sources_freshness()
    ]
    return {"sources": summarize_contracts(), "freshness": freshness}


@app.get("/api/status")
def status() -> dict[str, Any]:
    """Report whether Airflow, dbt, warehouse, and layers are ready for demos."""
    return build_stack_status()


@app.get("/api/overview")
def overview() -> dict[str, Any]:
    layers = [_layer_stats(layer) for layer in ("raw", "bronze", "silver", "gold")]
    stack = build_stack_status()
    return {
        "project": "Calebsons Datalake",
        "stack": ["Polars", "DuckDB", "Airflow", "dbt"],
        "ready": all(layer["exists"] for layer in layers),
        "demos_ready": stack["demos_ready"],
        "layers": layers,
        "services": stack["services"],
        "summary": stack["summary"],
        "checked_at": stack["checked_at"],
    }


@app.get("/api/use-cases")
def use_cases() -> dict[str, Any]:
    return {
        "use_cases": [
            {
                "slug": meta["slug"],
                "industry": meta["industry"],
                "entity": meta["entity"],
                "ready": meta["ready"],
                "path": f"/demos/{meta['slug']}",
            }
            for meta in list_scenario_meta()
        ]
    }


@app.get("/api/scenarios")
def scenarios() -> dict[str, Any]:
    return {"scenarios": list_scenario_meta()}


def _scenario_layer_stats(slug: str, layer: str) -> dict[str, Any]:
    paths = scenario_layer_paths(slug)
    path = paths[layer]
    exists = path.exists()
    rows = 0
    columns: list[str] = []
    updated_at = None
    if exists:
        frame = pl.read_csv(path) if layer == "raw" else pl.read_parquet(path)
        rows = frame.height
        columns = list(frame.columns)
        updated_at = datetime.fromtimestamp(path.stat().st_mtime).isoformat()
    labels = {"raw": "Raw", "bronze": "Bronze", "silver": "Silver", "gold": "Gold"}
    return {
        "id": layer,
        "label": labels[layer],
        "exists": exists,
        "rows": rows,
        "columns": columns,
        "updated_at": updated_at,
        "format": "csv" if layer == "raw" else "parquet",
    }


@app.get("/api/scenarios/{slug}")
def scenario_overview(slug: str) -> dict[str, Any]:
    if slug not in SCENARIOS:
        raise HTTPException(status_code=404, detail=f"Unknown scenario: {slug}")
    config = get_scenario_config(slug)
    layers = [_scenario_layer_stats(slug, layer) for layer in ("raw", "bronze", "silver", "gold")]
    return {
        "slug": slug,
        "industry": config["industry"],
        "entity": config["entity"],
        "group_label": config["group_col"].replace("_", " ").title(),
        "value_label": config["value_col"].replace("_", " ").title(),
        "ready": all(layer["exists"] for layer in layers),
        "layers": layers,
    }


@app.get("/api/scenarios/{slug}/gold")
def scenario_gold(slug: str) -> dict[str, Any]:
    if slug not in SCENARIOS:
        raise HTTPException(status_code=404, detail=f"Unknown scenario: {slug}")
    path = scenario_layer_paths(slug)["gold"]
    if not path.exists():
        raise HTTPException(status_code=404, detail="Scenario gold data missing. Run seed_scenarios.py")
    frame = pl.read_parquet(path)
    records = [
        {key: _json_value(value) for key, value in row.items()}
        for row in frame.to_dicts()
    ]
    total_value = float(frame["total_value"].sum()) if frame.height else 0.0
    total_transactions = int(frame["transaction_count"].sum()) if frame.height else 0
    return {
        "slug": slug,
        "categories": records,
        "totals": {
            "total_value": round(total_value, 2),
            "transaction_count": total_transactions,
            "category_count": frame.height,
        },
    }


@app.get("/api/scenarios/{slug}/orders")
def scenario_orders(
    slug: str,
    layer: str = Query(default="silver", pattern="^(raw|bronze|silver)$"),
    limit: int = Query(default=50, ge=1, le=500),
) -> dict[str, Any]:
    if slug not in SCENARIOS:
        raise HTTPException(status_code=404, detail=f"Unknown scenario: {slug}")
    path = scenario_layer_paths(slug)[layer]
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Scenario {layer} data missing. Run seed_scenarios.py")
    frame = (pl.read_csv(path) if layer == "raw" else pl.read_parquet(path)).head(limit)
    records = [
        {key: _json_value(value) for key, value in row.items()}
        for row in frame.to_dicts()
    ]
    return {
        "slug": slug,
        "layer": layer,
        "columns": list(frame.columns),
        "rows": records,
        "returned": len(records),
    }


@app.get("/api/gold")
def gold_summary() -> dict[str, Any]:
    frame = _read_layer("gold")
    records = [
        {key: _json_value(value) for key, value in row.items()}
        for row in frame.to_dicts()
    ]
    total_value = float(frame["total_value"].sum()) if frame.height else 0.0
    total_transactions = int(frame["transaction_count"].sum()) if frame.height else 0
    return {
        "categories": records,
        "totals": {
            "total_value": round(total_value, 2),
            "transaction_count": total_transactions,
            "category_count": frame.height,
        },
    }


@app.get("/api/orders")
def orders(
    layer: str = Query(default="silver", pattern="^(raw|bronze|silver)$"),
    limit: int = Query(default=50, ge=1, le=500),
) -> dict[str, Any]:
    frame = _read_layer(layer).head(limit)
    records = [
        {key: _json_value(value) for key, value in row.items()}
        for row in frame.to_dicts()
    ]
    return {
        "layer": layer,
        "columns": list(frame.columns),
        "rows": records,
        "returned": len(records),
    }
