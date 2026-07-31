"""Raw source contracts: paths, schema names, and freshness SLAs.

Day-1 foundation for ingest validation (schema) and Airflow freshness gates.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Literal

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
BRONZE_DIR = DATA_DIR / "bronze"

ColumnDtype = Literal["string", "int64", "float64", "date"]


@dataclass(frozen=True)
class ColumnSpec:
    """Expected column after raw headers are normalized to snake_case."""

    name: str
    dtype: ColumnDtype
    required: bool = True
    description: str = ""


@dataclass(frozen=True)
class FreshnessSLA:
    """How fresh a source must stay to pass quality / orchestration gates.

    ``max_file_age_hours`` bounds landing-file mtime (arrival SLA).
    ``max_data_age_hours`` bounds the newest value in ``timestamp_column``
    (content SLA). Either may be omitted; both default to required when set.
    """

    timestamp_column: str
    max_file_age_hours: int | None = 24
    max_data_age_hours: int | None = 48
    warn_file_age_hours: int | None = 12
    warn_data_age_hours: int | None = 24


@dataclass(frozen=True)
class SourceContract:
    """Contract for one raw landing dataset."""

    name: str
    schema_name: str
    description: str
    raw_path: Path
    bronze_path: Path
    columns: tuple[ColumnSpec, ...]
    freshness: FreshnessSLA
    primary_key: str
    format: Literal["csv"] = "csv"
    delimiter: str = ","


@dataclass(frozen=True)
class FreshnessCheckResult:
    source: str
    ok: bool
    warnings: tuple[str, ...]
    errors: tuple[str, ...]
    file_age_hours: float | None
    data_age_hours: float | None
    checked_at: datetime
    details: dict[str, str]


def _hours_between(older: datetime, newer: datetime) -> float:
    return max((newer - older).total_seconds() / 3600.0, 0.0)


def _ensure_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


SALES_ORDERS = SourceContract(
    name="sales_orders",
    schema_name="raw.sales_orders",
    description="Core shared sales orders CSV used by the lakehouse explorer.",
    raw_path=RAW_DIR / "sales_orders.csv",
    bronze_path=BRONZE_DIR / "sales_orders.parquet",
    primary_key="order_id",
    columns=(
        ColumnSpec("order_id", "int64", description="Order identifier"),
        ColumnSpec("customer_name", "string", description="Customer display name"),
        ColumnSpec("category", "string", description="Product category"),
        ColumnSpec("value_usd", "float64", description="Order value in USD"),
        ColumnSpec("status", "string", description="Order status"),
        ColumnSpec("order_date", "date", description="Business order date"),
    ),
    freshness=FreshnessSLA(
        timestamp_column="order_date",
        # File arrival SLA is tight; content SLA is wide for static demo fixtures.
        max_file_age_hours=24,
        max_data_age_hours=8760,
        warn_file_age_hours=12,
        warn_data_age_hours=720,
    ),
)

SOURCES: dict[str, SourceContract] = {
    SALES_ORDERS.name: SALES_ORDERS,
}

DEFAULT_SOURCE_NAME = SALES_ORDERS.name


def list_sources() -> list[SourceContract]:
    return list(SOURCES.values())


def get_source(name: str = DEFAULT_SOURCE_NAME) -> SourceContract:
    try:
        return SOURCES[name]
    except KeyError as exc:
        known = ", ".join(sorted(SOURCES))
        raise KeyError(f"Unknown source '{name}'. Known sources: {known}") from exc


def expected_column_names(source: SourceContract | str) -> list[str]:
    contract = get_source(source) if isinstance(source, str) else source
    return [column.name for column in contract.columns]


def required_column_names(source: SourceContract | str) -> list[str]:
    contract = get_source(source) if isinstance(source, str) else source
    return [column.name for column in contract.columns if column.required]


def column_dtype_map(source: SourceContract | str) -> dict[str, ColumnDtype]:
    contract = get_source(source) if isinstance(source, str) else source
    return {column.name: column.dtype for column in contract.columns}


def source_paths(source: SourceContract | str) -> dict[str, Path]:
    contract = get_source(source) if isinstance(source, str) else source
    return {
        "raw": contract.raw_path,
        "bronze": contract.bronze_path,
    }


def file_age_hours(
    path: Path,
    *,
    as_of: datetime | None = None,
) -> float | None:
    if not path.exists():
        return None
    as_of_utc = _ensure_aware(as_of or datetime.now(timezone.utc))
    mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    return _hours_between(mtime, as_of_utc)


def data_age_hours(
    contract: SourceContract,
    *,
    as_of: datetime | None = None,
) -> float | None:
    """Age of the newest value in the contract freshness timestamp column."""
    if not contract.raw_path.exists():
        return None

    import polars as pl

    timestamp_column = contract.freshness.timestamp_column
    frame = pl.read_csv(contract.raw_path)
    # Match cleaned names if ingest has not run yet — accept raw or cleaned headers.
    cleaned = {
        col.strip().lower().replace(" ", "_"): col for col in frame.columns
    }
    lookup = cleaned.get(timestamp_column)
    if lookup is None and timestamp_column in frame.columns:
        lookup = timestamp_column
    if lookup is None:
        return None

    series = frame.get_column(lookup)
    parsed = series.cast(pl.Utf8).str.to_date(strict=False)
    newest = parsed.drop_nulls().max()
    if newest is None:
        return None

    as_of_utc = _ensure_aware(as_of or datetime.now(timezone.utc))
    newest_dt = datetime(newest.year, newest.month, newest.day, tzinfo=timezone.utc)
    return _hours_between(newest_dt, as_of_utc)


def check_source_freshness(
    source: SourceContract | str = DEFAULT_SOURCE_NAME,
    *,
    as_of: datetime | None = None,
) -> FreshnessCheckResult:
    """Evaluate file-arrival and data-content freshness against the SLA."""
    contract = get_source(source) if isinstance(source, str) else source
    checked_at = _ensure_aware(as_of or datetime.now(timezone.utc))
    sla = contract.freshness
    errors: list[str] = []
    warnings: list[str] = []
    details: dict[str, str] = {
        "schema_name": contract.schema_name,
        "raw_path": str(contract.raw_path),
        "timestamp_column": sla.timestamp_column,
    }

    if not contract.raw_path.exists():
        errors.append(f"Raw file missing: {contract.raw_path}")
        return FreshnessCheckResult(
            source=contract.name,
            ok=False,
            warnings=tuple(warnings),
            errors=tuple(errors),
            file_age_hours=None,
            data_age_hours=None,
            checked_at=checked_at,
            details=details,
        )

    file_hours = file_age_hours(contract.raw_path, as_of=checked_at)
    data_hours = data_age_hours(contract, as_of=checked_at)

    if file_hours is not None:
        details["file_age_hours"] = f"{file_hours:.2f}"
        if sla.max_file_age_hours is not None and file_hours > sla.max_file_age_hours:
            errors.append(
                f"File age {file_hours:.1f}h exceeds SLA of {sla.max_file_age_hours}h"
            )
        elif (
            sla.warn_file_age_hours is not None
            and file_hours > sla.warn_file_age_hours
        ):
            warnings.append(
                f"File age {file_hours:.1f}h exceeds warn threshold of "
                f"{sla.warn_file_age_hours}h"
            )

    if data_hours is not None:
        details["data_age_hours"] = f"{data_hours:.2f}"
        if sla.max_data_age_hours is not None and data_hours > sla.max_data_age_hours:
            errors.append(
                f"Data age {data_hours:.1f}h (column {sla.timestamp_column}) "
                f"exceeds SLA of {sla.max_data_age_hours}h"
            )
        elif (
            sla.warn_data_age_hours is not None
            and data_hours > sla.warn_data_age_hours
        ):
            warnings.append(
                f"Data age {data_hours:.1f}h exceeds warn threshold of "
                f"{sla.warn_data_age_hours}h"
            )
    elif sla.max_data_age_hours is not None:
        errors.append(
            f"Could not read freshness column '{sla.timestamp_column}' from "
            f"{contract.raw_path.name}"
        )

    return FreshnessCheckResult(
        source=contract.name,
        ok=not errors,
        warnings=tuple(warnings),
        errors=tuple(errors),
        file_age_hours=file_hours,
        data_age_hours=data_hours,
        checked_at=checked_at,
        details=details,
    )


def check_all_sources_freshness(
    sources: Iterable[str] | None = None,
    *,
    as_of: datetime | None = None,
) -> list[FreshnessCheckResult]:
    names = list(sources) if sources is not None else list(SOURCES)
    return [check_source_freshness(name, as_of=as_of) for name in names]


def summarize_contracts() -> list[dict[str, object]]:
    """Lightweight dump for APIs / CLI introspection."""
    rows: list[dict[str, object]] = []
    for contract in list_sources():
        rows.append(
            {
                "name": contract.name,
                "schema_name": contract.schema_name,
                "description": contract.description,
                "raw_path": str(contract.raw_path),
                "bronze_path": str(contract.bronze_path),
                "format": contract.format,
                "primary_key": contract.primary_key,
                "columns": expected_column_names(contract),
                "freshness": {
                    "timestamp_column": contract.freshness.timestamp_column,
                    "max_file_age_hours": contract.freshness.max_file_age_hours,
                    "max_data_age_hours": contract.freshness.max_data_age_hours,
                    "warn_file_age_hours": contract.freshness.warn_file_age_hours,
                    "warn_data_age_hours": contract.freshness.warn_data_age_hours,
                },
            }
        )
    return rows
