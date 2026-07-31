from __future__ import annotations

from pathlib import Path
from typing import Any

from transformations.seed_scenarios import SCENARIOS, SCENARIOS_DIR

# Canonical silver column labels shown in the explorer UI.
SILVER_LABELS = {
    "id": "id",
    "name": "name",
    "group": "group",
    "value": "value",
    "status": "status",
    "event_date": "event_date",
}


def list_scenario_meta() -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for slug, config in SCENARIOS.items():
        root = SCENARIOS_DIR / slug
        items.append(
            {
                "slug": slug,
                "industry": config["industry"],
                "entity": config["entity"],
                "group_label": config["group_col"].replace("_", " ").title(),
                "value_label": config["value_col"].replace("_", " ").title(),
                "ready": (root / "gold" / config["gold_name"]).exists(),
                "paths": {
                    "raw": str(root / "raw" / config["raw_name"]),
                    "bronze": str(root / "bronze" / config["bronze_name"]),
                    "silver": str(root / "silver" / config["silver_name"]),
                    "gold": str(root / "gold" / config["gold_name"]),
                },
            }
        )
    return items


def get_scenario_config(slug: str) -> dict[str, Any]:
    if slug not in SCENARIOS:
        raise KeyError(slug)
    return SCENARIOS[slug]


def scenario_layer_paths(slug: str) -> dict[str, Path]:
    config = get_scenario_config(slug)
    root = SCENARIOS_DIR / slug
    return {
        "raw": root / "raw" / config["raw_name"],
        "bronze": root / "bronze" / config["bronze_name"],
        "silver": root / "silver" / config["silver_name"],
        "gold": root / "gold" / config["gold_name"],
    }
