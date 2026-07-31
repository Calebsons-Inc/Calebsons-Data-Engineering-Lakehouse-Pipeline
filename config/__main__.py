"""CLI: python -m config"""

from __future__ import annotations

import json

from config.sources import check_all_sources_freshness, summarize_contracts


def main() -> None:
    print(json.dumps(summarize_contracts(), indent=2))
    print()
    for result in check_all_sources_freshness():
        status = "OK" if result.ok else "FAIL"
        print(f"[{status}] {result.source}")
        for warning in result.warnings:
            print(f"  warn: {warning}")
        for error in result.errors:
            print(f"  error: {error}")
        if result.file_age_hours is not None:
            print(f"  file_age_hours={result.file_age_hours:.2f}")
        if result.data_age_hours is not None:
            print(f"  data_age_hours={result.data_age_hours:.2f}")


if __name__ == "__main__":
    main()
