#!/usr/bin/env python3
"""Build a single state-level master wide table: one row per observation week.

Hospitalization (state-native) and the emergency-
department state row join in directly; the emergency-department HSA-level
breakdown and every wastewater site are kept as their own region/site-
prefixed columns rather than collapsed into a computed state average -
nothing is aggregated by this script, since NWSS in particular has no
CDC-published statewide number and no defensible way to average across
sites. Census context is joined as
static columns, repeated on every week row, since it does not vary by
observation_week.

Read-only; does not touch any source file or any other processing step's output.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

from state_reference import resolve_state


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--latest-view", required=True, type=Path, help="{state}_latest_view.csv")
    parser.add_argument("--census-context", required=True, type=Path, help="{state}_census_context.csv")
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--state", default="SC", help="2-letter postal abbreviation, e.g. SC (default: SC)")
    return parser.parse_args()


def load_census_columns(path: Path) -> dict[str, str]:
    columns: dict[str, str] = {}
    with path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            columns[f"census_{row['variable']}"] = row["value"]
    return columns


def main() -> None:
    args = parse_args()
    state = resolve_state(args.state)
    prefix = state.abbr.lower()

    with args.latest_view.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    census_columns = load_census_columns(args.census_context)

    keyed: dict[str, dict[str, str]] = {}
    value_columns: set[str] = set()
    stream_counts = {"nhsn": 0, "nssp_state": 0, "nssp_hsa": 0, "nwss": 0}

    for row in rows:
        week = row["observation_week"]
        keyed.setdefault(week, {"observation_week": week})

        if row["stream_id"] == "nhsn":
            column = f"{row['feature_name']}_{row['pathogen']}"
            stream_counts["nhsn"] += 1
        elif row["stream_id"] == "nssp" and row["analysis_region_type"] == "state":
            column = f"{row['feature_name']}_{row['pathogen']}"
            stream_counts["nssp_state"] += 1
        elif row["stream_id"] == "nssp" and row["analysis_region_type"] == "sub_state_hsa":
            column = f"HSA{row['analysis_region_id']}_{row['feature_name']}_{row['pathogen']}"
            stream_counts["nssp_hsa"] += 1
        elif row["stream_id"] == "nwss":
            site_tag = row["analysis_region_id"].replace(":", "")
            column = f"{site_tag}_{row['feature_name']}_{row['pathogen']}"
            stream_counts["nwss"] += 1
        else:
            continue

        keyed[week][column] = row["weekly_feature"]
        keyed[week][f"{column}_flag"] = row["observation_state"]
        value_columns.add(column)

    for week in keyed:
        keyed[week].update(census_columns)

    ordered_value_columns: list[str] = []
    for column in sorted(value_columns):
        ordered_value_columns.append(column)
        ordered_value_columns.append(f"{column}_flag")
    fieldnames = ["observation_week"] + ordered_value_columns + sorted(census_columns.keys())

    wide_rows = sorted(keyed.values(), key=lambda r: r["observation_week"])

    args.output_dir.mkdir(parents=True, exist_ok=True)
    out_path = args.output_dir / f"{prefix}_master_state_wide.csv"
    with out_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, restval="", extrasaction="ignore")
        writer.writeheader()
        writer.writerows(wide_rows)

    print(f"master state-level wide table: {len(wide_rows)} rows, {len(fieldnames)} columns -> {out_path}")
    print(f"  source rows folded in: {stream_counts}")
    print(f"  {len(census_columns)} static census columns repeated on every row")


if __name__ == "__main__":
    main()
