#!/usr/bin/env python3
"""Pivot a canonical long-format table into one wide table per stream.

Each output row is one (analysis_region, observation_week); each canonical
(feature_name, pathogen) combination becomes two columns: the value itself
and a companion `_flag` column holding that cell's `observation_state`, so a
reader can see reliability alongside the number without cross-referencing
the long file. Read-only against the input file; does not touch any source
file or any other processing step's output.
"""

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path

from state_reference import resolve_state

KEY_FIELDS = [
    "analysis_region", "analysis_region_type", "analysis_region_id",
    "raw_geography_type", "raw_geography_id", "observation_week",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, type=Path, help="A canonical long-format CSV from build_observation_tables.py, e.g. <st>_latest_view.csv.")
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--state", default="SC", help="2-letter postal abbreviation, e.g. SC (default: SC)")
    return parser.parse_args()


def pivot_stream(rows: list[dict[str, str]]) -> tuple[list[str], list[dict[str, str]]]:
    keyed: dict[tuple, dict[str, str]] = {}
    value_columns: set[str] = set()
    for row in rows:
        key = tuple(row[f] for f in KEY_FIELDS)
        if key not in keyed:
            keyed[key] = {f: row[f] for f in KEY_FIELDS}
        column = f"{row['feature_name']}_{row['pathogen']}"
        keyed[key][column] = row["weekly_feature"]
        keyed[key][f"{column}_flag"] = row["observation_state"]
        value_columns.add(column)

    ordered_columns: list[str] = []
    for column in sorted(value_columns):
        ordered_columns.append(column)
        ordered_columns.append(f"{column}_flag")
    fieldnames = KEY_FIELDS + ordered_columns

    wide_rows = sorted(keyed.values(), key=lambda r: (r["analysis_region_id"], r["observation_week"]))
    return fieldnames, wide_rows


def main() -> None:
    args = parse_args()
    prefix = resolve_state(args.state).abbr.lower()
    with args.input.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    by_stream: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        by_stream[row["stream_id"]].append(row)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    for stream_id, stream_rows in sorted(by_stream.items()):
        fieldnames, wide_rows = pivot_stream(stream_rows)
        out_path = args.output_dir / f"{prefix}_{stream_id}_wide.csv"
        with out_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames, restval="", extrasaction="ignore")
            writer.writeheader()
            writer.writerows(wide_rows)
        print(f"{stream_id}: {len(wide_rows)} rows, {len(fieldnames)} columns -> {out_path}", flush=True)


if __name__ == "__main__":
    main()
