#!/usr/bin/env python3
"""Cross-reference a state's NSSP Health Service Areas against CDC's
per-county NSSP facility-participation status, to check whether every
county nominally covered by an HSA actually has a contributing facility.

The county-status file is CDC's own published coverage map
(https://www.cdc.gov/nssp/documents/coverage-map.csv, retained locally as
nssp_outputs/nssp_coverage_map_20260806_raw.csv). It is a point-in-time snapshot of current
facility participation, not a historical record, so it can identify present
non-contributing counties but cannot say whether that was always true over
the 2022-2026 series already ingested. Generalized to any state (default
`--state SC`).
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

from state_reference import resolve_state


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--coverage-map", required=True, type=Path)
    parser.add_argument("--nssp-sc-wide", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--state", default="SC", help="2-letter postal abbreviation, e.g. SC (default: SC)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    state = resolve_state(args.state)

    with args.coverage_map.open("r", encoding="utf-8-sig", newline="") as handle:
        coverage_rows = [r for r in csv.DictReader(handle) if r["State"] == state.abbr]
    status_by_county = {r["County"]: r["Status"] for r in coverage_rows}
    coverage_date = coverage_rows[0]["Date"] if coverage_rows else "unknown"

    with args.nssp_sc_wide.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    hsa_counties: dict[str, list[str]] = {}
    for row in rows:
        if row["analysis_region_type"] == "sub_state_hsa":
            hsa_counties[row["analysis_region"]] = [c.strip() for c in row["raw_geography_id"].split(",")]

    out_rows = []
    summary = {"all_contribute": 0, "mixed": 0, "none_contribute": 0}
    for hsa, counties in hsa_counties.items():
        statuses = [(c, status_by_county.get(c, "UNKNOWN")) for c in counties]
        contributing = [c for c, s in statuses if s == "Recent Data in NSSP"]
        no_facility = [c for c, s in statuses if s == "No Eligible Facilities"]
        no_recent = [c for c, s in statuses if s == "No Recent Data in NSSP"]
        if len(contributing) == len(counties):
            flag = "all_contribute"
        elif contributing:
            flag = "mixed"
        else:
            flag = "none_contribute"
        summary[flag] += 1
        out_rows.append(
            {
                "hsa": hsa,
                "member_county_count": len(counties),
                "contributing_counties": "; ".join(contributing),
                "no_eligible_facilities_counties": "; ".join(no_facility),
                "no_recent_data_counties": "; ".join(no_recent),
                "flag": flag,
            }
        )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["hsa", "member_county_count", "contributing_counties", "no_eligible_facilities_counties", "no_recent_data_counties", "flag"],
        )
        writer.writeheader()
        writer.writerows(out_rows)

    print(f"Coverage map date: {coverage_date}")
    print(f"DONE hsas={len(hsa_counties)} summary={summary} output={args.output}", flush=True)


if __name__ == "__main__":
    main()
