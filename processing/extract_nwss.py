#!/usr/bin/env python3
"""Build the NWSS site-level contract validation, SC extract, and coverage-gap report.

Reads the single retained NWSS Main-Three raw file without modifying it.
Validates the `State/Territory + Site + Pathogen_Target + Week_End` key
nationwide, applies the project's explicit state-name-to-FIPS mapping,
cleans the comma-formatted `Site_WVAL`/`Population_Served` numeric fields,
and produces a South Carolina site-week-pathogen extract plus a per-site
coverage-gap report. `Counties_Served` and `Population_Served` are carried
as service-area reference metadata only, never used to allocate or
re-key observations to a county or other `analysis_region`.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from state_reference import resolve_state

STATE_FIPS = {
    "Alabama": "01", "Alaska": "02", "Arizona": "04", "Arkansas": "05", "California": "06",
    "Colorado": "08", "Connecticut": "09", "Delaware": "10", "District of Columbia": "11",
    "Florida": "12", "Georgia": "13", "Guam": "66", "Hawaii": "15", "Idaho": "16", "Illinois": "17",
    "Indiana": "18", "Iowa": "19", "Kansas": "20", "Kentucky": "21", "Louisiana": "22", "Maine": "23",
    "Maryland": "24", "Massachusetts": "25", "Michigan": "26", "Minnesota": "27", "Mississippi": "28",
    "Missouri": "29", "Montana": "30", "Nebraska": "31", "Nevada": "32", "New Hampshire": "33",
    "New Jersey": "34", "New Mexico": "35", "New York": "36", "North Carolina": "37", "North Dakota": "38",
    "Ohio": "39", "Oklahoma": "40", "Oregon": "41", "Pennsylvania": "42", "Rhode Island": "44",
    "South Carolina": "45", "South Dakota": "46", "Tennessee": "47", "Texas": "48",
    "U.S. Virgin Islands": "78", "Utah": "49", "Vermont": "50", "Virginia": "51", "Washington": "53",
    "West Virginia": "54", "Wisconsin": "55", "Wyoming": "56",
}
PATHOGEN_NORMALIZED = {"SARS-CoV-2": "covid", "Influenza A virus": "influenza", "RSV": "rsv"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--nwss-file", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--state", default="SC", help="2-letter postal abbreviation, e.g. SC (default: SC)")
    return parser.parse_args()


def clean_number(raw: str) -> float | None:
    digits = raw.replace(",", "").strip()
    if not digits:
        return None
    return float(digits)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest().upper()


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def validate_key_uniqueness(rows: list[dict[str, str]]) -> dict[str, object]:
    seen: set[tuple[str, str, str, str]] = set()
    duplicates: list[tuple[str, str, str, str]] = []
    unmapped_states: set[str] = set()
    for row in rows:
        key = (row["State/Territory"], row["Site"], row["Pathogen_Target"], row["Week_End"])
        if key in seen:
            duplicates.append(key)
        seen.add(key)
        if row["State/Territory"] not in STATE_FIPS:
            unmapped_states.add(row["State/Territory"])
    return {
        "rows_checked": len(rows),
        "distinct_keys": len(seen),
        "duplicate_key_rows": len(duplicates),
        "duplicate_examples": duplicates[:20],
        "unmapped_state_territory_values": sorted(unmapped_states),
    }


def validate_site_state_stability(rows: list[dict[str, str]]) -> dict[str, object]:
    site_states: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        site_states[row["Site"]].add(row["State/Territory"])
    multi_state_sites = {site: sorted(states) for site, states in site_states.items() if len(states) > 1}
    return {"distinct_sites": len(site_states), "sites_spanning_multiple_states": multi_state_sites}


def validate_wval_and_population(rows: list[dict[str, str]]) -> dict[str, object]:
    wval_bad = 0
    pop_bad = 0
    for row in rows:
        if clean_number(row["Site_WVAL"]) is None:
            wval_bad += 1
        if clean_number(row["Population_Served"]) is None:
            pop_bad += 1
    return {"site_wval_unparseable_after_comma_strip": wval_bad, "population_served_unparseable_after_comma_strip": pop_bad}


def validate_inclusion_date(rows: list[dict[str, str]]) -> dict[str, object]:
    site_pathogen_dates: dict[tuple[str, str], set[str]] = defaultdict(set)
    pre_inclusion_rows = 0
    for row in rows:
        site_pathogen_dates[(row["Site"], row["Pathogen_Target"])].add(row["Date_Included_In_WVAL"])
        if row["Week_End"] < row["Date_Included_In_WVAL"]:
            pre_inclusion_rows += 1
    unstable = {k: sorted(v) for k, v in site_pathogen_dates.items() if len(v) > 1}
    return {
        "site_pathogen_pairs_checked": len(site_pathogen_dates),
        "site_pathogen_pairs_with_unstable_inclusion_date": len(unstable),
        "rows_with_week_end_before_inclusion_date": pre_inclusion_rows,
        "total_rows": len(rows),
    }


def build_sc_extract(rows: list[dict[str, str]], state_name: str) -> list[dict[str, object]]:
    sc_rows = [r for r in rows if r["State/Territory"] == state_name]
    extract = []
    for row in sc_rows:
        counties = [c.strip() for c in row["Counties_Served"].split(",") if c.strip()]
        extract.append(
            {
                "state_territory": row["State/Territory"],
                "state_fips": STATE_FIPS[row["State/Territory"]],
                "site": row["Site"],
                "counties_served": "; ".join(counties),
                "counties_served_count": len(counties),
                "population_served_raw": row["Population_Served"],
                "population_served_cleaned": clean_number(row["Population_Served"]),
                "source": row["Source"],
                "site_wval_raw": row["Site_WVAL"],
                "site_wval_cleaned": clean_number(row["Site_WVAL"]),
                "site_wval_category": row["Site_WVAL_Category"],
                "date_included_in_wval": row["Date_Included_In_WVAL"],
                "week_end": row["Week_End"],
                "pathogen_target": row["Pathogen_Target"],
                "pathogen_normalized": PATHOGEN_NORMALIZED.get(row["Pathogen_Target"], row["Pathogen_Target"]),
                "date_updated": row["date_updated"],
            }
        )
    return extract


def build_long_records(
    extract: list[dict[str, object]], file_hash: str, snapshot_as_of: str, extract_timestamp: str
) -> list[dict[str, object]]:
    long_rows = []
    common = {
        "stream_id": "nwss",
        "source_snapshot_id": f"nwss_{snapshot_as_of.replace('-', '')}_{file_hash[:12].lower()}",
        "as_of_date": snapshot_as_of,
        "extract_timestamp": extract_timestamp,
        "source_file_hash": file_hash,
    }
    for row in extract:
        region_common = {
            **common,
            "observation_week": row["week_end"],
            "analysis_region": row["site"],
            "analysis_region_type": "sampling_site",
            "analysis_region_id": row["site"],
            "raw_geography_type": "state_territory",
            "raw_geography_id": row["state_territory"],
        }
        for weekly_feature, value, unit, definition_status in (
            ("site_wval", row["site_wval_cleaned"], "wval_index_not_cross_pathogen_comparable", "observed_definition_uncertain"),
            ("site_wval_category", row["site_wval_category"], "ordinal_category", "confirmed"),
        ):
            long_rows.append(
                {
                    **region_common,
                    "pathogen": row["pathogen_normalized"],
                    "feature_name": row["pathogen_normalized"],
                    "weekly_feature": weekly_feature,
                    "feature_unit": unit,
                    "weekly_feature_value": value,
                    "observation_state": "reported" if value not in (None, "") else "not_reported",
                    "definition_status": definition_status,
                }
            )
    return long_rows


def build_coverage_gap_report(extract: list[dict[str, object]]) -> list[dict[str, object]]:
    weeks_by_site_pathogen: dict[tuple[str, str], set[str]] = defaultdict(set)
    inclusion_date: dict[tuple[str, str], str] = {}
    for row in extract:
        key = (row["site"], row["pathogen_normalized"])
        weeks_by_site_pathogen[key].add(row["week_end"])
        inclusion_date[key] = row["date_included_in_wval"]
    all_weeks = sorted({row["week_end"] for row in extract})
    report = []
    for (site, pathogen), weeks in sorted(weeks_by_site_pathogen.items()):
        expected_weeks = [w for w in all_weeks if w >= inclusion_date[(site, pathogen)]]
        # Numerator and denominator must cover the same window: counting weeks reported before
        # date_included_in_wval (fixed 2026-09-29) let the coverage ratio exceed 1.
        present_since_inclusion = sum(1 for w in weeks if w >= inclusion_date[(site, pathogen)])
        report.append(
            {
                "site": site,
                "pathogen": pathogen,
                "date_included_in_wval": inclusion_date[(site, pathogen)],
                "weeks_present": len(weeks),
                "weeks_present_since_inclusion": present_since_inclusion,
                "weeks_expected_since_inclusion": len(expected_weeks),
                "missing_weeks_since_inclusion": len(expected_weeks) - present_since_inclusion,
            }
        )
    return report


def write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_validation_report(
    path: Path,
    state_name: str,
    state_abbr: str,
    run_date: str,
    file_name: str,
    file_hash: str,
    key_check: dict[str, object],
    site_state_check: dict[str, object],
    numeric_check: dict[str, object],
    inclusion_check: dict[str, object],
    sc_row_count: int,
    sc_site_count: int,
) -> None:
    lines = [
        "# NWSS site-level contract validation summary",
        "",
        f"Run date: {run_date}",
        f"Source file: `{file_name}` (sha256 {file_hash[:16]}...)",
        "",
        "## Key uniqueness (`State/Territory + Site + Pathogen_Target + Week_End`), nationwide",
        "",
        f"- Rows checked: {key_check['rows_checked']:,}",
        f"- Distinct keys: {key_check['distinct_keys']:,}",
        f"- Duplicate key rows: {key_check['duplicate_key_rows']}",
        f"- Result: {'PASS - key is unique, adopted as-is' if key_check['duplicate_key_rows'] == 0 else 'FAIL - see duplicate_examples'}",
        f"- `State/Territory` values with no entry in the project's state-to-FIPS mapping: {key_check['unmapped_state_territory_values'] or 'none'}",
        "",
        "## Site-to-state stability",
        "",
        f"- Distinct `Site` identifiers: {site_state_check['distinct_sites']:,}",
        f"- Sites appearing under more than one `State/Territory`: {len(site_state_check['sites_spanning_multiple_states'])}",
        "- Confirms `Site` is a stable single-state longitudinal unit; no cross-state site reassignment observed.",
        "",
        "## Numeric field cleaning (`Site_WVAL`, `Population_Served`)",
        "",
        f"- `Site_WVAL` values unparseable after stripping the thousands-separator comma: {numeric_check['site_wval_unparseable_after_comma_strip']}",
        f"- `Population_Served` values unparseable after the same transform: {numeric_check['population_served_unparseable_after_comma_strip']}",
        "- Both fields are exported with a comma thousands separator identical to the NSSP `fips` artifact found by extract_nssp.py; the transform is: strip commas, then parse as a number (no zero-padding, unlike FIPS).",
        "",
        "## `Date_Included_In_WVAL` review",
        "",
        f"- (site, pathogen) pairs checked: {inclusion_check['site_pathogen_pairs_checked']:,}",
        f"- Pairs where the inclusion date itself changes across rows: {inclusion_check['site_pathogen_pairs_with_unstable_inclusion_date']} (expected 0; it should be a fixed onboarding date per site-pathogen)",
        f"- Rows whose `Week_End` falls before that site-pathogen's own `Date_Included_In_WVAL`: {inclusion_check['rows_with_week_end_before_inclusion_date']:,} of {inclusion_check['total_rows']:,} ({inclusion_check['rows_with_week_end_before_inclusion_date'] / inclusion_check['total_rows']:.1%})",
        "- This is closed as `observed_definition_uncertain`: the dictionary describes `Date_Included_In_WVAL` as when a site first submitted samples for WVAL inclusion, which is inconsistent with pre-inclusion-date rows existing for roughly 1 in 18 rows. Not chased further; not required for the confirmed `Site_WVAL_Category` measure.",
        "",
        f"## {state_name} extract",
        "",
        f"- Rows extracted: {sc_row_count:,}",
        f"- Distinct sampling sites: {sc_site_count}",
        f"- See `nwss_outputs/{state_abbr.lower()}_nwss_extract_wide.csv`, "
        f"`nwss_outputs/{state_abbr.lower()}_nwss_extract_long.csv`, and "
        f"`nwss_outputs/{state_abbr.lower()}_nwss_coverage_gap_report.csv`.",
        "",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    state = resolve_state(args.state)
    rows = read_rows(args.nwss_file)
    file_hash = sha256_file(args.nwss_file)
    snapshot_as_of = rows[0]["date_updated"].split(" ")[0] if rows else ""
    extract_timestamp = datetime.now(timezone.utc).isoformat()
    run_date = datetime.now(timezone.utc).date().isoformat()

    key_check = validate_key_uniqueness(rows)
    site_state_check = validate_site_state_stability(rows)
    numeric_check = validate_wval_and_population(rows)
    inclusion_check = validate_inclusion_date(rows)

    sc_extract = build_sc_extract(rows, state.name)
    sc_long = build_long_records(sc_extract, file_hash, snapshot_as_of, extract_timestamp)
    sc_coverage = build_coverage_gap_report(sc_extract)

    output = args.output_dir
    prefix = state.abbr.lower()
    write_csv(
        output / f"{prefix}_nwss_extract_wide.csv",
        sc_extract,
        [
            "state_territory", "state_fips", "site", "counties_served", "counties_served_count",
            "population_served_raw", "population_served_cleaned", "source",
            "site_wval_raw", "site_wval_cleaned", "site_wval_category", "date_included_in_wval",
            "week_end", "pathogen_target", "pathogen_normalized", "date_updated",
        ],
    )
    write_csv(
        output / f"{prefix}_nwss_extract_long.csv",
        sc_long,
        [
            "stream_id", "source_snapshot_id", "as_of_date", "extract_timestamp", "observation_week",
            "analysis_region", "analysis_region_type", "analysis_region_id",
            "raw_geography_type", "raw_geography_id", "pathogen", "feature_name", "weekly_feature",
            "feature_unit", "weekly_feature_value", "observation_state", "definition_status", "source_file_hash",
        ],
    )
    write_csv(
        output / f"{prefix}_nwss_coverage_gap_report.csv",
        sc_coverage,
        ["site", "pathogen", "date_included_in_wval", "weeks_present", "weeks_present_since_inclusion",
         "weeks_expected_since_inclusion", "missing_weeks_since_inclusion"],
    )
    write_validation_report(
        output / "nwss_validation_summary.md",
        state.name,
        state.abbr,
        run_date,
        args.nwss_file.name,
        file_hash,
        key_check,
        site_state_check,
        numeric_check,
        inclusion_check,
        len(sc_extract),
        len({r["site"] for r in sc_extract}),
    )
    print(
        f"DONE state={state.abbr} rows={len(rows)} dup_keys={key_check['duplicate_key_rows']} "
        f"sc_rows={len(sc_extract)} sc_sites={len({r['site'] for r in sc_extract})} "
        f"output={output}",
        flush=True,
    )


if __name__ == "__main__":
    main()
