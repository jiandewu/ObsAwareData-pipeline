#!/usr/bin/env python3
"""Build the per-state NSSP extract, standardized long records, and validation summary.

Reads the latest registered NSSP raw snapshot listed in the snapshot registry
(`nhsn_nssp_snapshot_registry.csv`, from register_nhsn_nssp_snapshots.py) without modifying any source file. Segregates
national, state, and Health Service Area (HSA) rows by the source's own
`trend_source` field, validates that HSA rows repeated across member counties
carry identical values before collapsing them to one record per HSA, cleans
the comma-formatted `fips` field, and emits a per-state wide extract (default
`--state SC`)
plus a standardized long-format record set and a validation summary.
"""

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from state_reference import resolve_state

PATHOGENS_WITH_TREND = ("covid", "influenza", "rsv")
ALL_PATHOGENS = ("combined", *PATHOGENS_WITH_TREND)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--nssp-file", required=True, type=Path)
    parser.add_argument("--registry", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--state", default="SC", help="2-letter postal abbreviation, e.g. SC (default: SC)")
    return parser.parse_args()


def load_registry_record(registry_path: Path, filename: str) -> dict[str, str]:
    with registry_path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["stream_id"] == "nssp" and row["filename"] == filename:
                return row
    raise ValueError(f"{filename} not found in {registry_path}")


def clean_fips(raw_fips: str, trend_source: str) -> str | None:
    if trend_source == "United States":
        return None
    digits = raw_fips.replace(",", "").strip()
    if not digits.isdigit():
        return None
    return digits.zfill(5)


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def validate_hsa_duplicate_consistency(sc_hsa_rows: list[dict[str, str]]) -> dict[str, object]:
    compare_fields = [
        "percent_visits_combined", "percent_visits_covid", "percent_visits_influenza", "percent_visits_rsv",
        "percent_visits_smoothed_combined", "percent_visits_smoothed_covid",
        "percent_visits_smoothed_influenza", "percent_visits_smoothed_rsv",
        "ed_trends_covid", "ed_trends_influenza", "ed_trends_rsv", "hsa", "hsa_nci_id",
    ]
    groups: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for row in sc_hsa_rows:
        groups[(row["week_end"], row["hsa_nci_id"])].append(row)
    inconsistent = []
    for key, members in groups.items():
        signatures = {tuple(member[field] for field in compare_fields) for member in members}
        if len(signatures) > 1:
            inconsistent.append({"week_end": key[0], "hsa_nci_id": key[1], "member_count": len(members)})
    return {
        "groups_checked": len(groups),
        "inconsistent_groups": len(inconsistent),
        "inconsistent_detail": inconsistent[:20],
    }


def validate_fips_stability(sc_rows: list[dict[str, str]], state_fips: str) -> dict[str, object]:
    county_fips: dict[str, set[str]] = defaultdict(set)
    non_five_digit = []
    for row in sc_rows:
        cleaned = clean_fips(row["fips"], row["trend_source"])
        if cleaned is None:
            continue
        if len(cleaned) != 5 or not cleaned.isdigit():
            non_five_digit.append({"county": row["county"], "raw_fips": row["fips"]})
        if row["trend_source"] == "HSA":
            county_fips[row["county"]].add(cleaned)
        elif row["trend_source"] == "State":
            county_fips["__state__"].add(cleaned)
    unstable = {county: sorted(values) for county, values in county_fips.items() if len(values) > 1}
    state_fips_prefix_ok = all(v.startswith(state_fips) for v in county_fips.get("__state__", set()))
    return {
        "malformed_fips_count": len(non_five_digit),
        "malformed_fips_detail": non_five_digit[:20],
        "counties_with_unstable_fips_across_weeks": unstable,
        "state_fips_matches_prefix": state_fips_prefix_ok,
    }


def build_wide_extract(
    sc_state_rows: list[dict[str, str]], sc_hsa_rows: list[dict[str, str]], state_name: str, state_fips: str
) -> tuple[list[dict[str, object]], dict[str, object]]:
    dedup_groups: dict[tuple[str, str], dict[str, str]] = {}
    for row in sc_hsa_rows:
        key = (row["week_end"], row["hsa_nci_id"])
        dedup_groups.setdefault(key, row)

    wide_rows: list[dict[str, object]] = []
    for row in sc_state_rows:
        wide_rows.append(
            {
                "week_end": row["week_end"],
                "analysis_region_type": "state",
                "analysis_region": state_name,
                "analysis_region_id": state_fips,
                "raw_geography_type": "state_fips_placeholder",
                "raw_geography_id": row["fips"],
                "raw_geography_id_cleaned": clean_fips(row["fips"], row["trend_source"]),
                **{field: row[field] for field in (
                    "percent_visits_combined", "percent_visits_covid", "percent_visits_influenza", "percent_visits_rsv",
                    "percent_visits_smoothed_combined", "percent_visits_smoothed_covid",
                    "percent_visits_smoothed_influenza", "percent_visits_smoothed_rsv",
                    "ed_trends_covid", "ed_trends_influenza", "ed_trends_rsv",
                )},
            }
        )
    for (week_end, hsa_nci_id), row in dedup_groups.items():
        wide_rows.append(
            {
                "week_end": week_end,
                "analysis_region_type": "sub_state_hsa",
                "analysis_region": row["hsa"],
                "analysis_region_id": hsa_nci_id,
                "raw_geography_type": "hsa_member_counties",
                "raw_geography_id": row["hsa_counties"],
                "raw_geography_id_cleaned": None,
                **{field: row[field] for field in (
                    "percent_visits_combined", "percent_visits_covid", "percent_visits_influenza", "percent_visits_rsv",
                    "percent_visits_smoothed_combined", "percent_visits_smoothed_covid",
                    "percent_visits_smoothed_influenza", "percent_visits_smoothed_rsv",
                    "ed_trends_covid", "ed_trends_influenza", "ed_trends_rsv",
                )},
            }
        )
    reconciliation = {
        "sc_state_input_rows": len(sc_state_rows),
        "sc_hsa_input_rows": len(sc_hsa_rows),
        "sc_hsa_deduplicated_groups": len(dedup_groups),
        "wide_output_rows": len(wide_rows),
    }
    return wide_rows, reconciliation


def build_long_records(
    wide_rows: list[dict[str, object]], registry_record: dict[str, str], extract_timestamp: str
) -> list[dict[str, object]]:
    long_rows: list[dict[str, object]] = []
    common = {
        "stream_id": "nssp",
        "source_snapshot_id": registry_record["source_snapshot_id"],
        "as_of_date": registry_record["as_of_date"],
        "extract_timestamp": extract_timestamp,
        "source_file_hash": registry_record["sha256"],
    }
    for row in wide_rows:
        region_common = {
            **common,
            "observation_week": row["week_end"],
            "analysis_region": row["analysis_region"],
            "analysis_region_type": row["analysis_region_type"],
            "analysis_region_id": row["analysis_region_id"],
            "raw_geography_type": row["raw_geography_type"],
            "raw_geography_id": row["raw_geography_id"],
        }
        for pathogen in ALL_PATHOGENS:
            for weekly_feature, source_field, unit, definition_status in (
                ("percent_visits", f"percent_visits_{pathogen}", "percent_ed_visits", "confirmed"),
                ("percent_visits_smoothed", f"percent_visits_smoothed_{pathogen}", "percent_ed_visits", "confirmed"),
            ):
                value = row[source_field]
                long_rows.append(
                    {
                        **region_common,
                        "pathogen": pathogen,
                        "feature_name": pathogen,
                        "weekly_feature": weekly_feature,
                        "feature_unit": unit,
                        "weekly_feature_value": value,
                        "observation_state": "reported" if value not in ("", None) else "not_reported",
                        "definition_status": definition_status,
                    }
                )
        for pathogen in PATHOGENS_WITH_TREND:
            value = row[f"ed_trends_{pathogen}"]
            long_rows.append(
                {
                    **region_common,
                    "pathogen": pathogen,
                    "feature_name": pathogen,
                    "weekly_feature": "ed_trend",
                    "feature_unit": "categorical_trend",
                    "weekly_feature_value": value,
                    "observation_state": "reported" if value not in ("", None) else "not_reported",
                    "definition_status": "observed_definition_uncertain",
                }
            )
    return long_rows


def write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_validation_report(
    path: Path,
    state_name: str,
    state_fips: str,
    registry_record: dict[str, str],
    reconciliation: dict[str, object],
    hsa_check: dict[str, object],
    fips_check: dict[str, object],
    long_row_count: int,
    run_date: str,
) -> None:
    expected_long_rows = (
        reconciliation["sc_state_input_rows"] + reconciliation["sc_hsa_deduplicated_groups"]
    ) * (2 * len(ALL_PATHOGENS) + len(PATHOGENS_WITH_TREND))
    lines = [
        f"# {state_name} NSSP extract validation summary",
        "",
        f"Run date: {run_date}",
        f"Source snapshot: `{registry_record['filename']}` (`{registry_record['source_snapshot_id']}`, as_of_date {registry_record['as_of_date']}, sha256 {registry_record['sha256'][:16]}...)",
        "",
        "## Row reconciliation",
        "",
        f"- SC state-level input rows (trend_source=State): {reconciliation['sc_state_input_rows']:,}",
        f"- SC HSA-level input rows (trend_source=HSA, one per member county): {reconciliation['sc_hsa_input_rows']:,}",
        f"- SC HSA rows after deduplication to one record per (hsa_nci_id, week): {reconciliation['sc_hsa_deduplicated_groups']:,}",
        f"- Wide extract output rows (state + deduplicated HSA): {reconciliation['wide_output_rows']:,}",
        f"- Long standardized records produced: {long_row_count:,} (expected {expected_long_rows:,} = output rows x (2 numeric features x {len(ALL_PATHOGENS)} pathogens + 1 trend feature x {len(PATHOGENS_WITH_TREND)} pathogens))",
        f"- Reconciliation check: {'PASS' if long_row_count == expected_long_rows else 'FAIL'}",
        "",
        "## HSA duplicate-consistency check",
        "",
        "Every county tagged to the same HSA and week must carry an identical value; if not, collapsing to one record per HSA would silently discard a real discrepancy.",
        "",
        f"- (hsa_nci_id, week) groups checked: {hsa_check['groups_checked']:,}",
        f"- Groups with inconsistent member values: {hsa_check['inconsistent_groups']:,}",
        f"- Result: {'PASS - safe to collapse to one record per HSA-week' if hsa_check['inconsistent_groups'] == 0 else 'FAIL - see detail below, do not collapse without review'}",
    ]
    if hsa_check["inconsistent_groups"]:
        lines.append(f"- First inconsistent groups: {hsa_check['inconsistent_detail']}")
    lines.extend(
        [
            "",
            "## FIPS validation",
            "",
            f"- Malformed cleaned-FIPS values (not a 5-digit numeric string) among SC rows: {fips_check['malformed_fips_count']}",
            f"- SC counties whose cleaned FIPS changes across weeks in this snapshot: {len(fips_check['counties_with_unstable_fips_across_weeks'])}",
            f"- SC state-row FIPS starts with the expected state prefix ({state_fips}): {'yes' if fips_check['state_fips_matches_prefix'] else 'NO'}",
        ]
    )
    if fips_check["malformed_fips_count"]:
        lines.append(f"- Malformed detail: {fips_check['malformed_fips_detail']}")
    if fips_check["counties_with_unstable_fips_across_weeks"]:
        lines.append(f"- Unstable-FIPS detail: {fips_check['counties_with_unstable_fips_across_weeks']}")
    lines.append("")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    state = resolve_state(args.state)
    registry_record = load_registry_record(args.registry, args.nssp_file.name)
    rows = read_rows(args.nssp_file)

    sc_state_rows = [r for r in rows if r["geography"] == state.name and r["trend_source"] == "State"]
    sc_hsa_rows = [r for r in rows if r["geography"] == state.name and r["trend_source"] == "HSA"]
    sc_rows_all = sc_state_rows + sc_hsa_rows

    hsa_check = validate_hsa_duplicate_consistency(sc_hsa_rows)
    fips_check = validate_fips_stability(sc_rows_all, state.fips)
    wide_rows, reconciliation = build_wide_extract(sc_state_rows, sc_hsa_rows, state.name, state.fips)

    extract_timestamp = datetime.now(timezone.utc).isoformat()
    long_rows = build_long_records(wide_rows, registry_record, extract_timestamp)

    output = args.output_dir
    prefix = state.abbr.lower()
    write_csv(
        output / f"{prefix}_nssp_extract_wide.csv",
        wide_rows,
        [
            "week_end", "analysis_region_type", "analysis_region", "analysis_region_id",
            "raw_geography_type", "raw_geography_id", "raw_geography_id_cleaned",
            "percent_visits_combined", "percent_visits_covid", "percent_visits_influenza", "percent_visits_rsv",
            "percent_visits_smoothed_combined", "percent_visits_smoothed_covid",
            "percent_visits_smoothed_influenza", "percent_visits_smoothed_rsv",
            "ed_trends_covid", "ed_trends_influenza", "ed_trends_rsv",
        ],
    )
    write_csv(
        output / f"{prefix}_nssp_extract_long.csv",
        long_rows,
        [
            "stream_id", "source_snapshot_id", "as_of_date", "extract_timestamp", "observation_week",
            "analysis_region", "analysis_region_type", "analysis_region_id",
            "raw_geography_type", "raw_geography_id", "pathogen", "feature_name", "weekly_feature",
            "feature_unit", "weekly_feature_value", "observation_state", "definition_status", "source_file_hash",
        ],
    )
    write_validation_report(
        output / "nssp_validation_summary.md",
        state.name,
        state.fips,
        registry_record,
        reconciliation,
        hsa_check,
        fips_check,
        len(long_rows),
        datetime.now(timezone.utc).date().isoformat(),
    )
    print(
        f"DONE state={state.abbr} wide_rows={len(wide_rows)} long_rows={len(long_rows)} "
        f"hsa_inconsistent={hsa_check['inconsistent_groups']} fips_malformed={fips_check['malformed_fips_count']} "
        f"output={output}",
        flush=True,
    )


if __name__ == "__main__":
    main()
