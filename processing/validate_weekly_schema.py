#!/usr/bin/env python3
"""Canonical weekly-schema harmonizer and validator.

Rewrites the NSSP (extract_nssp.py) and NWSS (extract_nwss.py) per-state long
extracts into the frozen canonical weekly schema (weekly_feature is
the value, feature_name/pathogen are separate identity fields, observation
state uses the data dictionary's controlled enum), then validates the
canonical tables against the schema's checks. Does not modify the
original extract_nssp.py/extract_nwss.py output files.

Generalized to any state:
the geography-eligibility check no longer compares against a hardcoded
`{"nssp": {"state": 1, "sub_state_hsa": 14}, "nwss": {"sampling_site": 31}}`
count for South Carolina specifically. NSSP's sub-state check is now a
cross-reference against an independent second source already in the
pipeline (CDC's own facility-participation coverage map, `--nssp-
coverage-map` - a genuinely separate CDC product, not derived from the NSSP
trajectories file being validated) rather than repeating a count the national
geography audit already produced back at itself. NWSS's site-count check falls
back to that audit's output (`--national-geography-summary`), since no comparably
independent second source for per-state NWSS site counts exists yet in this
pipeline.
"""

from __future__ import annotations

import argparse
import csv
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path

from state_reference import normalize_county_name, resolve_state

CANONICAL_FIELDS = [
    "stream_id", "source_snapshot_id", "as_of_date", "extract_timestamp", "observation_week",
    "analysis_region", "analysis_region_type", "analysis_region_id",
    "raw_geography_type", "raw_geography_id", "pathogen", "feature_name", "weekly_feature",
    "feature_unit", "observation_state", "definition_status", "source_file_hash",
]

STREAM_IDS = {"nhsn", "nssp", "nwss"}
ANALYSIS_REGION_TYPES = {"nation", "state", "county", "zcta", "sub_state_hsa", "sampling_site"}
PATHOGENS = {"covid", "influenza", "rsv", "combined", "not_applicable"}
OBSERVATION_STATES = {
    "observed_zero", "observed_valid", "observed_incomplete_or_delayed", "observed_low_coverage",
    "incidental_missing", "structural_absent", "flagged_anomalous",
}
DEFINITION_STATUSES = {"confirmed", "observed_definition_uncertain", "definition_uncertain", "not_available", "not_applicable"}
MISSING_STATES = {"incidental_missing", "structural_absent", "observed_low_coverage"}

NSSP_FEATURE_NAME_MAP = {
    "percent_visits": "ed_visit_percent",
    "percent_visits_smoothed": "ed_visit_percent_smoothed",
    "ed_trend": "ed_trend_category",
}
# CDC's own trend labels for "not enough data to classify a trend" - the row is
# real (not a blank cell), but the classification itself is below CDC's own
# reporting criterion, which is what observed_low_coverage is for.
NSSP_TREND_LOW_COVERAGE_LABELS = {"Data Unavailable", "Limited Data"}
NWSS_FEATURE_NAME_MAP = {
    "site_wval": "wastewater_activity_level",
    "site_wval_category": "wastewater_activity_category",
}
EXPECTED_NSSP_STATE_UNITS = 1  # every state has exactly one state-level NSSP row - not state-specific


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--nssp-long", required=True, type=Path)
    parser.add_argument("--nwss-long", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument(
        "--nssp-coverage-map", required=True, type=Path,
        help="nssp_outputs/nssp_coverage_map_*_raw.csv - CDC's own per-county facility-"
             "participation product (Date,State,County,Status), independent of the NSSP "
             "trajectories file - used as the second source for the sub_state_hsa check.",
    )
    parser.add_argument(
        "--national-geography-summary", required=True, type=Path,
        help="cross_state/national_geography_summary.csv from audit_national_"
             "geography.py - used for the NWSS sampling_site expected count "
             "(no independent second source for this one exists in the pipeline yet).",
    )
    parser.add_argument("--state", default="SC", help="2-letter postal abbreviation, e.g. SC (default: SC)")
    return parser.parse_args()


def load_coverage_map_counties(path: Path, state_abbr: str) -> set[str] | None:
    """Distinct county names CDC's own coverage-map file lists for this state, or None
    if the state isn't present in this product at all (e.g. DC, which this CDC product
    doesn't cover - a real gap to report, not to silently treat as zero counties)."""
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        counties = {r["County"] for r in csv.DictReader(handle) if r["State"] == state_abbr}
    return counties or None


def load_expected_nwss_site_count(path: Path, state_name: str) -> int | None:
    with path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["state"] == state_name:
                return int(row["nwss_site_count"])
    return None


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def harmonize_nssp(rows: list[dict[str, str]]) -> list[dict[str, object]]:
    out = []
    for row in rows:
        raw_value = row["weekly_feature_value"]
        feature_name = NSSP_FEATURE_NAME_MAP[row["weekly_feature"]]
        if feature_name == "ed_trend_category":
            value = raw_value if raw_value not in ("", None) else None
            if value is None:
                obs_state = "incidental_missing"
            elif value in NSSP_TREND_LOW_COVERAGE_LABELS:
                obs_state = "observed_low_coverage"
            else:
                obs_state = "observed_valid"
        else:
            value = float(raw_value) if raw_value not in ("", None) else None
            obs_state = observation_state_for(value)
        out.append(
            {
                "stream_id": row["stream_id"],
                "source_snapshot_id": row["source_snapshot_id"],
                "as_of_date": row["as_of_date"],
                "extract_timestamp": row["extract_timestamp"],
                "observation_week": row["observation_week"],
                "analysis_region": row["analysis_region"],
                "analysis_region_type": row["analysis_region_type"],
                "analysis_region_id": row["analysis_region_id"],
                "raw_geography_type": row["raw_geography_type"],
                "raw_geography_id": row["raw_geography_id"],
                "pathogen": row["pathogen"],
                "feature_name": feature_name,
                "weekly_feature": value,
                "feature_unit": row["feature_unit"],
                "observation_state": obs_state,
                "definition_status": row["definition_status"],
                "source_file_hash": row["source_file_hash"],
            }
        )
    return out


def harmonize_nwss(rows: list[dict[str, str]]) -> list[dict[str, object]]:
    out = []
    for row in rows:
        raw_value = row["weekly_feature_value"]
        feature_name = NWSS_FEATURE_NAME_MAP[row["weekly_feature"]]
        if feature_name == "wastewater_activity_level":
            value = float(raw_value) if raw_value not in ("", None) else None
            obs_state = observation_state_for(value)
        else:
            value = raw_value if raw_value not in ("", None) else None
            obs_state = "observed_valid" if value is not None else "incidental_missing"
        out.append(
            {
                "stream_id": row["stream_id"],
                "source_snapshot_id": row["source_snapshot_id"],
                "as_of_date": row["as_of_date"],
                "extract_timestamp": row["extract_timestamp"],
                "observation_week": row["observation_week"],
                "analysis_region": row["analysis_region"],
                "analysis_region_type": row["analysis_region_type"],
                "analysis_region_id": row["analysis_region_id"],
                "raw_geography_type": row["raw_geography_type"],
                "raw_geography_id": row["raw_geography_id"],
                "pathogen": row["pathogen"],
                "feature_name": feature_name,
                "weekly_feature": value,
                "feature_unit": row["feature_unit"],
                "observation_state": obs_state,
                "definition_status": row["definition_status"],
                "source_file_hash": row["source_file_hash"],
            }
        )
    return out


def observation_state_for(value: float | None) -> str:
    if value is None:
        return "incidental_missing"
    if value == 0:
        return "observed_zero"
    return "observed_valid"


def validate(
    rows: list[dict[str, object]],
    stream_id: str,
    coverage_map_counties: set[str] | None,
    expected_nwss_site_count: int | None,
) -> dict[str, object]:
    issues: list[str] = []

    missing_fields = [f for f in CANONICAL_FIELDS if any(f not in row for row in rows)]
    if missing_fields:
        issues.append(f"missing required fields: {missing_fields}")

    bad_stream = {r["stream_id"] for r in rows if r["stream_id"] not in STREAM_IDS}
    bad_region_type = {r["analysis_region_type"] for r in rows if r["analysis_region_type"] not in ANALYSIS_REGION_TYPES}
    bad_pathogen = {r["pathogen"] for r in rows if r["pathogen"] not in PATHOGENS}
    bad_obs_state = {r["observation_state"] for r in rows if r["observation_state"] not in OBSERVATION_STATES}
    bad_def_status = {r["definition_status"] for r in rows if r["definition_status"] not in DEFINITION_STATUSES}
    for label, bad_set in (
        ("stream_id", bad_stream), ("analysis_region_type", bad_region_type),
        ("pathogen", bad_pathogen), ("observation_state", bad_obs_state), ("definition_status", bad_def_status),
    ):
        if bad_set:
            issues.append(f"invalid {label} values: {bad_set}")

    week_bad = 0
    for r in rows:
        try:
            date.fromisoformat(str(r["observation_week"]))
        except ValueError:
            week_bad += 1
    if week_bad:
        issues.append(f"{week_bad} rows have an unparseable observation_week")

    key_counter: Counter[tuple] = Counter()
    for r in rows:
        key = (r["stream_id"], r["analysis_region_id"], r["analysis_region_type"], r["observation_week"], r["pathogen"], r["feature_name"], r["source_snapshot_id"])
        key_counter[key] += 1
    duplicate_keys = {k: v for k, v in key_counter.items() if v > 1}
    if duplicate_keys:
        issues.append(f"{len(duplicate_keys)} duplicate composite keys")

    consistency_bad = 0
    for r in rows:
        is_null = r["weekly_feature"] is None
        is_missing_state = r["observation_state"] in MISSING_STATES
        if r["feature_name"].endswith("_category"):
            continue  # categorical features carry their value outside weekly_feature by design
        if is_null != is_missing_state:
            consistency_bad += 1
    if consistency_bad:
        issues.append(f"{consistency_bad} rows have inconsistent weekly_feature/observation_state pairing")

    range_bad = 0
    for r in rows:
        if r["feature_unit"] == "percent_ed_visits" and r["weekly_feature"] is not None:
            if not (0 <= r["weekly_feature"] <= 100):
                range_bad += 1
        if r["feature_unit"] == "wval_index_not_cross_pathogen_comparable" and r["weekly_feature"] is not None:
            if r["weekly_feature"] < 0:
                range_bad += 1
    if range_bad:
        issues.append(f"{range_bad} rows fail their unit's value-range check")

    geography_counts = defaultdict(set)
    for r in rows:
        geography_counts[r["analysis_region_type"]].add(r["analysis_region_id"])
    geography_summary = {k: len(v) for k, v in geography_counts.items()}

    if stream_id == "nssp":
        actual_state_units = geography_summary.get("state", 0)
        if actual_state_units != EXPECTED_NSSP_STATE_UNITS:
            issues.append(
                f"geography eligibility mismatch: state has {actual_state_units} distinct regions, "
                f"expected {EXPECTED_NSSP_STATE_UNITS}"
            )
        # Independent-source check: the union of every sub_state_hsa row's member
        # counties (raw_geography_id, comma-separated) should cover exactly the counties
        # CDC's own coverage-map product lists for this state - a genuinely separate CDC
        # product from the NSSP trajectories file being validated here, so this checks
        # internal pipeline consistency rather than repeating a count back at itself.
        if coverage_map_counties is None:
            issues.append(
                "sub_state_hsa cross-check skipped: this state has no rows in the CDC "
                "facility-participation coverage-map product (not_available for this "
                "state in that source, e.g. DC) - cannot independently verify HSA county "
                "coverage without it."
            )
        else:
            nssp_covered_counties: set[str] = set()
            for r in rows:
                if r["analysis_region_type"] == "sub_state_hsa" and r["raw_geography_type"] == "hsa_member_counties":
                    nssp_covered_counties.update(c.strip() for c in str(r["raw_geography_id"]).split(","))
            # Found in multi-state test runs: these are two independent CDC products that format
            # county-equivalent names differently (suffix presence/case) for several states -
            # e.g. Alaska's coverage-map product omits "Borough"/"Census Area" while NSSP's own
            # HSA membership includes it ("Aleutians East" vs "Aleutians East Borough"), and
            # Virginia's coverage map omits "City" for independent cities ("Alexandria" vs
            # "Alexandria City"). A raw string diff flagged these as missing/extra even though
            # they are the same real jurisdiction, producing a false FAIL for every state with
            # this kind of suffix formatting difference (confirmed on Alaska, Virginia, Missouri
            # smoke-test runs). Normalizing both sides with the same `state_reference.
            # normalize_county_name` already used by build_hsa_combined.py/build_nwss_county_
            # geography.py for this exact cross-product naming problem closes it here too, while
            # still reporting each side's own original spelling in the diagnostic message.
            coverage_norm = {normalize_county_name(c): c for c in coverage_map_counties}
            nssp_norm = {normalize_county_name(c): c for c in nssp_covered_counties}
            missing = {coverage_norm[k] for k in set(coverage_norm) - set(nssp_norm)}
            extra = {nssp_norm[k] for k in set(nssp_norm) - set(coverage_norm)}
            if missing or extra:
                issues.append(
                    f"sub_state_hsa county coverage mismatch against the independent CDC coverage-map "
                    f"county list: {len(missing)} counties in the coverage map but not covered by any "
                    f"HSA ({sorted(missing)[:10]}), {len(extra)} counties covered by an HSA but not in "
                    f"the coverage map ({sorted(extra)[:10]})"
                )

    if stream_id == "nwss":
        actual_sites = geography_summary.get("sampling_site", 0)
        if expected_nwss_site_count is None:
            issues.append(
                "sampling_site count check skipped: this state has no row in the nationwide "
                "geography audit summary."
            )
        elif actual_sites != expected_nwss_site_count:
            issues.append(
                f"geography eligibility mismatch: sampling_site has {actual_sites} distinct regions, "
                f"expected {expected_nwss_site_count} per the nationwide geography audit"
            )

    return {
        "stream_id": stream_id,
        "row_count": len(rows),
        "geography_summary": geography_summary,
        "issues": issues,
        "passed": len(issues) == 0,
    }


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    clean_rows = [{k: v for k, v in r.items() if not k.startswith("_")} for r in rows]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CANONICAL_FIELDS)
        writer.writeheader()
        writer.writerows(clean_rows)


def main() -> None:
    args = parse_args()
    state = resolve_state(args.state)
    nssp_source = read_rows(args.nssp_long)
    nwss_source = read_rows(args.nwss_long)

    nssp_canonical = harmonize_nssp(nssp_source)
    nwss_canonical = harmonize_nwss(nwss_source)

    reconciliation = {
        "nssp": {"source_rows": len(nssp_source), "canonical_rows": len(nssp_canonical), "match": len(nssp_source) == len(nssp_canonical)},
        "nwss": {"source_rows": len(nwss_source), "canonical_rows": len(nwss_canonical), "match": len(nwss_source) == len(nwss_canonical)},
    }

    coverage_map_counties = load_coverage_map_counties(args.nssp_coverage_map, state.abbr)
    expected_nwss_site_count = load_expected_nwss_site_count(args.national_geography_summary, state.name)

    nssp_report = validate(nssp_canonical, "nssp", coverage_map_counties, expected_nwss_site_count)
    nwss_report = validate(nwss_canonical, "nwss", coverage_map_counties, expected_nwss_site_count)

    output = args.output_dir
    prefix = state.abbr.lower()
    write_csv(output / f"{prefix}_nssp_canonical.csv", nssp_canonical)
    write_csv(output / f"{prefix}_nwss_canonical.csv", nwss_canonical)

    lines = [
        "# Schema validation report",
        "",
        "## Source-to-canonical row reconciliation",
        "",
    ]
    for stream, rec in reconciliation.items():
        lines.append(f"- {stream}: source={rec['source_rows']:,} canonical={rec['canonical_rows']:,} match={'PASS' if rec['match'] else 'FAIL'}")
    lines.append("")
    for report in (nssp_report, nwss_report):
        lines.append(f"## Validation: {report['stream_id']}")
        lines.append("")
        lines.append(f"- Rows checked: {report['row_count']:,}")
        lines.append(f"- Geography summary: {report['geography_summary']}")
        lines.append(f"- Result: {'PASS - no issues found' if report['passed'] else 'FAIL'}")
        if report["issues"]:
            for issue in report["issues"]:
                lines.append(f"  - {issue}")
        lines.append("")
    (output / "schema_validation_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(
        f"DONE nssp_rows={len(nssp_canonical)} nssp_pass={nssp_report['passed']} "
        f"nwss_rows={len(nwss_canonical)} nwss_pass={nwss_report['passed']} output={output}",
        flush=True,
    )


if __name__ == "__main__":
    main()
