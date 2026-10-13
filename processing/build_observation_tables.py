#!/usr/bin/env python3
"""Build the per-state observation tables (any state; default `--state SC`).

Ingests one state's records from every retained NHSN, NSSP, and NWSS
historical snapshot (NWSS from its dated snapshot archive, which grows
weekly) - preserving both `observation_week` and `as_of_date` for all three
streams - plus the Census context (extract_census_population.py,
extract_census_acs.py), then produces an all-versions canonical table, a
deterministic latest-available view, and a point-in-time view reconstructed
from only the snapshots on or before a chosen cutoff date. Read-only
against all source files.
"""

from __future__ import annotations

import argparse
import csv
import re
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path

import extract_nssp as nssp_mod
import extract_nwss as nwss_mod
from state_reference import resolve_state

NHSN_PATHOGEN_FIELDS = {
    "covid": ("Total COVID-19 Admissions", "Number Hospitals Reporting COVID-19 Admissions"),
    "influenza": ("Total Influenza Admissions", "Number Hospitals Reporting Influenza Admissions"),
    "rsv": ("Total RSV Admissions", "Number Hospitals Reporting RSV Admissions"),
}
NHSN_WEEK_COLUMN = "Week Ending Date"
NHSN_GEO_COLUMN = "Geographic aggregation"

CANONICAL_FIELDS = [
    "stream_id", "source_snapshot_id", "as_of_date", "extract_timestamp", "observation_week",
    "analysis_region", "analysis_region_type", "analysis_region_id",
    "raw_geography_type", "raw_geography_id", "pathogen", "feature_name", "weekly_feature",
    "feature_unit", "observation_state", "definition_status", "source_file_hash",
]

# v1 observation-state extensions: low-coverage, structural-absence, and anomaly rules on
# top of the canonical observation states. None of these thresholds are clinically validated;
# every field they touch carries definition_status = observed_definition_uncertain.
NHSN_LOW_COVERAGE_THRESHOLD = 50
NHSN_RSV_STRUCTURAL_ABSENT_LAST_WEEK = "2024-08-03"  # inclusive
NHSN_ANOMALY_RELATIVE_CHANGE = 1.00  # |Delta hospital_admission_count| >= 100% week-over-week
NSSP_ANOMALY_FEATURE = "ed_visit_percent_smoothed"
NSSP_ANOMALY_RELATIVE_CHANGE = 0.35
# Pathogen-differentiated cutoffs: chosen because they flag a comparable ~19-23% share of each
# pathogen's site-pathogen pairs (extract_nwss.py coverage-ratio distributions), rather than the 3x disparity a single shared cutoff would produce.
# Re-derived 2026-09-29 (was 0.80/0.90/0.90): the coverage ratio's numerator used to count weeks
# reported before date_included_in_wval, inflating it above 1 for many sites. These values are
# derive_state_thresholds.py's percentile at the original SC target shares (7/31, 6/28, 5/26)
# on the corrected ratio, 2026-08-28 NWSS release; ties at the cutoff make the realized SC shares
# 22.6%/14.3%/11.5%.
NWSS_LOW_COVERAGE_THRESHOLD = {"covid": 0.75, "influenza": 0.76, "rsv": 0.76}
# >=3-tier Site_WVAL_Category jump, not a percent-change rule. A 2-tier threshold flagged about 14% of
# site-weeks nationally (2026-10-09 release), mostly ordinary swings and real season onsets; 3 tiers flags about 5%.
NWSS_ANOMALY_TIER_JUMP = 3
NWSS_CATEGORY_ORDER = {"Very Low": 0, "Low": 1, "Moderate": 2, "High": 3, "Very High": 4}
# (source_snapshot_id, site, pathogen, week) of every blended-source week, filled per snapshot by
# ingest_nwss_snapshot; only the run report's counts read it
NWSS_SOURCE_BLEND_KEYS: set[tuple[str, str, str, str]] = set()
# NSSP participation: NSSP publishes no visit-count denominator, so its own ed_trend_category
# low-coverage proxy (above) is kept as-is. This adds an independent, CDC-sourced structural
# signal instead: check_nssp_facility_coverage.py's per-HSA facility-participation check
# (sc_hsa_facility_coverage_check.csv, a single 2026-08-06 point-in-time snapshot, not a weekly
# series) reused verbatim, not re-thresholded.
NSSP_PARTICIPATION_LOW_COVERAGE_FLAGS = {"mixed", "none_contribute"}
# NWSS source blending. Profiling SC's own `source` field (extract_nwss.py wide
# extract) found no stable "multi-source site" category to give a different coverage threshold
# to (0 of 31 SC sites are ever consistently blended; only 2 sites show brief blended weeks,
# 0.7% of all SC site-pathogen-weeks, concentrated in covid). A blended-source week reads as a
# one-off lab/pipeline transition event, not a durable site property, so it is treated as the
# anomaly signal, not as a second coverage-threshold split.

# Two derived recency/persistence fields (weeks_since_last_valid, persistent_limitation_flag)
# on top of the v1 flags above. Computed only against
# a single already-built view (see annotate_recency_and_persistence): calling this on the
# point-in-time view, never the latest-available view, is what keeps it from leaking hindsight
# revisions into a signal meant to describe what was knowable in real time - the same concern
# the project already applies to NHSN/NSSP/NWSS snapshot retention itself.
PRIMARY_RECENCY_FEATURES = {
    "nhsn": "hospital_admission_count",
    "nssp": "ed_visit_percent",
    "nwss": "wastewater_activity_level",
}
DEGRADED_OBSERVATION_STATES = {"structural_absent", "incidental_missing", "observed_low_coverage"}
# Provisional v1 persistence window: NOT a validated cutoff.
PERSISTENCE_WINDOW_WEEKS = 8  # N
PERSISTENCE_MIN_DEGRADED_WEEKS = 6  # K (75%)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", required=True, type=Path)
    parser.add_argument("--nhsn-nssp-registry", required=True, type=Path)
    parser.add_argument("--census-extract-dir", required=True, type=Path)
    parser.add_argument(
        "--hsa-facility-coverage-check", required=True, type=Path,
        help="nssp_outputs/sc_hsa_facility_coverage_check.csv, used for the v1 NSSP participation_status rule.",
    )
    parser.add_argument(
        "--nwss-registry", required=True, type=Path,
        help="nwss_outputs/nwss_snapshot_registry.csv (from register_nwss_snapshots.py), "
             "listing every dated NWSS snapshot to ingest per-snapshot, same as NHSN/NSSP.",
    )
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument(
        "--large-output-dir", type=Path, default=None,
        help="Where to write the large all-versions table (~500 MB). Defaults to --output-dir. "
             "This file is not intended to be committed to git regardless of location.",
    )
    parser.add_argument("--point-in-time-cutoff", required=True, help="ISO date, e.g. 2025-12-31")
    parser.add_argument("--state", default="SC", help="2-letter postal abbreviation, e.g. SC (default: SC)")
    parser.add_argument(
        "--nhsn-low-coverage-threshold", type=float, default=NHSN_LOW_COVERAGE_THRESHOLD,
        help="hospital_reporting_count cutoff below which coverage_status = observed_low_coverage "
             "(default: SC's original fixed value, 50). Other states should pass in the "
             "state-specific value from derive_state_thresholds.py's output instead of reusing SC's.",
    )
    parser.add_argument(
        "--nhsn-anomaly-relative-change", type=float, default=NHSN_ANOMALY_RELATIVE_CHANGE,
        help="|week-over-week relative change| cutoff for hospital_admission_count flagged_anomalous "
             "(default: SC's original fixed value, 1.00 = 100%%). See derive_state_thresholds.py for per-state values.",
    )
    parser.add_argument(
        "--nssp-anomaly-relative-change", type=float, default=NSSP_ANOMALY_RELATIVE_CHANGE,
        help="|week-over-week relative change| cutoff for ed_visit_percent_smoothed flagged_anomalous "
             "(default: SC's original fixed value, 0.35). See derive_state_thresholds.py for per-state values.",
    )
    parser.add_argument(
        "--nwss-low-coverage-covid", type=float, default=NWSS_LOW_COVERAGE_THRESHOLD["covid"],
        help="NWSS coverage-ratio cutoff for covid participation_status = observed_low_coverage "
             "(default: SC's value, 0.75). See derive_state_thresholds.py for per-state values.",
    )
    parser.add_argument(
        "--nwss-low-coverage-influenza", type=float, default=NWSS_LOW_COVERAGE_THRESHOLD["influenza"],
        help="Same as --nwss-low-coverage-covid, for influenza (default: 0.76).",
    )
    parser.add_argument(
        "--nwss-low-coverage-rsv", type=float, default=NWSS_LOW_COVERAGE_THRESHOLD["rsv"],
        help="Same as --nwss-low-coverage-covid, for rsv (default: 0.76).",
    )
    return parser.parse_args()


def normalize_header(name: str) -> str:
    return re.sub(r"\s+", " ", name).strip()


def load_registry(path: Path, stream_id: str) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = [r for r in csv.DictReader(handle) if r["stream_id"] == stream_id]
    # Deterministic ordering: by as_of_date, then channel (standard preferred over
    # preliminary on a tie), then filename. Preliminary/final semantics remain
    # definition_uncertain (see register_nhsn_nssp_snapshots.py); this is a documented, deterministic tie-break,
    # not an assertion that "standard" means final.
    channel_rank = {"standard": 0, "preliminary": 1}
    rows.sort(key=lambda r: (r["as_of_date"], channel_rank.get(r["release_channel"], 2), r["filename"]))
    return rows


def clean_number(raw: str) -> float | None:
    text = raw.replace(",", "").strip()
    if not text:
        return None
    return float(text)


def observation_state_for(value: float | None) -> str:
    if value is None:
        return "incidental_missing"
    if value == 0:
        return "observed_zero"
    return "observed_valid"


def nhsn_is_rsv_structural_absent(pathogen: str, observation_week: str) -> bool:
    return pathogen == "rsv" and observation_week <= NHSN_RSV_STRUCTURAL_ABSENT_LAST_WEEK


def flag_anomalies(rows: list[dict[str, object]], feature_name: str, group_keys: tuple[str, ...], threshold: float) -> None:
    """Mutate observation_state to 'flagged_anomalous' in place for rows of
    `feature_name` whose week-over-week |relative change| within each group
    (grouped by `group_keys`, e.g. pathogen) meets or exceeds `threshold`,
    compared against the closest preceding non-null value in the same group.
    Only upgrades rows already 'observed_valid'; never overrides a more
    specific reliability state (observed_zero, incidental_missing,
    structural_absent, observed_low_coverage). A comparison against a zero
    prior value is skipped (relative change undefined). v1 rule.
    """
    groups: dict[tuple, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        if row["feature_name"] == feature_name:
            groups[tuple(row[k] for k in group_keys)].append(row)
    for group_rows in groups.values():
        group_rows.sort(key=lambda r: r["observation_week"])
        prev_value = None
        for row in group_rows:
            value = row["weekly_feature"]
            if (
                row["observation_state"] == "observed_valid"
                and value is not None
                and prev_value is not None
                and prev_value != 0
                and abs((value - prev_value) / prev_value) >= threshold
            ):
                row["observation_state"] = "flagged_anomalous"
            if value is not None:
                prev_value = value


def ingest_nhsn_snapshot(record: dict[str, str], data_root: Path, extract_timestamp: str, state) -> list[dict[str, object]]:
    path = data_root / record["source_relative_path"]
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        raw_header = next(reader)
        norm_to_actual = {normalize_header(h): h for h in raw_header}
        for count_col, denom_col in NHSN_PATHOGEN_FIELDS.values():
            for target in (count_col, denom_col):
                if normalize_header(target) not in norm_to_actual:
                    raise ValueError(f"{path.name}: missing expected column '{target}' (even after whitespace normalization)")
        week_idx = raw_header.index(norm_to_actual.get(normalize_header(NHSN_WEEK_COLUMN), NHSN_WEEK_COLUMN))
        geo_idx = raw_header.index(NHSN_GEO_COLUMN)
        field_indices = {
            pathogen: (
                raw_header.index(norm_to_actual[normalize_header(count_col)]),
                raw_header.index(norm_to_actual[normalize_header(denom_col)]),
            )
            for pathogen, (count_col, denom_col) in NHSN_PATHOGEN_FIELDS.items()
        }
        out = []
        common = {
            "stream_id": "nhsn",
            "source_snapshot_id": record["source_snapshot_id"],
            "as_of_date": record["as_of_date"],
            "extract_timestamp": extract_timestamp,
            "analysis_region": state.name,
            "analysis_region_type": "state",
            "analysis_region_id": state.fips,
            "raw_geography_type": "state_abbreviation",
            "raw_geography_id": state.abbr,
            "source_file_hash": record["sha256"],
        }
        for row in reader:
            if row[geo_idx] != state.abbr:
                continue
            observation_week = row[week_idx]
            for pathogen, (count_i, denom_i) in field_indices.items():
                count_val = clean_number(row[count_i])
                denom_val = clean_number(row[denom_i])
                if nhsn_is_rsv_structural_absent(pathogen, observation_week):
                    count_state = denom_state = coverage_state = "structural_absent"
                    coverage_value = None
                else:
                    count_state = observation_state_for(count_val)
                    denom_state = observation_state_for(denom_val)
                    if denom_val is None:
                        coverage_value, coverage_state = None, "incidental_missing"
                    elif denom_val < NHSN_LOW_COVERAGE_THRESHOLD:
                        coverage_value, coverage_state = "reduced_participation", "observed_low_coverage"
                    else:
                        coverage_value, coverage_state = "full_participation", "observed_valid"
                out.append(
                    {
                        **common,
                        "observation_week": observation_week,
                        "pathogen": pathogen,
                        "feature_name": "hospital_admission_count",
                        "weekly_feature": count_val,
                        "feature_unit": "count",
                        "observation_state": count_state,
                        "definition_status": "confirmed",
                    }
                )
                out.append(
                    {
                        **common,
                        "observation_week": observation_week,
                        "pathogen": pathogen,
                        "feature_name": "hospital_reporting_count",
                        "weekly_feature": denom_val,
                        "feature_unit": "count",
                        "observation_state": denom_state,
                        "definition_status": "confirmed",
                    }
                )
                out.append(
                    {
                        **common,
                        "observation_week": observation_week,
                        "pathogen": pathogen,
                        "feature_name": "coverage_status",
                        "weekly_feature": coverage_value,
                        "feature_unit": "categorical_coverage_status",
                        "observation_state": coverage_state,
                        "definition_status": "observed_definition_uncertain",
                    }
                )
        flag_anomalies(out, "hospital_admission_count", ("pathogen",), NHSN_ANOMALY_RELATIVE_CHANGE)
        return out


def ingest_all_nhsn(registry_rows: list[dict[str, str]], data_root: Path, extract_timestamp: str, state) -> list[dict[str, object]]:
    all_rows: list[dict[str, object]] = []
    for i, record in enumerate(registry_rows, start=1):
        rows = ingest_nhsn_snapshot(record, data_root, extract_timestamp, state)
        all_rows.extend(rows)
        print(f"NHSN {i}/{len(registry_rows)} {record['filename']} -> {len(rows)} {state.abbr} records", flush=True)
    return all_rows


NSSP_FEATURE_NAME_MAP = {
    "percent_visits": "ed_visit_percent",
    "percent_visits_smoothed": "ed_visit_percent_smoothed",
    "ed_trend": "ed_trend_category",
}
NSSP_PATHOGENS_WITH_TREND = ("covid", "influenza", "rsv")
NSSP_ALL_PATHOGENS = ("combined", *NSSP_PATHOGENS_WITH_TREND)
# CDC's own trend labels for "not enough data to classify a trend" - the row is
# real (not a blank cell), but the classification itself is below CDC's own
# reporting criterion, which is what observed_low_coverage is for.
NSSP_TREND_LOW_COVERAGE_LABELS = {"Data Unavailable", "Limited Data"}


def ingest_nssp_snapshot(record: dict[str, str], data_root: Path, extract_timestamp: str, state) -> list[dict[str, object]]:
    path = data_root / record["source_relative_path"]
    rows = nssp_mod.read_rows(path)
    sc_state_rows = [r for r in rows if r["geography"] == state.name and r["trend_source"] == "State"]
    sc_hsa_rows = [r for r in rows if r["geography"] == state.name and r["trend_source"] == "HSA"]
    wide_rows, _ = nssp_mod.build_wide_extract(sc_state_rows, sc_hsa_rows, state.name, state.fips)

    common = {
        "stream_id": "nssp",
        "source_snapshot_id": record["source_snapshot_id"],
        "as_of_date": record["as_of_date"],
        "extract_timestamp": extract_timestamp,
        "source_file_hash": record["sha256"],
    }
    out = []
    for wrow in wide_rows:
        region_common = {
            **common,
            "observation_week": wrow["week_end"],
            "analysis_region": wrow["analysis_region"],
            "analysis_region_type": wrow["analysis_region_type"],
            "analysis_region_id": wrow["analysis_region_id"],
            "raw_geography_type": wrow["raw_geography_type"],
            "raw_geography_id": wrow["raw_geography_id"],
        }
        for pathogen in NSSP_ALL_PATHOGENS:
            for weekly_feature_key, source_field, unit, def_status in (
                ("percent_visits", f"percent_visits_{pathogen}", "percent_ed_visits", "confirmed"),
                ("percent_visits_smoothed", f"percent_visits_smoothed_{pathogen}", "percent_ed_visits", "confirmed"),
            ):
                raw_value = wrow[source_field]
                value = float(raw_value) if raw_value not in ("", None) else None
                out.append(
                    {
                        **region_common,
                        "pathogen": pathogen,
                        "feature_name": NSSP_FEATURE_NAME_MAP[weekly_feature_key],
                        "weekly_feature": value,
                        "feature_unit": unit,
                        "observation_state": observation_state_for(value),
                        "definition_status": def_status,
                    }
                )
        for pathogen in NSSP_PATHOGENS_WITH_TREND:
            raw_value = wrow[f"ed_trends_{pathogen}"]
            value = raw_value if raw_value not in ("", None) else None
            if value is None:
                trend_obs_state = "incidental_missing"
            elif value in NSSP_TREND_LOW_COVERAGE_LABELS:
                trend_obs_state = "observed_low_coverage"
            else:
                trend_obs_state = "observed_valid"
            out.append(
                {
                    **region_common,
                    "pathogen": pathogen,
                    "feature_name": "ed_trend_category",
                    "weekly_feature": value,
                    "feature_unit": "categorical_trend",
                    "observation_state": trend_obs_state,
                    "definition_status": "observed_definition_uncertain",
                }
            )
    flag_anomalies(out, NSSP_ANOMALY_FEATURE, ("analysis_region_id", "pathogen"), NSSP_ANOMALY_RELATIVE_CHANGE)
    return out


def ingest_all_nssp(registry_rows: list[dict[str, str]], data_root: Path, extract_timestamp: str, state) -> list[dict[str, object]]:
    all_rows: list[dict[str, object]] = []
    for i, record in enumerate(registry_rows, start=1):
        rows = ingest_nssp_snapshot(record, data_root, extract_timestamp, state)
        all_rows.extend(rows)
        print(f"NSSP {i}/{len(registry_rows)} {record['filename']} -> {len(rows)} {state.abbr} records", flush=True)
    return all_rows


def load_nssp_hsa_participation(path: Path) -> dict[str, str]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    return {row["hsa"]: row["flag"] for row in rows}


def annotate_nssp(rows: list[dict[str, object]], participation_by_hsa: dict[str, str]) -> list[dict[str, object]]:
    """Adds a v1 `participation_status` companion feature to NSSP's
    sub-state HSA rows, reusing check_nssp_facility_coverage.py's CDC facility-participation coverage
    check verbatim (`all_contribute` -> observed_valid; `mixed`/
    `none_contribute` -> observed_low_coverage). That check is a single
    2026-08-06 point-in-time snapshot, not a per-snapshot series, so the same
    flag is repeated across every ingested week/snapshot for a given HSA.
    Facility participation has no pathogen dimension, so pathogen =
    not_applicable. The state-level row is untouched (the coverage check is HSA-only).
    """
    exclude = {"pathogen", "feature_name", "weekly_feature", "feature_unit", "observation_state", "definition_status"}
    seen: set[tuple] = set()
    participation_rows: list[dict[str, object]] = []
    for row in rows:
        if row["analysis_region_type"] != "sub_state_hsa":
            continue
        key = (row["source_snapshot_id"], row["analysis_region_id"], row["observation_week"])
        if key in seen:
            continue
        seen.add(key)
        flag = participation_by_hsa.get(row["analysis_region"])
        if flag is None:
            continue
        low_coverage = flag in NSSP_PARTICIPATION_LOW_COVERAGE_FLAGS
        common = {k: v for k, v in row.items() if k not in exclude}
        participation_rows.append(
            {
                **common,
                "pathogen": "not_applicable",
                "feature_name": "participation_status",
                "weekly_feature": "reduced_participation" if low_coverage else "full_participation",
                "feature_unit": "categorical_coverage_status",
                "observation_state": "observed_low_coverage" if low_coverage else "observed_valid",
                "definition_status": "observed_definition_uncertain",
            }
        )
    return rows + participation_rows


NWSS_FEATURE_NAME_MAP = {
    "site_wval": "wastewater_activity_level",
    "site_wval_category": "wastewater_activity_category",
}


def ingest_nwss_snapshot(record: dict[str, str], data_root: Path, extract_timestamp: str, state) -> list[dict[str, object]]:
    """Builds one NWSS snapshot's per-state canonical rows, reusing
    extract_nwss.py's own read/filter/extract functions unchanged and applying the same
    canonicalization mapping validate_weekly_schema.py uses for a single snapshot (present-
    nonzero -> observed_valid, present-zero -> observed_zero, null ->
    incidental_missing), generalized here across every registered snapshot. The
    participation_status and anomaly rules (annotate_nwss) use only this snapshot's own data:
    its per-site coverage ratios and blended-source weeks, so a published snapshot's rows do
    not change when later snapshots arrive.
    """
    path = data_root / record["source_relative_path"]
    raw_rows = nwss_mod.read_rows(path)
    sc_extract = nwss_mod.build_sc_extract(raw_rows, state.name)
    pre_canonical = nwss_mod.build_long_records(sc_extract, record["sha256"], record["as_of_date"], extract_timestamp)

    out = []
    for row in pre_canonical:
        value = row["weekly_feature_value"]
        feature_name = NWSS_FEATURE_NAME_MAP[row["weekly_feature"]]
        if feature_name == "wastewater_activity_category":
            observation_state = "incidental_missing" if value in (None, "") else "observed_valid"
            definition_status = "confirmed"
        else:
            observation_state = observation_state_for(value)
            definition_status = "observed_definition_uncertain"
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
                "observation_state": observation_state,
                "definition_status": definition_status,
                "source_file_hash": row["source_file_hash"],
            }
        )
    ratios = {(r["site"], r["pathogen"]): r["weeks_present_since_inclusion"] / r["weeks_expected_since_inclusion"]
              for r in nwss_mod.build_coverage_gap_report(sc_extract) if r["weeks_expected_since_inclusion"]}
    blend = {(r["site"], r["pathogen_normalized"], r["week_end"]) for r in sc_extract if "," in r["source"]}
    if out:
        NWSS_SOURCE_BLEND_KEYS.update((out[0]["source_snapshot_id"], *k) for k in blend)
    return annotate_nwss(out, ratios, blend)


def ingest_all_nwss(registry_rows: list[dict[str, str]], data_root: Path, extract_timestamp: str, state) -> list[dict[str, object]]:
    all_rows: list[dict[str, object]] = []
    for i, record in enumerate(registry_rows, start=1):
        rows = ingest_nwss_snapshot(record, data_root, extract_timestamp, state)
        all_rows.extend(rows)
        print(f"NWSS {i}/{len(registry_rows)} {record['filename']} -> {len(rows)} {state.abbr} records", flush=True)
    return all_rows


def annotate_nwss(
    rows: list[dict[str, object]],
    coverage_ratios: dict[tuple[str, str], float],
    source_blend_keys: set[tuple[str, str, str]],
) -> list[dict[str, object]]:
    """Adds v1 `participation_status` companion rows and applies the v1
    `flagged_anomalous` tier-jump anomaly rule and source-blend anomaly
    rule to one snapshot's canonical NWSS rows, with that snapshot's
    coverage ratios and blended-source weeks. All are provisional rules,
    not clinically validated.
    """
    exclude = {"feature_name", "weekly_feature", "feature_unit", "observation_state", "definition_status"}
    participation_rows: list[dict[str, object]] = []
    for row in rows:
        if row["feature_name"] != "wastewater_activity_level":
            continue
        threshold = NWSS_LOW_COVERAGE_THRESHOLD.get(row["pathogen"])
        ratio = coverage_ratios.get((row["analysis_region_id"], row["pathogen"]))
        if threshold is None or ratio is None:
            continue
        low_coverage = ratio < threshold
        common = {k: v for k, v in row.items() if k not in exclude}
        participation_rows.append(
            {
                **common,
                "feature_name": "participation_status",
                "weekly_feature": "reduced_participation" if low_coverage else "full_participation",
                "feature_unit": "categorical_coverage_status",
                "observation_state": "observed_low_coverage" if low_coverage else "observed_valid",
                "definition_status": "observed_definition_uncertain",
            }
        )

    category_groups: dict[tuple, list[dict[str, object]]] = defaultdict(list)
    level_by_key: dict[tuple, dict[str, object]] = {}
    for row in rows:
        key = (row["analysis_region_id"], row["pathogen"])
        if row["feature_name"] == "wastewater_activity_category":
            category_groups[key].append(row)
        elif row["feature_name"] == "wastewater_activity_level":
            level_by_key[(key, row["observation_week"])] = row

    for key, group_rows in category_groups.items():
        group_rows.sort(key=lambda r: r["observation_week"])
        prev_tier = None
        for row in group_rows:
            tier = NWSS_CATEGORY_ORDER.get(row["weekly_feature"])
            if (
                row["observation_state"] == "observed_valid"
                and tier is not None
                and prev_tier is not None
                and abs(tier - prev_tier) >= NWSS_ANOMALY_TIER_JUMP
            ):
                row["observation_state"] = "flagged_anomalous"
                level_row = level_by_key.get((key, row["observation_week"]))
                if level_row is not None and level_row["observation_state"] == "observed_valid":
                    level_row["observation_state"] = "flagged_anomalous"
            if tier is not None:
                prev_tier = tier

    for row in rows:
        if row["feature_name"] not in ("wastewater_activity_level", "wastewater_activity_category"):
            continue
        if row["observation_state"] != "observed_valid":
            continue
        key = (row["analysis_region_id"], row["pathogen"], row["observation_week"])
        if key in source_blend_keys:
            row["observation_state"] = "flagged_anomalous"

    return rows + participation_rows


def parse_week(observation_week: str) -> date:
    return datetime.strptime(observation_week, "%Y-%m-%d").date()


def build_reduced_participation_lookup(view_rows: list[dict[str, object]]) -> set[tuple[str, str, str | None, str]]:
    """(stream_id, analysis_region_id, pathogen-or-None, observation_week) keys whose
    coverage_status (NHSN) or participation_status (NSSP HSA, NWSS) companion feature
    reads 'reduced_participation' in this view. NSSP's participation_status has no
    pathogen dimension (pathogen='not_applicable' - see annotate_nssp), so it is keyed
    with pathogen=None and applies to every pathogen of that HSA/week; NHSN and NWSS
    already carry a real pathogen on their companion rows, so they key exactly.
    """
    keys: set[tuple[str, str, str | None, str]] = set()
    for row in view_rows:
        if row["feature_name"] not in ("coverage_status", "participation_status"):
            continue
        if row["weekly_feature"] != "reduced_participation":
            continue
        pathogen_key = None if row["pathogen"] == "not_applicable" else row["pathogen"]
        keys.add((row["stream_id"], row["analysis_region_id"], pathogen_key, row["observation_week"]))
    return keys


def is_degraded(row: dict[str, object], reduced_participation_keys: set[tuple[str, str, str | None, str]]) -> bool:
    if row["observation_state"] in DEGRADED_OBSERVATION_STATES:
        return True
    exact = (row["stream_id"], row["analysis_region_id"], row["pathogen"], row["observation_week"])
    region_wide = (row["stream_id"], row["analysis_region_id"], None, row["observation_week"])
    return exact in reduced_participation_keys or region_wide in reduced_participation_keys


def annotate_recency_and_persistence(view_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    """Adds two new
    companion fields to each stream's designated primary series (PRIMARY_RECENCY_FEATURES):

    - `weeks_since_last_valid`: weeks since the most recent *earlier* week in the same
      (stream, analysis_region_id, pathogen) series whose observation_state was
      observed_valid or observed_zero. `incidental_missing` (no companion value yet)
      when no such earlier week exists in this view.
    - `persistent_limitation_flag`: 'true' when at least PERSISTENCE_MIN_DEGRADED_WEEKS of
      the trailing PERSISTENCE_WINDOW_WEEKS weeks (fewer at the start of a series) are
      degraded - either the primary feature's own observation_state, or its coverage_status/
      participation_status companion reading reduced_participation for that region/week.
      The window itself is a provisional v1 placeholder, not a validated cutoff.

    `view_rows` must already be a single, already-selected view (e.g. one point-in-time
    reconstruction from build_view) - never a mix of rows from different cutoffs, since
    that would silently blend hindsight into a signal meant to describe what was knowable
    as of that one cutoff.
    """
    reduced_participation_keys = build_reduced_participation_lookup(view_rows)
    groups: dict[tuple, list[dict[str, object]]] = defaultdict(list)
    for row in view_rows:
        if row["feature_name"] == PRIMARY_RECENCY_FEATURES.get(row["stream_id"]):
            groups[(row["stream_id"], row["analysis_region_id"], row["pathogen"])].append(row)

    # Unlike annotate_nssp's participation_status (pathogen-independent, so it drops
    # pathogen), these companion fields describe their primary series' own pathogen and
    # must keep it - dropping it here would make build_key() KeyError on these new rows.
    exclude = {"feature_name", "weekly_feature", "feature_unit", "observation_state", "definition_status"}
    new_rows: list[dict[str, object]] = []
    for group_rows in groups.values():
        group_rows.sort(key=lambda r: r["observation_week"])
        last_valid_week: date | None = None
        degraded_history: list[bool] = []
        for row in group_rows:
            week = parse_week(row["observation_week"])
            weeks_since = (week - last_valid_week).days // 7 if last_valid_week is not None else None
            common = {k: v for k, v in row.items() if k not in exclude}
            new_rows.append(
                {
                    **common,
                    "feature_name": "weeks_since_last_valid",
                    "weekly_feature": weeks_since,
                    "feature_unit": "weeks",
                    "observation_state": "observed_valid" if weeks_since is not None else "incidental_missing",
                    "definition_status": "confirmed",
                }
            )

            degraded_history.append(is_degraded(row, reduced_participation_keys))
            window = degraded_history[-PERSISTENCE_WINDOW_WEEKS:]
            persistent = len(window) >= PERSISTENCE_MIN_DEGRADED_WEEKS and sum(window) >= PERSISTENCE_MIN_DEGRADED_WEEKS
            new_rows.append(
                {
                    **common,
                    "feature_name": "persistent_limitation_flag",
                    "weekly_feature": "true" if persistent else "false",
                    "feature_unit": "categorical_boolean",
                    "observation_state": "observed_valid",
                    "definition_status": "observed_definition_uncertain",
                }
            )

            if row["observation_state"] in ("observed_valid", "observed_zero"):
                last_valid_week = week

    return view_rows + new_rows


def build_key(row: dict[str, object]) -> tuple:
    return (row["stream_id"], row["analysis_region_id"], row["analysis_region_type"], row["observation_week"], row["pathogen"], row["feature_name"])


def build_view(all_versions: list[dict[str, object]], cutoff: str | None) -> list[dict[str, object]]:
    best: dict[tuple, dict[str, object]] = {}
    for row in all_versions:
        if cutoff is not None and row["as_of_date"] > cutoff:
            continue
        key = build_key(row)
        current = best.get(key)
        if current is None or row["as_of_date"] > current["as_of_date"]:
            best[key] = row
    return list(best.values())


def check_snapshot_ordering(all_versions: list[dict[str, object]]) -> dict[str, object]:
    by_key: dict[tuple, list[dict[str, object]]] = defaultdict(list)
    for row in all_versions:
        by_key[build_key(row)].append(row)
    violations = 0
    keys_with_multiple_snapshots = 0
    for key, versions in by_key.items():
        distinct_snapshots = {v["source_snapshot_id"] for v in versions}
        if len(distinct_snapshots) > 1:
            keys_with_multiple_snapshots += 1
        as_of_dates = sorted({v["as_of_date"] for v in versions})
        # strictly increasing as_of_date values are expected to be distinct; a
        # violation would be two different snapshots claiming the same as_of_date
        # AND a different value for the same key with no deterministic ordering.
        seen = set()
        for v in versions:
            k2 = (v["source_snapshot_id"], v["as_of_date"])
            if k2 in seen:
                violations += 1
            seen.add(k2)
    return {"keys_with_multiple_snapshots": keys_with_multiple_snapshots, "duplicate_snapshot_asof_pairs": violations}


def duplicate_check(rows: list[dict[str, object]]) -> int:
    counter: dict[tuple, int] = defaultdict(int)
    for row in rows:
        key = build_key(row) + (row["source_snapshot_id"],)
        counter[key] += 1
    return sum(1 for v in counter.values() if v > 1)


def join_census_context(census_extract_dir: Path, state) -> list[dict[str, object]]:
    prefix = state.abbr.lower()
    with (census_extract_dir / f"acs_{prefix}_state.csv").open("r", encoding="utf-8", newline="") as handle:
        acs_row = next(csv.DictReader(handle))
    with (census_extract_dir / f"{prefix}_population_estimates_state.csv").open("r", encoding="utf-8", newline="") as handle:
        pop_row = next(csv.DictReader(handle))
    context = []
    for field, value in acs_row.items():
        if field in ("geoid", "name"):
            continue
        context.append({"analysis_region": state.name, "analysis_region_type": "state", "analysis_region_id": state.fips, "product": "acs_5yr_2022", "variable": field, "value": value})
    context.append({"analysis_region": state.name, "analysis_region_type": "state", "analysis_region_id": state.fips, "product": "population_estimates_2022", "variable": "POPESTIMATE2022", "value": pop_row["POPESTIMATE2022"]})
    return context


def write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    global NHSN_LOW_COVERAGE_THRESHOLD, NHSN_ANOMALY_RELATIVE_CHANGE, NSSP_ANOMALY_RELATIVE_CHANGE, NWSS_LOW_COVERAGE_THRESHOLD
    args = parse_args()
    state = resolve_state(args.state)
    prefix = state.abbr.lower()
    extract_timestamp = datetime.now(timezone.utc).isoformat()
    output = args.output_dir
    output.mkdir(parents=True, exist_ok=True)

    # These four thresholds default to SC's original fixed values (see parse_args), so a
    # bare `--state SC` run reproduces the original SC prototype. Any other state should be
    # run with its own values from derive_state_thresholds.py's output, passed explicitly on
    # the command line - never SC's numbers reused unchanged.
    NHSN_LOW_COVERAGE_THRESHOLD = args.nhsn_low_coverage_threshold
    NHSN_ANOMALY_RELATIVE_CHANGE = args.nhsn_anomaly_relative_change
    NSSP_ANOMALY_RELATIVE_CHANGE = args.nssp_anomaly_relative_change
    NWSS_LOW_COVERAGE_THRESHOLD = {
        "covid": args.nwss_low_coverage_covid,
        "influenza": args.nwss_low_coverage_influenza,
        "rsv": args.nwss_low_coverage_rsv,
    }

    nhsn_registry = load_registry(args.nhsn_nssp_registry, "nhsn")
    nssp_registry = load_registry(args.nhsn_nssp_registry, "nssp")
    nwss_registry = load_registry(args.nwss_registry, "nwss")

    nhsn_rows = ingest_all_nhsn(nhsn_registry, args.data_root, extract_timestamp, state)
    nssp_rows = ingest_all_nssp(nssp_registry, args.data_root, extract_timestamp, state)
    nssp_hsa_participation = load_nssp_hsa_participation(args.hsa_facility_coverage_check)
    nssp_rows = annotate_nssp(nssp_rows, nssp_hsa_participation)
    nwss_rows = ingest_all_nwss(nwss_registry, args.data_root, extract_timestamp, state)

    all_versions = nhsn_rows + nssp_rows + nwss_rows
    large_output = args.large_output_dir or output
    write_csv(large_output / f"{prefix}_all_versions.csv", all_versions, CANONICAL_FIELDS)

    latest_view = build_view(all_versions, cutoff=None)
    write_csv(output / f"{prefix}_latest_view.csv", latest_view, CANONICAL_FIELDS)

    cutoff = args.point_in_time_cutoff
    pit_view = build_view(all_versions, cutoff=cutoff)
    # Recency/persistence are only added to the point-in-time view, never to
    # latest_view above - see annotate_recency_and_persistence's docstring for why.
    pit_view = annotate_recency_and_persistence(pit_view)
    write_csv(output / f"{prefix}_point_in_time_view_{cutoff}.csv", pit_view, CANONICAL_FIELDS)

    ordering = check_snapshot_ordering(all_versions)
    dup_all_versions = duplicate_check(all_versions)
    dup_latest = duplicate_check(latest_view)
    dup_pit = duplicate_check(pit_view)

    latest_by_key = {build_key(r): r for r in latest_view}
    pit_by_key = {build_key(r): r for r in pit_view}
    differing_keys = [
        k for k in pit_by_key
        if k in latest_by_key and pit_by_key[k]["weekly_feature"] != latest_by_key[k]["weekly_feature"]
    ]

    def count(rows: list[dict[str, object]], stream_id: str, feature_name: str, observation_state: str) -> int:
        return sum(
            1 for r in rows
            if r["stream_id"] == stream_id and r["feature_name"] == feature_name and r["observation_state"] == observation_state
        )

    v1_counts = {
        "nhsn_reduced_participation": count(latest_view, "nhsn", "coverage_status", "observed_low_coverage"),
        "nhsn_structural_absent": sum(1 for r in latest_view if r["stream_id"] == "nhsn" and r["observation_state"] == "structural_absent"),
        "nhsn_anomalous": count(latest_view, "nhsn", "hospital_admission_count", "flagged_anomalous"),
        "nssp_anomalous": count(latest_view, "nssp", NSSP_ANOMALY_FEATURE, "flagged_anomalous"),
        "nssp_reduced_participation": count(latest_view, "nssp", "participation_status", "observed_low_coverage"),
        "nwss_reduced_participation": count(latest_view, "nwss", "participation_status", "observed_low_coverage"),
        "nwss_anomalous": count(latest_view, "nwss", "wastewater_activity_level", "flagged_anomalous"),
        "nwss_source_blend_anomalous": sum(
            1 for r in latest_view
            if r["stream_id"] == "nwss" and r["feature_name"] == "wastewater_activity_level"
            and r["observation_state"] == "flagged_anomalous"
            and (r["source_snapshot_id"], r["analysis_region_id"], r["pathogen"], r["observation_week"])
            in NWSS_SOURCE_BLEND_KEYS
        ),
    }

    persistence_counts = {
        "persistent_limitation_true": sum(
            1 for r in pit_view if r["feature_name"] == "persistent_limitation_flag" and r["weekly_feature"] == "true"
        ),
        "weeks_since_last_valid_undefined": sum(
            1 for r in pit_view if r["feature_name"] == "weeks_since_last_valid" and r["observation_state"] == "incidental_missing"
        ),
        "weeks_since_last_valid_total": sum(1 for r in pit_view if r["feature_name"] == "weeks_since_last_valid"),
    }

    census_context = join_census_context(args.census_extract_dir, state)
    write_csv(output / f"{prefix}_census_context.csv", census_context, ["analysis_region", "analysis_region_type", "analysis_region_id", "product", "variable", "value"])

    nssp_hsa_count = len({r["analysis_region_id"] for r in nssp_rows if r["analysis_region_type"] == "sub_state_hsa"})
    nwss_site_count = len({r["analysis_region_id"] for r in nwss_rows})

    lines = [
        f"# {state.name} observation tables quality and exclusion report",
        "",
        f"Run date: {datetime.now(timezone.utc).date().isoformat()}",
        f"Point-in-time cutoff demonstrated: {cutoff}",
        "",
        "## Ingestion summary",
        "",
        f"- NHSN: {len(nhsn_registry)} historical snapshots ingested, {len(nhsn_rows):,} {state.abbr} records.",
        f"- NSSP: {len(nssp_registry)} historical snapshots ingested, {len(nssp_rows):,} {state.abbr} records.",
        f"- NWSS: {len(nwss_registry)} historical snapshots ingested, {len(nwss_rows):,} {state.abbr} records.",
        f"- All-versions canonical table: {len(all_versions):,} records.",
        f"- Latest-available view: {len(latest_view):,} records.",
        f"- Point-in-time view at {cutoff}: {len(pit_view):,} records.",
        "",
        "## Acceptance checks",
        "",
        f"- Duplicate (key + source_snapshot_id) rows in all-versions table: {dup_all_versions} (expected 0).",
        f"- Duplicate keys in latest-available view: {dup_latest} (expected 0 - one row per key by construction).",
        f"- Duplicate keys in point-in-time view: {dup_pit} (expected 0 - one row per key by construction).",
        f"- Observation keys with more than one distinct source_snapshot_id (i.e. historical versions coexist without key collision): {ordering['keys_with_multiple_snapshots']:,}.",
        f"- Duplicate (source_snapshot_id, as_of_date) pairs claiming the same key (would indicate non-deterministic ordering): {ordering['duplicate_snapshot_asof_pairs']} (expected 0).",
        f"- Observation keys where the point-in-time value differs from the latest-available value: {len(differing_keys):,} of {len(pit_by_key):,} shared keys - demonstrates the point-in-time view is a genuine reconstruction from earlier snapshots, not simply re-deriving the latest view.",
        "",
        "## Excluded rows and features",
        "",
        "- NHSN: only `Total <Pathogen> Admissions` (outcome) and `Number Hospitals Reporting <Pathogen> Admissions` (coverage denominator) are ingested per the confirmed outcome mapping (validate_weekly_schema.py). All other NHSN fields (occupancy/census measures, ICU-specific counts, age-stratified breakdowns, per-100,000 rates, week-over-week change measures) are excluded from this prototype; they were not part of the confirmed outcome decision.",
        f"- NSSP: the national (`United States`) row is out of scope, consistent with extract_nssp.py. All {nssp_hsa_count} {state.name} HSAs and the state-level row are included for every one of the {len(nssp_registry)} historical snapshots.",
        f"- NWSS: same site set as extract_nwss.py ({nwss_site_count} {state.name} sites); every retained dated snapshot is ingested, not just the latest one, the same as NHSN/NSSP.",
        "- Census: only the confirmed state-level ACS 5-year 2022 variable set and the 2022 state population estimate are joined as context, at the `state` geography level only. No county-, ZCTA-, or HSA-level Census join is performed in this prototype: NSSP's sub-state unit is the HSA, which has no official Census correspondence, and any HSA-to-county population weighting would rely on the Community Data crosswalk package whose weight provenance remains `definition_uncertain` (see extract_census_population.py). Joining unverified weights was avoided rather than asserted.",
        "",
        "## v1 observation-state extensions (all in the latest-available view)",
        "",
        "Provisional rules; none are clinically validated.",
        "",
        f"The four numeric cutoffs below (NHSN low-coverage/anomaly, NSSP anomaly, NWSS low-coverage) "
        f"were passed in via this run's `--nhsn-low-coverage-threshold`/`--nhsn-anomaly-relative-change`/"
        f"`--nssp-anomaly-relative-change`/`--nwss-low-coverage-*` arguments; for `--state SC` with no "
        f"override they equal SC's original fixed values, otherwise "
        f"they should be this state's own value from "
        f"`derive_state_thresholds.py`'s output, not SC's numbers reused unchanged.",
        "",
        f"- NHSN `coverage_status = observed_low_coverage` (hospital_reporting_count < {NHSN_LOW_COVERAGE_THRESHOLD}): {v1_counts['nhsn_reduced_participation']:,} pathogen-weeks.",
        f"- NHSN records reclassified `structural_absent` (RSV before {NHSN_RSV_STRUCTURAL_ABSENT_LAST_WEEK}): {v1_counts['nhsn_structural_absent']:,} records (hospital_admission_count + hospital_reporting_count + coverage_status).",
        f"- NHSN `hospital_admission_count` flagged `flagged_anomalous` (|week-over-week change| >= {NHSN_ANOMALY_RELATIVE_CHANGE:.0%}): {v1_counts['nhsn_anomalous']:,} pathogen-weeks.",
        f"- NSSP `{NSSP_ANOMALY_FEATURE}` flagged `flagged_anomalous` (|week-over-week change| >= {NSSP_ANOMALY_RELATIVE_CHANGE:.0%}): {v1_counts['nssp_anomalous']:,} region-pathogen-weeks.",
        f"- NSSP `participation_status = observed_low_coverage` (HSA facility-participation flag in {sorted(NSSP_PARTICIPATION_LOW_COVERAGE_FLAGS)}, from check_nssp_facility_coverage.py's 2026-08-06 CDC coverage-map check): {v1_counts['nssp_reduced_participation']:,} HSA-weeks.",
        f"- NWSS `participation_status = observed_low_coverage` (coverage ratio below the pathogen-specific cutoff - {', '.join(f'{k}: {v:.2f}' for k, v in NWSS_LOW_COVERAGE_THRESHOLD.items())}): {v1_counts['nwss_reduced_participation']:,} site-pathogen-weeks.",
        f"- NWSS `wastewater_activity_level` flagged `flagged_anomalous` (paired `wastewater_activity_category` tier jump >= {NWSS_ANOMALY_TIER_JUMP}, or a blended `source` value): {v1_counts['nwss_anomalous']:,} site-pathogen-weeks, of which {v1_counts['nwss_source_blend_anomalous']:,} were triggered by a blended-source week specifically.",
        "",
        f"## Recency/persistence fields (point-in-time view only - cutoff {cutoff})",
        "",
        "Computed only "
        f"against the point-in-time view at {cutoff}, never the latest-available view, so the values reflect "
        "only what that cutoff could have known. `persistent_limitation_flag`'s N/K window is a provisional "
        "v1 placeholder, not a validated cutoff.",
        "",
        f"- `weeks_since_last_valid` computed for {persistence_counts['weeks_since_last_valid_total']:,} primary-series pathogen/region-weeks "
        f"(NHSN `hospital_admission_count`, NSSP `ed_visit_percent`, NWSS `wastewater_activity_level`); "
        f"{persistence_counts['weeks_since_last_valid_undefined']:,} of those have no earlier valid week in this view "
        "(series start, or a structural-absence run with no preceding valid week).",
        f"- `persistent_limitation_flag = true` (>= {PERSISTENCE_MIN_DEGRADED_WEEKS} of the trailing {PERSISTENCE_WINDOW_WEEKS} weeks degraded, per the placeholder window above): {persistence_counts['persistent_limitation_true']:,} primary-series pathogen/region-weeks.",
        "",
        "## Closed definition uncertainties carried into this prototype",
        "",
        "- NHSN preliminary/final revision-maturity policy: `definition_uncertain`, unaffected by this build. Where a snapshot's `as_of_date` ties across channels, `standard` is deterministically preferred over `preliminary` for view selection; this is a documented tie-break, not an assertion that `standard` means final.",
        "- NSSP `ed_trend_category` and NWSS raw WVAL magnitude: `observed_definition_uncertain`, retained as optional/sensitivity fields only, consistent with extract_nssp.py, extract_nwss.py, and validate_weekly_schema.py.",
        "- NSSP sub-state predictor unit is the HSA, not the county (extract_nssp.py); NWSS remains site-level (extract_nwss.py). Neither is reinterpreted in this prototype.",
    ]
    (output / f"{prefix}_quality_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(
        f"DONE state={state.abbr} nhsn={len(nhsn_rows)} nssp={len(nssp_rows)} nwss={len(nwss_rows)} "
        f"all_versions={len(all_versions)} latest={len(latest_view)} pit={len(pit_view)} "
        f"differing_keys={len(differing_keys)} output={output} large_output={large_output}",
        flush=True,
    )


if __name__ == "__main__":
    main()
