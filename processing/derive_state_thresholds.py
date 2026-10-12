#!/usr/bin/env python3
"""Build the per-state observation-state threshold derivation for the 50-state processing.

`build_observation_tables.py`'s v1 observation-state thresholds (`NHSN_LOW_COVERAGE_THRESHOLD`,
`NHSN_ANOMALY_RELATIVE_CHANGE`, `NSSP_ANOMALY_RELATIVE_CHANGE`, `NWSS_LOW_COVERAGE_THRESHOLD`) were
all calibrated to South Carolina's own observed distributions - not clinically validated, and not
necessarily appropriate for a state with a very different reporting-coverage or ED-visit-volume
profile. This script recomputes each one per state using the same selection principle the NWSS
pathogen split already used (see `build_observation_tables.py`'s comment on `NWSS_LOW_COVERAGE_THRESHOLD`):
pick the target tail share SC's fixed cutoff currently flags, then find the value that flags that
same tail share of each state's own distribution, rather than reusing SC's fixed numbers unchanged.

Reads the single latest registered NHSN and NSSP snapshots (same file-selection convention as
`build_observation_tables.py`/`nhsn_nssp_snapshot_registry.csv`) and the single NWSS Main-Three file (same file
extract_nwss.py and audit_national_geography.py already read) - no new data acquisition, one nationwide
pass over each file computes every in-scope state's distributions at once, following
audit_national_geography.py's pattern. Read-only against all source
files.
"""

from __future__ import annotations

import argparse
import csv
import re
from collections import defaultdict
from pathlib import Path

import extract_nssp as nssp_mod
import extract_nwss as nwss_mod
from state_reference import ABBR_BY_NAME, IN_SCOPE_ABBRS, STATES

# The current SC-calibrated fixed values in build_observation_tables.py - kept here only as the baseline
# this script recomputes a target tail share *from*, never as a default any state should reuse.
SC_NHSN_LOW_COVERAGE_THRESHOLD = 50
SC_NHSN_ANOMALY_RELATIVE_CHANGE = 1.00
SC_NSSP_ANOMALY_RELATIVE_CHANGE = 0.35
# NWSS target tail shares are fixed directly (2026-09-29): SC's original 0.80/0.90/0.90 cutoffs were
# chosen to flag these shares of SC's site-pathogen pairs, but on a coverage ratio whose numerator
# counted pre-inclusion weeks (see extract_nwss.build_coverage_gap_report). On the corrected ratio the same
# cutoffs flag 29%/46%/46%, so the intent (the share) is kept and SC's cutoffs are re-derived.
SC_NWSS_TARGET_TAIL_SHARE = {"covid": 7 / 31, "influenza": 6 / 28, "rsv": 5 / 26}
NHSN_RSV_STRUCTURAL_ABSENT_LAST_WEEK = "2024-08-03"
NSSP_ANOMALY_FEATURE_FIELD = "percent_visits_smoothed_{}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", required=True, type=Path)
    parser.add_argument("--nhsn-nssp-registry", required=True, type=Path)
    parser.add_argument("--nwss-file", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser.parse_args()


def load_latest_registry_record(path: Path, stream_id: str) -> dict[str, str]:
    channel_rank = {"standard": 0, "preliminary": 1}
    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = [r for r in csv.DictReader(handle) if r["stream_id"] == stream_id]
    rows.sort(key=lambda r: (r["as_of_date"], channel_rank.get(r["release_channel"], 2), r["filename"]))
    return rows[-1]


def clean_number(raw: str) -> float | None:
    text = raw.replace(",", "").strip()
    if not text:
        return None
    return float(text)


def percentile(sorted_values: list[float], p: float) -> float:
    """Linear-interpolation percentile (p in [0, 1]) over an ascending-sorted list."""
    if not sorted_values:
        raise ValueError("cannot take a percentile of an empty distribution")
    if len(sorted_values) == 1:
        return sorted_values[0]
    rank = p * (len(sorted_values) - 1)
    lo = int(rank)
    hi = min(lo + 1, len(sorted_values) - 1)
    frac = rank - lo
    return sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * frac


def share_below(sorted_values: list[float], threshold: float) -> float:
    if not sorted_values:
        raise ValueError("cannot compute a tail share of an empty distribution")
    below = sum(1 for v in sorted_values if v < threshold)
    return below / len(sorted_values)


def share_at_or_above(sorted_values: list[float], threshold: float) -> float:
    if not sorted_values:
        raise ValueError("cannot compute a tail share of an empty distribution")
    above = sum(1 for v in sorted_values if v >= threshold)
    return above / len(sorted_values)


def relative_changes(week_values: list[tuple[str, float | None]]) -> list[float]:
    """|relative change| between each week's value and the closest earlier nonzero value in the
    same chronologically sorted series, replicating `build_observation_tables.py`'s `flag_anomalies`
    exactly: only defined when both the current and prior values are present and nonzero (a zero
    prior makes relative change undefined; a zero current value is `observed_zero`, never a
    flaggable anomaly candidate, so it is excluded from the *output* distribution but still updates
    what counts as "prior" for the next week, exactly as `flag_anomalies` does).
    """
    ordered = sorted(week_values, key=lambda wv: wv[0])
    changes: list[float] = []
    prev_value: float | None = None
    for _week, value in ordered:
        if value is not None and value != 0 and prev_value is not None and prev_value != 0:
            changes.append(abs((value - prev_value) / prev_value))
        if value is not None:
            prev_value = value
    return changes


# ---------------------------------------------------------------------------
# NHSN: per-state hospital_reporting_count distribution and admission-count relative changes.
# ---------------------------------------------------------------------------

NHSN_PATHOGEN_FIELDS = {
    "covid": ("Total COVID-19 Admissions", "Number Hospitals Reporting COVID-19 Admissions"),
    "influenza": ("Total Influenza Admissions", "Number Hospitals Reporting Influenza Admissions"),
    "rsv": ("Total RSV Admissions", "Number Hospitals Reporting RSV Admissions"),
}
NHSN_WEEK_COLUMN = "Week Ending Date"
NHSN_GEO_COLUMN = "Geographic aggregation"


def normalize_header(name: str) -> str:
    return re.sub(r"\s+", " ", name).strip()


def nhsn_is_rsv_structural_absent(pathogen: str, observation_week: str) -> bool:
    return pathogen == "rsv" and observation_week <= NHSN_RSV_STRUCTURAL_ABSENT_LAST_WEEK


def collect_nhsn_distributions(
    path: Path,
) -> tuple[dict[str, list[float]], dict[tuple[str, str], list[tuple[str, float | None]]]]:
    """Returns (state_abbr -> pooled hospital_reporting_count values across pathogens,
    (state_abbr, pathogen) -> [(week, hospital_admission_count), ...] - kept per-pathogen here so
    relative change is computed within one pathogen's own chronological series, mirroring
    `flag_anomalies`'s per-pathogen grouping; callers pool the resulting changes across pathogens.
    """
    reporting_counts: dict[str, list[float]] = defaultdict(list)
    admission_series: dict[tuple[str, str], list[tuple[str, float | None]]] = defaultdict(list)

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        raw_header = next(reader)
        norm_to_actual = {normalize_header(h): h for h in raw_header}
        week_idx = raw_header.index(norm_to_actual.get(normalize_header(NHSN_WEEK_COLUMN), NHSN_WEEK_COLUMN))
        geo_idx = raw_header.index(NHSN_GEO_COLUMN)
        field_indices = {
            pathogen: (
                raw_header.index(norm_to_actual[normalize_header(count_col)]),
                raw_header.index(norm_to_actual[normalize_header(denom_col)]),
            )
            for pathogen, (count_col, denom_col) in NHSN_PATHOGEN_FIELDS.items()
            if normalize_header(count_col) in norm_to_actual and normalize_header(denom_col) in norm_to_actual
        }
        for row in reader:
            state_abbr = row[geo_idx]
            if state_abbr not in IN_SCOPE_ABBRS:
                continue
            observation_week = row[week_idx]
            for pathogen, (count_i, denom_i) in field_indices.items():
                structural_absent = nhsn_is_rsv_structural_absent(pathogen, observation_week)
                count_val = clean_number(row[count_i])
                denom_val = clean_number(row[denom_i])
                admission_series[(state_abbr, pathogen)].append(
                    (observation_week, None if structural_absent else count_val)
                )
                if not structural_absent and denom_val is not None:
                    reporting_counts[state_abbr].append(denom_val)

    return reporting_counts, admission_series


# ---------------------------------------------------------------------------
# NSSP: per-state ed_visit_percent_smoothed relative changes (state + HSA regions, all 4
# pathogen columns, same grouping `flag_anomalies` uses).
# ---------------------------------------------------------------------------

def collect_nssp_series(path: Path) -> dict[str, dict[tuple[str, str], list[tuple[str, float | None]]]]:
    """Returns state_abbr -> (analysis_region_id, pathogen) -> [(week, value), ...]."""
    rows = nssp_mod.read_rows(path)
    by_state_name: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        by_state_name[row["geography"]].append(row)

    result: dict[str, dict[tuple[str, str], list[tuple[str, float | None]]]] = {}
    for state_name, state_rows in by_state_name.items():
        abbr = ABBR_BY_NAME.get(state_name)
        if abbr is None:
            continue
        state_info = STATES[abbr]
        state_state_rows = [r for r in state_rows if r["trend_source"] == "State"]
        state_hsa_rows = [r for r in state_rows if r["trend_source"] == "HSA"]
        wide_rows, _ = nssp_mod.build_wide_extract(state_state_rows, state_hsa_rows, state_info.name, state_info.fips)
        series: dict[tuple[str, str], list[tuple[str, float | None]]] = defaultdict(list)
        for wrow in wide_rows:
            for pathogen in nssp_mod.ALL_PATHOGENS:
                raw_value = wrow[NSSP_ANOMALY_FEATURE_FIELD.format(pathogen)]
                value = float(raw_value) if raw_value not in ("", None) else None
                series[(wrow["analysis_region_id"], pathogen)].append((wrow["week_end"], value))
        result[abbr] = series
    return result


# ---------------------------------------------------------------------------
# NWSS: per-state, per-pathogen coverage-ratio distribution (weeks_present / weeks_expected).
# ---------------------------------------------------------------------------

def collect_nwss_coverage_ratios(path: Path) -> dict[str, dict[str, list[float]]]:
    """Returns state_abbr -> pathogen -> [coverage_ratio, ...] (one per site)."""
    rows = nwss_mod.read_rows(path)
    by_state_name: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        by_state_name[row["State/Territory"]].append(row)

    result: dict[str, dict[str, list[float]]] = {}
    for state_name, state_rows in by_state_name.items():
        abbr = ABBR_BY_NAME.get(state_name)
        if abbr is None:
            continue
        extract = nwss_mod.build_sc_extract(state_rows, state_name)
        coverage = nwss_mod.build_coverage_gap_report(extract)
        ratios: dict[str, list[float]] = defaultdict(list)
        for entry in coverage:
            if entry["weeks_expected_since_inclusion"]:
                ratios[entry["pathogen"]].append(
                    entry["weeks_present_since_inclusion"] / entry["weeks_expected_since_inclusion"]
                )
        result[abbr] = ratios
    return result


def round_threshold(value: float, unit: str) -> float:
    if unit == "count":
        return round(value)
    return round(value, 2)


def main() -> None:
    args = parse_args()
    output = args.output_dir
    output.mkdir(parents=True, exist_ok=True)

    nhsn_record = load_latest_registry_record(args.nhsn_nssp_registry, "nhsn")
    nssp_record = load_latest_registry_record(args.nhsn_nssp_registry, "nssp")
    nhsn_path = args.data_root / nhsn_record["source_relative_path"]
    nssp_path = args.data_root / nssp_record["source_relative_path"]

    print(f"NHSN source: {nhsn_record['filename']} (as_of {nhsn_record['as_of_date']})", flush=True)
    print(f"NSSP source: {nssp_record['filename']} (as_of {nssp_record['as_of_date']})", flush=True)
    print(f"NWSS source: {args.nwss_file.name}", flush=True)

    reporting_counts, admission_series = collect_nhsn_distributions(nhsn_path)
    print("NHSN pass done", flush=True)
    nssp_series_by_state = collect_nssp_series(nssp_path)
    print("NSSP pass done", flush=True)
    nwss_ratios_by_state = collect_nwss_coverage_ratios(args.nwss_file)
    print("NWSS pass done", flush=True)

    def admission_changes_for(state_abbr: str) -> list[float]:
        changes: list[float] = []
        for pathogen in NHSN_PATHOGEN_FIELDS:
            series = admission_series.get((state_abbr, pathogen))
            if series:
                changes.extend(relative_changes(series))
        return changes

    def nssp_changes_for(state_abbr: str) -> list[float]:
        changes: list[float] = []
        for series in nssp_series_by_state.get(state_abbr, {}).values():
            changes.extend(relative_changes(series))
        return changes

    # --- SC baseline target tail shares, from the fixed values currently in build_observation_tables.py ---
    sc_reporting = sorted(reporting_counts["SC"])
    sc_admission_changes = sorted(admission_changes_for("SC"))
    sc_nssp_changes = sorted(nssp_changes_for("SC"))
    sc_nwss_ratios = {p: sorted(v) for p, v in nwss_ratios_by_state["SC"].items()}

    target_nhsn_coverage_share = share_below(sc_reporting, SC_NHSN_LOW_COVERAGE_THRESHOLD)
    target_nhsn_anomaly_share = share_at_or_above(sc_admission_changes, SC_NHSN_ANOMALY_RELATIVE_CHANGE)
    target_nssp_anomaly_share = share_at_or_above(sc_nssp_changes, SC_NSSP_ANOMALY_RELATIVE_CHANGE)
    target_nwss_coverage_share = dict(SC_NWSS_TARGET_TAIL_SHARE)

    print(
        f"SC target tail shares: nhsn_coverage={target_nhsn_coverage_share:.3f} "
        f"nhsn_anomaly={target_nhsn_anomaly_share:.4f} nssp_anomaly={target_nssp_anomaly_share:.4f} "
        f"nwss_coverage={ {k: round(v, 3) for k, v in target_nwss_coverage_share.items()} }",
        flush=True,
    )

    # --- per-state recomputation ---
    rows_out: list[dict[str, object]] = []
    for abbr in sorted(IN_SCOPE_ABBRS):
        reporting = sorted(reporting_counts.get(abbr, []))
        admission_chg = sorted(admission_changes_for(abbr))
        nssp_chg = sorted(nssp_changes_for(abbr))
        nwss_ratios = {p: sorted(v) for p, v in nwss_ratios_by_state.get(abbr, {}).items()}

        row: dict[str, object] = {"state": abbr, "state_name": STATES[abbr].name}

        if reporting:
            row["nhsn_low_coverage_threshold"] = round_threshold(
                percentile(reporting, target_nhsn_coverage_share), "count"
            )
            row["nhsn_reporting_count_n"] = len(reporting)
        else:
            row["nhsn_low_coverage_threshold"] = ""
            row["nhsn_reporting_count_n"] = 0

        if admission_chg:
            row["nhsn_anomaly_relative_change"] = round_threshold(
                percentile(admission_chg, 1 - target_nhsn_anomaly_share), "ratio"
            )
            row["nhsn_admission_change_n"] = len(admission_chg)
        else:
            row["nhsn_anomaly_relative_change"] = ""
            row["nhsn_admission_change_n"] = 0

        if nssp_chg:
            row["nssp_anomaly_relative_change"] = round_threshold(
                percentile(nssp_chg, 1 - target_nssp_anomaly_share), "ratio"
            )
            row["nssp_change_n"] = len(nssp_chg)
        else:
            row["nssp_anomaly_relative_change"] = ""
            row["nssp_change_n"] = 0

        for pathogen in ("covid", "influenza", "rsv"):
            ratios = nwss_ratios.get(pathogen, [])
            key = f"nwss_low_coverage_{pathogen}"
            n_key = f"nwss_{pathogen}_site_n"
            if ratios:
                row[key] = round_threshold(percentile(ratios, target_nwss_coverage_share[pathogen]), "ratio")
                row[n_key] = len(ratios)
            else:
                row[key] = ""
                row[n_key] = 0

        rows_out.append(row)

    fieldnames = [
        "state", "state_name",
        "nhsn_low_coverage_threshold", "nhsn_reporting_count_n",
        "nhsn_anomaly_relative_change", "nhsn_admission_change_n",
        "nssp_anomaly_relative_change", "nssp_change_n",
        "nwss_low_coverage_covid", "nwss_covid_site_n",
        "nwss_low_coverage_influenza", "nwss_influenza_site_n",
        "nwss_low_coverage_rsv", "nwss_rsv_site_n",
    ]
    with (output / "state_thresholds.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows_out)

    sc_row = next(r for r in rows_out if r["state"] == "SC")
    sc_regression_lines = [
        f"- NHSN low-coverage threshold: recomputed {sc_row['nhsn_low_coverage_threshold']} vs. fixed {SC_NHSN_LOW_COVERAGE_THRESHOLD}",
        f"- NHSN anomaly relative change: recomputed {sc_row['nhsn_anomaly_relative_change']} vs. fixed {SC_NHSN_ANOMALY_RELATIVE_CHANGE}",
        f"- NSSP anomaly relative change: recomputed {sc_row['nssp_anomaly_relative_change']} vs. fixed {SC_NSSP_ANOMALY_RELATIVE_CHANGE}",
        *(
            f"- NWSS {p} low-coverage: {sc_row[f'nwss_low_coverage_{p}']} (derived on the corrected ratio; "
            f"flags {share_below(sc_nwss_ratios[p], sc_row[f'nwss_low_coverage_{p}']):.1%} of SC's "
            f"{len(sc_nwss_ratios[p])} site-pathogen pairs vs. target {target_nwss_coverage_share[p]:.1%})"
            for p in ("covid", "influenza", "rsv")
        ),
    ]

    report_lines = [
        "# Per-state observation-state threshold derivation",
        "",
        "Recomputes `build_observation_tables.py`'s four v1 threshold parameters per state, using the same",
        "percentile-based selection principle already used for `NWSS_LOW_COVERAGE_THRESHOLD`'s SC/",
        "pathogen split: pick the tail share SC's current fixed value flags, then find the value that",
        "flags that same tail share of each state's own distribution. None of these are clinically",
        "validated for SC and remain equally unvalidated for every other state - this only removes the",
        "unjustified step of reusing SC's specific numbers unchanged nationwide.",
        "",
        f"Sources: `{nhsn_record['filename']}` (NHSN, as_of {nhsn_record['as_of_date']}), "
        f"`{nssp_record['filename']}` (NSSP, as_of {nssp_record['as_of_date']}), "
        f"`{args.nwss_file.name}` (NWSS).",
        "",
        "## SC target tail shares (recomputed from the fixed values currently in `build_observation_tables.py`)",
        "",
        f"- NHSN low-coverage (`hospital_reporting_count < {SC_NHSN_LOW_COVERAGE_THRESHOLD}`): {target_nhsn_coverage_share:.1%} of SC pathogen-weeks.",
        f"- NHSN anomaly (`|Δhospital_admission_count| >= {SC_NHSN_ANOMALY_RELATIVE_CHANGE:.0%}`): {target_nhsn_anomaly_share:.2%} of SC pathogen-weeks with a defined relative change.",
        f"- NSSP anomaly (`|Δed_visit_percent_smoothed| >= {SC_NSSP_ANOMALY_RELATIVE_CHANGE:.0%}`): {target_nssp_anomaly_share:.2%} of SC region-pathogen-weeks with a defined relative change.",
        f"- NWSS low-coverage: covid {target_nwss_coverage_share['covid']:.1%}, influenza {target_nwss_coverage_share['influenza']:.1%}, rsv {target_nwss_coverage_share['rsv']:.1%} of SC site-pathogen pairs - fixed directly (the shares SC's original 0.80/0.90/0.90 cutoffs flagged), since those cutoffs were set on a coverage ratio that counted pre-inclusion weeks; corrected 2026-09-29.",
        "",
        "## SC regression check (acceptance check: recomputing for SC should reproduce SC's existing values, or be close enough that the difference is explainable)",
        "",
        *sc_regression_lines,
        "",
        "NHSN's fixed values (50, 100%) were originally chosen from RSV onboarding-ramp evidence and",
        "expert judgment, not from this exact percentile method, so an exact match is not expected for",
        "those two; NSSP's and NWSS's fixed values are expected to reproduce closely since NWSS's",
        "pathogen split was already derived this way (see `build_observation_tables.py`'s comment on it) and",
        "NSSP's single fixed value has no other stated derivation basis to reconcile against.",
        "",
        "## Per-state thresholds",
        "",
        "See `state_thresholds.csv` for the full 50-states-plus-DC table (one row per state: all",
        "four recomputed threshold values, plus the sample size each was computed from).",
        "",
        f"- States processed: {len(rows_out)}",
        f"- States with zero NHSN reporting-count observations (threshold left blank): {sum(1 for r in rows_out if r['nhsn_reporting_count_n'] == 0)}",
        f"- States with zero NWSS sites for at least one pathogen (threshold left blank for that pathogen): {sum(1 for r in rows_out if 0 in (r['nwss_covid_site_n'], r['nwss_influenza_site_n'], r['nwss_rsv_site_n']))}",
        "",
    ]
    (output / "threshold_derivation_report.md").write_text("\n".join(report_lines) + "\n", encoding="utf-8")

    print(
        f"DONE states={len(rows_out)} sc_nhsn_low_coverage={sc_row['nhsn_low_coverage_threshold']} "
        f"sc_nhsn_anomaly={sc_row['nhsn_anomaly_relative_change']} sc_nssp_anomaly={sc_row['nssp_anomaly_relative_change']} "
        f"sc_nwss_covid={sc_row['nwss_low_coverage_covid']} sc_nwss_influenza={sc_row['nwss_low_coverage_influenza']} "
        f"sc_nwss_rsv={sc_row['nwss_low_coverage_rsv']} output={output}",
        flush=True,
    )


if __name__ == "__main__":
    main()
