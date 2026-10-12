#!/usr/bin/env python3
"""Derive a county-level wastewater view for a state's NWSS sites.

Read-only: joins the canonical long-format table (build_observation_tables.py)
to the site-to-county mapping extract_nwss.py already establishes from NWSS's
own Counties_Served field. A county with more than one site gets one prefixed
predictor column per site, not a merged value.

A site that genuinely serves more than one real county (the same rule as
`build_hsa_combined.py`'s HSA-level version) is **not excluded**: no
allocation weight is invented (there is no defensible drainage-area split),
but the
site's real measured value is written to *every* county it serves, flagged
`{site}_multi_county_shared = true` on each, so a downstream consumer knows
not to treat it as independent evidence for each county. This is different
from a genuine **name ambiguity** (one raw county name matching more than
one real jurisdiction after normalization, e.g. Virginia's `Fairfax`
city/county both sharing a base name) - there, only one of the candidates is
actually correct and we simply can't tell which, so writing to both would
assert something false (that the site really does serve both), not just
duplicate a real fact; that case is still excluded and reported, not shared.

Generalized to any state (default `--state SC`). The county-name match
between NWSS's
`Counties_Served` and Census's `CTYNAME` no longer assumes every county
ends in `" County"`: the nationwide geography audit found real county-equivalent
naming in use elsewhere (Louisiana's `Parish`, Alaska's `Borough`/`Census
Area`/`Municipality`, Virginia's/Missouri's/Maryland's independent `city`,
DC and Connecticut's no-suffix names) - `state_reference.COUNTY_NAME_SUFFIXES`
now covers all of these, tried longest/most-specific first, and the match
is case-insensitive (a real observed gap: NWSS's `District Of Columbia`
vs. Census's `District of Columbia`).

Testing the fix directly against Louisiana/Alaska/Virginia's real data (not
just asserting it would work) surfaced two more real gaps beyond suffix
vocabulary, both handled here rather than left as a residual crash:
- **Abbreviation/spacing mismatches** (Louisiana's `St. Bernard Parish` vs.
  NWSS's `Saint Bernard`; `LaSalle Parish` vs. `La Salle` - the same `St.`
  pattern also explains several of the audit's Missouri/Wisconsin unmatched-county
  findings, e.g. `Saint Louis`). `normalize_county_name` folds `St.`/`Saint`
  and removes spacing differences after suffix-stripping.
- **Genuine same-base-name collisions** (Virginia has 38 independent cities,
  several sharing a name with a same-state county - e.g. `Fairfax city` vs.
  `Fairfax County` both normalize to `fairfax`). No string normalization can
  resolve this: NWSS's raw `Counties_Served` value for such a site is
  genuinely ambiguous between two real jurisdictions. Per this project's
  standing exclude-and-report practice (the same pattern this script already
  uses for multi-county sites), an ambiguous normalized name is excluded and
  reported, not silently misassigned and not a whole-state crash.

Does not touch any source file or any other processing step's output.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

from state_reference import normalize_county_name, resolve_state

KEY_FIELDS = ["analysis_region", "analysis_region_type", "analysis_region_id", "observation_week"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--latest-view", required=True, type=Path, help="sc_latest_view.csv")
    parser.add_argument("--nwss-wide-extract", required=True, type=Path, help="sc_nwss_extract_wide.csv")
    parser.add_argument("--county-population-estimates", required=True, type=Path, help="sc_population_estimates_county.csv")
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--state", default="SC", help="2-letter postal abbreviation, e.g. SC (default: SC)")
    return parser.parse_args()


def load_site_county_map(path: Path) -> tuple[dict[str, str], dict[str, list[str]]]:
    site_counties: dict[str, set[str]] = {}
    with path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            counties = {c.strip() for c in row["counties_served"].split(";")}
            site_counties.setdefault(row["site"], set()).update(counties)

    single_county_site: dict[str, str] = {}
    multi_county_site: dict[str, list[str]] = {}
    for site, counties in site_counties.items():
        if len(counties) == 1:
            single_county_site[site] = next(iter(counties))
        else:
            multi_county_site[site] = sorted(counties)
    return single_county_site, multi_county_site


def load_county_fips(path: Path, state_fips: str) -> tuple[dict[str, str], dict[str, list[str]]]:
    """Normalized county name -> FIPS lookup, plus a report of ambiguous names.

    A name that two different counties in this state both normalize to (e.g.
    Virginia's `Fairfax` city/county) is *not* picked arbitrarily - it's left
    out of the returned lookup and listed in the second return value instead,
    so a site referencing it falls through to the normal "unmatched, exclude
    and report" path rather than being silently misassigned or crashing the
    whole state's run.
    """
    raw_names: dict[str, list[str]] = {}
    fips_by_key: dict[str, str] = {}
    with path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["STATE"] != state_fips or row["COUNTY"] == "000":
                continue  # skip other states' rows and the state-level summary row
            key = normalize_county_name(row["CTYNAME"])
            fips = row["STATE"] + row["COUNTY"]
            raw_names.setdefault(key, []).append(row["CTYNAME"])
            fips_by_key[key] = fips

    ambiguous = {k: v for k, v in raw_names.items() if len(v) > 1}
    lookup = {k: v for k, v in fips_by_key.items() if k not in ambiguous}
    return lookup, ambiguous


def classify_sites(
    single_county_site: dict[str, str],
    multi_county_site: dict[str, list[str]],
    county_fips: dict[str, str],
    ambiguous_names: dict[str, list[str]],
) -> tuple[dict[str, str], dict[str, list[tuple[str, str]]], dict[str, list[str]], dict[str, list[str]]]:
    """Classify every site into exactly one of four buckets:

    - resolved: single-county site -> the one FIPS it unambiguously belongs to.
    - shared: multi-county site, every county resolved -> list of (fips, county_name)
      pairs (genuine multi-county sharing - see module docstring).
    - ambiguous: site -> counties_served, where at least one county name matches more
      than one real jurisdiction after normalization (excluded and reported, not shared -
      writing to multiple candidates here would assert something false).
    - unmatched: site -> counties_served, where at least one county name has no FIPS
      candidate in this state's Census county reference at all. Excluded and reported, same as
      ambiguous - **not fatal**: real testing (SC's own `ID:2282`, serving Horry, SC and
      Columbus, NC) found this is a genuine, recurring structural case - a site whose
      catchment crosses a *state* line, not a data bug to halt the whole run over.
    """
    def resolve_one(county_name: str) -> tuple[str, str | None]:
        key = normalize_county_name(county_name)
        if key in county_fips:
            return "ok", county_fips[key]
        if key in ambiguous_names:
            return "ambiguous", None
        return "unmatched", None

    resolved: dict[str, str] = {}
    shared: dict[str, list[tuple[str, str]]] = {}
    ambiguous: dict[str, list[str]] = {}
    unmatched: dict[str, list[str]] = {}

    for site, county_name in single_county_site.items():
        status, fips = resolve_one(county_name)
        if status == "ok":
            resolved[site] = fips
        elif status == "ambiguous":
            ambiguous[site] = [county_name]
        else:
            unmatched[site] = [county_name]

    for site, counties in multi_county_site.items():
        pairs: list[tuple[str, str]] = []
        bad_status: str | None = None
        for county_name in counties:
            status, fips = resolve_one(county_name)
            if status == "ok":
                pairs.append((fips, county_name))
            else:
                bad_status = status
                break
        if bad_status == "ambiguous":
            ambiguous[site] = counties
        elif bad_status == "unmatched":
            unmatched[site] = counties
        else:
            shared[site] = pairs

    return resolved, shared, ambiguous, unmatched


def main() -> None:
    args = parse_args()
    state = resolve_state(args.state)

    single_county_site, multi_county_site = load_site_county_map(args.nwss_wide_extract)
    county_fips, ambiguous_names = load_county_fips(args.county_population_estimates, state.fips)
    resolved_site_fips, shared_site_fips, ambiguous_sites, unmatched_sites = classify_sites(
        single_county_site, multi_county_site, county_fips, ambiguous_names
    )

    with args.latest_view.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    nwss_rows = [r for r in rows if r["stream_id"] == "nwss"]

    keyed: dict[tuple, dict[str, str]] = {}
    value_columns: set[str] = set()
    shared_marker_columns: set[str] = set()
    excluded_row_count = 0
    shared_duplicate_writes = 0
    for row in nwss_rows:
        site = row["analysis_region_id"]
        if site in resolved_site_fips:
            targets = [(resolved_site_fips[site], single_county_site[site])]
            is_shared = False
        elif site in shared_site_fips:
            targets = shared_site_fips[site]
            is_shared = True
        else:
            excluded_row_count += 1
            continue
        site_tag = site.replace(":", "")
        column = f"{site_tag}_{row['feature_name']}_{row['pathogen']}"
        for fips, county_name in targets:
            key = (fips, row["observation_week"])
            if key not in keyed:
                keyed[key] = {
                    "analysis_region": county_name,
                    "analysis_region_type": "county",
                    "analysis_region_id": fips,
                    "observation_week": row["observation_week"],
                }
            keyed[key][column] = row["weekly_feature"]
            keyed[key][f"{column}_flag"] = row["observation_state"]
            if is_shared:
                marker_column = f"{site_tag}_multi_county_shared"
                keyed[key][marker_column] = "true"
                shared_marker_columns.add(marker_column)
        value_columns.add(column)
        if is_shared:
            shared_duplicate_writes += len(targets) - 1

    ordered_value_columns: list[str] = []
    for column in sorted(value_columns):
        ordered_value_columns.append(column)
        ordered_value_columns.append(f"{column}_flag")
    ordered_value_columns.extend(sorted(shared_marker_columns))
    fieldnames = KEY_FIELDS + ordered_value_columns

    wide_rows = sorted(keyed.values(), key=lambda r: (r["analysis_region_id"], r["observation_week"]))

    args.output_dir.mkdir(parents=True, exist_ok=True)
    prefix = state.abbr.lower()
    wide_path = args.output_dir / f"{prefix}_nwss_county_wide.csv"
    with wide_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, restval="", extrasaction="ignore")
        writer.writeheader()
        writer.writerows(wide_rows)

    pending_path = args.output_dir / f"{prefix}_nwss_county_pending_sites.csv"
    with pending_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["site", "counties_served", "reason"])
        writer.writeheader()
        for site, county_names in sorted(ambiguous_sites.items()):
            candidate_notes = []
            for county_name in county_names:
                candidates = ambiguous_names.get(normalize_county_name(county_name))
                if candidates:
                    candidate_notes.append(f"'{county_name}' matches {', '.join(sorted(candidates))}")
            writer.writerow(
                {
                    "site": site,
                    "counties_served": "; ".join(county_names),
                    "reason": (
                        "at least one served county name is ambiguous in this state after "
                        f"suffix/spelling normalization ({'; '.join(candidate_notes)}) - no way to "
                        "tell which real jurisdiction NWSS means from Counties_Served alone"
                    ),
                }
            )
        for site, county_names in sorted(unmatched_sites.items()):
            writer.writerow(
                {
                    "site": site,
                    "counties_served": "; ".join(county_names),
                    "reason": (
                        "at least one served county name has no match anywhere in this state's Census "
                        "county reference, even after normalization - likely a site whose catchment "
                        "crosses into a neighboring state (e.g. SC's ID:2282 serving Horry, SC and "
                        "Columbus, NC), not necessarily a data error"
                    ),
                }
            )

    shared_path = args.output_dir / f"{prefix}_nwss_county_shared_sites.csv"
    with shared_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["site", "counties_served", "reason"])
        writer.writeheader()
        for site, pairs in sorted(shared_site_fips.items()):
            county_names = [name for _, name in pairs]
            writer.writerow(
                {
                    "site": site,
                    "counties_served": "; ".join(county_names),
                    "reason": (
                        f"site's served counties genuinely span {len(pairs)} different real "
                        f"counties - its real measured value is written to every one of them (not "
                        f"split or weighted, since no allocation weight is defensible here), "
                        f"flagged '{site.replace(':', '')}_multi_county_shared' = true in the wide "
                        f"table so this isn't mistaken for independent evidence in each county"
                    ),
                }
            )

    counties_covered = len({r["analysis_region_id"] for r in wide_rows})
    print(f"county-level NWSS wide table: {len(wide_rows)} rows, {len(fieldnames)} columns, {counties_covered} counties -> {wide_path}")
    print(
        f"  {len(resolved_site_fips)} single-county sites resolved, {len(shared_site_fips)} shared "
        f"across multiple counties, {len(ambiguous_sites)} excluded as ambiguous, "
        f"{len(unmatched_sites)} excluded as no-match-at-all"
    )
    print(
        f"  {excluded_row_count} long-format rows excluded ({shared_duplicate_writes} extra writes "
        f"from sharing) -> {pending_path}, {shared_path}"
    )


if __name__ == "__main__":
    main()
