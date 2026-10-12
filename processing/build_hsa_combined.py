#!/usr/bin/env python3
"""Build one combined ED + wastewater wide table at the HSA level for a state.

The dataset is built at the HSA level (not the county level), NSSP's own
sub-state reporting unit: this derives one row per (HSA, observation_week)
from the canonical long-format table (build_observation_tables.py):

- ED (NSSP) columns come directly from its native `sub_state_hsa` rows - no
  re-keying needed, this is NSSP's own measurement grain.
- Wastewater (NWSS) columns are derived by mapping each site's
  `Counties_Served` (extract_nwss.py) through NSSP's own HSA-to-member-county
  membership (already present as `raw_geography_id` on its `sub_state_hsa`
  rows). For South Carolina specifically, this fully resolves all 31 sites
  with nothing excluded: its 14 HSAs happen to partition all 46 counties
  with no county belonging to more than one HSA, and every site's served
  counties happen to fall entirely within one HSA (verified
  programmatically, not assumed) - that clean result is a property of SC's
  geometry, not a guarantee this script can make for every state.

One county with more than one same-HSA site (there are several - e.g. all
of an HSA's sites) gets one site-prefixed predictor column per site, same
convention as the county-level table.

Generalized to any state (default `--state SC`). Three changes from the
original SC-only version:
- County-name matching between NWSS's `Counties_Served` and NSSP's own
  `raw_geography_id` member-county lists now goes through
  `state_reference.normalize_county_name` (case/`St.`-`Saint`/spacing-
  insensitive - see that function's docstring) instead of comparing raw
  strings, closing several of the naming mismatches the national geography
  audit (audit_national_geography.py) found
  (e.g. Missouri's/Wisconsin's `Saint Louis`/`Saint Croix`).
- A site whose counties genuinely don't match any HSA at all (a real gap,
  not an ambiguity) no longer crashes the whole state's run - it's excluded
  and reported in `{state}_hsa_pending_sites.csv` instead, the same
  exclude-and-report pattern `build_nwss_county_geography.py` already uses.
- **A site whose counties genuinely span more than one real HSA is no
  longer excluded either** (keeping the data rather than dropping it): its
  real measured value
  is written to *every* HSA it touches - not split, weighted, or guessed,
  since there is no defensible way to do that (same standing constraint
  that already rules out inventing a county-allocation weight) - just the
  same true number placed in more than one region, with a companion
  `{site}_multi_hsa_shared` column ("true") so any downstream consumer or
  model can identify and choose to down-weight or ignore the duplication,
  rather than silently double-counting one real observation as if it were
  independent evidence for two different regions. Listed for transparency
  in a new `{state}_hsa_shared_sites.csv`, separate from the pending
  (genuinely excluded) report. The national geography audit found multi-HSA
  sites are common nationally (19 of 53 states/territories have at least
  one), so this affects a real, non-trivial share of NWSS sites across all
  50 states - not a rare edge case.

Read-only; does not touch any source file or any other processing step's output.
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
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--state", default="SC", help="2-letter postal abbreviation, e.g. SC (default: SC)")
    return parser.parse_args()


def load_site_counties(path: Path) -> dict[str, set[str]]:
    site_counties: dict[str, set[str]] = {}
    with path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            counties = {c.strip() for c in row["counties_served"].split(";")}
            site_counties.setdefault(row["site"], set()).update(counties)
    return site_counties


def load_hsa_membership(rows: list[dict[str, str]]) -> tuple[dict[str, set[str]], dict[str, str]]:
    """Return (normalized county name -> set of HSA ids it belongs to, HSA id -> HSA name), from NSSP's own HSA rows."""
    county_to_hsa: dict[str, set[str]] = {}
    hsa_names: dict[str, str] = {}
    for row in rows:
        if row["stream_id"] != "nssp" or row["analysis_region_type"] != "sub_state_hsa":
            continue
        hsa_id = row["analysis_region_id"]
        hsa_names[hsa_id] = row["analysis_region"]
        for county in (c.strip() for c in row["raw_geography_id"].split(",")):
            county_to_hsa.setdefault(normalize_county_name(county), set()).add(hsa_id)
    return county_to_hsa, hsa_names


def resolve_site_hsa(
    site_counties: dict[str, set[str]], county_to_hsa: dict[str, set[str]]
) -> tuple[dict[str, str], dict[str, list[str]], dict[str, list[str]]]:
    """Classify every site into exactly one of three buckets:

    - resolved: site -> the one HSA id its counties unambiguously belong to.
    - shared: site -> sorted list of 2+ HSA ids its counties collectively touch -
      a genuine multi-HSA site (not excluded - see module docstring for the
      write-to-every-touched-HSA strategy).
    - unmatched: site -> sorted counties_served, none of which match any county in
      this state's NSSP HSA membership at all (a real gap, not an ambiguity -
      excluded and reported, since there's nothing to write).
    """
    resolved: dict[str, str] = {}
    shared: dict[str, list[str]] = {}
    unmatched: dict[str, list[str]] = {}
    for site, counties in site_counties.items():
        hsas_touched: set[str] = set()
        for county in counties:
            hsas_touched |= county_to_hsa.get(normalize_county_name(county), set())
        if len(hsas_touched) == 1:
            resolved[site] = next(iter(hsas_touched))
        elif len(hsas_touched) >= 2:
            shared[site] = sorted(hsas_touched)
        else:
            unmatched[site] = sorted(counties)
    return resolved, shared, unmatched


def main() -> None:
    args = parse_args()
    state = resolve_state(args.state)

    with args.latest_view.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    county_to_hsa, hsa_names = load_hsa_membership(rows)
    site_counties = load_site_counties(args.nwss_wide_extract)
    resolved_site_hsa, shared_site_hsas, unmatched_sites = resolve_site_hsa(site_counties, county_to_hsa)

    keyed: dict[tuple, dict[str, str]] = {}
    value_columns: set[str] = set()
    shared_marker_columns: set[str] = set()
    nssp_row_count = 0
    nwss_row_count = 0
    nwss_shared_duplicate_writes = 0
    excluded_row_count = 0

    for row in rows:
        if row["stream_id"] == "nssp" and row["analysis_region_type"] == "sub_state_hsa":
            hsa_id = row["analysis_region_id"]
            key = (hsa_id, row["observation_week"])
            if key not in keyed:
                keyed[key] = {
                    "analysis_region": row["analysis_region"],
                    "analysis_region_type": "sub_state_hsa",
                    "analysis_region_id": hsa_id,
                    "observation_week": row["observation_week"],
                }
            column = f"{row['feature_name']}_{row['pathogen']}"
            keyed[key][column] = row["weekly_feature"]
            keyed[key][f"{column}_flag"] = row["observation_state"]
            value_columns.add(column)
            nssp_row_count += 1
        elif row["stream_id"] == "nwss":
            site = row["analysis_region_id"]
            if site in resolved_site_hsa:
                target_hsas = [resolved_site_hsa[site]]
            elif site in shared_site_hsas:
                target_hsas = shared_site_hsas[site]
            else:
                excluded_row_count += 1
                continue
            is_shared = site in shared_site_hsas
            site_tag = site.replace(":", "")
            column = f"{site_tag}_{row['feature_name']}_{row['pathogen']}"
            for hsa_id in target_hsas:
                key = (hsa_id, row["observation_week"])
                if key not in keyed:
                    keyed[key] = {
                        "analysis_region": hsa_names[hsa_id],
                        "analysis_region_type": "sub_state_hsa",
                        "analysis_region_id": hsa_id,
                        "observation_week": row["observation_week"],
                    }
                keyed[key][column] = row["weekly_feature"]
                keyed[key][f"{column}_flag"] = row["observation_state"]
                if is_shared:
                    marker_column = f"{site_tag}_multi_hsa_shared"
                    keyed[key][marker_column] = "true"
                    shared_marker_columns.add(marker_column)
            value_columns.add(column)
            nwss_row_count += 1
            if is_shared:
                nwss_shared_duplicate_writes += len(target_hsas) - 1

    ordered_value_columns: list[str] = []
    for column in sorted(value_columns):
        ordered_value_columns.append(column)
        ordered_value_columns.append(f"{column}_flag")
    ordered_value_columns.extend(sorted(shared_marker_columns))
    fieldnames = KEY_FIELDS + ordered_value_columns

    wide_rows = sorted(keyed.values(), key=lambda r: (r["analysis_region_id"], r["observation_week"]))

    args.output_dir.mkdir(parents=True, exist_ok=True)
    prefix = state.abbr.lower()
    out_path = args.output_dir / f"{prefix}_hsa_combined_wide.csv"
    with out_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, restval="", extrasaction="ignore")
        writer.writeheader()
        writer.writerows(wide_rows)

    pending_path = args.output_dir / f"{prefix}_hsa_pending_sites.csv"
    with pending_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["site", "counties_served", "reason"])
        writer.writeheader()
        for site, counties in sorted(unmatched_sites.items()):
            writer.writerow(
                {
                    "site": site,
                    "counties_served": "; ".join(counties),
                    "reason": (
                        "no county in this site's Counties_Served matches any county in this "
                        "state's NSSP HSA membership, even after normalization - a real gap, not "
                        "an ambiguity"
                    ),
                }
            )

    shared_path = args.output_dir / f"{prefix}_hsa_shared_sites.csv"
    with shared_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["site", "counties_served", "hsas_shared_with", "reason"])
        writer.writeheader()
        for site, hsa_ids in sorted(shared_site_hsas.items()):
            writer.writerow(
                {
                    "site": site,
                    "counties_served": "; ".join(sorted(site_counties[site])),
                    "hsas_shared_with": "; ".join(hsa_ids),
                    "reason": (
                        f"site's served counties genuinely span {len(hsa_ids)} different HSAs - its "
                        f"real measured value is written to every one of them (not split or weighted, "
                        f"since no allocation weight is defensible here), flagged "
                        f"'{site.replace(':', '')}_multi_hsa_shared' = true in the wide table so this "
                        f"isn't mistaken for independent evidence in each HSA"
                    ),
                }
            )

    hsas_covered = len({r["analysis_region_id"] for r in wide_rows})
    print(f"combined HSA-level wide table: {len(wide_rows)} rows, {len(fieldnames)} columns, {hsas_covered} HSAs -> {out_path}")
    print(
        f"  {len(resolved_site_hsa)} of {len(site_counties)} NWSS {state.abbr} sites resolved to a single HSA, "
        f"{len(shared_site_hsas)} shared across multiple HSAs, {len(unmatched_sites)} excluded (no HSA match at all)"
    )
    print(
        f"  source long rows folded in: {nssp_row_count} NSSP (HSA-level) + {nwss_row_count} NWSS "
        f"({nwss_shared_duplicate_writes} extra writes from sharing, {excluded_row_count} NWSS rows excluded) "
        f"-> {pending_path}, {shared_path}"
    )


if __name__ == "__main__":
    main()
