#!/usr/bin/env python3
"""Build the nationwide geography audit for the 50-state processing.

Reads the same three nationwide raw files extract_nssp.py, extract_nwss.py,
and extract_census_population.py already read for South
Carolina (the NSSP ED trajectories file, the NWSS Main-Three wastewater file,
and the Census county Population Estimates file), but computes, per state:

- NSSP: number of distinct Health Service Areas (`hsa_nci_id`), and whether
  any county is tagged to more than one HSA (the clean-partition check
  `build_hsa_combined.py` currently assumes true for South Carolina only).
- NWSS: number of distinct sampling sites, and for every multi-county site,
  whether all of its `Counties_Served` fall inside a single NSSP HSA for
  that state (the check `build_hsa_combined.py` currently does for South
  Carolina only, then crashes via `SystemExit` if it ever fails elsewhere).
- Census: which county-equivalent naming suffix(es) each state's counties
  use (`County`, `Parish`, `Borough`, `city`, `Census Area`, `Municipio`,
  etc.), the input `build_nwss_county_geography.py`'s stripped-`" County"`
  string match cannot handle for non-`County` states.

This produces the real stress-test state list for the multi-state test runs,
rather than guessing it (Louisiana/Alaska/Virginia are fixed in the
stress-test list regardless of what this script finds; everything else in
that list comes from this script's actual counts).

Read-only: does not touch any source file or any other processing step's output.
"""

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

KNOWN_COUNTY_SUFFIXES = [
    " Census Area",
    " Borough",
    " Parish",
    " Municipio",
    " Municipality",
    " city",
    " County",
    " District",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--nssp-file", required=True, type=Path)
    parser.add_argument("--nwss-file", required=True, type=Path)
    parser.add_argument("--census-county-file", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument(
        "--extra-size-contrast-states",
        type=int,
        default=2,
        help="Number of largest and smallest (by NWSS site count) states to add to the stress-test list.",
    )
    return parser.parse_args()


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def audit_nssp(rows: list[dict[str, str]]) -> tuple[dict[str, set[str]], dict[str, dict[str, set[str]]]]:
    """Return (state -> set of hsa_nci_id, state -> county -> set of hsa_nci_id it's tagged to)."""
    hsa_ids_by_state: dict[str, set[str]] = defaultdict(set)
    county_to_hsas_by_state: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    for row in rows:
        if row["trend_source"] != "HSA":
            continue
        state = row["geography"]
        hsa_ids_by_state[state].add(row["hsa_nci_id"])
        county_to_hsas_by_state[state][row["county"]].add(row["hsa_nci_id"])
    return hsa_ids_by_state, county_to_hsas_by_state


def audit_nwss(rows: list[dict[str, str]]) -> tuple[dict[str, set[str]], dict[str, list[tuple[str, list[str]]]]]:
    """Return (state -> set of site ids, state -> [(site, counties_served)] for multi-county sites only)."""
    sites_by_state: dict[str, set[str]] = defaultdict(set)
    site_counties_by_state_site: dict[tuple[str, str], set[str]] = defaultdict(set)
    for row in rows:
        state = row["State/Territory"]
        site = row["Site"]
        sites_by_state[state].add(site)
        counties = {c.strip() for c in row["Counties_Served"].split(",") if c.strip()}
        site_counties_by_state_site[(state, site)].update(counties)

    multi_county_sites_by_state: dict[str, list[tuple[str, list[str]]]] = defaultdict(list)
    for (state, site), counties in site_counties_by_state_site.items():
        if len(counties) > 1:
            multi_county_sites_by_state[state].append((site, sorted(counties)))
    return sites_by_state, multi_county_sites_by_state


def resolve_multi_county_sites(
    multi_county_sites_by_state: dict[str, list[tuple[str, list[str]]]],
    county_to_hsas_by_state: dict[str, dict[str, set[str]]],
) -> dict[str, dict[str, object]]:
    """For each state's multi-county NWSS sites, check whether every site resolves to exactly one HSA
    (all its counties map to the same single hsa_nci_id, and every county name is found at all)."""
    result: dict[str, dict[str, object]] = {}
    for state, sites in multi_county_sites_by_state.items():
        county_to_hsas = county_to_hsas_by_state.get(state, {})
        unresolved: list[dict[str, object]] = []
        for site, counties in sites:
            hsas_touched: set[str] = set()
            unmatched_counties: list[str] = []
            for county in counties:
                hsas = county_to_hsas.get(county)
                if not hsas:
                    unmatched_counties.append(county)
                else:
                    hsas_touched |= hsas
            if unmatched_counties or len(hsas_touched) != 1:
                unresolved.append(
                    {
                        "site": site,
                        "counties_served": counties,
                        "unmatched_counties": unmatched_counties,
                        "hsas_touched": sorted(hsas_touched),
                    }
                )
        result[state] = {
            "multi_county_site_count": len(sites),
            "unresolved_count": len(unresolved),
            "unresolved_detail": unresolved,
        }
    return result


def county_suffix(ctyname: str) -> str:
    for suffix in KNOWN_COUNTY_SUFFIXES:
        if ctyname.endswith(suffix):
            return suffix.strip()
    return "(no recognized suffix)"


def audit_census_county_names(rows: list[dict[str, str]]) -> dict[str, set[str]]:
    suffixes_by_state: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        if row["SUMLEV"] != "050":
            continue
        suffixes_by_state[row["STNAME"]].add(county_suffix(row["CTYNAME"]))
    return suffixes_by_state


def build_summary_table(
    hsa_ids_by_state: dict[str, set[str]],
    county_to_hsas_by_state: dict[str, dict[str, set[str]]],
    sites_by_state: dict[str, set[str]],
    multi_county_resolution: dict[str, dict[str, object]],
    suffixes_by_state: dict[str, set[str]],
) -> list[dict[str, object]]:
    all_states = sorted(set(hsa_ids_by_state) | set(sites_by_state) | set(suffixes_by_state))
    table: list[dict[str, object]] = []
    for state in all_states:
        county_to_hsas = county_to_hsas_by_state.get(state, {})
        overlapping_counties = {c: sorted(h) for c, h in county_to_hsas.items() if len(h) > 1}
        clean_partition_pass = len(overlapping_counties) == 0
        resolution = multi_county_resolution.get(state, {"multi_county_site_count": 0, "unresolved_count": 0})
        multi_hsa_site_pass = resolution["unresolved_count"] == 0
        table.append(
            {
                "state": state,
                "nssp_hsa_count": len(hsa_ids_by_state.get(state, set())),
                "nwss_site_count": len(sites_by_state.get(state, set())),
                "nwss_multi_county_site_count": resolution["multi_county_site_count"],
                "county_naming_suffixes": "; ".join(sorted(suffixes_by_state.get(state, set()))) or "(no county rows found)",
                "clean_partition_pass": clean_partition_pass,
                "counties_tagged_to_multiple_hsas": len(overlapping_counties),
                "multi_hsa_site_resolution_pass": multi_hsa_site_pass,
                "unresolved_multi_county_sites": resolution["unresolved_count"],
            }
        )
    return table


def pick_stress_test_states(table: list[dict[str, object]], extra_size_contrast: int) -> dict[str, object]:
    fixed = ["Louisiana", "Alaska", "Virginia"]
    failing = sorted(
        row["state"]
        for row in table
        if not row["clean_partition_pass"] or not row["multi_hsa_site_resolution_pass"]
    )
    by_state = {row["state"]: row for row in table}
    non_fixed_rows = [row for row in table if row["state"] not in fixed]
    most_hsas = sorted(non_fixed_rows, key=lambda r: r["nssp_hsa_count"], reverse=True)[:extra_size_contrast]
    fewest_hsas = sorted(non_fixed_rows, key=lambda r: r["nssp_hsa_count"])[:extra_size_contrast]
    most_sites = sorted(non_fixed_rows, key=lambda r: r["nwss_site_count"], reverse=True)[:extra_size_contrast]
    fewest_sites = sorted(non_fixed_rows, key=lambda r: r["nwss_site_count"])[:extra_size_contrast]
    size_contrast = sorted(
        {r["state"] for r in most_hsas + fewest_hsas + most_sites + fewest_sites}
    )
    final_list = sorted(set(fixed) | set(failing) | set(size_contrast))
    return {
        "fixed": fixed,
        "clean_partition_or_resolution_failures": failing,
        "size_contrast_candidates": size_contrast,
        "final_stress_test_states": final_list,
        "sc_check": by_state.get("South Carolina"),
    }


def write_summary_csv(path: Path, table: list[dict[str, object]]) -> None:
    fieldnames = [
        "state", "nssp_hsa_count", "nwss_site_count", "nwss_multi_county_site_count",
        "county_naming_suffixes", "clean_partition_pass", "counties_tagged_to_multiple_hsas",
        "multi_hsa_site_resolution_pass", "unresolved_multi_county_sites",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(table)


def write_report(
    path: Path,
    run_date: str,
    table: list[dict[str, object]],
    stress_test: dict[str, object],
    multi_county_resolution: dict[str, dict[str, object]],
) -> None:
    sc = stress_test["sc_check"]
    sc_ok = bool(sc) and sc["nssp_hsa_count"] == 14 and sc["nwss_site_count"] == 31 and sc["clean_partition_pass"]
    lines = [
        "# Nationwide geography audit",
        "",
        f"Run date: {run_date}",
        "",
        "## SC acceptance check",
        "",
        f"- SC HSA count: {sc['nssp_hsa_count'] if sc else 'n/a'} (expected 14)",
        f"- SC NWSS site count: {sc['nwss_site_count'] if sc else 'n/a'} (expected 31)",
        f"- SC clean-partition pass: {sc['clean_partition_pass'] if sc else 'n/a'} (expected True)",
        f"- **Result: {'PASS - audit script confirmed against known SC numbers, other states trustworthy' if sc_ok else 'FAIL - fix the audit script before trusting any other state row'}**",
        "",
        "## Stress-test state list",
        "",
        f"- Fixed regardless of findings: {', '.join(stress_test['fixed'])}",
        f"- States failing the clean-partition or multi-HSA-site-resolution check: {', '.join(stress_test['clean_partition_or_resolution_failures']) or '(none)'}",
        f"- Size-contrast candidates (most/fewest HSAs, most/fewest NWSS sites, excluding the fixed three): {', '.join(stress_test['size_contrast_candidates'])}",
        f"- **Final stress-test state list: {', '.join(stress_test['final_stress_test_states'])}**",
        "",
        "## Per-state summary",
        "",
        "See `national_geography_summary.csv` for the full table (state, HSA count, NWSS site "
        "count, county-naming suffixes, clean-partition pass/fail, multi-HSA-site-resolution pass/fail).",
        "",
        f"- States audited: {len(table)}",
        f"- States failing clean-partition (a county tagged to >1 HSA): {sum(1 for r in table if not r['clean_partition_pass'])}",
        f"- States failing multi-HSA-site resolution (an NWSS multi-county site whose counties span >1 HSA, or a county name it can't match at all): {sum(1 for r in table if not r['multi_hsa_site_resolution_pass'])}",
        "",
        "## Unresolved multi-county NWSS site detail (states with >=1 unresolved site)",
        "",
    ]
    any_detail = False
    for state, resolution in sorted(multi_county_resolution.items()):
        if resolution["unresolved_count"] == 0:
            continue
        any_detail = True
        lines.append(f"### {state}")
        lines.append("")
        for item in resolution["unresolved_detail"]:
            lines.append(
                f"- site `{item['site']}`, counties_served={item['counties_served']}, "
                f"unmatched_against_nssp_county_field={item['unmatched_counties']}, "
                f"hsas_touched={item['hsas_touched']}"
            )
        lines.append("")
    if not any_detail:
        lines.append("(none - every multi-county NWSS site nationwide resolves to exactly one HSA)")
        lines.append("")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()

    nssp_rows = read_rows(args.nssp_file)
    hsa_ids_by_state, county_to_hsas_by_state = audit_nssp(nssp_rows)

    nwss_rows = read_rows(args.nwss_file)
    sites_by_state, multi_county_sites_by_state = audit_nwss(nwss_rows)

    census_rows = read_rows(args.census_county_file)
    suffixes_by_state = audit_census_county_names(census_rows)

    multi_county_resolution = resolve_multi_county_sites(multi_county_sites_by_state, county_to_hsas_by_state)

    table = build_summary_table(
        hsa_ids_by_state, county_to_hsas_by_state, sites_by_state, multi_county_resolution, suffixes_by_state
    )
    stress_test = pick_stress_test_states(table, args.extra_size_contrast_states)

    run_date = datetime.now(timezone.utc).date().isoformat()
    write_summary_csv(args.output_dir / "national_geography_summary.csv", table)
    write_report(
        args.output_dir / "national_geography_audit_report.md",
        run_date,
        table,
        stress_test,
        multi_county_resolution,
    )

    sc = stress_test["sc_check"]
    print(
        f"DONE states={len(table)} "
        f"sc_hsas={sc['nssp_hsa_count'] if sc else 'n/a'} sc_sites={sc['nwss_site_count'] if sc else 'n/a'} "
        f"sc_clean_partition={sc['clean_partition_pass'] if sc else 'n/a'} "
        f"failing_states={len(stress_test['clean_partition_or_resolution_failures'])} "
        f"stress_test_states={stress_test['final_stress_test_states']} "
        f"output={args.output_dir}",
        flush=True,
    )


if __name__ == "__main__":
    main()
