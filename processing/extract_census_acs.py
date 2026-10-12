#!/usr/bin/env python3
"""Materialize the bounded ACS 5-year 2022 variable set at state, county, and ZCTA level.

Queries the Census Data API (requires a key in the CENSUS_API_KEY environment
variable, optionally supplied via a gitignored `.env` file next to this
script - see `load_dotenv`; a real environment variable always takes
precedence over `.env`. Never hardcode a key directly in this file or in any
committed output; `.env` itself is excluded from git via `.gitignore`) for a
fixed,
documented variable list spanning population/density, age/sex, race/ethnicity,
income/poverty, education/employment, health insurance, household crowding,
vehicle access, and internet access. State/county queries already return every
state/county nationwide in one API call (`for=state:*`/`for=county:*`), so a
per-state extract is just a client-side filter on the FIPS prefix (default
`--state SC`). ZCTA-level queries have no
state-scoped geography hierarchy in this API, so ZCTAs are pulled nationwide
and then filtered to the target state directly from the raw nationwide
ZCTA-to-county relationship file (no separate per-state crosswalk file needed
- dropped the earlier SC-only `--sc-zcta-crosswalk` dependency, since the
nationwide relationship file is already read by `extract_census_population.py`
and can equally be read here).
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from state_reference import resolve_state

API_BASE = "https://api.census.gov/data/2022/acs/acs5"

# variable_code -> (standardized_name, domain)
PROFILE_VARS = {
    "DP05_0001E": ("total_population", "population_density"),
    "DP05_0001M": ("total_population_moe", "population_density"),
    "DP05_0018E": ("median_age", "age_sex"),
    "DP05_0018M": ("median_age_moe", "age_sex"),
    "DP05_0002PE": ("percent_male", "age_sex"),
    "DP05_0019E": ("population_under_18", "age_sex"),
    "DP05_0019M": ("population_under_18_moe", "age_sex"),
    "DP05_0024E": ("population_65_plus", "age_sex"),
    "DP05_0024M": ("population_65_plus_moe", "age_sex"),
    "DP05_0037PE": ("percent_white_alone", "race_ethnicity"),
    "DP05_0038PE": ("percent_black_alone", "race_ethnicity"),
    "DP05_0044PE": ("percent_asian_alone", "race_ethnicity"),
    "DP05_0073PE": ("percent_hispanic_or_latino", "race_ethnicity"),
    "DP03_0062E": ("median_household_income", "income_poverty"),
    "DP03_0062M": ("median_household_income_moe", "income_poverty"),
    "DP03_0128PE": ("percent_below_poverty_all_people", "income_poverty"),
    "DP02_0068PE": ("percent_bachelor_or_higher_25plus", "education_employment"),
    "DP03_0009PE": ("unemployment_rate", "education_employment"),
    "DP03_0099PE": ("percent_no_health_insurance", "health_insurance"),
    "DP04_0058PE": ("percent_no_vehicle_available", "vehicle_access"),
    "DP02_0154PE": ("percent_broadband_internet_subscription", "internet_access"),
}
SUBJECT_VARS = {
    "S2501_C02_007E": ("percent_occupants_per_room_1_01_to_1_50", "household_crowding"),
    "S2501_C02_008E": ("percent_occupants_per_room_1_51_plus", "household_crowding"),
}
MOE_NOT_APPLICABLE_SENTINELS = {"-555555555", "-666666666", "-888888888", "-999999999"}

GEO_FOR_CLAUSES = {
    "state": "state:*",
    "county": "county:*",
    "zcta": "zip%20code%20tabulation%20area:*",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--census-dir", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument(
        "--zcta-county-rel-file", required=True, type=Path,
        help="The raw nationwide ZCTA-to-county relationship file, e.g. "
             "<census-dir>/ZCTA Relationship Files 2020/tab20_zcta520_county20_natl.txt "
             "(same file extract_census_population.py reads) - used to derive the target "
             "state's ZCTA set directly, no per-state crosswalk file needed.",
    )
    parser.add_argument("--state", default="SC", help="2-letter postal abbreviation, e.g. SC (default: SC)")
    return parser.parse_args()


def zctas_for_state(zcta_county_rel_file: Path, state_fips: str) -> set[str]:
    with zcta_county_rel_file.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = csv.DictReader(handle, delimiter="|")
        return {
            r["GEOID_ZCTA5_20"]
            for r in rows
            if r["GEOID_ZCTA5_20"] and r["GEOID_COUNTY_20"].startswith(state_fips)
        }


def fetch(dataset: str, variables: dict[str, tuple[str, str]], geo_for: str, api_key: str) -> tuple[list[dict[str, str]], str]:
    var_codes = ["NAME", *variables.keys()]
    query = f"get={','.join(var_codes)}&for={geo_for}&key={api_key}"
    url = f"{API_BASE}/{dataset}?{query}"
    with urllib.request.urlopen(url, timeout=60) as response:
        payload = json.loads(response.read().decode("utf-8"))
    header, *data_rows = payload
    rows = [dict(zip(header, row)) for row in data_rows]
    redacted_url = url.replace(api_key, "REDACTED")
    return rows, redacted_url


def geoid_from_row(row: dict[str, str], level: str) -> str:
    if level == "state":
        return row["state"]
    if level == "county":
        return row["state"] + row["county"]
    return row["zip code tabulation area"]


def merge_rows(profile_rows: list[dict[str, str]], subject_rows: list[dict[str, str]], level: str) -> list[dict[str, str]]:
    subject_by_geoid = {geoid_from_row(r, level): r for r in subject_rows}
    merged = []
    for prow in profile_rows:
        geoid = geoid_from_row(prow, level)
        srow = subject_by_geoid.get(geoid, {})
        out = {"geoid": geoid, "name": prow["NAME"]}
        for code, (name, domain) in PROFILE_VARS.items():
            out[name] = prow.get(code, "")
        for code, (name, domain) in SUBJECT_VARS.items():
            out[name] = srow.get(code, "")
        merged.append(out)
    return merged


def moe_quality_summary(rows: list[dict[str, str]]) -> dict[str, int]:
    moe_fields = [name for code, (name, _) in PROFILE_VARS.items() if code.endswith("M")]
    counts = {field: 0 for field in moe_fields}
    for row in rows:
        for field in moe_fields:
            if row.get(field) in MOE_NOT_APPLICABLE_SENTINELS:
                counts[field] += 1
    return counts


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def load_dotenv(path: Path) -> None:
    """Minimal stdlib-only `.env` loader (`KEY=VALUE` lines, `#` comments).
    Never overrides a variable already set in the real environment - the
    `.env` file is a local fallback (gitignored), not the source of truth."""
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())


def main() -> None:
    args = parse_args()
    state = resolve_state(args.state)
    load_dotenv(Path(__file__).parent / ".env")
    api_key = os.environ.get("CENSUS_API_KEY")
    if not api_key:
        raise SystemExit("Set CENSUS_API_KEY in the environment before running this script.")

    output = args.output_dir
    output.mkdir(parents=True, exist_ok=True)
    retrieved_at = datetime.now(timezone.utc).isoformat()

    sc_zctas = zctas_for_state(args.zcta_county_rel_file, state.fips)

    query_log: list[dict[str, str]] = []
    all_level_stats: list[dict[str, object]] = []

    for level, geo_for in GEO_FOR_CLAUSES.items():
        profile_rows, profile_url = fetch("profile", PROFILE_VARS, geo_for, api_key)
        subject_rows, subject_url = fetch("subject", SUBJECT_VARS, geo_for, api_key)
        query_log.append({"level": level, "dataset": "profile", "url": profile_url, "retrieved_at": retrieved_at, "row_count": len(profile_rows)})
        query_log.append({"level": level, "dataset": "subject", "url": subject_url, "retrieved_at": retrieved_at, "row_count": len(subject_rows)})

        merged = merge_rows(profile_rows, subject_rows, level)
        write_csv(output / f"acs_{level}_nationwide.csv", merged)

        if level == "zcta":
            sc_rows = [r for r in merged if r["geoid"] in sc_zctas]
        else:
            sc_rows = [r for r in merged if r["geoid"].startswith(state.fips)]
        write_csv(output / f"acs_{state.abbr.lower()}_{level}.csv", sc_rows)

        moe_summary = moe_quality_summary(merged)
        all_level_stats.append({
            "level": level,
            "nationwide_rows": len(merged),
            "sc_rows": len(sc_rows),
            "moe_not_applicable_counts": moe_summary,
        })
        print(f"LEVEL {level}: nationwide={len(merged)} {state.abbr.lower()}={len(sc_rows)}", flush=True)

    variable_doc_rows = []
    for code, (name, domain) in {**PROFILE_VARS, **SUBJECT_VARS}.items():
        variable_doc_rows.append({"variable_code": code, "standardized_name": name, "domain": domain})
    write_csv(output / "acs_variable_list.csv", variable_doc_rows)
    write_csv(output / "acs_query_log.csv", query_log)

    lines = [
        "# ACS 5-year 2022 extraction summary",
        "",
        f"Run date: {datetime.now(timezone.utc).date().isoformat()}",
        f"Vintage: 2022 ACS 5-year (Data Profile + Subject Table datasets), retrieved {retrieved_at}",
        "",
        "## Row counts by geography level",
        "",
        f"| Level | Nationwide rows | {state.name} rows |",
        "|---|---|---|",
    ]
    for stat in all_level_stats:
        lines.append(f"| {stat['level']} | {stat['nationwide_rows']:,} | {stat['sc_rows']:,} |")
    lines.extend(
        [
            "",
            "## Margin-of-error sentinel counts",
            "",
            "Census uses negative sentinel codes (e.g. `-555555555`) in margin-of-error fields to mean \"not applicable\" (typically a count controlled to independent population/housing totals), not a real margin of zero.",
            "",
        ]
    )
    for stat in all_level_stats:
        lines.append(f"- {stat['level']}: {stat['moe_not_applicable_counts']}")
    lines.extend(
        [
            "",
            "## Query log",
            "",
            "API keys are redacted from all logged URLs; see `acs_query_log.csv` for the full list.",
            "",
            "## Variable list",
            "",
            "See `acs_variable_list.csv` for the full variable-code-to-standardized-name-to-domain mapping.",
        ]
    )
    (output / "acs_extraction_summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"DONE output={output}", flush=True)


if __name__ == "__main__":
    main()
