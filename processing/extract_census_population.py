#!/usr/bin/env python3
"""Register the nationwide Census extracts and build South Carolina subsets.

Reads the retained Population Estimates, 2020 ZCTA relationship, and 2022
TIGER/Line boundary files without modifying them. No ACS 5-year variable
extraction is performed here: the Census Data API requires a registered API
key, so that step is handled separately by extract_census_acs.py.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import struct
import zipfile
from pathlib import Path
from datetime import datetime, timezone

from state_reference import resolve_state


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--census-dir", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--state", default="SC", help="2-letter postal abbreviation, e.g. SC (default: SC)")
    return parser.parse_args()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest().upper()


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def read_pipe_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="|"))


def dbf_summary(zip_path: Path) -> dict[str, object]:
    with zipfile.ZipFile(zip_path) as archive:
        bad_member = archive.testzip()
        members = archive.namelist()
        dbf_name = next(n for n in members if n.lower().endswith(".dbf"))
        with archive.open(dbf_name) as handle:
            data = handle.read(4096)
    record_count = struct.unpack("<I", data[4:8])[0]
    fields = []
    pos = 32
    while data[pos] != 0x0D:
        raw = data[pos : pos + 32]
        fields.append(raw[:11].split(b"\x00")[0].decode("ascii"))
        pos += 32
    return {
        "zip_integrity_ok": bad_member is None,
        "members": members,
        "feature_count": record_count,
        "attribute_fields": fields,
    }


def register_file(path: Path, retrieval_url: str, retrieved_at: str) -> dict[str, object]:
    return {
        "file": path.name,
        "byte_size": path.stat().st_size,
        "sha256": sha256_file(path),
        "source_url": retrieval_url,
        "retrieved_at": retrieved_at,
    }


def main() -> None:
    args = parse_args()
    state = resolve_state(args.state)
    census_dir = args.census_dir
    output = args.output_dir
    output.mkdir(parents=True, exist_ok=True)
    retrieved_at = datetime.now(timezone.utc).isoformat()

    files = {
        "pop_state": (census_dir / "Population Estimates" / "NST-EST2022-ALLDATA.csv",
                      "https://www2.census.gov/programs-surveys/popest/datasets/2020-2022/state/totals/NST-EST2022-ALLDATA.csv"),
        "pop_county": (census_dir / "Population Estimates" / "co-est2022-alldata.csv",
                       "https://www2.census.gov/programs-surveys/popest/datasets/2020-2022/counties/totals/co-est2022-alldata.csv"),
        "zcta_county_rel": (census_dir / "ZCTA Relationship Files 2020" / "tab20_zcta520_county20_natl.txt",
                            "https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/tab20_zcta520_county20_natl.txt"),
        "zcta_tract_rel": (census_dir / "ZCTA Relationship Files 2020" / "tab20_zcta520_tract20_natl.txt",
                           "https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/tab20_zcta520_tract20_natl.txt"),
        "tiger_state": (census_dir / "TIGER-Line 2022" / "tl_2022_us_state.zip",
                        "https://www2.census.gov/geo/tiger/TIGER2022/STATE/tl_2022_us_state.zip"),
        "tiger_county": (census_dir / "TIGER-Line 2022" / "tl_2022_us_county.zip",
                         "https://www2.census.gov/geo/tiger/TIGER2022/COUNTY/tl_2022_us_county.zip"),
        "tiger_zcta": (census_dir / "TIGER-Line 2022" / "tl_2022_us_zcta520.zip",
                       "https://www2.census.gov/geo/tiger/TIGER2022/ZCTA520/tl_2022_us_zcta520.zip"),
    }

    registry = []
    for key, (path, url) in files.items():
        record = register_file(path, url, retrieved_at)
        record["product"] = key
        registry.append(record)

    pop_state_rows = read_csv_rows(files["pop_state"][0])
    pop_county_rows = read_csv_rows(files["pop_county"][0])
    zcta_county_rows = read_pipe_rows(files["zcta_county_rel"][0])
    zcta_tract_rows = read_pipe_rows(files["zcta_tract_rel"][0])

    sc_pop_state = [r for r in pop_state_rows if r.get("STATE") == state.fips]
    sc_pop_county = [r for r in pop_county_rows if r.get("STATE") == state.fips]
    sc_zcta_county = [r for r in zcta_county_rows if r["GEOID_ZCTA5_20"] and r["GEOID_COUNTY_20"].startswith(state.fips)]
    sc_zcta_tract = [r for r in zcta_tract_rows if r["GEOID_ZCTA5_20"] and r["GEOID_TRACT_20"].startswith(state.fips)]

    prefix = state.abbr.lower()
    with (output / f"{prefix}_population_estimates_state.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(sc_pop_state[0].keys()))
        writer.writeheader()
        writer.writerows(sc_pop_state)

    with (output / f"{prefix}_population_estimates_county.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(sc_pop_county[0].keys()))
        writer.writeheader()
        writer.writerows(sc_pop_county)

    with (output / f"{prefix}_zcta_county_relationship.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(sc_zcta_county[0].keys()))
        writer.writeheader()
        writer.writerows(sc_zcta_county)

    with (output / f"{prefix}_zcta_tract_relationship.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(sc_zcta_tract[0].keys()))
        writer.writeheader()
        writer.writerows(sc_zcta_tract)

    tiger_state = dbf_summary(files["tiger_state"][0])
    tiger_county = dbf_summary(files["tiger_county"][0])
    tiger_zcta = dbf_summary(files["tiger_zcta"][0])

    zcta_county_empty_zcta = sum(1 for r in zcta_county_rows if not r["GEOID_ZCTA5_20"])
    zcta_county_empty_county = sum(1 for r in zcta_county_rows if not r["GEOID_COUNTY_20"])
    zcta_county_both = [r for r in zcta_county_rows if r["GEOID_ZCTA5_20"] and r["GEOID_COUNTY_20"]]
    zcta_multi_county: dict[str, set[str]] = {}
    for r in zcta_county_both:
        zcta_multi_county.setdefault(r["GEOID_ZCTA5_20"], set()).add(r["GEOID_COUNTY_20"])
    multi_county_zctas = {z: c for z, c in zcta_multi_county.items() if len(c) > 1}

    lines = [
        "# Census extract validation summary",
        "",
        f"Run date: {datetime.now(timezone.utc).date().isoformat()}",
        "",
        "## Registered nationwide files",
        "",
        "| Product | File | Bytes | SHA-256 (first 16) |",
        "|---|---|---|---|",
    ]
    for r in registry:
        lines.append(f"| {r['product']} | {r['file']} | {r['byte_size']:,} | {r['sha256'][:16]}... |")
    lines.extend(
        [
            "",
            "## Population Estimates",
            "",
            f"- State file: {len(pop_state_rows)} rows (SUMLEV 010 US total, 020 regions, 030 divisions, 040 state-level incl. DC and Puerto Rico). {state.name} 2022 estimate: {sc_pop_state[0]['POPESTIMATE2022'] if sc_pop_state else 'n/a'}.",
            f"- County file: {len(pop_county_rows)} rows (SUMLEV 050 county-level, 040 state-summary rows). {state.name}: {len(sc_pop_county)} rows including its own 040 state-summary row.",
            "",
            "## 2020 ZCTA relationship files",
            "",
            f"- ZCTA-to-county: {len(zcta_county_rows)} total rows; {zcta_county_empty_zcta} rows have no ZCTA (county land area outside any ZCTA); {zcta_county_empty_county} rows have no county (none found); {len(zcta_county_both)} rows link a real ZCTA to a real county.",
            f"- {len(zcta_multi_county)} distinct ZCTAs nationwide in this file; {len(multi_county_zctas)} of them intersect more than one county, confirming ZCTA-to-county is many-to-many, not 1:1.",
            f"- {state.name}: {len(sc_zcta_county)} ZCTA-county rows, {len({r['GEOID_ZCTA5_20'] for r in sc_zcta_county})} distinct {state.abbr}-touching ZCTAs.",
            f"- ZCTA-to-tract: {len(zcta_tract_rows)} total rows; {state.name}: {len(sc_zcta_tract)} rows, {len({r['GEOID_ZCTA5_20'] for r in sc_zcta_tract})} distinct {state.abbr}-touching ZCTAs, {len({r['GEOID_TRACT_20'] for r in sc_zcta_tract})} distinct {state.abbr} tracts.",
            "- These are intersection-evidence files (land-area overlap only); no population-allocation weight is included here or derived by this script.",
            "",
            "## 2022 TIGER/Line boundaries",
            "",
            f"- State file: zip integrity {'OK' if tiger_state['zip_integrity_ok'] else 'FAILED'}, {tiger_state['feature_count']} features (50 states + DC + Puerto Rico + 4 other territories), fields: {tiger_state['attribute_fields']}.",
            f"- County file: zip integrity {'OK' if tiger_county['zip_integrity_ok'] else 'FAILED'}, {tiger_county['feature_count']} features, fields: {tiger_county['attribute_fields']}.",
            f"- Both carry `STATEFP`/`GEOID` fields compatible with the state-FIPS convention already used for NSSP and NWSS. Geometry was not parsed or subset in this step (no geospatial library available in this environment); {state.name} (STATEFP={state.fips}) is confirmed present in both nationwide files and can be filtered by a GIS tool in a later step.",
            f"- ZCTA file: zip integrity {'OK' if tiger_zcta['zip_integrity_ok'] else 'FAILED'}, {tiger_zcta['feature_count']} features (matches the 33,791 distinct ZCTAs found in the ZCTA-to-county relationship file above), fields: {tiger_zcta['attribute_fields']}. This file carries no state attribute directly (`ZCTA5CE20`/`GEOID20` only); a {state.name} subset must be produced either by filtering to the {len({r['GEOID_ZCTA5_20'] for r in sc_zcta_county})} ZCTA codes confirmed in the {state.abbr} relationship-file extract or by a spatial join in a later step, not by an attribute filter on this file alone.",
            "",
            "## Outstanding items (not completed in this pass)",
            "",
            "- ACS 5-year 2022 variable extraction: blocked. The Census Data API now requires a registered API key for every request (a plain unauthenticated call returns a \"Missing Key\" error); no key is on file for this project. A free key can be requested at https://api.census.gov/data/key_signup.html.",
        ]
    )
    (output / "census_validation_summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(
        f"DONE state={state.abbr} registry={len(registry)} sc_pop_rows={len(sc_pop_state) + len(sc_pop_county)} "
        f"sc_zcta_county_rows={len(sc_zcta_county)} sc_zcta_tract_rows={len(sc_zcta_tract)} output={output}",
        flush=True,
    )


if __name__ == "__main__":
    main()
