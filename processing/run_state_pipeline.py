#!/usr/bin/env python3
"""Run the generalized 11-step processing pipeline for one or more states into a versioned output root.

Layout under --out-root:
    inputs/                        registries filtered to --as-of, and the snapshots selected
    work/<ST>/{census,nssp,nwss,validation}/   intermediate step outputs
    by_state/<ST>/                 deliverables (all-versions table gzipped, not deleted)
    logs/<ST>.log                  full stdout/stderr of every step

Source snapshots: every step reads the NHSN/NSSP and NWSS snapshot registries filtered to
as_of_date <= --as-of (default: no filter). The single-snapshot inputs - the NSSP file for
extract_nssp.py, the NWSS file for extract_nwss.py's coverage ratio and blended-source flags, and the NSSP coverage map - are
the newest on or before --as-of, by the registry's parsed release date (as_of_date), not by
filename order: filename spellings differ across releases. Rerun the registries
(register_nhsn_nssp_snapshots.py, register_nwss_snapshots.py) first to pick up new files.

Thresholds: SC runs with build_observation_tables.py's own defaults; every other state passes its row
from --thresholds (derive_state_thresholds.py output). A blank NWSS cutoff (no sites for that
pathogen) falls back to the default, which then flags nothing.

Usage:
    python run_state_pipeline.py --out-root <dir> --thresholds <state_thresholds.csv>
        --national-geography-summary <national_geography_summary.csv> [--states SC CA] [--workers 4]
        --nhsn-nssp-registry <csv> --nwss-registry <csv> [--as-of 2026-10-02]

Environment: OAD_DATA_ROOT is the raw-data root holding "CDC Data/" and "Census Data/" (the
registries' source_relative_path values are relative to it). The NSSP coverage maps and the
national geography audit and state threshold tables live in ../reference/.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import os
import re
import shutil
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date
from pathlib import Path

from state_reference import IN_SCOPE_ABBRS

HERE = Path(__file__).resolve().parent
if not os.environ.get("OAD_DATA_ROOT"):
    raise SystemExit("Set OAD_DATA_ROOT to the raw-data root (holding 'CDC Data/' and 'Census Data/').")
DATA = Path(os.environ["OAD_DATA_ROOT"])
CENSUS = DATA / "Census Data"
# CDC's per-county NSSP facility status (https://www.cdc.gov/nssp/documents/coverage-map.csv),
# retained by hand as nssp_coverage_map_<YYYYMMDD>_raw.csv; not part of the automated downloads.
COVERAGE_MAP_DIR = HERE.parent / "reference"
COVERAGE_MAP_RE = re.compile(r"^nssp_coverage_map_(\d{8})_raw\.csv$")
PIT_CUTOFF = "2026-02-01"
RETRIES = 3  # a synced storage drive can occasionally fail to open a file mid-write


def filter_registry(src: Path, dst: Path, as_of: str | None) -> list[dict[str, str]]:
    """Copy a registry keeping rows with as_of_date <= as_of (all rows when None); same columns."""
    with src.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        fields = reader.fieldnames
        rows = [r for r in reader if as_of is None or r["as_of_date"] <= as_of]
    with dst.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)
    return rows


def newest(rows: list[dict[str, str]], stream_id: str) -> dict[str, str]:
    """Newest snapshot of a stream by release date; same tie-break as build_observation_tables.load_registry
    (standard before preliminary, then filename)."""
    channel_rank = {"standard": 0, "preliminary": 1}
    cand = [r for r in rows if r["stream_id"] == stream_id]
    if not cand:
        raise SystemExit(f"no {stream_id} snapshot on or before the as-of date")
    latest = max(date.fromisoformat(r["as_of_date"]) for r in cand).isoformat()
    return min((r for r in cand if r["as_of_date"] == latest),
               key=lambda r: (channel_rank.get(r["release_channel"], 2), r["filename"]))


def select_inputs(out_root: Path, nhsn_nssp_registry: Path, nwss_registry: Path, as_of: str | None) -> dict[str, Path]:
    """Write as-of-filtered registries under out_root/inputs and pick the single-snapshot inputs."""
    d = out_root / "inputs"
    d.mkdir(parents=True, exist_ok=True)
    nhsn_nssp_rows = filter_registry(nhsn_nssp_registry, d / "nhsn_nssp_snapshot_registry.csv", as_of)
    nwss_rows = filter_registry(nwss_registry, d / "nwss_snapshot_registry.csv", as_of)
    nssp, nwss = newest(nhsn_nssp_rows, "nssp"), newest(nwss_rows, "nwss")
    maps = sorted((date(int(m[1][:4]), int(m[1][4:6]), int(m[1][6:])).isoformat(), p)
                  for p in COVERAGE_MAP_DIR.iterdir() if (m := COVERAGE_MAP_RE.match(p.name)))
    maps = [(dt, p) for dt, p in maps if as_of is None or dt <= as_of]
    if not maps:
        raise SystemExit("no NSSP coverage map on or before the as-of date")
    # Provenance: the single-snapshot inputs, then each stream's newest snapshot and count in the
    # filtered registries (what the all-versions and latest-view tables are built from).
    selected = [("nssp_file", nssp["as_of_date"], nssp["filename"], ""),
                ("nwss_file", nwss["as_of_date"], nwss["filename"], ""),
                ("coverage_map", maps[-1][0], maps[-1][1].name, "")]
    for s, rows in (("nhsn", nhsn_nssp_rows), ("nssp", nhsn_nssp_rows), ("nwss", nwss_rows)):
        r = newest(rows, s)
        selected.append((f"{s}_newest", r["as_of_date"], r["filename"], sum(x["stream_id"] == s for x in rows)))
    with (d / "selected_snapshots.csv").open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["as_of", "input", "as_of_date", "filename", "registered_snapshots"])
        w.writerows((as_of or "", *r) for r in selected)
    for name, dt, _, n in selected:  # filenames can hold non-ASCII spaces; the CSV has them
        print(f"{name}: {dt}" + (f" ({n} registered)" if n != "" else ""), flush=True)
    return {"nhsn_nssp_registry": d / "nhsn_nssp_snapshot_registry.csv", "nwss_registry": d / "nwss_snapshot_registry.csv",
            "nssp_file": DATA / nssp["source_relative_path"], "nwss_file": DATA / nwss["source_relative_path"],
            "coverage_map": maps[-1][1]}


def threshold_args(state: str, thresholds: dict[str, dict[str, str]]) -> list[str]:
    if state == "SC":
        return []
    row = thresholds[state]
    mapping = {
        "--nhsn-low-coverage-threshold": "nhsn_low_coverage_threshold",
        "--nhsn-anomaly-relative-change": "nhsn_anomaly_relative_change",
        "--nssp-anomaly-relative-change": "nssp_anomaly_relative_change",
        "--nwss-low-coverage-covid": "nwss_low_coverage_covid",
        "--nwss-low-coverage-influenza": "nwss_low_coverage_influenza",
        "--nwss-low-coverage-rsv": "nwss_low_coverage_rsv",
    }
    args = []
    for flag, col in mapping.items():
        if row[col] != "":
            args += [flag, row[col]]
    return args


def run_state(state: str, out_root: Path, thresholds: dict, national_geography_summary: Path, inputs: dict[str, Path],
              pit_cutoff: str) -> tuple[str, str, float]:
    t0 = time.time()
    lo = state.lower()
    work = out_root / "work" / state
    final = out_root / "by_state" / state
    for d in (work / "census", work / "nssp", work / "nwss", work / "validation", final):
        d.mkdir(parents=True, exist_ok=True)
    log_path = out_root / "logs" / f"{state}.log"
    st = ["--state", state]
    steps = [
        ["extract_census_population.py", "--census-dir", CENSUS, "--output-dir", work / "census", *st],
        ["extract_census_acs.py", "--census-dir", CENSUS, "--output-dir", work / "census", "--zcta-county-rel-file",
         CENSUS / "ZCTA Relationship Files 2020" / "tab20_zcta520_county20_natl.txt", *st],
        ["extract_nssp.py", "--nssp-file", inputs["nssp_file"], "--registry", inputs["nhsn_nssp_registry"], "--output-dir", work / "nssp", *st],
        ["check_nssp_facility_coverage.py", "--coverage-map", inputs["coverage_map"],
         "--nssp-sc-wide", work / "nssp" / f"{lo}_nssp_extract_wide.csv",
         "--output", work / "nssp" / f"{lo}_hsa_facility_coverage_check.csv", *st],
        ["extract_nwss.py", "--nwss-file", inputs["nwss_file"], "--output-dir", work / "nwss", *st],
        ["validate_weekly_schema.py", "--nssp-long", work / "nssp" / f"{lo}_nssp_extract_long.csv",
         "--nwss-long", work / "nwss" / f"{lo}_nwss_extract_long.csv", "--output-dir", work / "validation",
         "--nssp-coverage-map", inputs["coverage_map"], "--national-geography-summary", national_geography_summary, *st],
        ["build_observation_tables.py", "--data-root", DATA, "--nhsn-nssp-registry", inputs["nhsn_nssp_registry"], "--census-extract-dir", work / "census",
         "--hsa-facility-coverage-check", work / "nssp" / f"{lo}_hsa_facility_coverage_check.csv",
         "--nwss-registry", inputs["nwss_registry"],
         "--nwss-coverage-gap-report", work / "nwss" / f"{lo}_nwss_coverage_gap_report.csv",
         "--nwss-wide-extract", work / "nwss" / f"{lo}_nwss_extract_wide.csv",
         "--output-dir", final, "--point-in-time-cutoff", pit_cutoff, *st, *threshold_args(state, thresholds)],
        ["build_hsa_combined.py", "--latest-view", final / f"{lo}_latest_view.csv",
         "--nwss-wide-extract", work / "nwss" / f"{lo}_nwss_extract_wide.csv", "--output-dir", final, *st],
        ["build_nwss_county_geography.py", "--latest-view", final / f"{lo}_latest_view.csv",
         "--nwss-wide-extract", work / "nwss" / f"{lo}_nwss_extract_wide.csv",
         "--county-population-estimates", work / "census" / f"{lo}_population_estimates_county.csv",
         "--output-dir", final, *st],
        ["build_wide_tables.py", "--input", final / f"{lo}_latest_view.csv", "--output-dir", final, *st],
        ["build_all_sources_wide.py", "--latest-view", final / f"{lo}_latest_view.csv",
         "--census-context", final / f"{lo}_census_context.csv", "--output-dir", final, *st],
    ]
    env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1", "PYTHONIOENCODING": "utf-8"}
    with open(log_path, "w", encoding="utf-8") as log:
        for i, step in enumerate(steps, 1):
            cmd = [sys.executable, *map(str, step)]
            for attempt in range(1, RETRIES + 1):
                log.write(f"=== step {i} attempt {attempt}: {step[0]}\n")
                log.flush()
                proc = subprocess.run(cmd, cwd=HERE, env=env, stdout=log, stderr=subprocess.STDOUT)
                if proc.returncode == 0:
                    break
                time.sleep(5)
            else:
                return state, f"FAIL at step {i} ({step[0]})", time.time() - t0
        av = final / f"{lo}_all_versions.csv"
        with open(av, "rb") as fin, gzip.GzipFile(str(av) + ".gz", "wb", compresslevel=9, mtime=0) as fout:
            shutil.copyfileobj(fin, fout, length=16 * 1024 * 1024)
        av.unlink()
        log.write("=== all-versions table gzipped\n")
    return state, "PASS", time.time() - t0


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--out-root", required=True, type=Path)
    p.add_argument("--thresholds", required=True, type=Path)
    p.add_argument("--national-geography-summary", required=True, type=Path)
    p.add_argument("--states", nargs="*")
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--as-of", type=date.fromisoformat,
                   help="Use only source snapshots released on or before this date (default: all registered)")
    p.add_argument("--nhsn-nssp-registry", required=True, type=Path)
    p.add_argument("--nwss-registry", required=True, type=Path)
    p.add_argument("--pit-cutoff", default=PIT_CUTOFF, help="Point-in-time view cutoff (research output)")
    args = p.parse_args()
    with args.thresholds.open(encoding="utf-8", newline="") as f:
        thresholds = {r["state"]: r for r in csv.DictReader(f)}
    states = args.states or sorted(IN_SCOPE_ABBRS)
    (args.out_root / "logs").mkdir(parents=True, exist_ok=True)
    inputs = select_inputs(args.out_root, args.nhsn_nssp_registry, args.nwss_registry,
                           args.as_of.isoformat() if args.as_of else None)
    with ProcessPoolExecutor(max_workers=args.workers) as ex:
        futs = [ex.submit(run_state, s, args.out_root, thresholds, args.national_geography_summary, inputs, args.pit_cutoff) for s in states]
        for fut in as_completed(futs):
            s, status, secs = fut.result()
            print(f"{s}: {status} ({secs:.0f}s)", flush=True)


if __name__ == "__main__":
    main()
