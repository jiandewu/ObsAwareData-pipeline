"""Rebuild the raw-data folder the processing step reads from the archive.

Writes every archived file (or those released on or before --as-of) decompressed under --out in
the layout the snapshot registries expect: dated `<prefix>_<YYYYMMDD>.csv` files in one folder per
dataset. `box_archive` files keep their original names, so their bytes, and therefore their
snapshot ids (which embed the file's SHA-256), are unchanged; `cdc_api` files are named from the
dataset's prefix and their US Eastern release date. Point OAD_DATA_ROOT at --out.

Compressed files are downloaded once into --cache (reused across runs, e.g. with actions/cache)
and every file is checked against the registry's SHA-256 before and after decompression. Files
already present under --out with the right size are kept.

Usage:
    python archive/materialize.py --out <data root> [--as-of 2026-10-09] [--cache <dir>]
        [--registry archive/registry.csv]
"""
from __future__ import annotations

import argparse
import hashlib
import lzma
import os
import re
import subprocess
import time
import urllib.request
from pathlib import Path

from archive_cdc import RETRIES, RETRY_WAIT, read_registry

# (stream_id, release_channel) -> (folder under the data root, file name prefix before _YYYYMMDD.csv)
LAYOUT = {
    ("nhsn", "standard"): ("CDC Data/CDC Hospital Repository Data/Raw data",
                           "Weekly_Hospital_Respiratory_Data_(HRD)_Metrics_by_Jurisdiction,_National_Healthcare_Safety_Network_(NHSN)"),
    ("nhsn", "preliminary"): ("CDC Data/CDC Hospital Repository Data/Raw data",
                              "Weekly_Hospital_Respiratory_Data_(HRD)_Metrics_by_Jurisdiction,_National_Healthcare_Safety_Network_(NHSN)_(Preliminary)"),
    ("nssp", "standard"): ("CDC Data/CDC ED Repository Data/Raw data files",
                           "NSSP_Emergency_Department_Visit_Trajectories_by_State_and_Sub_State_Regions-_COVID-19,_Flu,_RSV,_Combined"),
    ("nwss", "standard"): ("CDC Data/CDC Wastewater Surveillance Data/Main Three/Old Files",
                           "CDC_Wastewater_Viral_Activity_Level_for_SARS-CoV-2,_Influenza_A_and_RSV"),
}


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 24), b""):
            h.update(chunk)
    return h.hexdigest()


def fetch(url: str, dest: Path, expected_sha: str) -> None:
    for attempt in range(1, RETRIES + 1):
        try:
            tmp = dest.with_suffix(dest.suffix + ".part")
            urllib.request.urlretrieve(url, tmp)
            if sha256(tmp) != expected_sha:
                raise ValueError("SHA-256 differs from the registry")
            tmp.replace(dest)
            return
        except Exception as e:  # noqa: BLE001 - network errors of any kind are retried
            if attempt == RETRIES:
                raise SystemExit(f"{url}: {e}")
            print(f"  retry {attempt}/{RETRIES - 1} for {dest.name}: {e}", flush=True)
            time.sleep(RETRY_WAIT * attempt)


def origin_repo() -> str | None:
    """<owner>/<name> of this checkout's GitHub origin, if any."""
    url = subprocess.run(["git", "-C", str(Path(__file__).resolve().parent), "remote", "get-url", "origin"],
                         capture_output=True, text=True).stdout.strip()
    m = re.search(r"github\.com[:/](.+?/.+?)(?:\.git)?$", url)
    return m.group(1) if m else None


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--out", required=True, type=Path)
    p.add_argument("--as-of", help="Only releases on or before this date (YYYY-MM-DD)")
    p.add_argument("--cache", type=Path, default=Path("archive_cache"))
    p.add_argument("--registry", type=Path, default=Path(__file__).resolve().parent / "registry.csv")
    p.add_argument("--repo", default=os.environ.get("GITHUB_REPOSITORY") or origin_repo(),
                   help="<owner>/<name> holding the archive (default: GITHUB_REPOSITORY, else the git origin)")
    args = p.parse_args()

    rows = [r for r in read_registry(args.registry) if not args.as_of or r["as_of_date"] <= args.as_of]
    targets = {}
    for r in rows:
        folder, prefix = LAYOUT[(r["stream_id"], r["release_channel"])]
        name = r["original_filename"] or f"{prefix}_{r['as_of_date'].replace('-', '')}.csv"
        dest = args.out / folder / name
        if dest in targets:
            raise SystemExit(f"two archived files map to {dest} ({targets[dest]['file']}, {r['file']})")
        targets[dest] = r
    args.cache.mkdir(parents=True, exist_ok=True)
    done = 0
    for dest, r in sorted(targets.items(), key=lambda t: (t[1]["as_of_date"], t[1]["file"])):
        if dest.exists() and dest.stat().st_size == int(r["raw_bytes"]):
            done += 1
            continue
        xz = args.cache / r["file"]
        if not (xz.exists() and xz.stat().st_size == int(r["xz_bytes"])):
            fetch(f"https://github.com/{args.repo}/releases/download/{r['release_tag']}/{r['file']}", xz, r["xz_sha256"])
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(".csv.part")
        h = hashlib.sha256()
        with lzma.open(xz, "rb") as fin, open(tmp, "wb") as fout:
            for chunk in iter(lambda: fin.read(1 << 24), b""):
                h.update(chunk)
                fout.write(chunk)
        if h.hexdigest() != r["raw_sha256"]:
            tmp.unlink()
            raise SystemExit(f"{r['file']}: decompressed SHA-256 differs from the registry")
        tmp.replace(dest)
        done += 1
        print(f"  {r['as_of_date']} {r['stream_id']}/{r['release_channel']} ({r['source']}): {dest.name}", flush=True)
    print(f"{done} files under {args.out}" + (f" (as of {args.as_of})" if args.as_of else ""), flush=True)


if __name__ == "__main__":
    main()
