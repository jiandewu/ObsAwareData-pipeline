"""One-time seed of the archive with CDC releases saved before the API archive started.

Those releases exist only as dated files kept by an earlier download process, described by two
snapshot registries (NHSN/NSSP and NWSS; columns stream_id, release_channel, filename,
source_relative_path, as_of_date, sha256, ...). Each listed file is checked against its registry
SHA-256, stored unchanged but xz-compressed as an asset of release `raw-<Friday>` (the Friday ending
its Saturday-to-Friday week), and added to archive/registry.csv with source `box_archive`, its
original file name, and a blank rows_updated_at (only the file's date is known). A release whose
(stream, channel, date) is already archived from the API is skipped, so each release is archived
once. Files already in the registry (same raw SHA-256) are skipped, so the script can be rerun.

Usage:
    python archive/seed_from_box.py --data-root <dir> --source-registry <nhsn_nssp.csv> <nwss.csv>
        [--registry archive/registry.csv] [--work <dir>] [--limit N] [--dry-run]
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import lzma
import time
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path

from archive_cdc import DATASETS, RETRIES, read_registry, release_friday, upload, write_registry

DATASET_ID = {(s, c): d for s, c, d in DATASETS}


def read_file(path: Path) -> bytes:
    for attempt in range(1, RETRIES + 1):  # synced cloud folders occasionally fail to open a file
        try:
            return path.read_bytes()
        except OSError as e:
            if attempt == RETRIES:
                raise
            print(f"  retry {attempt}/{RETRIES - 1} reading {path.name}: {e}", flush=True)
            time.sleep(10 * attempt)
    raise AssertionError


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--data-root", required=True, type=Path)
    p.add_argument("--source-registry", required=True, type=Path, nargs="+")
    p.add_argument("--registry", type=Path, default=Path(__file__).resolve().parent / "registry.csv")
    p.add_argument("--work", type=Path, default=Path("archive_work"))
    p.add_argument("--limit", type=int, help="Process at most this many files (test runs)")
    p.add_argument("--dry-run", action="store_true", help="Compress into --work only; no upload, registry unchanged")
    args = p.parse_args()

    registry = read_registry(args.registry)
    api_releases = {(r["stream_id"], r["release_channel"], r["as_of_date"]) for r in registry if r["source"] == "cdc_api"}
    known = {r["raw_sha256"] for r in registry}
    todo, skipped = [], defaultdict(int)
    for reg in args.source_registry:
        with reg.open(encoding="utf-8", newline="") as f:
            for r in csv.DictReader(f):
                key = (r["stream_id"], r["release_channel"], r["as_of_date"])
                if key in api_releases:
                    skipped["already archived from the API"] += 1
                elif r["sha256"].lower() in known:
                    skipped["already seeded"] += 1
                else:
                    todo.append(r)
    todo.sort(key=lambda r: (r["as_of_date"], r["stream_id"], r["release_channel"]))
    print(f"{len(todo)} files to seed; skipped: {dict(skipped) or 'none'}", flush=True)
    if args.limit:
        todo = todo[:args.limit]

    by_tag = defaultdict(list)
    for r in todo:
        d = date.fromisoformat(r["as_of_date"])
        by_tag["raw-" + release_friday(datetime(d.year, d.month, d.day, 12, tzinfo=timezone.utc))].append(r)
    args.work.mkdir(parents=True, exist_ok=True)
    added = 0
    for tag, rows in sorted(by_tag.items()):
        files, new_rows = [], []
        for r in rows:
            src = args.data_root / Path(*r["source_relative_path"].split("/"))
            raw = read_file(src)
            sha = hashlib.sha256(raw).hexdigest()
            if sha != r["sha256"].lower():
                raise SystemExit(f"{src}: SHA-256 {sha} differs from its registry ({r['sha256']})")
            reader = csv.reader(io.StringIO(raw.decode("utf-8-sig"), newline=""))
            cols = len(next(reader))
            n = sum(1 for _ in reader)
            xz = lzma.compress(raw, preset=6)
            name = f"{r['stream_id']}_{r['release_channel']}_{r['as_of_date']}_box.csv.xz"
            (args.work / name).write_bytes(xz)
            files.append(args.work / name)
            new_rows.append({
                "stream_id": r["stream_id"], "release_channel": r["release_channel"],
                "dataset_id": DATASET_ID[(r["stream_id"], r["release_channel"])], "rows_updated_at": "",
                "as_of_date": r["as_of_date"],
                "retrieved_at": datetime.fromtimestamp(src.stat().st_mtime, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "release_tag": tag, "file": name, "metadata_file": "", "raw_bytes": len(raw), "raw_sha256": sha,
                "xz_bytes": len(xz), "xz_sha256": hashlib.sha256(xz).hexdigest(), "row_count": n,
                "column_count": cols, "source": "box_archive", "original_filename": src.name})
            print(f"  {tag}/{name}: {n:,} rows, {len(raw) / 1e6:.1f} MB -> {len(xz) / 1e6:.1f} MB", flush=True)
        if not args.dry_run:  # upload, then record: a registry row always points to an uploaded asset
            upload(tag, files)
            registry += new_rows
            write_registry(args.registry, registry)
            for f in files:
                f.unlink()
        added += len(new_rows)
    print(f"seeded {added} files" + (" (dry run)" if args.dry_run else ""), flush=True)


if __name__ == "__main__":
    main()
