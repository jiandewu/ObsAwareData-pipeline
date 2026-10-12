#!/usr/bin/env python3
"""Register dated NWSS Main-Three snapshot files into a CSV shaped like the
NHSN/NSSP snapshot registry, so `build_observation_tables.py` can ingest NWSS the same way it already
ingests NHSN/NSSP: per-snapshot, preserving both `observation_week` and
`as_of_date`.

CDC's combined wastewater file is revised after first publication, so a
dated copy of it is archived each Friday in a folder that keeps growing
indefinitely. This script
registers whatever dated copies are present there at run time - it is meant
to be rerun as that folder accumulates more weeks, the same way the NHSN/NSSP
registry (register_nhsn_nssp_snapshots.py) is rebuilt as more NHSN/NSSP snapshots are retained. Read-only
against the source files.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import re
from pathlib import Path

FILENAME_DATE_RE = re.compile(r"_(\d{4})(\d{2})(\d{2})\.csv$")

FIELDNAMES = [
    "stream_id", "release_channel", "source_snapshot_id", "filename",
    "source_relative_path", "as_of_date", "as_of_date_basis",
    "byte_size", "sha256", "row_count",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", required=True, type=Path)
    parser.add_argument(
        "--source-dir", required=True, type=Path,
        help="Directory relative to --data-root holding dated NWSS files, "
             "e.g. 'CDC Data/CDC Wastewater Surveillance Data/Main Three/Old Files'.",
    )
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest().upper()


def count_rows(path: Path) -> int:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return sum(1 for _ in handle) - 1


def main() -> None:
    args = parse_args()
    source_dir = args.data_root / args.source_dir

    rows = []
    skipped = []
    for path in sorted(source_dir.glob("*.csv")):
        match = FILENAME_DATE_RE.search(path.name)
        if not match:
            skipped.append(path.name)
            continue
        as_of_date = f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
        sha256 = sha256_file(path)
        rows.append(
            {
                "stream_id": "nwss",
                "release_channel": "standard",
                "source_snapshot_id": f"nwss_{as_of_date.replace('-', '')}_{sha256[:12].lower()}",
                "filename": path.name,
                "source_relative_path": str((args.source_dir / path.name)).replace("\\", "/"),
                "as_of_date": as_of_date,
                "as_of_date_basis": "filename_date",
                "byte_size": path.stat().st_size,
                "sha256": sha256,
                "row_count": count_rows(path),
            }
        )

    rows.sort(key=lambda r: r["as_of_date"])
    duplicate_snapshot_ids = len(rows) - len({r["source_snapshot_id"] for r in rows})

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Registered {len(rows)} NWSS snapshots (skipped {len(skipped)} non-dated files: {skipped})")
    print(f"Duplicate source_snapshot_id count: {duplicate_snapshot_ids} (expected 0)")
    print(f"DONE output={args.output}", flush=True)


if __name__ == "__main__":
    main()
