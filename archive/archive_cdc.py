"""Archive every new release of the CDC datasets ObsAwareData is built from.

CDC's data.cdc.gov API serves only the current version of each dataset, so revision history exists
only if each release is saved before the next one replaces it. For each dataset in DATASETS this
script reads the release time (`rowsUpdatedAt`) and, when it is newer than the newest one in the
registry, downloads the full CSV export, checks it, and stores it unchanged but xz-compressed,
together with the dataset's metadata, as assets of the GitHub release `raw-<Friday>` (the Friday
ending the Saturday-to-Friday week of the CDC release, US Eastern time). Each archived file gets
one row in the registry CSV. Assets are uploaded before the registry is written, so a registry
row always points to an existing asset.

Checks before a file is archived: the API's own row count (`count(*)`) equals the number of CSV
records, and `rowsUpdatedAt` is the same before and after the download (no release mid-download).

Usage:
    python archive/archive_cdc.py [--registry archive/registry.csv] [--work <dir>] [--dry-run]

--dry-run downloads, checks, and compresses into --work but uploads nothing and leaves the
registry unchanged. Uploading uses the GitHub CLI (`gh`, with GH_TOKEN) on the repository in
GITHUB_REPOSITORY (default: the current directory's repository).
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import lzma
import os
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

API = "https://data.cdc.gov"
EASTERN = ZoneInfo("America/New_York")
# (stream_id, release_channel, data.cdc.gov dataset id)
DATASETS = [
    ("nhsn", "standard", "ua7e-t2fy"),     # Weekly Hospital Respiratory Data (HRD) Metrics by Jurisdiction, NHSN
    ("nhsn", "preliminary", "mpgq-jmmr"),  # the same, preliminary release
    ("nssp", "standard", "rdmq-nq56"),     # NSSP Emergency Department Visit Trajectories by State and Sub State Regions
    ("nwss", "standard", "atcp-73re"),     # CDC Wastewater Viral Activity Level for SARS-CoV-2, Influenza A and RSV
]
FIELDS = ["stream_id", "release_channel", "dataset_id", "rows_updated_at", "as_of_date", "retrieved_at",
          "release_tag", "file", "metadata_file", "raw_bytes", "raw_sha256", "xz_bytes", "xz_sha256",
          "row_count", "column_count", "source", "original_filename"]
RETRIES = 8  # data.cdc.gov intermittently serves a certificate for another host
RETRY_WAIT = 60  # seconds before retry n: RETRY_WAIT * n


def get(url: str, timeout: int = 120) -> bytes:
    for attempt in range(1, RETRIES + 1):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as r:
                return r.read()
        except Exception as e:  # noqa: BLE001 - network errors of any kind are retried
            if attempt == RETRIES:
                raise
            print(f"  retry {attempt}/{RETRIES - 1} after error: {e}", flush=True)
            time.sleep(RETRY_WAIT * attempt)
    raise AssertionError


def metadata(dataset_id: str) -> tuple[dict, datetime]:
    meta = json.loads(get(f"{API}/api/views/{dataset_id}.json"))
    return meta, datetime.fromtimestamp(meta["rowsUpdatedAt"], timezone.utc)


def api_row_count(dataset_id: str) -> int:
    q = urllib.parse.urlencode({"$select": "count(*) AS n"})
    return int(json.loads(get(f"{API}/resource/{dataset_id}.json?{q}"))[0]["n"])


def release_friday(t: datetime) -> str:
    d = t.astimezone(EASTERN).date()
    return (d + timedelta(days=(4 - d.weekday()) % 7)).isoformat()


def read_registry(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def write_registry(path: Path, rows: list[dict]) -> None:
    """Write registry rows sorted by release date, stream, and channel (columns added later are blank)."""
    rows = sorted(rows, key=lambda r: (r["as_of_date"], r["rows_updated_at"], r["stream_id"], r["release_channel"]))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, restval="", lineterminator="\n")
        w.writeheader()
        w.writerows(rows)


def gh(*args: str) -> subprocess.CompletedProcess:
    repo = os.environ.get("GITHUB_REPOSITORY")
    cmd = ["gh", *args] + (["--repo", repo] if repo else [])
    return subprocess.run(cmd, capture_output=True, text=True)


def upload(tag: str, files: list[Path]) -> None:
    if gh("release", "view", tag).returncode != 0:
        notes = ("Unmodified CDC data.cdc.gov releases from the week ending " + tag[4:] +
                 " (US Eastern), xz-compressed. See archive/registry.csv for each file's source and checksums.")
        p = gh("release", "create", tag, "--title", tag, "--notes", notes, "--latest=false")
        if p.returncode != 0:
            raise SystemExit(f"gh release create {tag} failed: {p.stderr.strip()}")
    p = gh("release", "upload", tag, *map(str, files), "--clobber")
    if p.returncode != 0:
        raise SystemExit(f"gh release upload {tag} failed: {p.stderr.strip()}")


def archive_one(stream: str, channel: str, ds: str, newest: datetime | None, work: Path,
                dry_run: bool) -> dict | None:
    meta, updated = metadata(ds)
    label = f"{stream}/{channel} ({ds})"
    if newest is not None and updated <= newest:
        print(f"{label}: no new release (newest {updated:%Y-%m-%d %H:%M} UTC already archived)", flush=True)
        return None
    print(f"{label}: new release {updated:%Y-%m-%d %H:%M:%S} UTC, downloading", flush=True)
    for attempt in range(1, RETRIES + 1):
        retrieved = datetime.now(timezone.utc)
        raw = get(f"{API}/api/views/{ds}/rows.csv?accessType=DOWNLOAD", timeout=900)
        expected = api_row_count(ds)
        _, after = metadata(ds)
        if after != updated:
            print(f"  dataset changed during download ({after:%H:%M:%S} UTC); starting over", flush=True)
            meta, updated = metadata(ds)
            continue
        reader = csv.reader(io.StringIO(raw.decode("utf-8-sig"), newline=""))
        cols = len(next(reader))
        rows = sum(1 for _ in reader)
        if rows != expected:
            print(f"  {rows} CSV rows but the API counts {expected}; retrying ({attempt}/{RETRIES})", flush=True)
            time.sleep(60)
            continue
        break
    else:
        raise SystemExit(f"{label}: no consistent download after {RETRIES} attempts")
    stamp = updated.strftime("%Y-%m-%dT%H%M%SZ")
    name = f"{stream}_{channel}_{stamp}.csv.xz"
    meta_name = f"{stream}_{channel}_{stamp}.metadata.json.xz"
    xz = lzma.compress(raw, preset=6)
    meta_xz = lzma.compress(json.dumps(meta, indent=1, sort_keys=True).encode("utf-8"), preset=6)
    work.mkdir(parents=True, exist_ok=True)
    (work / name).write_bytes(xz)
    (work / meta_name).write_bytes(meta_xz)
    tag = "raw-" + release_friday(updated)
    row = {"stream_id": stream, "release_channel": channel, "dataset_id": ds,
           "rows_updated_at": updated.strftime("%Y-%m-%dT%H:%M:%SZ"),
           "as_of_date": updated.astimezone(EASTERN).date().isoformat(),
           "retrieved_at": retrieved.strftime("%Y-%m-%dT%H:%M:%SZ"), "release_tag": tag, "file": name,
           "metadata_file": meta_name, "raw_bytes": len(raw), "raw_sha256": hashlib.sha256(raw).hexdigest(),
           "xz_bytes": len(xz), "xz_sha256": hashlib.sha256(xz).hexdigest(), "row_count": rows,
           "column_count": cols, "source": "cdc_api", "original_filename": ""}
    print(f"  {rows:,} rows x {cols} columns, {len(raw) / 1e6:.1f} MB -> {len(xz) / 1e6:.1f} MB xz; {tag}/{name}",
          flush=True)
    if not dry_run:
        upload(tag, [work / name, work / meta_name])
    return row


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--registry", type=Path, default=Path(__file__).resolve().parent / "registry.csv")
    p.add_argument("--work", type=Path, default=Path("archive_work"))
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    registry = read_registry(args.registry)
    new_rows, failed = [], []
    for stream, channel, ds in DATASETS:
        times = [datetime.fromisoformat(r["rows_updated_at"].replace("Z", "+00:00"))
                 for r in registry if r["dataset_id"] == ds and r["rows_updated_at"]]  # Box seeds have none
        try:
            row = archive_one(stream, channel, ds, max(times, default=None), args.work, args.dry_run)
        except (Exception, SystemExit) as e:  # noqa: BLE001 - one dataset's failure must not stop the others
            print(f"{stream}/{channel} ({ds}): FAILED: {e}", flush=True)
            failed.append(ds)
            continue
        if row:
            new_rows.append(row)
    if new_rows and not args.dry_run:
        write_registry(args.registry, registry + new_rows)
    print(f"archived {len(new_rows)} new release(s)" + (" (dry run)" if args.dry_run else "")
          + (f"; FAILED: {', '.join(failed)}" if failed else ""), flush=True)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
