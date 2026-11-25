"""Archive every new release of the CDC datasets ObsAwareData is built from.

CDC's data.cdc.gov API serves only the current version of each dataset, so revision history exists
only if each release is saved before the next one replaces it. For each dataset in DATASETS this
script reads the release time (`rowsUpdatedAt`) and, when it is newer than the newest one in the
registry, downloads the dataset, checks it, and stores it xz-compressed, together with the
dataset's metadata, as assets of the GitHub release `raw-<Friday>` (the Friday ending the
Saturday-to-Friday week of the CDC release, US Eastern time). Each archived file gets one row in
the registry CSV. Assets are uploaded before the registry is written, so a registry row always
points to an existing asset.

How the dataset is downloaded: until 2026-11, from data.cdc.gov's full CSV export
(`/api/views/<id>/rows.csv`), stored unchanged. That export was retired (HTTP 410), and its
replacement formats numbers for display (thousands separators, and half-up rounding to each
column's display precision). Since then the SODA API (`/resource/<id>.csv`, all pages in `:id`
order) is downloaded instead and stored unchanged as `soda_file`, and the main `file` is rebuilt
from it in the former export's layout (`legacy_csv`), so the files the pipeline reads keep the
same layout and values. The rebuild was checked byte for byte against the former export for the
same CDC releases of NHSN (standard and preliminary) and NWSS, and value for value for NSSP.

Checks before a file is archived: the API's own row count (`count(*)`) equals the number of CSV
records, `rowsUpdatedAt` is the same before and after the download (no release mid-download), and
the release has at least MIN_ROW_SHARE of the previous archived release's rows. CDC replaces a
dataset in steps: on 2026-11-25 NSSP showed 10,000 rows (4 geographies) for about 10 minutes, with a
release time of its own, before the full ~690,000. A smaller release is checked again every
SHRINK_WAIT seconds, up to SHRINK_WAITS times, and then fails; `--allow-shrink` archives it anyway
(for a deliberate cut by CDC).

Usage:
    python cdc_archive/archive_cdc.py [--registry cdc_archive/registry.csv] [--work <dir>] [--dry-run]
                                      [--allow-shrink]

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
from decimal import ROUND_HALF_EVEN, Decimal
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
          "row_count", "column_count", "source", "original_filename", "soda_file", "soda_sha256"]
RETRIES = 8  # data.cdc.gov intermittently serves a certificate for another host
RETRY_WAIT = 60  # seconds before retry n: RETRY_WAIT * n
PAGE_ROWS = 200_000  # SODA rows per request
MIN_ROW_SHARE = 0.9  # of the previous archived release's rows; no archived release has ever shrunk
SHRINK_WAIT = 600  # seconds between checks of a release that is still too small
SHRINK_WAITS = 3


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


def csv_rows(data: bytes) -> list[list[str]]:
    return list(csv.reader(io.StringIO(data.decode("utf-8-sig"), newline="")))


def soda_csv(dataset_id: str) -> bytes:
    """The dataset as the SODA API serves it (field names, unformatted values), all pages in :id
    order joined under one header."""
    parts: list[bytes] = []
    offset = 0
    while True:
        q = urllib.parse.urlencode({"$order": ":id", "$limit": PAGE_ROWS, "$offset": offset})
        page = get(f"{API}/resource/{dataset_id}.csv?{q}", timeout=900)
        head, _, body = page.partition(b"\n")
        if parts and head + b"\n" != parts[0]:
            raise SystemExit(f"{dataset_id}: SODA page header changed at offset {offset}")
        if not parts:
            parts.append(head + b"\n")
        parts.append(body)
        n = len(csv_rows(body))
        offset += n
        if n < PAGE_ROWS:
            return b"".join(parts)


def format_value(v: str, col: dict) -> str:
    """A SODA value as the former full export wrote it."""
    if v == "":
        return v
    kind, fmt = col["dataTypeName"], col.get("format") or {}
    if kind == "number":
        d = Decimal(v)
        if "precision" in fmt:  # fixed decimals, half to even: 484.5 -> 484, 3.5 -> 3.50
            return format(d.quantize(Decimal(1).scaleb(-int(fmt["precision"])), rounding=ROUND_HALF_EVEN), "f")
        return format(d.normalize(), "f")  # shortest form: 100.0 -> 100
    if kind == "calendar_date" and v.endswith("T00:00:00.000"):
        return v[:10]
    return v


def legacy_csv(soda: bytes, meta: dict) -> bytes:
    """Rebuild the former full CSV export from a SODA download: columns in the view's order under
    their display names, values formatted as that export wrote them, quoted only when needed."""
    cols = [c for c in meta["columns"] if not c["fieldName"].startswith(":")]
    rows = csv_rows(soda)
    idx = [rows[0].index(c["fieldName"]) for c in cols]
    out = io.StringIO()
    w = csv.writer(out, lineterminator="\n")
    w.writerow([c["name"] for c in cols])
    for row in rows[1:]:
        w.writerow([format_value(row[i], c) for i, c in zip(idx, cols)])
    return out.getvalue().encode("utf-8")


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
        notes = ("CDC data.cdc.gov releases from the week ending " + tag[4:] +
                 " (US Eastern), xz-compressed. See cdc_archive/registry.csv for each file's source and checksums,"
                 " and cdc_archive/archive_cdc.py for how each was downloaded.")
        p = gh("release", "create", tag, "--title", tag, "--notes", notes, "--latest=false")
        if p.returncode != 0:
            raise SystemExit(f"gh release create {tag} failed: {p.stderr.strip()}")
    p = gh("release", "upload", tag, *map(str, files), "--clobber")
    if p.returncode != 0:
        raise SystemExit(f"gh release upload {tag} failed: {p.stderr.strip()}")


def archive_one(stream: str, channel: str, ds: str, newest: datetime | None, previous_rows: int | None,
                work: Path, dry_run: bool, allow_shrink: bool = False) -> dict | None:
    meta, updated = metadata(ds)
    label = f"{stream}/{channel} ({ds})"
    if newest is not None and updated <= newest:
        print(f"{label}: no new release (newest {updated:%Y-%m-%d %H:%M} UTC already archived)", flush=True)
        return None
    print(f"{label}: new release {updated:%Y-%m-%d %H:%M:%S} UTC, downloading", flush=True)
    shrink_waits = 0
    for attempt in range(1, RETRIES + 1):
        retrieved = datetime.now(timezone.utc)
        soda = soda_csv(ds)
        raw = legacy_csv(soda, meta)
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
        if previous_rows and rows < MIN_ROW_SHARE * previous_rows and not allow_shrink:
            msg = f"{rows:,} rows, under {MIN_ROW_SHARE:.0%} of the previous archived release ({previous_rows:,})"
            if shrink_waits == SHRINK_WAITS:
                raise SystemExit(f"{msg}; not archived (CDC may still be loading it; --allow-shrink archives it)")
            shrink_waits += 1
            print(f"  {msg}; CDC may still be loading it, checking again in {SHRINK_WAIT // 60} min "
                  f"({shrink_waits}/{SHRINK_WAITS})", flush=True)
            time.sleep(SHRINK_WAIT)
            meta, updated = metadata(ds)
            continue
        break
    else:
        raise SystemExit(f"{label}: no consistent download after {RETRIES} attempts")
    stamp = updated.strftime("%Y-%m-%dT%H%M%SZ")
    name = f"{stream}_{channel}_{stamp}.csv.xz"
    meta_name = f"{stream}_{channel}_{stamp}.metadata.json.xz"
    soda_name = f"{stream}_{channel}_{stamp}.soda.csv.xz"
    xz = lzma.compress(raw, preset=6)
    meta_xz = lzma.compress(json.dumps(meta, indent=1, sort_keys=True).encode("utf-8"), preset=6)
    soda_xz = lzma.compress(soda, preset=6)
    work.mkdir(parents=True, exist_ok=True)
    (work / name).write_bytes(xz)
    (work / meta_name).write_bytes(meta_xz)
    (work / soda_name).write_bytes(soda_xz)
    tag = "raw-" + release_friday(updated)
    row = {"stream_id": stream, "release_channel": channel, "dataset_id": ds,
           "rows_updated_at": updated.strftime("%Y-%m-%dT%H:%M:%SZ"),
           "as_of_date": updated.astimezone(EASTERN).date().isoformat(),
           "retrieved_at": retrieved.strftime("%Y-%m-%dT%H:%M:%SZ"), "release_tag": tag, "file": name,
           "metadata_file": meta_name, "raw_bytes": len(raw), "raw_sha256": hashlib.sha256(raw).hexdigest(),
           "xz_bytes": len(xz), "xz_sha256": hashlib.sha256(xz).hexdigest(), "row_count": rows,
           "column_count": cols, "source": "cdc_api", "original_filename": "",
           "soda_file": soda_name, "soda_sha256": hashlib.sha256(soda).hexdigest()}
    print(f"  {rows:,} rows x {cols} columns, {len(raw) / 1e6:.1f} MB -> {len(xz) / 1e6:.1f} MB xz; {tag}/{name}",
          flush=True)
    if not dry_run:
        upload(tag, [work / name, work / meta_name, work / soda_name])
    return row


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--registry", type=Path, default=Path(__file__).resolve().parent / "registry.csv")
    p.add_argument("--work", type=Path, default=Path("archive_work"))
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="archive a release with far fewer rows than the last")
    args = p.parse_args()
    registry = read_registry(args.registry)
    new_rows, failed = [], []
    for stream, channel, ds in DATASETS:
        mine = sorted((r for r in registry if r["dataset_id"] == ds),
                      key=lambda r: (r["as_of_date"], r["rows_updated_at"]))
        times = [datetime.fromisoformat(r["rows_updated_at"].replace("Z", "+00:00"))
                 for r in mine if r["rows_updated_at"]]  # Box seeds have none
        counts = [int(r["row_count"]) for r in mine if r["row_count"]]
        try:
            row = archive_one(stream, channel, ds, max(times, default=None), counts[-1] if counts else None,
                              args.work, args.dry_run, args.allow_shrink)
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
