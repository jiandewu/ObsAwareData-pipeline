"""Assemble the public ObsAwareData repository from the per-state processing outputs.

Copies the selected per-state tables under public file names, regenerates a clean per-state
quality report from the published files themselves (no internal process notes), cross-checks
every wide table against the long table, and writes national summary tables plus a SHA-256
manifest.

The latest-value long table is published as one file per observation year
(<st>_long_latest_<year>.csv, by the year of `observation_week`), so no file grows past GitHub's
size limits. Each of its rows points to the release that first published its current content
(processing/build_observation_tables.py, anchor_to_first_release), and it has no
`extract_timestamp`, so a row changes only when its value or flags do and the repository's history
grows with the actual changes. Published long tables carry no `source_file_hash` column: each
source snapshot's SHA-256, and with --archive-registry its file in the archive of CDC releases, is
listed once in metadata/source_snapshots.csv.

Revision history (all-versions table, one row per value and source snapshot) is published only as
GitHub Release assets (--assets-dir <assets root>/<tag>), gzipped deterministically, never in git.
Each state's history is a chain listed in metadata/release_assets.csv: one `full` file, then one
`increment` per later release holding only the rows of source snapshots first published in that
release; concatenating the chain in order (dropping repeated headers) gives the complete table.
In revision history, `extract_timestamp` is the as-of date of the release that first published
the row's snapshot, so published rows never change. Each release compares every already-published
snapshot with the new processing output (per-snapshot row count and row-hash sum, excluding
`extract_timestamp` and `source_file_hash`). If any differs, a new BUILD_VERSION gets a new `full`
file, while under unchanged build rules the build stops: the processing is per snapshot, so this
points to a bug. A state also gets a new `full` file when its last full file is REBASE_DAYS or more
older than this release, when it has no published history, or with --full; otherwise an
increment, even across a version change that leaves the history as it was. Earlier chain files are read from
<assets root>/<their tag>/ and downloaded from their release URL when missing.

The processing scripts (../processing/) produce the per-state tables; this script only packages them.

Usage:
    python build_obsawaredata.py --by-state <processing by_state dir> --sc-dir <by_state/SC>
        --repo <ObsAwareData dir> [--states CA TX] [--no-copy]
        [--assets-dir <assets root>/<tag> --release-tag v1.0.0+2026-10-16 --extract-timestamp 2026-10-16 [--full]]
        [--archive-registry <archive/registry.csv> --archive-repo <owner>/<name>]
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import re
import shutil
import urllib.request
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

# source suffix (after {st}_) -> public suffix (after {st}_); the latest view is split by year (split_latest)
FILE_MAP = {
    "nhsn_wide.csv": "nhsn_wide.csv",
    "nssp_wide.csv": "nssp_wide.csv",
    "nwss_wide.csv": "nwss_wide.csv",
    "master_state_wide.csv": "all_sources_wide.csv",
    "census_context.csv": "census_context.csv",
}
RELEASE_BASE_URL = "https://github.com/DMA-PRIME/ObsAwareData/releases/download"
# Version of the build rules (the <version> in tag v<version>+<data date>; see the repo CHANGELOG).
# Bump it here, together with a CHANGELOG entry, whenever this script or the processing rules change.
BUILD_VERSION = "2.0.0"
REBASE_DAYS = 365  # a state's chain restarts with a full file at least this often
ASSET_FIELDS = ["tag", "version", "state", "file", "kind", "as_of", "snapshots", "rows", "bytes", "sha256", "url"]
SNAPSHOT_FIELDS = ["source_snapshot_id", "stream_id", "release_channel", "as_of_date", "source_file_hash", "archive_url"]
HASH_COL = b"source_file_hash"  # last column of the processing long tables; never published in them
LATEST_FILE = re.compile(r"_long_latest_(\d{4})\.csv$")
CHUNK_ROWS = 1_000_000
# In the processing long tables the first five fields (stream, snapshot id, as-of date, extract_timestamp,
# observation_week) and the last (source_file_hash) never hold a comma or quote, so they are read and
# replaced line by line without parsing the CSV.
WIDE_KEY_COLS = ["analysis_region", "analysis_region_type", "analysis_region_id",
                 "raw_geography_type", "raw_geography_id", "observation_week"]
LONG_KEY = ["stream_id", "observation_week", "analysis_region_type", "analysis_region_id", "pathogen", "feature_name"]
STATE_ORDER = ["observed_valid", "observed_zero", "observed_low_coverage", "flagged_anomalous",
               "incidental_missing", "structural_absent"]
STREAM_LABEL = {"nhsn": "Hospital admissions (NHSN)", "nssp": "Emergency department visits (NSSP)",
                "nwss": "Wastewater (NWSS)"}
PATHOGENS = ["covid", "influenza", "rsv"]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--by-state", required=True, type=Path)
    p.add_argument("--sc-dir", required=True, type=Path, help="Current-pipeline SC output directory")
    p.add_argument("--repo", required=True, type=Path)
    p.add_argument("--states", nargs="*", help="Subset of states (default: all)")
    p.add_argument("--no-copy", action="store_true", help="Reuse files already in the repo")
    p.add_argument("--assets-dir", type=Path,
                   help="<assets root>/<tag>: write this release's revision-history assets here")
    p.add_argument("--release-tag", help="Tag the assets are uploaded to (required with --assets-dir)")
    p.add_argument("--extract-timestamp", help="The release's as-of date: the revision history's extract_timestamp for "
                   "newly published snapshots (required with --assets-dir)")
    p.add_argument("--full", action="store_true", help="Publish a full revision-history file for every state")
    p.add_argument("--archive-registry", type=Path,
                   help="archive/registry.csv: link each source snapshot to its archived file")
    p.add_argument("--archive-repo", help="<owner>/<name> holding the archive (required with --archive-registry)")
    args = p.parse_args()
    if args.archive_registry and not args.archive_repo:
        p.error("--archive-repo is required with --archive-registry")
    if args.assets_dir and not (args.release_tag and args.extract_timestamp):
        p.error("--release-tag and --extract-timestamp are required with --assets-dir")
    return args


def read_str(path: Path, **kw) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=False, **kw)


def norm(v: str) -> str:
    """Normalize a cell for comparison: numeric strings compare by value."""
    v = v.strip()
    if v == "":
        return ""
    try:
        return repr(float(v.replace(",", "")))
    except ValueError:
        return v


def drop_last_field(line: bytes) -> bytes:
    """A CSV line without its last field (source_file_hash), keeping its line ending."""
    end = b"\r\n" if line.endswith(b"\r\n") else b"\n" if line.endswith(b"\n") else b""
    return line[:line.rindex(b",")] + end


def check_long_header(header: bytes, src: Path) -> None:
    cols = header.rstrip(b"\r\n").split(b",")
    if cols[:5] != [b"stream_id", b"source_snapshot_id", b"as_of_date", b"extract_timestamp",
                    b"observation_week"] or cols[-1] != HASH_COL:
        raise ValueError(f"{src}: unexpected long-table columns")


def split_latest(src: Path, out_dir: Path, lo: str) -> dict[str, dict]:
    """Write the latest view as <lo>_long_latest_<year>.csv (by the year of observation_week), without
    extract_timestamp and source_file_hash: every remaining field changes only when the row's
    content does (the processing anchors each row to the release that first published its content).
    Returns each source snapshot's stream, as-of date, and file hash."""
    files, snaps = {}, {}
    try:
        with open(src, "rb") as fin:
            header = fin.readline()
            check_long_header(header, src)
            f = header.split(b",", 4)
            header = drop_last_field(b",".join(f[:3] + f[4:]))
            for line in fin:
                f = line.split(b",", 4)
                if f[1] not in snaps:
                    snaps[f[1]] = {"stream_id": f[0].decode(), "as_of_date": f[2].decode(),
                                   "source_file_hash": line.rstrip(b"\r\n").rsplit(b",", 1)[1].decode()}
                year = f[4][:4].decode()
                if year not in files:
                    files[year] = open(out_dir / f"{lo}_long_latest_{year}.csv", "wb")
                    files[year].write(header)
                files[year].write(drop_last_field(b",".join(f[:3] + f[4:])))
    finally:
        for fh in files.values():
            fh.close()
    return {k.decode(): v for k, v in snaps.items()}


def latest_files(out_dir: Path, lo: str) -> list[Path]:
    """A state's published latest-value files, in year order."""
    return sorted(p for p in out_dir.glob(f"{lo}_long_latest_*.csv") if LATEST_FILE.search(p.name))


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(16 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_revision_file(src: Path, dst: Path, stamps: dict[bytes, bytes]) -> tuple[int, int]:
    """Gzip (no embedded name or timestamp) the rows of the snapshots in `stamps`
    (source_snapshot_id -> extract_timestamp) from an all-versions table, without source_file_hash;
    return (snapshots, rows)."""
    rows, seen = 0, set()
    opener = gzip.open if src.suffix == ".gz" else open
    with opener(src, "rb") as fin, open(dst, "wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, compresslevel=9, mtime=0) as fout:
            header = fin.readline()
            check_long_header(header, src)
            fout.write(drop_last_field(header))
            for line in fin:
                f = line.split(b",", 4)
                stamp = stamps.get(f[1])
                if stamp is None:
                    continue
                f[3] = stamp
                fout.write(drop_last_field(b",".join(f)))
                rows += 1
                seen.add(f[1])
    return len(seen), rows


def snapshot_digest(paths: list[Path]) -> dict[str, dict]:
    """Per source snapshot in all-versions tables: row count, order-independent sum (mod 2**64) of
    row hashes over every column except extract_timestamp and source_file_hash (published files
    have no source_file_hash), its extract_timestamp, and its stream, as-of date, and file hash."""
    out: dict[str, dict] = {}
    hash_col = HASH_COL.decode()
    for path in paths:
        for chunk in pd.read_csv(path, dtype=str, keep_default_na=False, chunksize=CHUNK_ROWS):
            h = pd.util.hash_pandas_object(chunk.drop(columns=["extract_timestamp", hash_col], errors="ignore"),
                                           index=False).to_numpy()
            codes, snaps = pd.factorize(chunk.source_snapshot_id)
            sums = np.zeros(len(snaps), dtype=np.uint64)
            np.add.at(sums, codes, h)  # wraps mod 2**64
            counts = np.bincount(codes, minlength=len(snaps))
            first = chunk.drop_duplicates("source_snapshot_id").set_index("source_snapshot_id")
            for s, n, hs in zip(snaps, counts, sums):
                r = first.loc[s]
                d = out.setdefault(s, {"rows": 0, "hsum": 0, "stamp": r.extract_timestamp, "info": {
                    "stream_id": r.stream_id, "as_of_date": r.as_of_date,
                    "source_file_hash": r[hash_col] if hash_col in first.columns else ""}})
                d["rows"] += int(n)
                d["hsum"] = (d["hsum"] + int(hs)) % 2**64
    return out


def read_chains(repo: Path) -> dict[str, list[dict]]:
    """Each state's published revision-history chain, in order, from the repo's current
    metadata/release_assets.csv."""
    path = repo / "metadata" / "release_assets.csv"
    chains: dict[str, list[dict]] = {}
    if not path.exists():
        return chains
    for r in read_str(path).to_dict("records"):
        chains.setdefault(r["state"], []).append({k: r[k] for k in ASSET_FIELDS})
    return chains


def chain_paths(chain: list[dict], assets_root: Path) -> list[Path]:
    """Local copies of a chain's files (<assets root>/<tag>/<file>), downloaded when missing."""
    paths = []
    for a in chain:
        p = assets_root / a["tag"] / a["file"]
        if not p.exists():
            p.parent.mkdir(parents=True, exist_ok=True)
            urllib.request.urlretrieve(a["url"], p)
        if sha256(p) != a["sha256"]:
            raise SystemExit(f"{p}: SHA-256 differs from metadata/release_assets.csv")
        paths.append(p)
    return paths


def build_revision_assets(st: str, av_src: Path, rev: dict) -> tuple[list[dict], list[Path], str, dict]:
    """Write a state's revision-history asset for this release (a full file or an increment) into
    rev['dir']; return its new chain, the local files of its complete history in chain order,
    why a full file was written ('' for an increment or no new snapshot), and each source snapshot's
    stream, as-of date, and file hash."""
    lo, stamp, chain = st.lower(), rev["stamp"], rev["chain"]
    src = av_src if av_src.exists() else Path(str(av_src) + ".gz")
    new = snapshot_digest([src])
    info = {s: d["info"] for s, d in new.items()}
    old, paths, changed = {}, [], []
    if chain:
        paths = chain_paths(chain, rev["dir"].parent)
        old = snapshot_digest(paths)
        changed = [s for s, d in old.items()
                   if s not in new or (new[s]["rows"], new[s]["hsum"]) != (d["rows"], d["hsum"])]
    published_version = chain[-1]["version"] if chain else ""
    if rev["full"]:
        reason = "requested (--full)"
    elif not chain:
        reason = "no published revision history"
    elif changed and published_version == BUILD_VERSION:  # same rules, same archived files
        raise SystemExit(f"{st}: {len(changed)} published snapshot(s) changed or missing under the same build rules "
                         f"({BUILD_VERSION}), e.g. {', '.join(sorted(changed)[:3])}. Fix the processing, or bump "
                         "BUILD_VERSION if the rules changed (--full rebuilds every state's history).")
    elif changed:
        reason = f"build rules changed ({published_version} to {BUILD_VERSION}) and {len(changed)} published snapshot(s) differ"
    elif (date.fromisoformat(stamp) - date.fromisoformat(chain[0]["as_of"])).days >= REBASE_DAYS:
        reason = f"last full file is from {chain[0]['as_of']}"
    else:
        reason = ""
    if reason:  # unchanged snapshots keep the date they were first published
        kind, name = "full", f"{lo}_long_all_versions.csv.gz"
        stamps = {s.encode(): (old[s]["stamp"] if s in old and s not in changed else stamp).encode() for s in new}
    else:
        kind, name = "increment", f"{lo}_long_all_versions_added_{stamp}.csv.gz"
        stamps = {s.encode(): stamp.encode() for s in new if s not in old}
        if not stamps:
            return chain, paths, "", info
    rev["dir"].mkdir(parents=True, exist_ok=True)
    dst = rev["dir"] / name
    n_snap, n_rows = write_revision_file(src, dst, stamps)
    row = {"tag": rev["tag"], "version": BUILD_VERSION, "state": st, "file": name, "kind": kind, "as_of": stamp,
           "snapshots": n_snap, "rows": n_rows, "bytes": dst.stat().st_size, "sha256": sha256(dst),
           "url": f"{RELEASE_BASE_URL}/{rev['tag'].replace('+', '%2B')}/{name}"}
    if kind == "full":
        return [row], [dst], reason, info
    return chain + [row], paths + [dst], "", info


def parse_thresholds(report_text: str) -> dict[str, str]:
    """Read the thresholds the pipeline actually applied from its own run report."""
    out = {}
    m = re.search(r"hospital_reporting_count < ([\d.]+)\)", report_text)
    out["nhsn_min_reporting_hospitals"] = m.group(1) if m else ""
    m = re.search(r"NHSN `hospital_admission_count` flagged `flagged_anomalous` \(\|week-over-week change\| >= ([\d.]+)%", report_text)
    out["nhsn_admission_change_pct"] = m.group(1) if m else ""
    m = re.search(r"NSSP `ed_visit_percent_smoothed` flagged `flagged_anomalous` \(\|week-over-week change\| >= ([\d.]+)%", report_text)
    out["nssp_smoothed_change_pct"] = m.group(1) if m else ""
    m = re.search(r"pathogen-specific cutoff - ([^)]*)\)", report_text)
    cut = dict(re.findall(r"(covid|influenza|rsv): ([\d.]+)", m.group(1))) if m else {}
    for p in PATHOGENS:
        out[f"nwss_min_coverage_ratio_{p}"] = cut.get(p, "")
    return {k: re.sub(r"\.0$", "", v) for k, v in out.items()}


def wide_cells(wide: pd.DataFrame, keyed: bool = True) -> dict:
    """Map (region type, region id, week, column) -> (value, flag); region is blank when not keyed."""
    cells = {}
    blank = pd.Series("", index=wide.index)
    rtype = wide["analysis_region_type"] if keyed else blank
    rid = wide["analysis_region_id"] if keyed else blank
    value_cols = [c for c in wide.columns if c not in WIDE_KEY_COLS and not c.endswith("_flag")]
    for c in value_cols:
        flag = wide[c + "_flag"] if c + "_flag" in wide.columns else blank
        for t, r, wk, v, f in zip(rtype, rid, wide["observation_week"], wide[c], flag):
            if v == "" and f == "":
                continue
            key = (t, r, wk, c)
            if key in cells:
                raise ValueError(f"duplicate wide cell {key}")
            cells[key] = (norm(v), f)
    return cells


def long_cells(long: pd.DataFrame, master: bool = False) -> dict:
    cells = {}
    for r in long.itertuples(index=False):
        col = f"{r.feature_name}_{r.pathogen}"
        rtype, rid = r.analysis_region_type, r.analysis_region_id
        if master:
            if rtype == "sub_state_hsa":
                col = f"HSA{rid}_{col}"
            elif rtype == "sampling_site":
                col = f"{rid.replace(':', '')}_{col}"
            rtype = rid = ""
        key = (rtype, rid, r.observation_week, col)
        if key in cells:
            raise ValueError(f"duplicate long cell {key}")
        cells[key] = (norm(r.weekly_feature), r.observation_state)
    return cells


def compare(expected: dict, actual: dict) -> tuple[bool, str]:
    missing = expected.keys() - actual.keys()
    extra = actual.keys() - expected.keys()
    diff = sum(1 for k in expected.keys() & actual.keys() if expected[k] != actual[k])
    ok = not (missing or extra or diff)
    return ok, f"{len(expected):,} cells; missing {len(missing)}, unexpected {len(extra)}, differing {diff}"


def md_table(df: pd.DataFrame) -> str:
    cols = list(df.columns)
    lines = ["| " + " | ".join(cols) + " |", "|" + "|".join("---" for _ in cols) + "|"]
    for row in df.itertuples(index=False):
        lines.append("| " + " | ".join(str(v) for v in row) + " |")
    return "\n".join(lines)


def fmt_int(n) -> str:
    return f"{int(n):,}"


def norm_series(s: pd.Series) -> pd.Series:
    """Vectorized cell normalization for the revision-history checks: numeric strings compare by
    value. Only used to compare the latest and all-versions tables with each other, so it only
    has to be self-consistent."""
    t = s.str.strip()
    num = pd.to_numeric(t.str.replace(",", "", regex=False), errors="coerce").astype(float)
    out = t.copy()
    m = num.notna()
    out[m] = num[m].astype(str)
    return out


def iter_chunks(paths: list[Path]):
    for path in paths:
        yield from pd.read_csv(path, dtype=str, keep_default_na=False, compression="gzip", chunksize=CHUNK_ROWS)


def revision_history_stats(paths: list[Path], long: pd.DataFrame) -> dict:
    """Stream a state's complete revision history (its chain files, in order) in chunks (large
    states do not fit in memory as strings) and compute its summary and consistency checks with
    64-bit key hashes."""
    snap_key = LONG_KEY + ["source_snapshot_id"]
    latest_pos = pd.Index(pd.util.hash_pandas_object(long[snap_key], index=False).to_numpy())
    latest_val = norm_series(long.weekly_feature).to_numpy(object)
    found = np.zeros(len(latest_pos), dtype=bool)
    vdiff = 0
    full_h, key_h, val_h, snaps, rows_by_stream = [], [], [], [], {}
    for chunk in iter_chunks(paths):
        fh =pd.util.hash_pandas_object(chunk[snap_key], index=False).to_numpy()
        vn = norm_series(chunk.weekly_feature)
        full_h.append(fh)
        key_h.append(pd.util.hash_pandas_object(chunk[LONG_KEY], index=False).to_numpy())
        val_h.append(pd.util.hash_array(vn.to_numpy(object)))
        loc = latest_pos.get_indexer(fh)
        hit = loc >= 0
        found[loc[hit]] = True
        vdiff += int((latest_val[loc[hit]] != vn.to_numpy(object)[hit]).sum())
        snaps.append(chunk[["stream_id", "source_snapshot_id", "as_of_date"]].drop_duplicates())
        for k, n in chunk.stream_id.value_counts().items():
            rows_by_stream[k] = rows_by_stream.get(k, 0) + int(n)
    full = np.concatenate(full_h)
    kv = pd.DataFrame({"k": np.concatenate(key_h), "v": np.concatenate(val_h)}).drop_duplicates()
    nver = kv.groupby("k").size()
    sn = pd.concat(snaps).drop_duplicates()
    per = sn.groupby("stream_id").agg(snapshots=("source_snapshot_id", "nunique"),
                                      first=("as_of_date", "min"), last=("as_of_date", "max")).reset_index()
    per["rows"] = per.stream_id.map(rows_by_stream)
    return {"rows": len(full), "duplicates": len(full) - len(np.unique(full)), "n_keys": len(nver),
            "revised": int((nver > 1).sum()), "missing": int((~found).sum()), "vdiff": vdiff, "per": per}


def build_state(st: str, src_dir: Path, repo: Path, copy: bool,
                rev: dict | None = None) -> tuple[dict, dict, list[dict] | None, str, dict]:
    """Build one state's folder; with `rev` (assets dir, tag, stamp, chain, full), also its
    revision-history asset. Returns the summary row, thresholds row, new chain, full-file reason, and
    the source snapshots seen (id -> stream, as-of date, file hash)."""
    lo = st.lower()
    out_dir = repo / "data" / st
    out_dir.mkdir(parents=True, exist_ok=True)
    av_src = src_dir / f"{lo}_all_versions.csv"
    source_snaps: dict[str, dict] = {}
    if copy:
        for s, d in FILE_MAP.items():
            shutil.copyfile(src_dir / f"{lo}_{s}", out_dir / f"{lo}_{d}")
        for p in latest_files(out_dir, lo):  # last release's years
            p.unlink()
        source_snaps = split_latest(src_dir / f"{lo}_latest_view.csv", out_dir, lo)
    chain, av_paths, full_reason = None, [], ""
    if rev:
        chain, av_paths, full_reason, rev_snaps = build_revision_assets(st, av_src, rev)
        source_snaps.update(rev_snaps)

    f = {k: out_dir / f"{lo}_{v}" for k, v in FILE_MAP.items()}
    years = latest_files(out_dir, lo)
    parts = [read_str(p) for p in years]
    long = pd.concat(parts, ignore_index=True)
    wides = {s: read_str(f[f"{s}_wide.csv"]) for s in ("nhsn", "nssp", "nwss")}
    master = read_str(f["master_state_wide.csv"])
    census = read_str(f["census_context.csv"])

    state_rows = long[long.stream_id == "nhsn"]
    state_name = state_rows.analysis_region.iloc[0]
    fips = state_rows.analysis_region_id.iloc[0]

    # ---- checks
    checks = []
    off_year = sum(int((d.observation_week.str[:4] != LATEST_FILE.search(p.name).group(1)).sum())
                   for p, d in zip(years, parts))
    checks.append((f"`{lo}_long_latest_<year>.csv`: each file holds only weeks ending in its year", off_year == 0,
                   f"{len(years)} files ({', '.join(LATEST_FILE.search(p.name).group(1) for p in years)}); "
                   f"{off_year} rows in the wrong file"))
    dup = int(long.duplicated(LONG_KEY).sum())
    checks.append(("Long table: one row per (source, week, region type, region, pathogen, measure)", dup == 0, f"{dup} duplicate keys"))
    for s, w in wides.items():
        ok, msg = compare(long_cells(long[long.stream_id == s]), wide_cells(w))
        checks.append((f"`{lo}_{s}_wide.csv` matches the long table", ok, msg))
    census_cols = {c for c in master.columns if c.startswith("census_")}
    exp_master = long_cells(long, master=True)
    act_master = {k: v for k, v in wide_cells(master, keyed=False).items()
                  if not k[3].startswith("census_")}
    ok_m, msg_m = compare(exp_master, act_master)
    checks.append((f"`{lo}_all_sources_wide.csv` matches the long table", ok_m, msg_m))
    cen = dict(zip("census_" + census.variable, census.value))
    cen_ok = set(cen) == census_cols and all((master[c] == v).all() for c, v in cen.items())
    checks.append((f"`{lo}_all_sources_wide.csv` census columns match `{lo}_census_context.csv`", cen_ok,
                   f"{len(census_cols)} columns, constant across all weeks"))
    weeks = sorted(master.observation_week)
    steps = {(date.fromisoformat(b) - date.fromisoformat(a)).days for a, b in zip(weeks, weeks[1:])}
    wk_ok = steps == {7} and len(set(weeks)) == len(weeks)
    union = set(long.observation_week)
    checks.append(("All-sources table has one row per week, weekly with no gaps, covering every week in the long table",
                   wk_ok and union <= set(weeks), f"{len(weeks)} weeks, {weeks[0]} to {weeks[-1]}"))

    # ---- coverage
    usable = long[long.observation_state.isin(["observed_valid", "observed_zero", "observed_low_coverage", "flagged_anomalous"])]
    cov_rows = []
    for s, feat in (("nhsn", "hospital_admission_count"), ("nssp", "ed_visit_percent"), ("nwss", "wastewater_activity_level")):
        for p in PATHOGENS + (["combined"] if s == "nssp" else []):
            g = usable[(usable.stream_id == s) & (usable.feature_name == feat) & (usable.pathogen == p)]
            n_units = g.analysis_region_id.nunique()
            cov_rows.append({"Source": STREAM_LABEL[s], "Pathogen": p,
                             "Regions or sites with data": n_units,
                             "First week with data": g.observation_week.min() if len(g) else "none",
                             "Last week with data": g.observation_week.max() if len(g) else "none"})
    cov = pd.DataFrame(cov_rows)
    n_hsa = long[(long.stream_id == "nssp") & (long.analysis_region_type == "sub_state_hsa")].analysis_region_id.nunique()
    sites = {p: long[(long.stream_id == "nwss") & (long.feature_name == "wastewater_activity_level") & (long.pathogen == p)]
             .analysis_region_id.nunique() for p in PATHOGENS}
    n_sites = long[long.stream_id == "nwss"].analysis_region_id.nunique()
    thresholds = parse_thresholds(
        (src_dir / (f"{lo}_quality_report.md" if (src_dir / f"{lo}_quality_report.md").exists()
                    else "quality_report.md")).read_text(encoding="utf-8"))
    for p in PATHOGENS:  # a cutoff is only meaningful where the state has sites for the pathogen
        if sites[p] == 0:
            thresholds[f"nwss_min_coverage_ratio_{p}"] = ""
    snaps = long.groupby("stream_id").as_of_date.agg(
        lambda x: x.max() if x.nunique() == 1 else f"{x.min()} to {x.max()} ({x.nunique()} releases)")

    # ---- observation-state counts
    st_counts = (long.groupby(["stream_id", "feature_name", "observation_state"]).size()
                 .unstack(fill_value=0).reindex(columns=STATE_ORDER, fill_value=0).reset_index())
    st_counts.insert(0, "Source", st_counts.pop("stream_id").map(lambda s: s.upper()))
    st_counts = st_counts.rename(columns={"feature_name": "Measure"})
    for c in STATE_ORDER:
        st_counts[c] = st_counts[c].map(fmt_int)

    sentinel = sorted(census.loc[census.value.str.startswith("-"), "variable"])

    # ---- files
    files = []
    tables = {"master_state_wide.csv": master, "census_context.csv": census,
              "nhsn_wide.csv": wides["nhsn"], "nssp_wide.csv": wides["nssp"], "nwss_wide.csv": wides["nwss"]}
    for path, df in [*zip(years, parts), *((f[k], tables[k]) for k in FILE_MAP)]:
        files.append({"File": f"`{path.name}`", "Rows": fmt_int(len(df)), "Columns": fmt_int(df.shape[1]),
                      "Size (MB)": f"{path.stat().st_size / 1e6:.1f}"})

    # ---- revision history
    rev_md = ""
    if av_paths:
        r = revision_history_stats(av_paths, long)
        checks.append(("Revision history: one row per (key, source snapshot)", r["duplicates"] == 0,
                       f"{r['duplicates']} duplicates"))
        checks.append(("Every latest-table row appears, with the same value, in the revision history",
                       r["missing"] == 0 and r["vdiff"] == 0, f"{r['missing']} missing, {r['vdiff']} differing"))
        per = r["per"]
        rev_tbl = pd.DataFrame({"Source": per.stream_id.str.upper(), "Snapshots": per.snapshots,
                                "Earliest snapshot": per["first"], "Latest snapshot": per["last"],
                                "Rows": per.rows.map(fmt_int)})
        rev_md = f"""
## Revision history (`{lo}_long_all_versions*.csv.gz`)

Location: GitHub Release assets, not stored in the repository. `metadata/release_assets.csv` lists this state's files in order, with download links and checksums: one `full` file, then one `increment` per later release holding only the source snapshots first published in it. Concatenating them in that order, keeping only the first header line, gives the complete table described here.

Every published version of every value, one row per (source, week, region, pathogen, measure, source snapshot): {fmt_int(r['rows'])} rows covering {fmt_int(r['n_keys'])} distinct values. Of those, {fmt_int(r['revised'])} ({r['revised'] / r['n_keys']:.1%}) took more than one distinct value across snapshots, i.e. were revised after first publication.

{md_table(rev_tbl)}
"""

    all_ok = all(ok for _, ok, _ in checks)
    chk = pd.DataFrame([{"Check": c, "Result": "PASS" if ok else "FAIL", "Detail": d} for c, ok, d in checks])
    na = lambda v: v if v else "not applicable (no sites)"
    thr_md = f"""| Rule | Value used for {st} |
|---|---|
| Hospital admissions: `coverage_status = reduced_participation` when fewer reporting hospitals than | {thresholds['nhsn_min_reporting_hospitals']} |
| Hospital admissions: `flagged_anomalous` when week-over-week change is at least | {thresholds['nhsn_admission_change_pct']}% |
| ED visits: smoothed percent `flagged_anomalous` when week-over-week change is at least | {thresholds['nssp_smoothed_change_pct']}% |
| Wastewater: `reduced_participation` when a site's reporting coverage ratio is below (COVID-19) | {na(thresholds['nwss_min_coverage_ratio_covid'])} |
| Wastewater: same, influenza A | {na(thresholds['nwss_min_coverage_ratio_influenza'])} |
| Wastewater: same, RSV | {na(thresholds['nwss_min_coverage_ratio_rsv'])} |"""

    report = f"""# {state_name} ({st}): data quality report

Generated from the published files in this folder. Overall result: **{'all checks passed' if all_ok else 'SOME CHECKS FAILED'}**.

## Summary

- State FIPS code: {fips}
- Emergency-department regions (CDC health service areas): {n_hsa}
- Wastewater sampling sites: {n_sites} (with COVID-19 data: {sites['covid']}; influenza A: {sites['influenza']}; RSV: {sites['rsv']})
- Source releases cited by the latest values (each row cites the release that first published its current value): hospital admissions {snaps.get('nhsn', 'none')}; ED visits {snaps.get('nssp', 'none')}; wastewater {snaps.get('nwss', 'none')}

## Files

{md_table(pd.DataFrame(files))}

## Coverage

A week counts as "with data" when the primary measure (weekly admissions, ED visit percent, or wastewater activity level) has a value, including values flagged low-coverage or anomalous.

{md_table(cov)}

## Reliability flags

Number of rows in `{lo}_long_latest_<year>.csv` (all years) by source, measure, and `observation_state` (see `DATA_DICTIONARY.md`).

{md_table(st_counts)}

Thresholds behind `observed_low_coverage` and `flagged_anomalous` for this state (method in `METHODS.md`):

{thr_md}

Census margin-of-error variables carrying the Census Bureau's negative placeholder code instead of a real value: {', '.join(f'`{v}`' for v in sentinel) if sentinel else 'none'}.
{rev_md}
## Consistency checks

{md_table(chk)}
"""
    (out_dir / f"{lo}_quality_report.md").write_text(report, encoding="utf-8", newline="\n")

    summary = {"state": st, "state_name": state_name, "state_fips": fips, "n_ed_regions": n_hsa,
               "n_wastewater_sites": n_sites, **{f"n_wastewater_sites_{p}": sites[p] for p in PATHOGENS}}
    for s, feat in (("nhsn", "hospital_admission_count"), ("nssp", "ed_visit_percent"), ("nwss", "wastewater_activity_level")):
        g = usable[(usable.stream_id == s) & (usable.feature_name == feat)]
        summary[f"{s}_first_week"] = g.observation_week.min() if len(g) else ""
        summary[f"{s}_last_week"] = g.observation_week.max() if len(g) else ""
    summary.update({"long_latest_rows": len(long), "all_sources_wide_columns": master.shape[1],
                    "all_checks_passed": all_ok})
    return summary, {"state": st, **thresholds}, chain, full_reason, source_snaps


def write_snapshots(path: Path, snaps: dict[str, dict], merge: bool, registry: Path | None,
                    archive_repo: str | None) -> None:
    """metadata/source_snapshots.csv: one row per source snapshot in the published tables and revision
    history, with its file's SHA-256 and, with an archive registry, the URL of the archived file (matched
    by stream, as-of date, and SHA-256). With `merge` (a subset build), rows of other snapshots are kept."""
    rows = {}
    if merge and path.exists():
        rows = {r["source_snapshot_id"]: r for r in read_str(path).to_dict("records")}
    archive = {}
    if registry:
        for r in read_str(registry).to_dict("records"):
            archive.setdefault((r["stream_id"], r["as_of_date"], r["raw_sha256"].lower()), []).append(
                f"https://github.com/{archive_repo}/releases/download/{r['release_tag']}/{r['file']}")
    for sid, d in snaps.items():
        parts = sid.split("_")
        url = ""
        if registry:
            hits = archive.get((d["stream_id"], d["as_of_date"], d["source_file_hash"].lower()), [])
            if len(hits) != 1:
                raise SystemExit(f"{sid}: {len(hits)} archived files match (stream, as-of date, SHA-256)")
            url = hits[0]
        rows[sid] = {"source_snapshot_id": sid, "stream_id": d["stream_id"],
                     "release_channel": parts[2] if len(parts) == 4 else "", "as_of_date": d["as_of_date"],
                     "source_file_hash": d["source_file_hash"], "archive_url": url}
    df = pd.DataFrame(rows.values(), columns=SNAPSHOT_FIELDS).sort_values(["stream_id", "as_of_date", "source_snapshot_id"])
    df.to_csv(path, index=False, lineterminator="\n")


def main() -> None:
    args = parse_args()
    states = args.states or sorted(p.name for p in args.by_state.iterdir() if p.is_dir())
    chains = read_chains(args.repo)  # before this build rewrites metadata/
    summaries, thresholds, snapshots = [], [], {}
    for st in states:
        src = args.sc_dir if st == "SC" else args.by_state / st
        rev = ({"dir": args.assets_dir, "tag": args.release_tag, "stamp": args.extract_timestamp,
                "chain": chains.get(st, []), "full": args.full} if args.assets_dir else None)
        s, t, chain, reason, snaps = build_state(st, src, args.repo, copy=not args.no_copy,
                                                 rev=rev)
        summaries.append(s)
        thresholds.append(t)
        for sid, d in snaps.items():
            if snapshots.setdefault(sid, d) != d:
                raise SystemExit(f"{st}: {sid} differs from another state's ({d} vs {snapshots[sid]})")
        note = ""
        if rev:
            last = chain[-1] if chain else None
            note = (" (revision history: no new snapshot)" if not last or last["tag"] != args.release_tag else
                    f" (revision history: full, {reason})" if last["kind"] == "full" else
                    f" (revision history: increment, {last['snapshots']} new snapshots)")
            chains[st] = chain
        print(f"{st}: {'PASS' if s['all_checks_passed'] else 'FAIL'}{note}", flush=True)

    meta = args.repo / "metadata"
    meta.mkdir(exist_ok=True)
    if snapshots:
        write_snapshots(meta / "source_snapshots.csv", snapshots, merge=bool(args.states),
                        registry=args.archive_registry, archive_repo=args.archive_repo)
    if not args.states:
        pd.DataFrame(summaries).to_csv(meta / "coverage_summary.csv", index=False, lineterminator="\n")
        pd.DataFrame(thresholds).to_csv(meta / "observation_state_thresholds.csv", index=False, lineterminator="\n")
        rows = []
        for path in sorted((args.repo / "data").rglob("*")):
            if path.is_file():
                rows.append({"path": path.relative_to(args.repo).as_posix(), "bytes": path.stat().st_size,
                             "sha256": sha256(path)})
        pd.DataFrame(rows).to_csv(meta / "manifest.csv", index=False, lineterminator="\n")
    if args.assets_dir:  # every state's chain (states not rebuilt keep theirs)
        assets = [a for st in sorted(chains) for a in chains[st]]
        pd.DataFrame(assets, columns=ASSET_FIELDS).to_csv(meta / "release_assets.csv", index=False,
                                                          lineterminator="\n")
    failed = [s["state"] for s in summaries if not s["all_checks_passed"]]
    print("FAILED:", failed if failed else "none")


if __name__ == "__main__":
    main()
