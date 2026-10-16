"""One-command data refresh of ObsAwareData: register new CDC snapshots, rerun the 51-state processing as
of a date, rebuild the repository, check it, and summarize what changed since the last commit.

Stages (run in order; --start-at resumes after a failure without redoing earlier stages):
    register  NHSN/NSSP (--registry-only) and NWSS snapshot registries over every file now
              under OAD_DATA_ROOT, kept in <work-root>/registry/
    checks    before processing (skip with --skip-checks): the ObsAwareData working tree is clean (the
              last refresh was committed or discarded); each source has a release in the as-of week
              (Saturday to Friday; NHSN must be the final, not the preliminary, release); and the newest
              release is newer than the latest tag's data date. If any fails, nothing runs and the
              reasons are written to <work-root>/<as-of>/not_run.md (exit code 2).
    process   run_state_pipeline.py --as-of into <work-root>/<as-of>/; every state must PASS
    build     build_obsawaredata.py into the ObsAwareData working tree, with every state's
              revision history written to <assets-root>/<tag>/ and each source snapshot linked to
              its file in the archive (cdc_archive/registry.csv); every state's checks must pass
    summary   compare the rebuilt latest tables with the repository's HEAD and write
              <work-root>/<as-of>/change_summary.md (+ .csv)
    docs      update_docs.py: refresh the dated facts in the repository's documentation, CHANGELOG
              entry, and CITATION.cff (skipped with --states)
    cleanup  delete this run's intermediate work/ folder (~2.6 GB) and keep only the newest
              --keep-weeks dated output folders (~4 GB each); the published tables and
              revision-history assets already carry the data history

The data date in the tag is the newest source release actually used (CHANGELOG versioning rule);
the version part is build_obsawaredata.BUILD_VERSION (the build rules) unless --version is given. Every row's extract_timestamp is set to the as-of date. Nothing is
committed, tagged, or uploaded: the script ends by printing those commands.

CDC releases NHSN final and NWSS on Fridays and NSSP on Wednesdays, so a run over the weekend (the
scheduled run is on Sunday) uses the default as-of date, the most recent Friday.

Environment: OAD_DATA_ROOT (raw-data root holding "CDC Data/" and "Census Data/") and
OAD_OUTPUT_ROOT (holding weekly_runs/, the ObsAwareData working tree, and
ObsAwareData_release_assets/). See README.md.

Usage:
    python weekly_refresh.py [--as-of 2026-10-02] [--states RI SC] [--workers 4] [--start-at process]
"""
from __future__ import annotations

import argparse
import csv
import io
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from build_obsawaredata import BUILD_VERSION, LATEST_FILE, LONG_KEY, latest_files, norm_series
from update_docs import change_warnings

HERE = Path(__file__).resolve().parent
PROCESSING = HERE.parent / "processing"
for _var in ("OAD_DATA_ROOT", "OAD_OUTPUT_ROOT"):
    if not os.environ.get(_var):
        raise SystemExit(f"Set {_var} (see README.md).")
DATA = Path(os.environ["OAD_DATA_ROOT"])
OUTPUTS = Path(os.environ["OAD_OUTPUT_ROOT"])
CROSS_STATE = HERE.parent / "reference"  # national geography audit and state threshold outputs
ARCHIVE_REGISTRY = HERE.parent / "cdc_archive" / "registry.csv"
STAGES = ["register", "process", "build", "summary"]
STREAM_FEATURE = {"nhsn": "hospital_admission_count", "nssp": "ed_visit_percent", "nwss": "wastewater_activity_level"}


def last_friday(d: date) -> date:
    return d - timedelta(days=(d.weekday() - 4) % 7)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--as-of", type=date.fromisoformat, default=last_friday(date.today()),
                   help="Use source snapshots released on or before this date (default: the most recent Friday)")
    p.add_argument("--work-root", type=Path, default=OUTPUTS / "weekly_runs")
    p.add_argument("--repo", type=Path, default=OUTPUTS / "ObsAwareData")
    p.add_argument("--assets-root", type=Path, default=OUTPUTS / "ObsAwareData_release_assets")
    p.add_argument("--states", nargs="*", help="Subset of states (test runs; build/summary then skip national metadata)")
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--start-at", choices=STAGES, default="register")
    p.add_argument("--version", default=BUILD_VERSION, help=f"Build-rule version for the tag (default: {BUILD_VERSION})")
    p.add_argument("--keep-weeks", type=int, default=3, help="Dated output folders to keep under --work-root")
    p.add_argument("--skip-checks", action="store_true", help="Run even if the pre-run checks fail")
    return p.parse_args()


def run(cmd: list, cwd: Path, log: Path | None = None, attempts: int = 1, before_retry=None) -> str:
    """Run a step with this interpreter, echo and return its output; stop on a non-zero exit.
    A synced storage drive can occasionally fail to open a file, so steps can be retried (`before_retry` resets any
    partial output first)."""
    cmd = [sys.executable, *map(str, cmd)]
    for attempt in range(1, attempts + 1):
        proc = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, encoding="utf-8", errors="replace",
                              env={**os.environ, "PYTHONIOENCODING": "utf-8", "PYTHONDONTWRITEBYTECODE": "1"})
        out = proc.stdout + proc.stderr
        if log:
            log.write_text(out, encoding="utf-8")
        print(out.encode("ascii", "replace").decode(), end="", flush=True)
        if proc.returncode == 0:
            return out
        if attempt < attempts:
            print(f"step failed (exit {proc.returncode}), retrying ({attempt + 1}/{attempts})", flush=True)
            time.sleep(60)
            if before_retry:
                before_retry()
    raise SystemExit(f"step failed (exit {proc.returncode}): {Path(str(cmd[1])).name}")


def git(repo: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True,
                          encoding="utf-8", check=True).stdout


def archive_repo() -> str:
    """<owner>/<name> holding the archive of CDC releases: GITHUB_REPOSITORY, else this checkout's
    remote named 'public' (a development clone whose origin is elsewhere), else its origin."""
    if os.environ.get("GITHUB_REPOSITORY"):
        return os.environ["GITHUB_REPOSITORY"]
    remote = "public" if "public" in git(HERE, "remote").split() else "origin"
    m = re.search(r"github\.com[:/](.+?/.+?)(?:\.git)?$", git(HERE, "remote", "get-url", remote).strip())
    if not m:
        raise SystemExit("Cannot tell which GitHub repository holds the archive; set GITHUB_REPOSITORY.")
    return m.group(1)


def register(reg: Path) -> None:
    reg.mkdir(parents=True, exist_ok=True)
    # register_nhsn_nssp_snapshots.py reuses a profile when a file's size and mtime are unchanged: seed its cache from the last
    # registry in this folder, if any, so only new files are read (the first run profiles every file).
    seed = reg / "nhsn_nssp_snapshot_registry.csv"
    if seed.exists():
        shutil.copyfile(seed, reg / "nhsn_nssp_snapshot_registry.partial.csv")
    cdc = DATA / "CDC Data"
    run(["register_nhsn_nssp_snapshots.py", "--data-root", DATA, "--registry-only", "--output-dir", reg,
         "--nhsn-dir", cdc / "CDC Hospital Repository Data" / "Raw data",
         "--nssp-dir", cdc / "CDC ED Repository Data" / "Raw data files"], PROCESSING, reg / "register_nhsn_nssp.log", attempts=3)
    run(["register_nwss_snapshots.py", "--data-root", DATA, "--output", reg / "nwss_snapshot_registry.csv",
         "--source-dir", "CDC Data/CDC Wastewater Surveillance Data/Main Three/Old Files"], PROCESSING, reg / "register_nwss.log",
        attempts=3)


def process(args: argparse.Namespace, reg: Path, out: Path) -> None:
    cmd = ["run_state_pipeline.py", "--out-root", out, "--as-of", args.as_of.isoformat(),
           "--nhsn-nssp-registry", reg / "nhsn_nssp_snapshot_registry.csv", "--nwss-registry", reg / "nwss_snapshot_registry.csv",
           "--thresholds", CROSS_STATE / "state_thresholds.csv",
           "--national-geography-summary", CROSS_STATE / "national_geography_summary.csv", "--workers", args.workers]
    if args.states:
        cmd += ["--states", *args.states]
    text = run(cmd, PROCESSING, out / "run_state_pipeline.log")
    status = dict(re.findall(r"^([A-Z]{2}): (PASS|FAIL[^\n]*)", text, flags=re.M))
    failed = [s for s, v in status.items() if v != "PASS"]
    expected = len(args.states) if args.states else 51
    if failed or len(status) != expected:
        raise SystemExit(f"processing: {len(status)}/{expected} states reported, failed: {failed} (logs in {out / 'logs'})")


def data_date(out: Path) -> str:
    """Newest source release used = the CHANGELOG's <data date>."""
    with (out / "inputs" / "selected_snapshots.csv").open(encoding="utf-8", newline="") as f:
        return max(r["as_of_date"] for r in csv.DictReader(f) if r["input"].endswith("_newest"))


def latest_tag_data_date(repo: Path) -> str:
    tags = git(repo, "tag", "--list", "v*").split()
    return max((t.split("+", 1)[1] for t in tags if "+" in t), default="")


def checks(args: argparse.Namespace, reg: Path) -> list[str]:
    """Reasons not to run this week (empty = go)."""
    problems = []
    if git(args.repo, "status", "--porcelain").strip():
        problems.append(f"{args.repo} has uncommitted changes: publish or discard the previous refresh first.")
    as_of = args.as_of.isoformat()
    week_start = (args.as_of - timedelta(days=6)).isoformat()
    rows = []
    for name in ("nhsn_nssp_snapshot_registry.csv", "nwss_snapshot_registry.csv"):
        with (reg / name).open(encoding="utf-8", newline="") as f:
            rows += [r for r in csv.DictReader(f) if r["as_of_date"] <= as_of]
    for label, keep in (("NHSN final", lambda r: r["stream_id"] == "nhsn" and r["release_channel"] == "standard"),
                        ("NSSP", lambda r: r["stream_id"] == "nssp"), ("NWSS", lambda r: r["stream_id"] == "nwss")):
        newest = max((r["as_of_date"] for r in rows if keep(r)), default="none")
        if newest < week_start:
            problems.append(f"No {label} release from {week_start} to {as_of} under OAD_DATA_ROOT (newest: {newest}).")
    tagged = latest_tag_data_date(args.repo)
    if max(r["as_of_date"] for r in rows) <= tagged:
        problems.append(f"Nothing newer than the latest release's data date ({tagged}).")
    return problems


def release_tag(out: Path, version: str) -> str:
    return f"v{version}+{data_date(out)}"


def build(args: argparse.Namespace, out: Path, tag: str) -> None:
    if git(args.repo, "status", "--porcelain").strip():
        raise SystemExit(f"{args.repo} has uncommitted changes; the change summary diffs against HEAD")
    cmd = ["build_obsawaredata.py", "--by-state", out / "by_state", "--sc-dir", out / "by_state" / "SC",
           "--repo", args.repo, "--assets-dir", args.assets_root / tag, "--release-tag", tag,
           "--extract-timestamp", args.as_of.isoformat(),
           "--archive-registry", ARCHIVE_REGISTRY, "--archive-repo", archive_repo()]
    if args.states:
        cmd += ["--states", *args.states]
    def restore() -> None:  # the tree was clean before the build, so this only drops partial output
        git(args.repo, "checkout", "--", ".")
        git(args.repo, "clean", "-fdq")
    text = run(cmd, HERE, out / "build_obsawaredata.log", attempts=3, before_retry=restore)
    if "FAILED: none" not in text:
        raise SystemExit("build: some states failed their consistency checks (see the quality reports)")


def read_long(sources: list) -> pd.DataFrame:
    """Concatenate long tables (paths or CSV text); none gives an empty table."""
    cols = LONG_KEY + ["weekly_feature", "observation_state"]
    sources = sources or [",".join(cols) + "\n"]
    return pd.concat([pd.read_csv(io.StringIO(s) if isinstance(s, str) else s, dtype=str, keep_default_na=False,
                                  usecols=cols) for s in sources], ignore_index=True)


def compare_state(repo: Path, st: str) -> dict:
    old_files = [p for p in git(repo, "ls-tree", "--name-only", "HEAD", f"data/{st}/").split()
                 if LATEST_FILE.search(p)]
    old = read_long([git(repo, "show", f"HEAD:{p}") for p in old_files])
    new = read_long(latest_files(repo / "data" / st, st.lower()))
    m = old.merge(new, on=LONG_KEY, how="outer", suffixes=("_old", "_new"), indicator=True)
    both = m[m._merge == "both"]
    row = {"state": st, "rows_old": len(old), "rows_new": len(new),
           "added": int((m._merge == "right_only").sum()), "removed": int((m._merge == "left_only").sum()),
           "revised_values": int((norm_series(both.weekly_feature_old) != norm_series(both.weekly_feature_new)).sum()),
           "changed_flags": int((both.observation_state_old != both.observation_state_new).sum())}
    usable = ["observed_valid", "observed_zero", "observed_low_coverage", "flagged_anomalous"]
    for s, feat in STREAM_FEATURE.items():
        for tag, df in (("old", old), ("new", new)):
            g = df[(df.stream_id == s) & (df.feature_name == feat) & df.observation_state.isin(usable)]
            row[f"{s}_last_week_{tag}"] = g.observation_week.max() if len(g) else ""
    return row


def summary(args: argparse.Namespace, out: Path, tag: str) -> None:
    states = args.states or sorted(p.name for p in (args.repo / "data").iterdir() if p.is_dir())
    rows = [compare_state(args.repo, st) for st in states]
    df = pd.DataFrame(rows)
    df.to_csv(out / "change_summary.csv", index=False, lineterminator="\n")
    warn = change_warnings(rows)
    last = {s: (df[f"{s}_last_week_old"].max(), df[f"{s}_last_week_new"].max()) for s in STREAM_FEATURE}
    with (out / "inputs" / "selected_snapshots.csv").open(encoding="utf-8", newline="") as f:
        sel = [r for r in csv.DictReader(f) if r["input"].endswith("_newest")]
    changed = git(args.repo, "status", "--porcelain").splitlines()
    tot = lambda c: f"{int(df[c].sum()):,}"
    md = [f"# ObsAwareData refresh {tag} (as of {args.as_of})", "",
          f"Compared with the repository's HEAD ({git(args.repo, 'describe', '--tags', '--always').strip()}).", "",
          "## Source releases used", "",
          "| Source | Newest release | Snapshots registered |", "|---|---|---|",
          *[f"| {r['input'].split('_')[0].upper()} | {r['as_of_date']} | {r['registered_snapshots']} |" for r in sel], "",
          "## Latest week with data (any state)", "",
          "| Source | Previous | Now |", "|---|---|---|",
          *[f"| {s.upper()} | {a} | {b} |" for s, (a, b) in last.items()], "",
          "## Latest-value tables", "",
          f"States: {len(rows)}. Rows added {tot('added')}, removed {tot('removed')}; values revised {tot('revised_values')}; "
          f"observation_state changed {tot('changed_flags')}. Per state: `change_summary.csv`.", "",
          f"Files changed in the working tree: {len(changed)}.", "",
          "## Source changes", "", *([f"- {w}" for w in warn] or ["None."]), "",
          "## Publishing", "",
          f"The refresh has filled in the documentation's dates and counts, CITATION.cff, and the CHANGELOG entry "
          f"`{tag[1:]}`, including the source changes above. Once every check has passed, the changes are merged "
          f"into main and the release `{tag}` is published with its assets (`{args.assets_root / tag}`)."]
    (out / "change_summary.md").write_text("\n".join(md) + "\n", encoding="utf-8", newline="\n")
    print("\n".join(md).encode("ascii", "replace").decode())


def cleanup(work_root: Path, out: Path, keep: int) -> None:
    shutil.rmtree(out / "work", ignore_errors=True)
    dated = sorted(d for d in work_root.iterdir() if d.is_dir() and re.fullmatch(r"\d{4}-\d{2}-\d{2}", d.name)
                   and (d / "change_summary.md").exists())  # completed runs only
    for d in dated[:-keep] if keep > 0 else []:
        shutil.rmtree(d, ignore_errors=True)
        print(f"removed old output folder {d.name}", flush=True)


def main() -> None:
    args = parse_args()
    tags = [t for t in git(args.repo, "tag", "--list", "v*").split() if "+" in t]
    last_version = max(tags, key=lambda t: t.split("+", 1)[1])[1:].split("+")[0] if tags else ""
    if last_version != args.version and not (HERE / "changes" / f"{args.version}.md").exists():
        raise SystemExit(f"Version {args.version} is new (latest release: {last_version or 'none'}): describe it "
                         f"in publish/changes/{args.version}.md first.")
    reg, out = args.work_root / "registry", args.work_root / args.as_of.isoformat()
    out.mkdir(parents=True, exist_ok=True)
    todo = STAGES[STAGES.index(args.start_at):]
    if "register" in todo:
        register(reg)
    if "process" in todo and not args.skip_checks and (problems := checks(args, reg)):
        note = [f"# Weekly refresh not run (as of {args.as_of})", ""] + [f"- {p}" for p in problems]
        (out / "not_run.md").write_text("\n".join(note) + "\n", encoding="utf-8", newline="\n")
        print("\n".join(note), flush=True)
        sys.exit(2)
    (out / "not_run.md").unlink(missing_ok=True)
    if "process" in todo:
        process(args, reg, out)
    tag = release_tag(out, args.version)
    print(f"release tag: {tag}", flush=True)
    if "build" in todo:
        build(args, out, tag)
    summary(args, out, tag)
    if not args.states:  # a subset build leaves the national metadata, so the docs' facts, unchanged
        run(["update_docs.py", "--repo", args.repo, "--tag", tag, "--as-of", args.as_of.isoformat(),
             "--archive-registry", ARCHIVE_REGISTRY,
             "--change-summary", out / "change_summary.csv"], HERE, out / "update_docs.log")
    cleanup(args.work_root, out, args.keep_weeks)


if __name__ == "__main__":
    main()
