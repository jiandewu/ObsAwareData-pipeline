"""Update the release-specific facts in the ObsAwareData documentation.

Facts that change with every data refresh (weeks covered, number and dates of the source releases
used, the release tag) sit in the documents between markers:

    <!-- auto:KEY -->value<!-- /auto:KEY -->

and this script rewrites every marked value from the build outputs. It also sets `version` and
`date-released` in CITATION.cff and writes the release's CHANGELOG entry: a new entry is added when
the tag has none (marked as a data refresh when the version is unchanged; for the first release or a
new version, with the description written in changes/<version>.md next to this script, which must
exist), and that entry's generated block (only that entry's; older entries keep theirs) is rewritten
on every run:
the source releases used, the revision-history assets, the comparison with the previous release, and
the source changes found in it (rows no longer present, a source's last week moving back). Text
outside the markers is never changed.

Sources of the facts: the archive registry (releases on or before the as-of date), the data
repository's metadata/coverage_summary.csv and metadata/release_assets.csv, its latest tag (the
previous release), and the refresh's change_summary.csv.

Usage:
    python update_docs.py --repo <ObsAwareData dir> --tag v1.0.0+2026-10-16 --as-of 2026-10-16
        --archive-registry <cdc_archive/registry.csv> [--change-summary <change_summary.csv>]
        [--release-date YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
import csv
import re
import subprocess
from datetime import date, datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
STREAMS = {"nhsn": "NHSN", "nssp": "NSSP", "nwss": "NWSS"}
DOCS = ["README.md", "DATA_SOURCES.md", "DATA_DICTIONARY.md", "METHODS.md", "CHANGELOG.md"]
MARKER = re.compile(r"<!-- auto:([a-z0-9_]+) -->(.*?)<!-- /auto:\1 -->", re.S)
MONTHS = ["January", "February", "March", "April", "May", "June", "July", "August", "September",
          "October", "November", "December"]


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def month_range(first: str, last: str) -> str:
    a, b = date.fromisoformat(first), date.fromisoformat(last)
    if (a.year, a.month) == (b.year, b.month):
        return f"{MONTHS[a.month - 1]} {a.year}"
    if a.year == b.year:
        return f"{MONTHS[a.month - 1]} to {MONTHS[b.month - 1]} {b.year}"
    return f"{MONTHS[a.month - 1]} {a.year} to {MONTHS[b.month - 1]} {b.year}"


def grouped_dates(newest: dict[str, str]) -> str:
    """'2026-10-09 (NHSN, NWSS) and 2026-10-07 (NSSP)'."""
    by_date: dict[str, list[str]] = {}
    for s, d in newest.items():
        by_date.setdefault(d, []).append(STREAMS[s])
    parts = [f"{d} ({', '.join(names)})" for d, names in sorted(by_date.items(), reverse=True)]
    return parts[0] if len(parts) == 1 else ", ".join(parts[:-1]) + " and " + parts[-1]


def previous_tag(repo: Path) -> str:
    p = subprocess.run(["git", "-C", str(repo), "describe", "--tags", "--abbrev=0", "HEAD"],
                       capture_output=True, text=True)
    return p.stdout.strip()


def facts(args: argparse.Namespace) -> dict[str, str]:
    regs = [r for r in read_csv(args.archive_registry) if r["as_of_date"] <= args.as_of]
    f: dict[str, str] = {}
    newest, windows = {}, []
    for s, label in STREAMS.items():
        dates = sorted(r["as_of_date"] for r in regs if r["stream_id"] == s)
        f[f"releases_{s}"] = str(len(dates))
        f[f"first_{s}"] = dates[0]
        f[f"newest_{s}"] = newest[s] = dates[-1]
        windows.append(f"{month_range(dates[0], dates[-1])} ({label})")
    f["latest_releases"] = grouped_dates(newest)
    f["release_windows"] = ", ".join(windows[:-1]) + ", and " + windows[-1]

    cov = read_csv(args.repo / "metadata" / "coverage_summary.csv")
    ends = {}
    for s in STREAMS:
        firsts = [r[f"{s}_first_week"] for r in cov if r[f"{s}_first_week"]]
        lasts = [r[f"{s}_last_week"] for r in cov if r[f"{s}_last_week"]]
        f[f"weeks_{s}"] = f"{min(firsts)} to {max(lasts)}"
        ends[s] = max(lasts)
    every = [r[c] for r in cov for c in r if c.endswith(("_first_week", "_last_week")) and r[c]]
    f["weeks_all_sources"] = f"{min(every)} to {max(every)}"
    if len(set(ends.values())) == 1:
        f["source_end_dates"] = f"All three sources end on {ends['nhsn']}."
    else:  # "NHSN and NSSP end on 2026-10-03; NWSS ends on 2026-09-26."
        by_date: dict[str, list[str]] = {}
        for s, d in ends.items():
            by_date.setdefault(d, []).append(STREAMS[s])
        f["source_end_dates"] = "; ".join(
            f"{' and '.join(n)} {'end' if len(n) > 1 else 'ends'} on {d}"
            for d, n in sorted(by_date.items(), reverse=True)) + "."
    f["release_tag"] = args.tag
    f["release_tag_url"] = args.tag.replace("+", "%2B")
    f["changelog_data"] = changelog_block(args, newest, ends)
    return f


def changelog_block(args: argparse.Namespace, newest: dict[str, str], ends: dict[str, str]) -> str:
    lines = [f"- Source releases used, latest of each: "
             + ", ".join(f"{STREAMS[s]} {d}" for s, d in newest.items()) + ". Weeks covered: "
             + ", ".join(f"{STREAMS[s]} through {d}" for s, d in ends.items()) + "."]
    regs = [r for r in read_csv(args.archive_registry) if r["as_of_date"] <= args.as_of]
    counts = ", ".join(f"{sum(r['stream_id'] == s for r in regs)} {STREAMS[s]}" for s in STREAMS)
    assets = [a for a in read_csv(args.repo / "metadata" / "release_assets.csv") if a.get("tag") == args.tag]
    kinds = {k: sum(a["kind"] == k for a in assets) for k in ("full", "increment")}
    what = " and ".join(f"{n} {k}" for k, n in kinds.items() if n) or "none"
    lines.append(f"- Revision history covers {counts} releases. Release assets added in this release: {what} "
                 "(see `metadata/release_assets.csv`).")
    prev = previous_tag(args.repo)
    if prev and args.change_summary and args.change_summary.exists():
        cs = read_csv(args.change_summary)
        tot = {c: sum(int(r[c]) for r in cs) for c in ("added", "removed", "revised_values", "changed_flags")}
        lines.append(f"- Compared with {prev[1:]}: {tot['added']:,} rows added and "
                     f"{tot['removed']:,} removed in the latest-value tables; {tot['revised_values']:,} values revised; "
                     f"{tot['changed_flags']:,} `observation_state` values changed.")
        notes = change_warnings(cs)
        if notes:
            lines.append("- Source changes found in this release:")
            lines += [f"  - {n}" for n in notes]
    return "\n" + "\n".join(lines) + "\n"


def change_warnings(rows: list[dict]) -> list[str]:
    """Per-state source changes worth noting, from change_summary rows: rows of the previous release no
    longer in the latest-value tables, and a source's last week with data moving back."""
    out = []
    for r in rows:
        if int(r["removed"]):
            out.append(f"{r['state']}: {int(r['removed']):,} rows of the previous release are no longer in the "
                       "latest-value tables")
        for s, label in STREAMS.items():
            old, new = r[f"{s}_last_week_old"], r[f"{s}_last_week_new"]
            if new < old:
                out.append(f"{r['state']}: {label}'s last week with data moved back from {old} to {new or 'none'}")
    return out


def set_entry_block(text: str, version: str, block: str) -> str:
    """Rewrite the changelog_data block of this version's entry only; older entries keep theirs."""
    m = re.search(rf"^## {re.escape(version)} \(released [0-9-]+\)$", text, re.M)
    nxt = re.search(r"^## ", text[m.end():], re.M)
    stop = m.end() + nxt.start() if nxt else len(text)
    section = MARKER.sub(lambda b: f"<!-- auto:changelog_data -->{block}<!-- /auto:changelog_data -->"
                         if b.group(1) == "changelog_data" else b.group(0), text[m.end():stop])
    return text[:m.end()] + section + text[stop:]


def update_changelog(text: str, args: argparse.Namespace, block: str, released: str) -> str:
    version = args.tag[1:]
    head = re.compile(rf"^## {re.escape(version)} \(released [0-9-]+\)$", re.M)
    if head.search(text):
        return set_entry_block(head.sub(f"## {version} (released {released})", text), version, block)
    prev = previous_tag(args.repo)
    if prev and prev[1:].split("+")[0] == version.split("+")[0]:
        intro = "Data refresh with newer CDC source releases; the build rules are unchanged."
    else:  # the first release, or new build rules
        changes = HERE / "changes" / f"{version.split('+')[0]}.md"
        if not changes.exists():
            raise SystemExit(f"{changes} is missing: describe the rule changes of version {version.split('+')[0]} there")
        intro = changes.read_text(encoding="utf-8").strip()
    entry = (f"## {version} (released {released})\n\n{intro}\n\n"
             f"<!-- auto:changelog_data -->{block}<!-- /auto:changelog_data -->\n\n")
    m = re.search(r"^## \d", text, re.M)
    return text[:m.start()] + entry + text[m.start():] if m else text.rstrip("\n") + "\n\n" + entry


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--repo", required=True, type=Path)
    p.add_argument("--tag", required=True)
    p.add_argument("--as-of", required=True)
    p.add_argument("--archive-registry", required=True, type=Path)
    p.add_argument("--change-summary", type=Path)
    p.add_argument("--release-date", default=datetime.now(timezone.utc).date().isoformat())
    args = p.parse_args()

    f = facts(args)
    used, unknown = set(), set()
    for name in [*DOCS, "CITATION.cff"]:
        path = args.repo / name
        with path.open(encoding="utf-8", newline="") as h:
            raw = h.read()
        crlf = "\r\n" in raw  # a Windows checkout may have CRLF; edit in LF, write back as found
        text = raw.replace("\r\n", "\n")
        if name == "CITATION.cff":
            new = re.sub(r'^version: ".*"$', f'version: "{args.tag[1:]}"', text, flags=re.M)
            new = re.sub(r"^date-released: .*$", f"date-released: {args.release_date}", new, flags=re.M)
        elif name == "CHANGELOG.md":
            new = update_changelog(text, args, f["changelog_data"], args.release_date)
        else:
            new = text

        def fill(m: re.Match) -> str:
            key = m.group(1)
            if key not in f:
                unknown.add(f"{name}:{key}")
                return m.group(0)
            used.add(key)
            if name == "CHANGELOG.md" and key == "changelog_data":  # set per entry by update_changelog
                return m.group(0)
            return f"<!-- auto:{key} -->{f[key]}<!-- /auto:{key} -->"

        new = MARKER.sub(fill, new)
        if new != text:
            with path.open("w", encoding="utf-8", newline="") as h:
                h.write(new.replace("\n", "\r\n") if crlf else new)
            print(f"updated {name}", flush=True)
    print(f"markers filled: {', '.join(sorted(used)) or 'none'}", flush=True)
    if unknown:
        raise SystemExit(f"unknown markers: {', '.join(sorted(unknown))}")


if __name__ == "__main__":
    main()
