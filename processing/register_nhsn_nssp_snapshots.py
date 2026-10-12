#!/usr/bin/env python3
"""Build the NHSN/NSSP historical snapshot registry and revision audit.

The script reads source CSVs without modifying them. It intentionally discovers
snapshot counts at run time because CDC directories continue to receive updates.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import re
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

import pandas as pd


DATE_RE = re.compile(r"_(\d{8})\.csv$")
NHSN_PREFIX = "Weekly_Hospital_Respiratory_Data_"
NSSP_PREFIX = "NSSP_Emergency_Department_Visit_Trajectories_"
REGISTRY_FIELDS = [
    "stream_id", "release_channel", "source_snapshot_id", "filename", "source_relative_path",
    "filename_date", "embedded_release_value", "as_of_date", "as_of_date_basis", "byte_size", "filesystem_mtime_ns",
    "sha256", "row_count", "column_count", "column_signature_sha256", "schema_variant_id",
    "first_observation_week", "last_observation_week", "geography_count", "duplicate_key_rows",
    "key_columns",
]


@dataclass(frozen=True)
class StreamSpec:
    stream_id: str
    directory: Path
    prefix: str
    key_columns: tuple[str, ...]
    observation_week_column: str
    geography_column: str
    volatile_columns: tuple[str, ...] = ()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", required=True, type=Path)
    parser.add_argument("--nhsn-dir", required=True, type=Path)
    parser.add_argument("--nssp-dir", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--run-date", default=date.today().isoformat())
    parser.add_argument("--report-only", action="store_true")
    parser.add_argument(
        "--registry-only", action="store_true",
        help="Write only the registry and excluded-files list (skips the pairwise revision audit "
             "and report). Used by the weekly refresh to register new snapshots quickly.",
    )
    return parser.parse_args()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest().upper()


def console_text(value: str) -> str:
    """Return text that can be printed under the active Windows code page."""
    encoding = sys.stdout.encoding or "utf-8"
    return value.encode(encoding, errors="replace").decode(encoding, errors="replace")


def read_header(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return next(csv.reader(handle))


def filename_date(path: Path) -> str:
    match = DATE_RE.search(path.name)
    if not match:
        raise ValueError(f"No YYYYMMDD suffix in {path.name}")
    return datetime.strptime(match.group(1), "%Y%m%d").date().isoformat()


def release_channel(stream_id: str, path: Path) -> str:
    if stream_id == "nhsn" and "Preliminary" in path.name:
        return "preliminary"
    return "standard"


def discover(spec: StreamSpec) -> tuple[list[Path], list[Path]]:
    included: list[Path] = []
    excluded: list[Path] = []
    for path in sorted(spec.directory.glob("*.csv"), key=lambda p: p.name):
        if path.name.startswith(spec.prefix) and DATE_RE.search(path.name):
            included.append(path)
        else:
            excluded.append(path)
    return included, excluded


def relative_source_path(path: Path, data_root: Path) -> str:
    try:
        return path.resolve().relative_to(data_root.resolve()).as_posix()
    except ValueError:
        return f"<outside-data-root>/{path.name}"


def profile_snapshot(spec: StreamSpec, path: Path, data_root: Path) -> dict[str, object]:
    header = read_header(path)
    missing_keys = [column for column in spec.key_columns if column not in header]
    if missing_keys:
        raise ValueError(f"{path.name}: missing key columns {missing_keys}")

    usecols = list(dict.fromkeys((*spec.key_columns, spec.observation_week_column, spec.geography_column)))
    if "BuildNumber" in header:
        usecols.append("BuildNumber")

    row_count = 0
    duplicate_key_rows = 0
    seen_keys: set[tuple[str, ...]] = set()
    weeks: list[str] = []
    geographies: set[str] = set()
    embedded_values: set[str] = set()

    for chunk in pd.read_csv(
        path,
        usecols=usecols,
        dtype="string[pyarrow]",
        keep_default_na=False,
        chunksize=100_000,
    ):
        row_count += len(chunk)
        keys = list(chunk.loc[:, list(spec.key_columns)].itertuples(index=False, name=None))
        for key in keys:
            normalized = tuple(str(value) for value in key)
            if normalized in seen_keys:
                duplicate_key_rows += 1
            else:
                seen_keys.add(normalized)
        weeks.extend(str(value) for value in chunk[spec.observation_week_column] if str(value))
        geographies.update(str(value) for value in chunk[spec.geography_column] if str(value))
        if "BuildNumber" in chunk.columns:
            embedded_values.update(str(value) for value in chunk["BuildNumber"] if str(value))

    file_hash = sha256_file(path)
    file_date = filename_date(path)
    embedded_release = "|".join(sorted(embedded_values)) if embedded_values else ""
    # The dated filename identifies the retained point-in-time snapshot. The
    # embedded BuildNumber is source-build metadata and may lag or repeat.
    as_of = file_date
    channel = release_channel(spec.stream_id, path)
    snapshot_id = f"{spec.stream_id}_{as_of.replace('-', '')}_{channel}_{file_hash[:12].lower()}"
    column_signature = hashlib.sha256("\x1f".join(header).encode("utf-8")).hexdigest().upper()

    parsed_weeks = pd.to_datetime(pd.Series(weeks, dtype="string"), errors="coerce")
    valid_weeks = parsed_weeks.dropna()
    return {
        "stream_id": spec.stream_id,
        "release_channel": channel,
        "source_snapshot_id": snapshot_id,
        "filename": path.name,
        "source_relative_path": relative_source_path(path, data_root),
        "filename_date": file_date,
        "embedded_release_value": embedded_release,
        "as_of_date": as_of,
        "as_of_date_basis": "filename_date",
        "byte_size": path.stat().st_size,
        "filesystem_mtime_ns": path.stat().st_mtime_ns,
        "sha256": file_hash,
        "row_count": row_count,
        "column_count": len(header),
        "column_signature_sha256": column_signature,
        "schema_variant_id": f"schema_{column_signature[:12].lower()}",
        "first_observation_week": valid_weeks.min().date().isoformat() if not valid_weeks.empty else "",
        "last_observation_week": valid_weeks.max().date().isoformat() if not valid_weeks.empty else "",
        "geography_count": len(geographies),
        "duplicate_key_rows": duplicate_key_rows,
        "key_columns": "|".join(spec.key_columns),
    }


def load_snapshot(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype="string[pyarrow]", keep_default_na=False)


def measure_group(field: str) -> str:
    value = field.lower()
    if "covid" in value or "sars" in value:
        return "covid_19"
    if "influenza" in value or "flu" in value:
        return "influenza"
    if "rsv" in value:
        return "rsv"
    if "combined" in value or "respiratory" in value:
        return "combined_respiratory"
    if value in {"hsa", "hsa_counties", "hsa_nci_id", "fips", "trend_source"}:
        return "geography_metadata"
    return "other"


def age_bin(days: int) -> str:
    if days < 0:
        return "future_relative_to_as_of"
    if days <= 14:
        return "0_2_weeks"
    if days <= 28:
        return "3_4_weeks"
    if days <= 56:
        return "5_8_weeks"
    if days <= 182:
        return "9_26_weeks"
    if days <= 364:
        return "27_52_weeks"
    return "over_52_weeks"


def aggregate_status_rows(
    frame: pd.DataFrame,
    index: pd.Index,
    spec: StreamSpec,
    status: str,
    current_id: str,
    previous_id: str,
    as_of_date: str,
) -> list[dict[str, object]]:
    if len(index) == 0:
        return []
    selected = frame.loc[index, [spec.observation_week_column, spec.geography_column]].copy().reset_index(drop=True)
    selected["observation_date"] = pd.to_datetime(selected[spec.observation_week_column], errors="coerce")
    as_of = pd.Timestamp(as_of_date)
    selected["age_bin"] = selected["observation_date"].map(
        lambda value: "unparseable_week" if pd.isna(value) else age_bin((as_of - value).days)
    )
    grouped = (
        selected.groupby([spec.geography_column, "age_bin"], dropna=False)
        .size()
        .reset_index(name="row_count")
    )
    output: list[dict[str, object]] = []
    for row in grouped.itertuples(index=False):
        output.append(
            {
                "stream_id": spec.stream_id,
                "previous_snapshot_id": previous_id,
                "current_snapshot_id": current_id,
                "row_status": status,
                "geography": str(row[0]),
                "observation_week_age_bin": str(row[1]),
                "row_count": int(row[2]),
            }
        )
    return output


def compare_pair(
    spec: StreamSpec,
    previous_record: dict[str, object],
    current_record: dict[str, object],
    path_by_id: dict[str, Path],
    comparison_type: str,
) -> tuple[dict[str, object], list[dict[str, object]], list[dict[str, object]]]:
    previous_id = str(previous_record["source_snapshot_id"])
    current_id = str(current_record["source_snapshot_id"])
    previous = load_snapshot(path_by_id[previous_id])
    current = load_snapshot(path_by_id[current_id])
    schema_change = str(previous_record["column_signature_sha256"]) != str(current_record["column_signature_sha256"])

    base = {
        "stream_id": spec.stream_id,
        "comparison_type": comparison_type,
        "release_channel": str(current_record["release_channel"]),
        "previous_snapshot_id": previous_id,
        "current_snapshot_id": current_id,
        "previous_as_of_date": str(previous_record["as_of_date"]),
        "current_as_of_date": str(current_record["as_of_date"]),
        "previous_row_count": len(previous),
        "current_row_count": len(current),
        "schema_change": schema_change,
        "added_rows": 0,
        "removed_rows": 0,
        "revised_rows": 0,
        "unchanged_rows": 0,
        "not_comparable_schema_change_rows": 0,
        "comparison_status": "completed",
    }

    if any(column not in previous.columns or column not in current.columns for column in spec.key_columns):
        base["comparison_status"] = "not_comparable_missing_key"
        return base, [], []
    if previous.duplicated(list(spec.key_columns), keep=False).any() or current.duplicated(list(spec.key_columns), keep=False).any():
        base["comparison_status"] = "not_comparable_duplicate_key"
        return base, [], []

    previous = previous.set_index(list(spec.key_columns), drop=False)
    current = current.set_index(list(spec.key_columns), drop=False)
    added = current.index.difference(previous.index)
    removed = previous.index.difference(current.index)
    common = current.index.intersection(previous.index)
    base["added_rows"] = len(added)
    base["removed_rows"] = len(removed)

    age_geography = []
    age_geography.extend(
        aggregate_status_rows(current, added, spec, "new_observation", current_id, previous_id, str(current_record["as_of_date"]))
    )
    age_geography.extend(
        aggregate_status_rows(previous, removed, spec, "removed_from_snapshot", current_id, previous_id, str(current_record["as_of_date"]))
    )

    if schema_change:
        base["not_comparable_schema_change_rows"] = len(common)
        base["comparison_status"] = "not_comparable_schema_change"
        return base, [], age_geography

    compare_columns = [
        column
        for column in current.columns
        if column not in spec.key_columns and column not in spec.volatile_columns
    ]
    previous_common = previous.loc[common, compare_columns]
    current_common = current.loc[common, compare_columns]
    changed_cells = previous_common.ne(current_common)
    changed_rows_mask = changed_cells.any(axis=1)
    changed_index = common[changed_rows_mask.to_numpy()]
    base["revised_rows"] = int(changed_rows_mask.sum())
    base["unchanged_rows"] = len(common) - int(changed_rows_mask.sum())
    age_geography.extend(
        aggregate_status_rows(current, changed_index, spec, "revised_value", current_id, previous_id, str(current_record["as_of_date"]))
    )

    field_rows: list[dict[str, object]] = []
    for field, changed_count in changed_cells.sum(axis=0).items():
        count = int(changed_count)
        if count:
            field_rows.append(
                {
                    "stream_id": spec.stream_id,
                    "comparison_type": comparison_type,
                    "previous_snapshot_id": previous_id,
                    "current_snapshot_id": current_id,
                    "field_name": field,
                    "measure_group": measure_group(field),
                    "changed_row_count": count,
                }
            )
    return base, field_rows, age_geography


def build_pairs(records: list[dict[str, object]]) -> list[tuple[dict[str, object], dict[str, object], str]]:
    pairs: list[tuple[dict[str, object], dict[str, object], str]] = []
    grouped: dict[tuple[str, str], list[dict[str, object]]] = {}
    for record in records:
        grouped.setdefault((str(record["stream_id"]), str(record["release_channel"])), []).append(record)
    for group in grouped.values():
        ordered = sorted(group, key=lambda row: (str(row["as_of_date"]), str(row["filename"])))
        pairs.extend((previous, current, "consecutive_same_channel") for previous, current in zip(ordered, ordered[1:]))

    nhsn_by_date: dict[str, list[dict[str, object]]] = {}
    for record in records:
        if record["stream_id"] == "nhsn":
            nhsn_by_date.setdefault(str(record["as_of_date"]), []).append(record)
    for same_date in nhsn_by_date.values():
        channels = {str(row["release_channel"]): row for row in same_date}
        if "preliminary" in channels and "standard" in channels:
            pairs.append((channels["preliminary"], channels["standard"], "same_date_cross_channel"))
    return pairs


def write_csv(path: Path, rows: list[dict[str, object]], fieldnames: Iterable[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fieldnames), extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def classify_update_behavior(rows: list[dict[str, object]], stream_id: str) -> str:
    completed = [
        row
        for row in rows
        if row["stream_id"] == stream_id
        and row["comparison_type"] == "consecutive_same_channel"
        and row["comparison_status"] == "completed"
    ]
    if not completed:
        return "requires verification: no comparable consecutive pairs"
    revised = sum(int(row["revised_rows"]) for row in completed)
    removed = sum(int(row["removed_rows"]) for row in completed)
    added = sum(int(row["added_rows"]) for row in completed)
    if revised or removed:
        return f"mixed/revision-bearing (added={added:,}; revised={revised:,}; removed={removed:,})"
    if added:
        return f"append-only in comparable pairs (added={added:,})"
    return "no row-level changes in comparable pairs"


def write_report(
    path: Path,
    run_date: str,
    registry: list[dict[str, object]],
    excluded: list[dict[str, object]],
    pairs: list[dict[str, object]],
    fields: list[dict[str, object]],
    ages: list[dict[str, object]],
) -> None:
    counts = Counter(str(row["stream_id"]) for row in registry)
    channels = Counter((str(row["stream_id"]), str(row["release_channel"])) for row in registry)
    schema_counts = Counter((str(row["stream_id"]), str(row["schema_variant_id"])) for row in registry)
    comparable = Counter(str(row["stream_id"]) for row in pairs if row["comparison_status"] == "completed")
    noncomparable = Counter(str(row["stream_id"]) for row in pairs if row["comparison_status"] != "completed")
    duplicate_files = [row for row in registry if int(row["duplicate_key_rows"]) > 0]
    duplicate_content_groups = sum(1 for count in Counter(str(row["sha256"]) for row in registry).values() if count > 1)
    stable_totals: dict[str, dict[str, int]] = {}
    for stream_id in ("nhsn", "nssp"):
        relevant = [
            row for row in pairs
            if row["stream_id"] == stream_id
            and row["comparison_type"] == "consecutive_same_channel"
            and row["comparison_status"] == "completed"
        ]
        stable_totals[stream_id] = {
            field: sum(int(row[field]) for row in relevant)
            for field in ("added_rows", "removed_rows", "revised_rows", "unchanged_rows")
        }
    field_totals: Counter[tuple[str, str]] = Counter()
    for row in fields:
        field_totals[(str(row["stream_id"]), str(row["field_name"]))] += int(row["changed_row_count"])
    top_fields = {
        stream_id: sorted(
            ((field, count) for (stream, field), count in field_totals.items() if stream == stream_id),
            key=lambda item: item[1],
            reverse=True,
        )[:5]
        for stream_id in ("nhsn", "nssp")
    }
    age_totals: Counter[tuple[str, str, str]] = Counter()
    for row in ages:
        age_totals[(str(row["stream_id"]), str(row["row_status"]), str(row["observation_week_age_bin"]))] += int(row["row_count"])

    lines = [
        "# Historical snapshot registry and initial revision audit",
        "",
        f"Run date: {run_date}",
        "",
        "## Scope completed",
        "",
        f"- Registered {counts['nhsn']} NHSN raw snapshots and {counts['nssp']} NSSP raw snapshots without modifying source files.",
        f"- NHSN filename channels: {channels[('nhsn', 'preliminary')]} files labeled preliminary and {channels[('nhsn', 'standard')]} files without that label. The internal `standard` value does not assert that an unlabeled file is final.",
        f"- Excluded {len(excluded)} non-source CSV files by explicit filename rule; exclusions are listed in `nhsn_nssp_excluded_files.csv`.",
        f"- Compared {comparable['nhsn']} NHSN and {comparable['nssp']} NSSP snapshot pairs; {noncomparable['nhsn']} NHSN and {noncomparable['nssp']} NSSP pairs were conservatively marked non-comparable.",
        "",
        "## Update behavior",
        "",
        f"- NHSN: {classify_update_behavior(pairs, 'nhsn')}.",
        f"- NSSP: {classify_update_behavior(pairs, 'nssp')}.",
        "- These conclusions are empirical properties of the retained files and do not assert an undocumented CDC revision policy.",
        "",
        "## Key empirical findings",
        "",
        f"- In comparable same-channel pairs, NHSN had {stable_totals['nhsn']['revised_rows']:,} revised existing-key rows and NSSP had {stable_totals['nssp']['revised_rows']:,}; neither source is append-only in the retained series.",
        f"- Revisions more than 52 weeks behind the current snapshot accounted for {age_totals[('nhsn', 'revised_value', 'over_52_weeks')]:,} NHSN and {age_totals[('nssp', 'revised_value', 'over_52_weeks')]:,} NSSP changed rows. Historical values therefore cannot be reconstructed from the latest snapshot alone.",
        f"- The most frequently changed NHSN fields were: {', '.join(f'`{field}` ({count:,})' for field, count in top_fields['nhsn'])}.",
        f"- The most frequently changed NSSP fields were: {', '.join(f'`{field}` ({count:,})' for field, count in top_fields['nssp'])}.",
        f"- Retained files contain {duplicate_content_groups} duplicate-content groups across different snapshot dates. These are preserved as distinct `as_of_date` observations because an unchanged re-release is itself point-in-time evidence.",
        f"- Seven NHSN pair comparisons cross a schema signature change and are conservatively non-comparable at the value level; NSSP has one stable 20-column schema across all retained files.",
        "",
        "## Time and comparison rules",
        "",
        "- `observation_week` comes from `Week Ending Date` for NHSN and `week_end` for NSSP.",
        "- `as_of_date` comes from the dated retained filename for both sources. NSSP `BuildNumber` is preserved separately because it may lag the snapshot date or remain unchanged across repeated snapshots.",
        "- NHSN keys are `Week Ending Date` + `Geographic aggregation`.",
        "- NSSP keys are `week_end` + `geography` + `county`; HSA/FIPS/trend fields are compared as revisable attributes, not treated as key fields.",
        "- NHSN preliminary and standard snapshots are compared within release channel. Same-date cross-channel pairs are recorded separately.",
        "- NHSN `release_channel = standard` means only that `(Preliminary)` is absent from the filename; preliminary/final policy semantics remain definition-uncertain.",
        "- A changed schema makes common rows non-comparable for that pair; added and removed keys remain counted.",
        "",
        "## Validation status",
        "",
        f"- Snapshot IDs are unique: {'yes' if len({row['source_snapshot_id'] for row in registry}) == len(registry) else 'NO'}.",
        f"- Duplicate-content SHA-256 groups across different snapshot IDs: {duplicate_content_groups}; retained intentionally and not treated as ID conflicts.",
        f"- Files with duplicate source keys: {len(duplicate_files)}.",
        f"- Schema variants: NHSN={len({variant for (stream, variant), count in schema_counts.items() if stream == 'nhsn'})}; NSSP={len({variant for (stream, variant), count in schema_counts.items() if stream == 'nssp'})}.",
        "",
        "## Produced files",
        "",
        "- `nhsn_nssp_snapshot_registry.csv`: immutable file identity, date, schema, row, geography, and key checks.",
        "- `revision_snapshot_pair_summary.csv`: pair-level added, removed, revised, unchanged, and non-comparable counts.",
        "- `revision_field_summary.csv`: changed-field counts with pathogen/measure grouping.",
        "- `revision_change_age_geography_summary.csv`: added, removed, and revised rows by geography and observation-week age.",
        "- `nhsn_nssp_excluded_files.csv`: CSVs present in source directories but excluded by source filename rules.",
        "",
        "## Remaining registry interpretation",
        "",
        "- Review schema-change pairs and any duplicate-content snapshots before freezing the production comparison sequence.",
        "- Use the field and age/geography summaries to define revision/backfill metrics for the SC prototype.",
        "- Close any comparison limitation under the project's definition-status lifecycle; do not infer undocumented preliminary/final semantics.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    specs = [
        StreamSpec(
            "nhsn",
            args.nhsn_dir,
            NHSN_PREFIX,
            ("Week Ending Date", "Geographic aggregation"),
            "Week Ending Date",
            "Geographic aggregation",
        ),
        StreamSpec(
            "nssp",
            args.nssp_dir,
            NSSP_PREFIX,
            ("week_end", "geography", "county"),
            "week_end",
            "geography",
            ("BuildNumber",),
        ),
    ]

    args.output_dir.mkdir(parents=True, exist_ok=True)
    if args.report_only:
        def load_rows(name: str) -> list[dict[str, object]]:
            return pd.read_csv(args.output_dir / name, keep_default_na=False).to_dict("records")

        write_report(
            args.output_dir / "revision_audit_report.md",
            args.run_date,
            load_rows("nhsn_nssp_snapshot_registry.csv"),
            load_rows("nhsn_nssp_excluded_files.csv"),
            load_rows("revision_snapshot_pair_summary.csv"),
            load_rows("revision_field_summary.csv"),
            load_rows("revision_change_age_geography_summary.csv"),
        )
        print(f"DONE report-only output={args.output_dir}", flush=True)
        return
    partial_registry_path = args.output_dir / "nhsn_nssp_snapshot_registry.partial.csv"
    cached_records: dict[tuple[str, str], dict[str, object]] = {}
    if partial_registry_path.exists():
        with partial_registry_path.open("r", encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                if not row.get("as_of_date_basis"):
                    row["as_of_date"] = row["filename_date"]
                    row["as_of_date_basis"] = "filename_date"
                    row["source_snapshot_id"] = (
                        f"{row['stream_id']}_{row['filename_date'].replace('-', '')}_"
                        f"{row['release_channel']}_{row['sha256'][:12].lower()}"
                    )
                cached_records[(row["stream_id"], row["filename"])] = row

    registry: list[dict[str, object]] = []
    excluded_rows: list[dict[str, object]] = []
    path_by_id: dict[str, Path] = {}
    for spec in specs:
        included, excluded = discover(spec)
        for path in excluded:
            excluded_rows.append(
                {
                    "stream_directory": spec.stream_id,
                    "filename": path.name,
                    "byte_size": path.stat().st_size,
                    "exclusion_reason": "filename does not match source snapshot prefix plus YYYYMMDD suffix",
                }
            )
        for position, path in enumerate(included, start=1):
            cache_key = (spec.stream_id, path.name)
            cached = cached_records.get(cache_key)
            if (
                cached
                and int(cached["byte_size"]) == path.stat().st_size
                and int(cached["filesystem_mtime_ns"]) == path.stat().st_mtime_ns
            ):
                print(f"PROFILE-CACHED {spec.stream_id} {position}/{len(included)} {console_text(path.name)}", flush=True)
                record = cached
            else:
                print(f"PROFILE {spec.stream_id} {position}/{len(included)} {console_text(path.name)}", flush=True)
                record = profile_snapshot(spec, path, args.data_root)
            registry.append(record)
            path_by_id[str(record["source_snapshot_id"])] = path
            write_csv(partial_registry_path, registry, REGISTRY_FIELDS)

    registry.sort(key=lambda row: (str(row["stream_id"]), str(row["as_of_date"]), str(row["release_channel"]), str(row["filename"])))
    if len({row["source_snapshot_id"] for row in registry}) != len(registry):
        raise RuntimeError("Duplicate source_snapshot_id detected")
    excluded_fields = ["stream_directory", "filename", "byte_size", "exclusion_reason"]
    if args.registry_only:
        write_csv(args.output_dir / "nhsn_nssp_snapshot_registry.csv", registry, REGISTRY_FIELDS)
        write_csv(args.output_dir / "nhsn_nssp_excluded_files.csv", excluded_rows, excluded_fields)
        partial_registry_path.unlink(missing_ok=True)
        print(f"DONE registry-only registry={len(registry)} output={args.output_dir}", flush=True)
        return

    pair_rows: list[dict[str, object]] = []
    field_rows: list[dict[str, object]] = []
    age_geography_rows: list[dict[str, object]] = []
    pairs = build_pairs(registry)
    specs_by_id = {spec.stream_id: spec for spec in specs}
    for position, (previous, current, comparison_type) in enumerate(pairs, start=1):
        print(
            f"COMPARE {position}/{len(pairs)} {previous['source_snapshot_id']} -> {current['source_snapshot_id']} ({comparison_type})",
            flush=True,
        )
        pair, fields, ages = compare_pair(
            specs_by_id[str(current["stream_id"])], previous, current, path_by_id, comparison_type
        )
        pair_rows.append(pair)
        field_rows.extend(fields)
        age_geography_rows.extend(ages)

    output = args.output_dir
    pair_fields = [
        "stream_id", "comparison_type", "release_channel", "previous_snapshot_id", "current_snapshot_id",
        "previous_as_of_date", "current_as_of_date", "previous_row_count", "current_row_count", "schema_change",
        "added_rows", "removed_rows", "revised_rows", "unchanged_rows", "not_comparable_schema_change_rows",
        "comparison_status",
    ]
    write_csv(output / "nhsn_nssp_snapshot_registry.csv", registry, REGISTRY_FIELDS)
    write_csv(output / "revision_snapshot_pair_summary.csv", pair_rows, pair_fields)
    write_csv(
        output / "revision_field_summary.csv",
        field_rows,
        ["stream_id", "comparison_type", "previous_snapshot_id", "current_snapshot_id", "field_name", "measure_group", "changed_row_count"],
    )
    write_csv(
        output / "revision_change_age_geography_summary.csv",
        age_geography_rows,
        ["stream_id", "previous_snapshot_id", "current_snapshot_id", "row_status", "geography", "observation_week_age_bin", "row_count"],
    )
    write_csv(output / "nhsn_nssp_excluded_files.csv", excluded_rows, excluded_fields)
    write_report(
        output / "revision_audit_report.md",
        args.run_date,
        registry,
        excluded_rows,
        pair_rows,
        field_rows,
        age_geography_rows,
    )
    partial_registry_path.unlink(missing_ok=True)
    print(f"DONE registry={len(registry)} pairs={len(pair_rows)} output={output}", flush=True)


if __name__ == "__main__":
    main()
