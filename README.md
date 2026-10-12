# ObsAwareData pipeline

Code that builds [ObsAwareData](https://github.com/DMA-PRIME/ObsAwareData), a weekly,
observation-aware dataset of U.S. respiratory surveillance signals, from archived CDC releases.

It has two parts: the **archive of CDC releases** (below) and the **pipeline** that turns the
archived releases into the published dataset (see [Pipeline](#pipeline)).

## Why an archive

CDC's [data.cdc.gov](https://data.cdc.gov) API serves only the current version of each dataset;
every release replaces the previous one. ObsAwareData publishes how values were revised over time,
so each release has to be saved before the next one replaces it.
[`archive/archive_cdc.py`](archive/archive_cdc.py) runs every day except Sunday and saves every new release of:

| Stream | Channel | Dataset | data.cdc.gov id |
|---|---|---|---|
| `nhsn` | `standard` | Weekly Hospital Respiratory Data (HRD) Metrics by Jurisdiction, National Healthcare Safety Network (NHSN) | [`ua7e-t2fy`](https://data.cdc.gov/d/ua7e-t2fy) |
| `nhsn` | `preliminary` | The same dataset's preliminary release | [`mpgq-jmmr`](https://data.cdc.gov/d/mpgq-jmmr) |
| `nssp` | `standard` | NSSP Emergency Department Visit Trajectories by State and Sub State Regions | [`rdmq-nq56`](https://data.cdc.gov/d/rdmq-nq56) |
| `nwss` | `standard` | CDC Wastewater Viral Activity Level for SARS-CoV-2, Influenza A and RSV | [`atcp-73re`](https://data.cdc.gov/d/atcp-73re) |

A release is detected by the dataset's `rowsUpdatedAt` time. Its full CSV export is stored
byte-for-byte as downloaded, compressed with xz, together with the dataset's metadata (column
definitions and description), as assets of the GitHub release `raw-<YYYY-MM-DD>`: the Friday that
ends the Saturday-to-Friday week of the CDC release (US Eastern time). Before a file is archived,
its record count must equal the API's own row count, and the release time must be unchanged after
the download.

## Registry

[`archive/registry.csv`](archive/registry.csv) lists every archived file, one row each:

| Column | Meaning |
|---|---|
| `stream_id`, `release_channel`, `dataset_id` | Which dataset (see the table above) |
| `rows_updated_at` | CDC release time (`rowsUpdatedAt`, UTC); blank for `box_archive` files |
| `as_of_date` | Release date in US Eastern time |
| `retrieved_at` | When the file was downloaded (UTC; for `box_archive`, the saved file's modification time) |
| `release_tag`, `file`, `metadata_file` | Where the file is: `https://github.com/<this repository>/releases/download/<release_tag>/<file>` |
| `raw_bytes`, `raw_sha256` | Size and SHA-256 of the CSV as downloaded (after decompression) |
| `xz_bytes`, `xz_sha256` | Size and SHA-256 of the stored `.xz` file |
| `row_count`, `column_count` | CSV records (excluding the header) and columns |
| `source` | `cdc_api` for files saved by `archive_cdc.py`; `box_archive` for earlier releases (below) |
| `original_filename` | For `box_archive` files, the dated file name they were saved under |

To use a file: download it, check `xz_sha256`, decompress (`xz -d`), and check `raw_sha256`.

### Releases before the API archive

Releases from April 2025 to early October 2026 were saved at the time by a separate download
process as dated CSV files; [`archive/seed_from_box.py`](archive/seed_from_box.py) added them here
once, unchanged and checked against the SHA-256 recorded when they were saved. Their dates come
from the file names (the day the release was downloaded), and they carry no `rows_updated_at` or
metadata file. Each release is archived once: where both copies existed, the API copy is kept.
Those earlier files were exported in a different CSV style (every field quoted, thousands
separators in numbers) and, for some NHSN values ending in .5, rounded half up where the API export
rounds half to even; the values are otherwise the same.

## Pipeline

| Folder | Contents |
|---|---|
| [`archive/`](archive/) | The archive scripts above, and [`materialize.py`](archive/materialize.py), which rebuilds the raw-data folder the processing step reads (dated CSV files per dataset) from the archive |
| [`processing/`](processing/) | Snapshot registries (`register_*.py`), per-state steps (`extract_*.py`, `check_nssp_facility_coverage.py`, `validate_weekly_schema.py`, `build_*.py`), the multi-state driver [`run_state_pipeline.py`](processing/run_state_pipeline.py), and the one-time cross-state derivations (`audit_national_geography.py`, `derive_state_thresholds.py`) |
| [`publish/`](publish/) | [`build_obsawaredata.py`](publish/build_obsawaredata.py) (processing outputs to the ObsAwareData repository and its release assets) and [`weekly_refresh.py`](publish/weekly_refresh.py) (register, pre-run checks, processing, build, change summary) |
| [`reference/`](reference/) | Fixed inputs: CDC's NSSP coverage map (from https://www.cdc.gov/nssp/documents/coverage-map.csv), the national geography summary, and the per-state thresholds |
| [`tools/`](tools/) | [`fetch_census.py`](tools/fetch_census.py): the Census Bureau files the processing step uses |

Each week the dataset is rebuilt as of the most recent Friday and reviewed before it is published.
Revision history is published as release assets: per state, one full file followed by weekly
increments, listed in the data repository's `metadata/release_assets.csv`.

Running it locally (Python 3.11, `pip install -r requirements.txt`):

```bash
python archive/materialize.py --out data            # raw CDC files from the archive
python tools/fetch_census.py --data-root data       # Census inputs
export OAD_DATA_ROOT=$PWD/data OAD_OUTPUT_ROOT=$PWD/out CENSUS_API_KEY=<key>
git clone https://github.com/DMA-PRIME/ObsAwareData out/ObsAwareData
python publish/weekly_refresh.py [--as-of 2026-10-09] [--states SC]
```

## Data and license

The archived files are CDC data, unmodified; see each dataset's page on data.cdc.gov for its terms
and suggested citation. The code in this repository is released under the [MIT License](LICENSE).
