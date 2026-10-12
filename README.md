# ObsAwareData pipeline

Code that builds [ObsAwareData](https://github.com/DMA-PRIME/ObsAwareData), a weekly,
observation-aware dataset of U.S. respiratory surveillance signals, from archived CDC releases.

The first part in this repository is the **archive of CDC releases**. The ETL and repository-build
code will be added here later.

## Why an archive

CDC's [data.cdc.gov](https://data.cdc.gov) API serves only the current version of each dataset;
every release replaces the previous one. ObsAwareData publishes how values were revised over time,
so each release has to be saved before the next one replaces it. A daily GitHub Actions workflow
([`.github/workflows/archive.yml`](.github/workflows/archive.yml)) runs
[`archive/archive_cdc.py`](archive/archive_cdc.py), which saves every new release of:

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
| `rows_updated_at` | CDC release time (`rowsUpdatedAt`, UTC) |
| `as_of_date` | Release date in US Eastern time |
| `retrieved_at` | When the file was downloaded (UTC) |
| `release_tag`, `file`, `metadata_file` | Where the file is: `https://github.com/<this repository>/releases/download/<release_tag>/<file>` |
| `raw_bytes`, `raw_sha256` | Size and SHA-256 of the CSV as downloaded (after decompression) |
| `xz_bytes`, `xz_sha256` | Size and SHA-256 of the stored `.xz` file |
| `row_count`, `column_count` | CSV records (excluding the header) and columns |
| `source` | `cdc_api` for files archived by this workflow |

To use a file: download it, check `xz_sha256`, decompress (`xz -d`), and check `raw_sha256`.

## Data and license

The archived files are CDC data, unmodified; see each dataset's page on data.cdc.gov for its terms
and suggested citation. The code in this repository is released under the [MIT License](LICENSE).
