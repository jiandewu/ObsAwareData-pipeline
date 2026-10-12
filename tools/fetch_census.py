"""Download the fixed Census Bureau inputs of the processing step into <data root>/Census Data/.

The same files and URLs as processing/extract_census_population.py expects; files already present are kept.

Usage:
    python tools/fetch_census.py --data-root <dir>
"""
from __future__ import annotations

import argparse
import time
import urllib.request
from pathlib import Path

FILES = {
    "Population Estimates/NST-EST2022-ALLDATA.csv":
        "https://www2.census.gov/programs-surveys/popest/datasets/2020-2022/state/totals/NST-EST2022-ALLDATA.csv",
    "Population Estimates/co-est2022-alldata.csv":
        "https://www2.census.gov/programs-surveys/popest/datasets/2020-2022/counties/totals/co-est2022-alldata.csv",
    "ZCTA Relationship Files 2020/tab20_zcta520_county20_natl.txt":
        "https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/tab20_zcta520_county20_natl.txt",
    "ZCTA Relationship Files 2020/tab20_zcta520_tract20_natl.txt":
        "https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/tab20_zcta520_tract20_natl.txt",
    "TIGER-Line 2022/tl_2022_us_state.zip": "https://www2.census.gov/geo/tiger/TIGER2022/STATE/tl_2022_us_state.zip",
    "TIGER-Line 2022/tl_2022_us_county.zip": "https://www2.census.gov/geo/tiger/TIGER2022/COUNTY/tl_2022_us_county.zip",
    "TIGER-Line 2022/tl_2022_us_zcta520.zip": "https://www2.census.gov/geo/tiger/TIGER2022/ZCTA520/tl_2022_us_zcta520.zip",
}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--data-root", required=True, type=Path)
    args = p.parse_args()
    for rel, url in FILES.items():
        dest = args.data_root / "Census Data" / rel
        if dest.exists() and dest.stat().st_size > 0:
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        for attempt in range(1, 6):
            try:
                urllib.request.urlretrieve(url, dest.with_suffix(dest.suffix + ".part"))
                dest.with_suffix(dest.suffix + ".part").replace(dest)
                break
            except Exception as e:  # noqa: BLE001 - network errors of any kind are retried
                if attempt == 5:
                    raise SystemExit(f"{url}: {e}")
                print(f"retry {attempt}/4 for {rel}: {e}", flush=True)
                time.sleep(60 * attempt)
        print(f"{rel}: {dest.stat().st_size / 1e6:.1f} MB", flush=True)


if __name__ == "__main__":
    main()
