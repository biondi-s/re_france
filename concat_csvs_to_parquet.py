#!/usr/bin/env python3
"""
Concatenate the yearly DVF CSVs in `data/` into a single DataFrame, add a
`year` column inferred from the filename (e.g., 2020full.csv -> year=2020),
and write the result to Parquet.
"""

# from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
# import pyarrow as pa
# import pyarrow.parquet as pq


DATA_DIR = Path("data")
OUTPUT_PATH = Path("data/all_years.parquet")
# Expect filenames like 2020full.csv, 2021full.csv, etc.
YEAR_PATTERN = re.compile(r"^(?P<year>\d{4})full\.csv$")
CHUNK_SIZE = 100_000


def find_csvs() -> list[Path]:
    """Return CSV files matching the expected naming convention."""
    return sorted(p for p in DATA_DIR.glob("*full.csv") if YEAR_PATTERN.match(p.name))


def year_from_name(csv_path: Path) -> int:
    match = YEAR_PATTERN.match(csv_path.name)
    if not match:
        raise ValueError(f"Filename does not match expected pattern: {csv_path.name}")
    return int(match.group("year"))


def stream_to_parquet(csv_files: list[Path]) -> int:
    """Stream CSVs into a single Parquet without holding everything in memory."""
    writer: pq.ParquetWriter | None = None
    total_rows = 0

    try:
        for csv_path in csv_files:
            year = year_from_name(csv_path)
            print(f"Loading {csv_path} …")
            for chunk in pd.read_csv(csv_path, dtype=str, chunksize=CHUNK_SIZE):
                chunk["year"] = year
                table = pa.Table.from_pandas(chunk)
                if writer is None:
                    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
                    writer = pq.ParquetWriter(OUTPUT_PATH, table.schema, compression="snappy")
                writer.write_table(table)
                total_rows += len(chunk)
    finally:
        if writer is not None:
            writer.close()

    return total_rows


def main() -> None:
    csv_files = find_csvs()
    if not csv_files:
        raise SystemExit(f"No CSV files found in {DATA_DIR} matching '*full.csv'.")

    total_rows = stream_to_parquet(csv_files)
    print(f"Combined rows written: {total_rows:,}")
    print(f"Wrote Parquet to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
