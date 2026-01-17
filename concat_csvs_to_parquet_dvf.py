#!/usr/bin/env python3
"""
Concatenate the yearly DVF CSVs in `data/` into a single DataFrame, add a
`year` column inferred from the filename (e.g., 2020full.csv -> year=2020),
and write the result to Parquet.
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd


DATA_DIR = Path("data")
OUTPUT_PATH = Path("data/dvf_2020_2025.parquet")
# Expect filenames like 2020full.csv, 2021full.csv, etc.
YEAR_PATTERN = re.compile(r"^(?P<year>\d{4})full\.csv$")


def find_csvs() -> list[Path]:
    """Return CSV files matching the expected naming convention."""
    return sorted(
        p for p in DATA_DIR.glob("*full.csv") if YEAR_PATTERN.match(p.name)
    )


def year_from_name(csv_path: Path) -> int:
    match = YEAR_PATTERN.match(csv_path.name)
    if not match:
        raise ValueError(
            f"Filename does not match expected pattern: {csv_path.name}"
        )
    return int(match.group("year"))


def stream_to_parquet(csv_files: list[Path]) -> int:
    """Load each CSV into a DataFrame, concatenate,
    and write a single Parquet."""
    frames: list[pd.DataFrame] = []
    for csv_path in csv_files:
        year = year_from_name(csv_path)
        print(f"Loading {csv_path} …")
        df = pd.read_csv(csv_path, dtype=str, sep=",")
        df["year"] = year
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(OUTPUT_PATH, engine="pyarrow", compression="snappy")
    return len(combined)


def main() -> None:
    csv_files = find_csvs()
    if not csv_files:
        raise SystemExit(
            f"No CSV files found in {DATA_DIR} matching '*full.csv'."
        )

    total_rows = stream_to_parquet(csv_files)
    print(f"Combined rows written: {total_rows:,}")
    print(f"Wrote Parquet to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
