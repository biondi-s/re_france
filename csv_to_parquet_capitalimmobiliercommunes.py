#!/usr/bin/env python3
"""
Convert capitalimmobiliercommunes.csv to Parquet for analysis.
Preserves identifier/name columns as strings and lets Pandas infer numerics.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


DEFAULT_INPUT = Path("data/capitalimmobiliercommunes.csv")
DEFAULT_OUTPUT = Path("data/cic_1790_2022.parquet")
STRING_COLUMNS = [
    "dep",
    "nomdep",
    "codecommune",
    "nomcommune",
    "plm",
    "paris",
]
CAPITALIMMO_PREFIX = "capitalimmo"


def load_csv(path: Path) -> pd.DataFrame:
    dtype_map = {col: "string" for col in STRING_COLUMNS}
    df = pd.read_csv(path, dtype=dtype_map, low_memory=False)
    return df


def reshape_capitalimmo(df: pd.DataFrame) -> pd.DataFrame:
    ratio_cols = [c for c in df.columns if c.startswith(CAPITALIMMO_PREFIX) and "agglo" not in c]
    if not ratio_cols:
        raise ValueError(
            f"No columns found with prefix '{CAPITALIMMO_PREFIX}'."
        )

    long_df = df[STRING_COLUMNS + ratio_cols].copy()

    long_df = long_df.melt(
        id_vars=STRING_COLUMNS,
        value_vars=ratio_cols,
        var_name="year",
        value_name=CAPITALIMMO_PREFIX,
    )
    long_df["year"] = long_df["year"].str[len(CAPITALIMMO_PREFIX):]
    return long_df


def main() -> None:
    df = load_csv(DEFAULT_INPUT)
    df_long = reshape_capitalimmo(df)
    DEFAULT_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    df_long.to_parquet(DEFAULT_OUTPUT, engine="pyarrow", compression="snappy")
    print(f"Wrote {len(df_long):,} rows to {DEFAULT_OUTPUT}")


if __name__ == "__main__":
    main()
