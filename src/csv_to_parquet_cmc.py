#!/usr/bin/env python3
"""
Convert capitalimmobiliercommunes.csv to Parquet for analysis.
Preserves identifier/name columns as strings and lets Pandas infer numerics.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


DEFAULT_INPUT = Path("data/raw/capitalimmobiliercommunes.csv")
DEFAULT_OUTPUT = Path("data/processed/cic_1970_2022.parquet")
STRING_COLUMNS = [
    "dep",
    "nomdep",
    "codecommune",
    "nomcommune",
    "plm",
    "paris",
]
PRIXBIEN_PREFIX = "prixbien"
PRIXM2_PREFIX = "prixm2"


def load_csv(path: Path) -> pd.DataFrame:
    dtype_map = {col: "string" for col in STRING_COLUMNS}
    df = pd.read_csv(path, dtype=dtype_map, low_memory=False)
    return df


def reshape_capitalimmo(df: pd.DataFrame) -> pd.DataFrame:
    prefixes = (PRIXBIEN_PREFIX, PRIXM2_PREFIX)
    cols = [c for c in df.columns if c.startswith(prefixes) and "ratio" not in c]
    if not cols:
        raise ValueError(
            f"No columns found with prefixes {prefixes}."
        )

    long_df = df[STRING_COLUMNS + cols].copy()

    long_df = long_df.melt(
        id_vars=STRING_COLUMNS,
        value_vars=cols,
        var_name="metric_year",
        value_name="value",
    )
    long_df[["metric", "year"]] = long_df["metric_year"].str.extract(
        rf"^({'|'.join(prefixes)})(.+)$"
    )
    long_df = long_df.drop(columns=["metric_year"])
    return long_df


def main() -> None:
    df = load_csv(DEFAULT_INPUT)
    df_long = reshape_capitalimmo(df)
    DEFAULT_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    df_long.to_parquet(DEFAULT_OUTPUT, engine="pyarrow", compression="snappy")
    print(f"Wrote {len(df_long):,} rows to {DEFAULT_OUTPUT}")


if __name__ == "__main__":
    main()
