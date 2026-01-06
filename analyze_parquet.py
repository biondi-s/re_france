#!/usr/bin/env python3
"""
Basic exploratory checks on the consolidated DVF Parquet file.
Prints shape, schema, missingness, key distributions, and simple numeric stats.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


DEFAULT_PATH = Path("data/dvf_2020_2025.parquet")
DEFAULT_SAMPLE = None  # set an int to sample that many rows for quicker runs


def load_data(path: Path, sample: int | None = DEFAULT_SAMPLE) -> pd.DataFrame:
    df = pd.read_parquet(path)
    if sample is not None and len(df) > sample:
        df = df.sample(sample, random_state=0)
        print(f"Sampled {sample:,} rows from {path}")
    else:
        print(f"Loaded {len(df):,} rows from {path}")
    return df


def summarize_missingness(df: pd.DataFrame) -> pd.DataFrame:
    missing = df.isna().sum().sort_values(ascending=False)
    pct = (missing / len(df) * 100).round(2)
    summary = pd.DataFrame({"missing": missing, "missing_pct": pct})
    return summary


def maybe_summarize_categories(df: pd.DataFrame, columns: list[str]) -> None:
    for col in columns:
        if col in df.columns:
            counts = df[col].value_counts(dropna=False).head(10)
            print(f"\nTop values for {col}:")
            print(counts.to_string())


def summarize_numeric(df: pd.DataFrame) -> pd.DataFrame:
    numeric_cols = df.select_dtypes(include=["number"]).columns
    if len(numeric_cols) == 0:
        return pd.DataFrame()
    return df[numeric_cols].describe(percentiles=[0.25, 0.5, 0.75, 0.9, 0.99]).T


def summarize_price_per_m2(df: pd.DataFrame) -> pd.DataFrame | None:
    price_col_candidates = ["valeur_fonciere", "prix"]
    area_col_candidates = ["surface_reelle_bati", "surface", "surface_terrain"]

    price_col = next((c for c in price_col_candidates if c in df.columns), None)
    area_col = next((c for c in area_col_candidates if c in df.columns), None)
    if not price_col or not area_col:
        return None

    price = pd.to_numeric(df[price_col], errors="coerce")
    area = pd.to_numeric(df[area_col], errors="coerce")
    price_per_m2 = price / area
    return price_per_m2.describe(percentiles=[0.25, 0.5, 0.75, 0.9, 0.99])


def main() -> None:
    parser = argparse.ArgumentParser(description="Run quick EDA on dvf_2020_2025.parquet")
    parser.add_argument("--path", type=Path, default=DEFAULT_PATH, help="Parquet file to analyze")
    parser.add_argument("--sample", type=int, default=DEFAULT_SAMPLE, help="Optional row sample for speed")
    args = parser.parse_args()

    df = load_data(args.path, sample=args.sample)

    print("\nShape and columns")
    print(f"Rows: {len(df):,}, Columns: {df.shape[1]}")
    print(df.columns.to_list())

    print("\nDtypes")
    print(df.dtypes)

    if "year" in df.columns:
        print("\nTransactions by year")
        print(df["year"].value_counts().sort_index())

    print("\nMissingness (top 10)")
    missing_summary = summarize_missingness(df).head(10)
    print(missing_summary.to_string())

    print("\nNumeric summary")
    numeric_summary = summarize_numeric(df)
    if numeric_summary.empty:
        print("No numeric columns found.")
    else:
        print(numeric_summary)

    maybe_summarize_categories(
        df,
        ["nature_mutation", "type_local", "code_departement", "code_commune", "code_postal"],
    )

    ppm2 = summarize_price_per_m2(df)
    if ppm2 is not None:
        print("\nPrice per m² summary")
        print(ppm2)


if __name__ == "__main__":
    main()
