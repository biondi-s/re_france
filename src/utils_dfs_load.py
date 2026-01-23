from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pandas as pd
import numpy as np


REPO_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_CIC_PATH = REPO_ROOT / "data/processed/cic_1970_2022.parquet"
DEFAULT_COMMUNES_PATH = REPO_ROOT / "data/raw/communes.geojson"
DEFAULT_ARRONDISSEMENTS_PATH = (
    REPO_ROOT / "data/raw/arrondissements_municipaux.geojson"
)
DEFAULT_DVF_PATH = REPO_ROOT / "data/processed/dvf_2020_2025.parquet"

DEFAULT_CIC_CODE = "codecommune"
DEFAULT_COMMUNES_CODE = "code"
DEFAULT_ARRONDISSEMENTS_CODE = "code_insee"


def merge_communes_then_arrondissements_cic(
    cic_path: Path | str = DEFAULT_CIC_PATH,
    communes_geojson_path: Path | str = DEFAULT_COMMUNES_PATH,
    arrondissements_geojson_path: Path | str = DEFAULT_ARRONDISSEMENTS_PATH,
    cic_code_col: str = DEFAULT_CIC_CODE,
    communes_code_col: str = DEFAULT_COMMUNES_CODE,
    arrondissements_code_col: str = DEFAULT_ARRONDISSEMENTS_CODE,
    clean_iqr: bool = False,
    verbose: bool = False
) -> gpd.GeoDataFrame:

    cic_df = pd.read_parquet(cic_path)
    cic_df[cic_code_col] = cic_df[cic_code_col].astype("string")

    communes_gdf = gpd.read_file(communes_geojson_path)
    communes_gdf[communes_code_col] = communes_gdf[communes_code_col].astype(
        "string"
    )
    communes_gdf = communes_gdf.loc[:, [communes_code_col, "geometry"]]

    merged_communes = cic_df.merge(
        communes_gdf,
        left_on=cic_code_col,
        right_on=communes_code_col,
        how="left",
    )

    missing_mask = merged_communes["geometry"].isna()
    matched = merged_communes[~missing_mask]

    if missing_mask.any():

        missing_cic = cic_df.loc[missing_mask].copy()

        arr_gdf = gpd.read_file(arrondissements_geojson_path)
        arr_gdf[arrondissements_code_col] = arr_gdf[
            arrondissements_code_col
        ].astype("string")
        arr_gdf = arr_gdf.loc[:, [arrondissements_code_col, "geometry"]]

        merged_arr = missing_cic.merge(
            arr_gdf,
            left_on=cic_code_col,
            right_on=arrondissements_code_col,
            how="left",
        )

        merged = pd.concat([matched, merged_arr], ignore_index=True)
    else:
        merged = matched

    merged = merged[merged["value"].notna()]

    crs = communes_gdf.crs or (arr_gdf.crs if missing_mask.any() else None)

    if verbose:
        print(
            "Dropping the following communes/arrondissements "
            "with missing geometry:"
        )
        print(merged[merged["geometry"].isna()]["nomcommune"].unique())

    merged = merged[~merged["geometry"].isna()]

    if clean_iqr:

        cleaned = []
        dropped_total = 0
        for metric, df_metric in merged.groupby("metric", dropna=False):
            q1 = df_metric["value"].quantile(0.25)
            q3 = df_metric["value"].quantile(0.75)
            iqr = q3 - q1
            lower = q1 - 2.5 * iqr
            upper = q3 + 2.5 * iqr
            before = len(df_metric)
            df_metric = df_metric[df_metric["value"].between(lower, upper)]
            dropped_total += before - len(df_metric)
            cleaned.append(df_metric)

        if verbose:
            print(
                f"Dropped {dropped_total:,} rows by IQR cleaning, "
                f"{np.round(dropped_total / len(merged), 4)}, of the total")

        merged = pd.concat(cleaned, ignore_index=True)

    # Remove the departments of Corsica (2A, 2B) due to their distance from
    # the rest of the territory and consequent insignificance for the study.
    merged = merged[~merged["dep"].isin(["2A", "2B"])]

    return gpd.GeoDataFrame(merged, geometry="geometry", crs=crs)


def load_and_filter_dvf(
    dvf_path: Path | str = DEFAULT_DVF_PATH,
) -> pd.DataFrame:

    dvf_df = pd.read_parquet(dvf_path)

    required_cols = [
        "date_mutation",
        "valeur_fonciere",
        "surface_reelle_bati",
        "latitude",
        "longitude",
    ]
    df = dvf_df.loc[:, required_cols].copy()

    df = df.assign(
        date_mutation=pd.to_datetime(
            df["date_mutation"],
            errors="coerce"
        ),
        valeur_fonciere=pd.to_numeric(
            df["valeur_fonciere"],
            errors="coerce"
        ),
        surface_reelle_bati=pd.to_numeric(
            df["surface_reelle_bati"],
            errors="coerce"
        ),
    )

    total_len = len(df)

    print(f"Total rows: {total_len/1e6:.1f} mln")

    df = df.dropna(
        subset=[
            "date_mutation",
            "valeur_fonciere",
            "latitude",
            "longitude",
        ]
    )

    df = df[df["valeur_fonciere"] > 0]

    print(
        "After drop_na and price > 0: ",
        f"{len(df)/1e6:.1f} mln\n",
        f"({len(df) / total_len:.2f} of total)"
    )

    df["price_sqm"] = np.where(
        df["surface_reelle_bati"] > 0,
        df["valeur_fonciere"] / df["surface_reelle_bati"],
        np.nan,
    )

    df["log_price"] = np.log(df["valeur_fonciere"])

    df["log_price_sqm"] = np.log(df["price_sqm"])

    return df
