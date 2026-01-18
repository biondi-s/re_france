from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_CIC_PATH = REPO_ROOT / "data/processed/cic_1970_2022.parquet"
DEFAULT_COMMUNES_PATH = REPO_ROOT / "data/raw/communes.geojson"
DEFAULT_ARRONDISSEMENTS_PATH = (
    REPO_ROOT / "data/raw/arrondissements_municipaux.geojson"
)


def merge_cic_communes_then_arrondissements(
    cic_path: Path | str = DEFAULT_CIC_PATH,
    communes_geojson_path: Path | str = DEFAULT_COMMUNES_PATH,
    arrondissements_geojson_path: Path | str = DEFAULT_ARRONDISSEMENTS_PATH,
    cic_code_col: str = "codecommune",
    communes_code_col: str = "code",
    arrondissements_code_col: str = "code_insee",
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

    return gpd.GeoDataFrame(merged, geometry="geometry", crs=crs)
