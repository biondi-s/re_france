from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pandas as pd


def merge_cic_with_communes(
    cic_path: Path | str,
    geojson_path: Path | str,
    *,
    cic_code_col: str = "codecommune",
    geo_code_col: str = "code",
) -> gpd.GeoDataFrame:
    
    cic_df = pd.read_parquet(cic_path)
    communes_gdf = gpd.read_file(geojson_path)

    cic_codes = cic_df[cic_code_col].astype("string")
    geo_codes = communes_gdf[geo_code_col].astype("string")
    cic_df = cic_df.assign(**{cic_code_col: cic_codes})
    communes_gdf = communes_gdf.assign(**{geo_code_col: geo_codes})

    merged = communes_gdf.merge(
        cic_df,
        left_on=geo_code_col,
        right_on=cic_code_col,
        how="left",
    )
    return merged
