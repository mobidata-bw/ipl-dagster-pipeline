import logging
import os
import warnings
from typing import Any

import geopandas as gpd
import pandas as pd
from dagster import (
    AutoMaterializePolicy,
    EnvVar,
    ExperimentalWarning,
    FreshnessPolicy,
    asset,
)

from pipeline.transformer.cifs import DatexII2CifsTransformer
from pipeline.util.urllib import download, get

WEB_ROOT = os.getenv('WWW_ROOT_DIR', './tmp/www')
ROADWORKS_DATEX2_DOWNLOAD_URL = os.getenv('ROADWORKS_SVZBW_DATEX2_DOWNLOAD_URL', '')
ROADWORKS_DATEXII_FIILENAME = 'roadworks_svzbw.datex2.xml'
ROADWORKS_ASSET_KEY_PREFIX = ['traffic', 'roadworks']

# In Dagster 1.6.6 AutoMaterializePolicy and shortcut for referencing upstream dependencies without fully qualified
# path are experimental and might break, even between dot-releases. Nevertheless, we ignore the warning, but should
# check when migrating to newer versions
warnings.filterwarnings('ignore', category=ExperimentalWarning)

logger = logging.getLogger(__name__)


@asset(
    compute_kind='DATEX2',
    group_name='traffic',
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60 * 24, cron_schedule='0/5 * * * *'),
    auto_materialize_policy=AutoMaterializePolicy.eager(),
    key_prefix=ROADWORKS_ASSET_KEY_PREFIX,
)
def roadworks_svzbw_datex2() -> None:
    """
    Downloads roadworks from SVZ-BW ad republishes this DATEX2 dataset.
    """
    # Download and republish, if changed
    destination_folder = os.path.join(WEB_ROOT, *ROADWORKS_ASSET_KEY_PREFIX)
    download(ROADWORKS_DATEX2_DOWNLOAD_URL, destination_folder, ROADWORKS_DATEXII_FIILENAME, create_precompressed=True)


@asset(
    # TODO extend here if further roadwork sources are addedd
    non_argument_deps={'roadworks_svzbw_datex2'},
    compute_kind='CIFS',
    group_name='traffic',
    io_manager_key='json_webasset_io_manager',
    auto_materialize_policy=AutoMaterializePolicy.eager(),
    key_prefix=ROADWORKS_ASSET_KEY_PREFIX,
)
def roadworks_cifs() -> dict:
    """
    Transforms roadworks datasets into waze cifs format and publishes them.
    """
    source = os.path.join(WEB_ROOT, *ROADWORKS_ASSET_KEY_PREFIX, ROADWORKS_DATEXII_FIILENAME)
    # TODO extend here if further roadwork sources are addedd
    return DatexII2CifsTransformer('MobiData BW').transform(source, 'cifs')


@asset(
    non_argument_deps={'roadworks_svzbw_datex2'},
    compute_kind='GeoJSON',
    group_name='traffic',
    io_manager_key='json_webasset_io_manager',
    auto_materialize_policy=AutoMaterializePolicy.eager(),
    key_prefix=ROADWORKS_ASSET_KEY_PREFIX,
)
def roadworks_geojson() -> dict:
    """
    Transforms roadworks datasets into GeoJSON (mapping waze cifs to a commonly diggestable format)
    and publishes them.
    Note: these may include roadworks with geometry type point. A point geometry type
    is not recommended as downstream standards like e.g. CIFS can't handle them.
    """
    source = os.path.join(WEB_ROOT, *ROADWORKS_ASSET_KEY_PREFIX, ROADWORKS_DATEXII_FIILENAME)
    return DatexII2CifsTransformer('MobiData BW').transform(source, 'geojson')


@asset(
    compute_kind='PostGIS',
    group_name='traffic',
    io_manager_key='pg_gpd_io_manager',
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def roadworks(roadworks_geojson: dict[str, Any]) -> pd.DataFrame:
    """
    Imports the roadworks into PostGIS, from where they can be accessed e.g. via WMS/WFS.
    Note: only roadworks with geom_type are imported!
    """
    roadworks_gdf = gpd.GeoDataFrame.from_features(roadworks_geojson['features'], crs='epsg:4326')
    roadworks_not_linestrings = roadworks_gdf[roadworks_gdf.geom_type != 'LineString']
    if len(roadworks_not_linestrings) > 0:
        logger.warn(
            f'''Ignored {len(roadworks_not_linestrings)} which had
            geom_type!=LineString, e.g. with id="{roadworks_not_linestrings["id"].iloc[0]}'''
        )
        roadworks_only_linestrings_gdf = roadworks_gdf[roadworks_gdf.geom_type == 'LineString']
        return roadworks_only_linestrings_gdf.set_index('id')
    return roadworks_gdf.set_index('id')
