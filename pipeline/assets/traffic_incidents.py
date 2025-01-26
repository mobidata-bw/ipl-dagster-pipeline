import os
import warnings

import geopandas as gpd
import pandas as pd
from dagster import (
    AutomationCondition,
    ExperimentalWarning,
    asset,
)

from pipeline.transformer.cifs import DatexII2CifsTransformer
from pipeline.util.urllib import download

WEB_ROOT = os.getenv('WWW_ROOT_DIR', './tmp/www')
ROADWORKS_DATEX2_DOWNLOAD_URL = os.getenv('ROADWORKS_SVZBW_DATEX2_DOWNLOAD_URL', '')
ROADWORKS_DATEXII_FIILENAME = 'roadworks_svzbw.datex2.xml'
ROADWORKS_ASSET_KEY_PREFIX = ['traffic', 'roadworks']

warnings.filterwarnings('ignore', category=ExperimentalWarning)


@asset(
    compute_kind='DATEX2',
    group_name='traffic',
    automation_condition=(AutomationCondition.on_cron('0/5 * * * *') & ~AutomationCondition.in_progress() | AutomationCondition.eager()),
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
    deps={'roadworks_svzbw_datex2'},
    compute_kind='CIFS',
    group_name='traffic',
    io_manager_key='json_webasset_io_manager',
    automation_condition=AutomationCondition.eager(),
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
    deps={'roadworks_svzbw_datex2'},
    compute_kind='GeoJSON',
    group_name='traffic',
    io_manager_key='json_webasset_io_manager',
    automation_condition=AutomationCondition.eager(),
    key_prefix=ROADWORKS_ASSET_KEY_PREFIX,
)
def roadworks_geojson() -> dict:
    """
    Transforms roadworks datasets into GeoJSON (mapping waze cifs to a commonly diggestable format)
    and publishes them.
    """
    source = os.path.join(WEB_ROOT, *ROADWORKS_ASSET_KEY_PREFIX, ROADWORKS_DATEXII_FIILENAME)
    return DatexII2CifsTransformer('MobiData BW').transform(source, 'geojson')


@asset(
    compute_kind='PostGIS',
    group_name='traffic',
    io_manager_key='pg_gpd_io_manager',
    automation_condition=AutomationCondition.eager(),
)
def roadworks(roadworks_geojson) -> pd.DataFrame:
    """
    Imports the roadworks into PostGIS, from where they can be accessed e.g. via WMS/WFS.
    """
    return gpd.GeoDataFrame.from_features(roadworks_geojson['features'], crs='epsg:4326')
