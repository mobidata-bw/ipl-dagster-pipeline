# Copyright 2023 Holger Bruch (hb@mfdz.de)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os
from typing import Any

import geopandas as gpd
import pandas as pd
from dagster import AutomationCondition, asset

from pipeline.transformer.cifs import DatexII2CifsTransformer
from pipeline.util.urllib import download

WEB_ROOT = os.getenv('WWW_ROOT_DIR', './tmp/www')
ROADWORKS_DATEX2_DOWNLOAD_URL = os.getenv('ROADWORKS_SVZBW_DATEX2_DOWNLOAD_URL', '')
ROADWORKS_DATEXII_FIILENAME = 'roadworks_svzbw.datex2.xml'
ROADWORKS_ASSET_KEY_PREFIX = ['traffic', 'roadworks']

logger = logging.getLogger(__name__)


@asset(
    compute_kind='DATEX2',
    group_name='traffic',
    automation_condition=(
        AutomationCondition.on_cron('0/5 * * * *') & ~AutomationCondition.in_progress() | AutomationCondition.eager()
    ),
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
    Note: these may include roadworks with geometry type point. A point geometry type
    is not recommended as downstream standards like e.g. CIFS can't handle them.
    """
    source = os.path.join(WEB_ROOT, *ROADWORKS_ASSET_KEY_PREFIX, ROADWORKS_DATEXII_FIILENAME)
    return DatexII2CifsTransformer('MobiData BW').transform(source, 'geojson')


@asset(
    compute_kind='PostGIS',
    group_name='traffic',
    io_manager_key='pg_gpd_io_manager',
    automation_condition=AutomationCondition.eager(),
)
def roadworks(roadworks_geojson: dict[str, Any]) -> pd.DataFrame:
    """
    Imports the roadworks into PostGIS, from where they can be accessed e.g. via WMS/WFS.
    Note: only roadworks with geom_type are imported!
    """
    roadworks_gdf = gpd.GeoDataFrame.from_features(roadworks_geojson['features'], crs='epsg:4326')
    roadworks_not_linestrings = roadworks_gdf[roadworks_gdf.geom_type != 'LineString']
    if len(roadworks_not_linestrings) > 0:
        logger.warning(
            f'Ignored {len(roadworks_not_linestrings)} which had'
            'geom_type!=LineString, e.g. with id="{roadworks_not_linestrings["id"].iloc[0]}'
        )
        roadworks_only_linestrings_gdf = roadworks_gdf[roadworks_gdf.geom_type == 'LineString']
        return roadworks_only_linestrings_gdf.set_index('id')
    return roadworks_gdf.set_index('id')
