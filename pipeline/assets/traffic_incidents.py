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
import tempfile
from pathlib import Path
from typing import Any

import datex2_xml2json.merge as datex2_merge
import datex2_xml2json.transformer as datex2_transformer
import geopandas as gpd
import pandas as pd
from dagster import AutomationCondition, asset

from pipeline.transformer.cifs import DatexII2CifsTransformer
from pipeline.util.urllib import download

WEB_ROOT = os.getenv('WWW_ROOT_DIR', './tmp/www')
GEOPARQUET_TEMP_DIR = os.getenv('GEOPARQUET_TEMP_DIR', './tmp/geoparquet/traffic')
IPL_MOBILITHEK_CERT_PATH = os.getenv('IPL_MOBILITHEK_CERT_PATH', '')
IPL_MOBILITHEK_KEY_PATH = os.getenv('IPL_MOBILITHEK_KEY_PATH', '')

ROADWORKS_SVZBW_DATEX2_DOWNLOAD_URL = os.getenv('ROADWORKS_SVZBW_DATEX2_DOWNLOAD_URL', '')
ROADWORKS_ADB_SHORT_TERM_DATEX2_DOWNLOAD_URL = os.getenv('IPL_ROADWORKS_ADB_SHORT_TERM_DATEX2_DOWNLOAD_URL', '')
ROADWORKS_ADB_LONG_TERM_DATEX2_DOWNLOAD_URL = os.getenv('IPL_ROADWORKS_ADB_LONG_TERM_DATEX2_DOWNLOAD_URL', '')
INCIDENTS_LMSBW_DATEX2_DOWNLOAD_URL = os.getenv('IPL_INCIDENTS_LMSBW_DATEX2_DOWNLOAD_URL', '')

ROADWORKS_ADB_SHORT_TERM_DATEX2_FIILENAME = 'roadworks_adb_short_term.datex2.xml'
ROADWORKS_ADB_LONG_TERM_DATEX2_FIILENAME = 'roadworks_adb_long_term.datex2.xml'
ROADWORKS_SVZBW_DATEX2_FILENAME = 'roadworks_svzbw.datex2.xml'
INCIDENTS_LMSBW_DATEX2_FIILENAME = 'incidents_lmsbw.datex2.xml'
# Resulting file is named roadworks_and_events_bw, which will shortly be added in a follow up PR
ROADWORKS_AND_EVENTS_PARQUET_FILENAME = 'roadworks_and_events_bw.parquet'

# Source identification, custom shortnames we use as IDs for our datasources
ROADWORKS_ADB_SHORT_TERM_SOURCE_IDENTIFICATION = 'roadworks_adb_short_term'
ROADWORKS_ADB_LONG_TERM_SOURCE_IDENTIFICATION = 'roadworks_adb_long_term'
ROADWORKS_SVZBW_SOURCE_IDENTIFICATION = 'roadworks_svzbw'

# Publication identifiers, for Mobilithek publications, we use their Mobilithek dataset ID
ROADWORKS_ADB_SHORT_TERM_PUBLICATION_IDENTIFIER = 'https://mobilithek.info/offers/641235133820284928'
ROADWORKS_ADB_LONG_TERM_PUBLICATION_IDENTIFIER = 'https://mobilithek.info/offers/641228542228467712'
ROADWORKS_SVZBW_PUBLICATION_IDENTIFIER = 'https://mobilithek.info/offers/-5342395678156131804'

ROADWORKS_ASSET_KEY_PREFIX = ['traffic', 'roadworks']
INCIDENTS_ASSET_KEY_PREFIX = ['traffic', 'incidents']

logger = logging.getLogger(__name__)


def _datex2_to_geoparquet(datex2_filename: str, source_identification: str, publication_identifier: str) -> None:
    """
    Transforms the published DATEX2 file `datex2_filename` (in the roadworks web folder) into
    `GEOPARQUET_TEMP_DIR/<stem>.parquet`, from where `roadworks_and_events_bw_geoparquet` merges it.

    The parquet file is generated in a private temp dir inside GEOPARQUET_TEMP_DIR (same filesystem, and
    not matched by the merge's `*.parquet` glob) and then atomically moved into place, so a concurrently
    running merge never reads a partially written file.
    """
    source_file = Path(WEB_ROOT, *ROADWORKS_ASSET_KEY_PREFIX, datex2_filename)
    target_dir = Path(GEOPARQUET_TEMP_DIR)
    target_dir.mkdir(parents=True, exist_ok=True)
    # transform() names its output <stem of input file><format suffix>
    parquet_filename = source_file.stem + '.parquet'

    with tempfile.TemporaryDirectory(dir=target_dir, prefix='.tmp-') as tmp_dir:
        datex2_transformer.transform(
            filename=str(source_file),
            outDir=tmp_dir,
            input_format=datex2_transformer.InputFormat.DATEX2_2_2_XML,
            output_format=datex2_transformer.OutputFormat.GEOPARQUET,
            validate=False,
            sourceIdentification=source_identification,
            publicationIdentifier=publication_identifier,
        )
        Path(tmp_dir, parquet_filename).replace(target_dir / parquet_filename)


@asset(
    compute_kind='DATEX2',
    group_name='traffic',
    automation_condition=(
        # every minute, as incidents might be more urgent and important than roadworks
        AutomationCondition.on_cron('* * * * *') & ~AutomationCondition.in_progress() | AutomationCondition.eager()
    ),
    key_prefix=INCIDENTS_ASSET_KEY_PREFIX,
)
def incidents_lmsbw_datex2() -> None:
    """
    Downloads dataset `Verkehrsmeldungsdaten der Landesmeldestelle BW`
    (https://mobilithek.info/offers/857127500689977344)
    from Mobilithek and republishes this DATEX2 dataset.
    """
    # Download and republish, if changed
    destination_folder = os.path.join(WEB_ROOT, *INCIDENTS_ASSET_KEY_PREFIX)
    cert = (
        IPL_MOBILITHEK_CERT_PATH,
        IPL_MOBILITHEK_KEY_PATH,
    )
    download(
        INCIDENTS_LMSBW_DATEX2_DOWNLOAD_URL,
        destination_folder,
        INCIDENTS_LMSBW_DATEX2_FIILENAME,
        cert=cert,
        create_precompressed=True,
    )


@asset(
    compute_kind='DATEX2',
    group_name='traffic',
    automation_condition=(
        # every 5 minutes
        AutomationCondition.on_cron('0/5 * * * *') & ~AutomationCondition.in_progress() | AutomationCondition.eager()
    ),
    key_prefix=ROADWORKS_ASSET_KEY_PREFIX,
)
def roadworks_adb_short_term_datex2() -> None:
    """
    Downloads dataset `Arbeitsstellen kürzerer Dauer (AkD) auf BAB in DE - Planungsdaten`
    (https://mobilithek.info/offers/641235133820284928 )
    from Mobilithek and republishes this DATEX2 dataset.
    """
    # Download and republish, if changed
    destination_folder = os.path.join(WEB_ROOT, *ROADWORKS_ASSET_KEY_PREFIX)
    cert = (
        IPL_MOBILITHEK_CERT_PATH,
        IPL_MOBILITHEK_KEY_PATH,
    )
    download(
        ROADWORKS_ADB_SHORT_TERM_DATEX2_DOWNLOAD_URL,
        destination_folder,
        ROADWORKS_ADB_SHORT_TERM_DATEX2_FIILENAME,
        cert=cert,
        create_precompressed=True,
    )


@asset(
    compute_kind='GeoParquet',
    deps={'roadworks_adb_short_term_datex2'},
    group_name='traffic',
    automation_condition=AutomationCondition.eager(),
    key_prefix=ROADWORKS_ASSET_KEY_PREFIX,
)
def roadworks_adb_short_term_geoparquet() -> None:
    """
    Transforms short term roadworks from AdB to GeoParquet.
    """
    _datex2_to_geoparquet(
        ROADWORKS_ADB_SHORT_TERM_DATEX2_FIILENAME,
        ROADWORKS_ADB_SHORT_TERM_SOURCE_IDENTIFICATION,
        ROADWORKS_ADB_SHORT_TERM_PUBLICATION_IDENTIFIER,
    )


@asset(
    compute_kind='DATEX2',
    group_name='traffic',
    automation_condition=(
        # every 5 minutes
        AutomationCondition.on_cron('0/5 * * * *') & ~AutomationCondition.in_progress() | AutomationCondition.eager()
    ),
    key_prefix=ROADWORKS_ASSET_KEY_PREFIX,
)
def roadworks_adb_long_term_datex2() -> None:
    """
    Downloads dataset `Arbeitsstellen längerer Dauer (AlD) auf BAB in DE - aktuelle und in den nächsten 14 Tagen beginnende`
    (https://mobilithek.info/offers/641228542228467712)
    from Mobilithek and republishes this DATEX2 dataset.
    """
    # Download and republish, if changed
    destination_folder = os.path.join(WEB_ROOT, *ROADWORKS_ASSET_KEY_PREFIX)
    cert = (
        IPL_MOBILITHEK_CERT_PATH,
        IPL_MOBILITHEK_KEY_PATH,
    )
    download(
        ROADWORKS_ADB_LONG_TERM_DATEX2_DOWNLOAD_URL,
        destination_folder,
        ROADWORKS_ADB_LONG_TERM_DATEX2_FIILENAME,
        cert=cert,
        create_precompressed=True,
    )


@asset(
    compute_kind='GeoParquet',
    deps={'roadworks_adb_long_term_datex2'},
    group_name='traffic',
    automation_condition=AutomationCondition.eager(),
    key_prefix=ROADWORKS_ASSET_KEY_PREFIX,
)
def roadworks_adb_long_term_geoparquet() -> None:
    """
    Transforms long term roadworks from AdB to GeoParquet.
    """
    _datex2_to_geoparquet(
        ROADWORKS_ADB_LONG_TERM_DATEX2_FIILENAME,
        ROADWORKS_ADB_LONG_TERM_SOURCE_IDENTIFICATION,
        ROADWORKS_ADB_LONG_TERM_PUBLICATION_IDENTIFIER,
    )


@asset(
    compute_kind='DATEX2',
    group_name='traffic',
    automation_condition=(
        # every 5 minutes
        AutomationCondition.on_cron('0/5 * * * *') & ~AutomationCondition.in_progress() | AutomationCondition.eager()
    ),
    key_prefix=ROADWORKS_ASSET_KEY_PREFIX,
)
def roadworks_svzbw_datex2() -> None:
    """
    Downloads roadworks from SVZ-BW and republishes this DATEX2 dataset.
    """
    # Download and republish, if changed
    destination_folder = os.path.join(WEB_ROOT, *ROADWORKS_ASSET_KEY_PREFIX)
    download(
        ROADWORKS_SVZBW_DATEX2_DOWNLOAD_URL,
        destination_folder,
        ROADWORKS_SVZBW_DATEX2_FILENAME,
        create_precompressed=True,
    )


@asset(
    compute_kind='GeoParquet',
    deps={'roadworks_svzbw_datex2'},
    group_name='traffic',
    automation_condition=AutomationCondition.eager(),
    key_prefix=ROADWORKS_ASSET_KEY_PREFIX,
)
def roadworks_svzbw_geoparquet() -> None:
    """
    Transforms roadworks from SVZ-BW to GeoParquet.
    """
    _datex2_to_geoparquet(
        ROADWORKS_SVZBW_DATEX2_FILENAME,
        ROADWORKS_SVZBW_SOURCE_IDENTIFICATION,
        ROADWORKS_SVZBW_PUBLICATION_IDENTIFIER,
    )


@asset(
    compute_kind='GeoParquet',
    deps={'roadworks_svzbw_geoparquet', 'roadworks_adb_long_term_geoparquet', 'roadworks_adb_short_term_geoparquet'},
    group_name='traffic',
    automation_condition=AutomationCondition.eager(),
    key_prefix=ROADWORKS_ASSET_KEY_PREFIX,
)
def roadworks_and_events_bw_geoparquet() -> None:
    """
    Merges all roadwork and event .parquet files into a single GeoParquet.
    """
    destination_file = Path(WEB_ROOT, *ROADWORKS_ASSET_KEY_PREFIX, ROADWORKS_AND_EVENTS_PARQUET_FILENAME)
    destination_file.parent.mkdir(parents=True, exist_ok=True)
    # Merge into a temp file next to the destination (same filesystem, so the final replace is an atomic
    # rename and web clients never see a partially written file)...
    tmp_file = destination_file.with_name(destination_file.name + '.tmp')
    datex2_merge.merge_partition(os.path.join(GEOPARQUET_TEMP_DIR, '*.parquet'), str(tmp_file))
    # ...then replace the former file
    tmp_file.replace(destination_file)


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
    source = os.path.join(WEB_ROOT, *ROADWORKS_ASSET_KEY_PREFIX, ROADWORKS_SVZBW_DATEX2_FILENAME)
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
    source = os.path.join(WEB_ROOT, *ROADWORKS_ASSET_KEY_PREFIX, ROADWORKS_SVZBW_DATEX2_FILENAME)
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
