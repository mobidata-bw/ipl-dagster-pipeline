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

import os

from dagster import AutomationCondition, EnvVar, asset

from pipeline.resources.gdal import Ogr2OgrResource
from pipeline.util.urllib import download

WEB_ROOT = os.getenv('WWW_ROOT_DIR', './tmp/www')
VG25_GEOPACKAGE_URL = os.getenv(
    'VG25_GEOPACKAGE_URL',
    'https://mobidata-bw.de/daten/ipl/verwaltungsgrenzen/251231_DE_VG25.gpkg',
)
VG25_OUT_FILENAME = 'VG25.gpkg'


@asset(
    compute_kind='Geopackage',
    group_name='admin_areas',
    automation_condition=(
        AutomationCondition.on_cron('0 1 * * *') & ~AutomationCondition.in_progress() | AutomationCondition.eager()
    ),
)
def admin_area_download() -> None:
    """
    Downloads admin_areas geopackage and republishes
    """
    category = 'admin_areas'
    destination_folder = os.path.join(WEB_ROOT, category)

    download(
        VG25_GEOPACKAGE_URL,
        destination_folder,
        VG25_OUT_FILENAME,
        timeout=120,
        create_precompressed=True,
    )


@asset(
    deps=['admin_area_download'],
    compute_kind='PostGIS',
    group_name='admin_areas',
    automation_condition=AutomationCondition.eager(),
    metadata={
        'geom_col': 'wkb_geometry',
    },
)
def vg25_gem(ogr2ogr: Ogr2OgrResource) -> None:
    """
    Imports table vg25_geom from admin areas geopackage into postgres.
    """
    category = 'admin_areas'
    file_to_import = os.path.join(WEB_ROOT, category, VG25_OUT_FILENAME)
    ogr2ogr.import_file(file_to_import, layer='vg25_gem')
