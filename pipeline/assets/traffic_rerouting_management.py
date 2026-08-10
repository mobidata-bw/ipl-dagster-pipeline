# Copyright 2026 Holger Bruch (hb@mfdz.de)
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

REROUTING_MANAGEMENT_SVZBW_DATEX2_DOWNLOAD_URL = os.getenv('REROUTING_MANAGEMENT_SVZBW_DATEX2_DOWNLOAD_URL', '')
REROUTING_MANAGEMENT_SVZBW_DATEX2_FILENAME = 'rerouting_management_svzbw.datex2.xml'
REROUTING_MANAGEMENT_ASSET_KEY_PREFIX = ['traffic', 'rerouting_management']


logger = logging.getLogger(__name__)


@asset(
    compute_kind='DATEX2',
    group_name='traffic',
    automation_condition=(
        # every 5 minutes
        AutomationCondition.on_cron('0/5 * * * *') & ~AutomationCondition.in_progress() | AutomationCondition.eager()
    ),
    key_prefix=REROUTING_MANAGEMENT_ASSET_KEY_PREFIX,
)
def rerouting_management_svzbw_datex2() -> None:
    """
    Downloads rerouting management dataset from SVZ-BW and republishes this DATEX2 dataset.
    """
    # Download and republish, if changed
    destination_folder = os.path.join(WEB_ROOT, *REROUTING_MANAGEMENT_ASSET_KEY_PREFIX)
    download(
        REROUTING_MANAGEMENT_SVZBW_DATEX2_DOWNLOAD_URL,
        destination_folder,
        REROUTING_MANAGEMENT_SVZBW_DATEX2_FILENAME,
        create_precompressed=True,
    )
