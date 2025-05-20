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

from dagster import (
    Definitions,
    EnvVar,
    PipesSubprocessClient,
    load_assets_from_modules,
)

from .assets import gtfs, radvis, sharing, traffic_incidents, webcams
from .resources import JsonWebAssetIOManager, LamassuResource, PostGISGeoPandasIOManager
from .resources.gdal import Ogr2OgrResource

assets = load_assets_from_modules([
    sharing,
    radvis,
    gtfs,
    traffic_incidents,
    webcams,
])

defs = Definitions(
    assets=assets,
    schedules=[sharing.update_sharing_station_status_and_vehicles_minutely],
    resources={
        'lamassu': LamassuResource(lamassu_base_url=EnvVar('IPL_LAMASSU_INTERNAL_BASE_URL')),
        'pg_gpd_io_manager': PostGISGeoPandasIOManager(
            host=EnvVar('IPL_POSTGRES_HOST'),
            user=EnvVar('IPL_POSTGRES_USER'),
            port=EnvVar.int('IPL_POSTGRES_PORT'),
            password=EnvVar('IPL_POSTGRES_PASSWORD'),
            database=EnvVar('IPL_POSTGRES_DB'),
        ),
        'pipes_subprocess_client': PipesSubprocessClient(),
        'json_webasset_io_manager': JsonWebAssetIOManager(
            destination_directory=EnvVar('WWW_ROOT_DIR'),
        ),
        'ogr2ogr': Ogr2OgrResource(
            host=EnvVar('IPL_POSTGRES_HOST'),
            username=EnvVar('IPL_POSTGRES_USER'),
            port=EnvVar.int('IPL_POSTGRES_PORT'),
            password=EnvVar('IPL_POSTGRES_PASSWORD'),
            database=EnvVar('IPL_POSTGRES_DB'),
        ),
    },
)
