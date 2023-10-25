import os
import warnings

from dagster import (
    AutoMaterializePolicy,
    EnvVar,
    ExperimentalWarning,
    FreshnessPolicy,
    asset,
)

from pipeline.resources.gdal import Ogr2OgrResource
from pipeline.util.urllib import download

warnings.filterwarnings('ignore', category=ExperimentalWarning)

WEB_ROOT = os.getenv('WWW_ROOT_DIR', './tmp/www')
RADVIS_DOWNLOAD_URL = 'https://data.mfdz.de/vm/radvis/Wegweisung.gpkg'
RADVIS_OUT_FILENAME = 'radnetz_bw.gpkg'


@asset(
    compute_kind='Geopackage',
    group_name='radvis',
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60 * 24, cron_schedule='0 1 * * *'),
)
def radnetz_bw_download() -> None:
    """
    Downloads radvis geopackage and republishes
    """
    category = 'radvis'
    destination_folder = os.path.join(WEB_ROOT, category)
    download(RADVIS_DOWNLOAD_URL, destination_folder, RADVIS_OUT_FILENAME, create_precompressed=True)


@asset(
    non_argument_deps={'radnetz_bw_download'},
    compute_kind='PostGIS',
    group_name='radvis',
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def radnetz_bw_postgis(ogr2ogr: Ogr2OgrResource) -> None:
    """
    Imports radnetz_bw geopackage into postgres.
    """
    category = 'radvis'
    file_to_import = os.path.join(WEB_ROOT, category, RADVIS_OUT_FILENAME)
    ogr2ogr.import_file(file_to_import)
