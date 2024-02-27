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
RADVIS_WFS_USER = os.getenv('RADVIS_WFS_USER')
RADVIS_WFS_PASSWORD = os.getenv('RADVIS_WFS_PASSWORD')
RADVIS_DOWNLOAD_URL = os.getenv(
    'RADVIS_WFS_DOWNLOAD_URL',
    'https://radvis.landbw.de/api/geoserver/basicauth/balm/wfs?service=WFS&version=1.0.0&request=GetFeature&typeName=balm:Wegweisung,balm:Route,balm:Streckenabschnitt,balm:Knoten&outputFormat=application/x-gpkg&format_options=filename:balm.gpkg',
)
RADVIS_OUT_FILENAME = 'radnetz_bw.gpkg'


@asset(
    compute_kind='Geopackage',
    group_name='radvis',
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60 * 24, cron_schedule='0 1 * * *'),
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def radnetz_bw_download() -> None:
    """
    Downloads radvis geopackage and republishes
    """
    category = 'radvis'
    destination_folder = os.path.join(WEB_ROOT, category)
    auth = (
        (RADVIS_WFS_USER, RADVIS_WFS_PASSWORD)
        if RADVIS_WFS_USER is not None and RADVIS_WFS_PASSWORD is not None
        else None
    )

    download(
        RADVIS_DOWNLOAD_URL,
        destination_folder,
        RADVIS_OUT_FILENAME,
        timeout=120,
        create_precompressed=True,
        auth=auth,
    )


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
