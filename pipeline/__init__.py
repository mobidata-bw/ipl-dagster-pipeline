from dagster import (
    DefaultScheduleStatus,
    Definitions,
    EnvVar,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
)

from .assets import radvis, sharing
from .resources import LamassuResource, PostGISGeoPandasIOManager
from .resources.gdal import Ogr2OgrResource

assets = load_assets_from_modules([sharing, radvis])

defs = Definitions(
    assets=assets,
    schedules=[sharing.update_stations_and_vehicles_minutely],
    resources={
        'lamassu': LamassuResource(lamassu_base_url=EnvVar('IPL_LAMASSU_BASE_URL')),
        'pg_gpd_io_manager': PostGISGeoPandasIOManager(
            host=EnvVar('IPL_POSTGRES_HOST'),
            user=EnvVar('IPL_POSTGRES_USER'),
            port=EnvVar.int('IPL_POSTGRES_PORT'),
            password=EnvVar('IPL_POSTGRES_PASSWORD'),
            database=EnvVar('IPL_POSTGRES_DB'),
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
