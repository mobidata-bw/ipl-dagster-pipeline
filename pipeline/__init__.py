from dagster import (
    DefaultScheduleStatus,
    Definitions,
    EnvVar,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
)

from .assets import sharing
from .resources import LamassuResource

assets = load_assets_from_modules([sharing])

defs = Definitions(
    assets=assets,
    sensors=[sharing.gbfs_feeds_sensor],
    schedules=[sharing.update_stations_and_vehicles_minutely],
    resources={
        'lamassu': LamassuResource(lamassu_base_url=EnvVar('IPL_LAMASSU_BASE_URL')),
    },
)
