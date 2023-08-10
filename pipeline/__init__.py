from dagster import (
    Definitions,
    load_assets_from_modules,
    define_asset_job,
    DefaultScheduleStatus,
    ScheduleDefinition
)

from .assets import sharing

assets = load_assets_from_modules([sharing])

defs = Definitions(
    assets=assets,
    sensors=[sharing.gbfs_feeds_sensor],
    schedules=[sharing.update_stations_and_vehicles_minutely],
)
