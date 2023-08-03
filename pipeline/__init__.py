from dagster import (
    Definitions,
    load_assets_from_modules,
    define_asset_job,
    ScheduleDefinition
)

from .assets import sharing

assets = load_assets_from_modules([sharing])

defs = Definitions(
    assets=assets,
    schedules=[
        ScheduleDefinition(
            job=define_asset_job(
                "sharing_data_job", selection=["sharing_data"]
            ),
            cron_schedule="* * * * *",  # every minute
        )
    ],
)
