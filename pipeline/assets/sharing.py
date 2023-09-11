import pandas as pd
from dagster import (
    DefaultScheduleStatus,
    DefaultSensorStatus,
    DynamicPartitionsDefinition,
    RunRequest,
    ScheduleDefinition,
    SensorResult,
    asset,
    define_asset_job,
    schedule,
    sensor,
)

from pipeline.resources import LamassuResource


@asset(
    io_manager_key='pg_gpd_io_manager',
    compute_kind='Lamassu',
    group_name='sharing',
)
def stations(context, lamassu: LamassuResource) -> pd.DataFrame:
    """
    Pushes stations published by lamassu
    to table in a postgis database.
    """

    # Instead of handling each system as a partition, we iterate over all of them
    # and insert them as one large batch.
    # This works around dagster's inefficient job handling for many small sized, frequently updated partitions.
    # See also this discussion in dagster slack: https://dagster.slack.com/archives/C01U954MEER/p1694188602187579
    systems = lamassu.get_systems()
    data_frames = []
    for system in systems:
        system_id = system['id']
        feeds = lamassu.get_system_feeds(system_id)
        stations = lamassu.get_stations_as_frame(feeds, system_id)
        if stations:
            data_frames.append(stations)
    return pd.concat(data_frames)


@asset(
    io_manager_key='pg_gpd_io_manager',
    compute_kind='Lamassu',
    group_name='sharing',
)
def vehicles(context, lamassu: LamassuResource) -> pd.DataFrame:
    """
    Pushes vehicles published by lamassu
    to table in a postgis database.
    """
    # Instead of handling each system as a partition, we iterate over all of them
    # and insert them as one large batch.
    # This works around dagster's inefficient job handling for many small sized, frequently updated partitions.
    # See also this discussion in dagster slack: https://dagster.slack.com/archives/C01U954MEER/p1694188602187579
    systems = lamassu.get_systems()
    data_frames = []
    for system in systems:
        system_id = system['id']
        feeds = lamassu.get_system_feeds(system_id)
        vehicles = lamassu.get_vehicles_as_frame(feeds, system_id)
        if vehicles:
            data_frames.append(vehicles)
    return pd.concat(data_frames)


'''
Default execution mode (which could be overriden for the whole code location)
is multiprocess, resulting in a new process started for every new job execution.
That results in a large overhead for launching a new process, initializing db connections etc.,
so we want high frequency jobs to be execucted in process.
Note: this config has to be provided for job definitions and for RunRequests.
'''
in_process_job_config: dict = {'execution': {'config': {'in_process': {}}}}

'''
Define asset job grouping update of stations and vehicles asset.
'''
stations_and_vehicles_job = define_asset_job(
    'stations_and_vehicles_job',
    selection=[stations, vehicles],
    config=in_process_job_config,
    description='Pushes stations and vehicles from Lamassu to PostGIS',
)


@schedule(job=stations_and_vehicles_job, cron_schedule='* * * * *', default_status=DefaultScheduleStatus.RUNNING)
def update_stations_and_vehicles_minutely(context):
    """
    Run stations_and_vehicles_job in process on the provided schedule (minutely).
    """
    return [RunRequest(run_config=in_process_job_config)]
