import logging

import pandas as pd
from dagster import (
    DefaultScheduleStatus,
    DefaultSensorStatus,
    DynamicPartitionsDefinition,
    FreshnessPolicy,
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
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=1),
)
def sharing_stations(context, lamassu: LamassuResource) -> pd.DataFrame:
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
        try:
            system_id = system['id']
            feeds = lamassu.get_system_feeds(system_id)
            stations = lamassu.get_stations_as_frame(feeds, system_id)
            if stations is not None:
                data_frames.append(stations)
        except Exception:
            logging.exception(f'Error retrieving stations for system {system}')
    return pd.concat(data_frames)


@asset(
    io_manager_key='pg_gpd_io_manager',
    compute_kind='Lamassu',
    group_name='sharing',
)
def sharing_station_status(context, lamassu: LamassuResource) -> pd.DataFrame:
    """
    Pushes station_statuss published by lamassu
    to table in a postgis database.
    """

    # Instead of handling each system as a partition, we iterate over all of them
    # and insert them as one large batch.
    # This works around dagster's inefficient job handling for many small sized, frequently updated partitions.
    # See also this discussion in dagster slack: https://dagster.slack.com/archives/C01U954MEER/p1694188602187579
    systems = lamassu.get_systems()
    data_frames = []
    for system in systems:
        try:
            system_id = system['id']
            feeds = lamassu.get_system_feeds(system_id)
            station_status = lamassu.get_station_status_by_form_factor_as_frame(feeds, system_id)
            if station_status is not None:
                data_frames.append(station_status)
        except Exception:
            logging.exception(f'Error retrieving sharing_station_status for system {system}')

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
        try:
            system_id = system['id']
            feeds = lamassu.get_system_feeds(system_id)
            vehicles = lamassu.get_vehicles_as_frame(feeds, system_id)
            if vehicles is not None:
                data_frames.append(vehicles)
        except Exception:
            logging.exception(f'Error retrieving vehicles for system {system}')
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
sharing_station_status_and_vehicles_job = define_asset_job(
    'sharing_station_status_and_vehicles_job',
    selection=[sharing_station_status, vehicles],
    config=in_process_job_config,
    description='Pushes sharing_station_status and vehicles from Lamassu to PostGIS',
)


@schedule(
    job=sharing_station_status_and_vehicles_job, cron_schedule='* * * * *', default_status=DefaultScheduleStatus.RUNNING
)
def update_sharing_station_status_and_vehicles_minutely(context):
    """
    Run stations_and_vehicles_job in process on the provided schedule (minutely).
    """
    return [RunRequest(run_config=in_process_job_config)]
