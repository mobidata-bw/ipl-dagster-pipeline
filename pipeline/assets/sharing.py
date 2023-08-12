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

'''
Define a dynamic partition gbfs_systems, which will be checked periodically by a sensor
'''
gbfs_system_partitions_def = DynamicPartitionsDefinition(name='gbfs_systems')


@asset(
    partitions_def=gbfs_system_partitions_def,
    io_manager_key='pg_gpd_io_manager',
    compute_kind='Lamassu',
    group_name='sharing',
)
def stations(context, lamassu: LamassuResource):
    """
    Pushes stations published by lamassu
    to table in a postgis database.
    """
    system_id = context.asset_partition_key_for_output()
    feeds = lamassu.get_system_feeds(system_id)
    return lamassu.get_stations_as_frame(feeds, system_id)


@asset(
    partitions_def=gbfs_system_partitions_def,
    io_manager_key='pg_gpd_io_manager',
    compute_kind='Lamassu',
    group_name='sharing',
)
def vehicles(context, lamassu: LamassuResource):
    """
    Pushes vehicles published by lamassu
    to table in a postgis database.
    """
    system_id = context.asset_partition_key_for_output()
    feeds = lamassu.get_system_feeds(system_id)
    return lamassu.get_vehicles_as_frame(feeds, system_id)


'''
Default execution mode (which could be overriden for the whole code location)
is multiprocess, resulting in a new process startet for every new job execution.
That results in a large overhead for launching aa new process, initializing db connections etc.,
so we want high frequency jobs to be execucted in process.
Note: this config has to be provided for job definitions and for RunRequests.
'''
in_process_job_config: dict = {'execution': {'config': {'in_process': {}}}}

'''
Define asset job grouping update of stations and vehicles asset.
'''
stations_and_vehicles_job = define_asset_job(
    'stations_and_vehicles_job', selection=[stations, vehicles], config=in_process_job_config,
    description='Pushes stations and vehicles from Lamassu to PostGIS'
)


@schedule(job=stations_and_vehicles_job, cron_schedule='* * * * *', default_status=DefaultScheduleStatus.RUNNING)
def update_stations_and_vehicles_minutely(context):
    """
    For currently registered systems (which we treat as partition),
    the stations_and_vehicles_job is run on the provided schedule (minutely).
    """
    system_ids = gbfs_system_partitions_def.get_partition_keys(dynamic_partitions_store=context.instance)
    return [
        RunRequest(partition_key=system_id, run_key=system_id, run_config=in_process_job_config)
        for system_id in system_ids
    ]


@sensor(job=stations_and_vehicles_job, default_status=DefaultSensorStatus.RUNNING)
def gbfs_feeds_sensor(context, lamassu: LamassuResource):
    current_systems = [system['id'] for system in lamassu.get_systems()]
    new_systems = [
        system_id
        for system_id in current_systems
        if not context.instance.has_dynamic_partition(gbfs_system_partitions_def.name, system_id)
    ]
    deleted_systems = [
        system_id
        for system_id in context.instance.get_dynamic_partitions(gbfs_system_partitions_def.name)
        if system_id not in current_systems
    ]
    return SensorResult(
        run_requests=[
            # TODO is specifying a partition_key only sufficient
            RunRequest(partition_key=system_id, run_config=in_process_job_config)
            for system_id in new_systems
        ]
        # TODO append RunRequests for deleted systems
        # RunRequest(partition_key=system_id, run_config=in_process_job_config, job_name=TODO_delete) for system_id in deleted_systems
        ,
        dynamic_partitions_requests=[
            gbfs_system_partitions_def.build_add_request(new_systems),
            gbfs_system_partitions_def.build_delete_request(deleted_systems),
        ],
    )
