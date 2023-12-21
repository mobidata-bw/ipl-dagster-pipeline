# mypy: disable-error-code="operator,arg-type"
# We ignore operator and arg-type warnings, as failing if env vars are unset is explicitly intended
import logging
import os
import os.path
import warnings

import docker
from dagster import (
    AutoMaterializePolicy,
    FreshnessPolicy,
    graph_asset,
    op,
)
from dagster_docker import docker_container_op

import_op = docker_container_op.configured(
    {
        'image': {'env': 'IPL_GTFS_IMPORTER_IMAGE'},
        'networks': [os.getenv('IPL_GTFS_IMPORTER_NETWORK')],
        'env_vars': [
            'PGHOST=' + os.getenv('IPL_GTFS_DB_POSTGRES_HOST'),
            'PGUSER=' + os.getenv('IPL_GTFS_DB_POSTGRES_USER'),
            'PGPASSWORD=' + os.getenv('IPL_GTFS_DB_POSTGRES_PASSWORD'),
            'PGDATABASE=' + os.getenv('IPL_GTFS_DB_POSTGRES_DB'),
            'GTFS_DOWNLOAD_URL=' + os.getenv('IPL_GTFS_IMPORTER_GTFS_DOWNLOAD_URL'),
            'GTFS_DOWNLOAD_USER_AGENT=' + os.getenv('IPL_GTFS_IMPORTER_GTFS_DOWNLOAD_USER_AGENT'),
            'GTFS_IMPORTER_DB_PREFIX=' + os.getenv('IPL_GTFS_DB_POSTGRES_DB_PREFIX'),
            'GTFS_IMPORTER_DSN_FILE=/var/gtfs/pgbouncer-dsn.txt',
            'GTFS_TMP_DIR=/var/gtfs',
            'POSTGREST_USER=' + os.getenv('IPL_GTFS_DB_POSTGREST_USER'),
            'POSTGREST_PASSWORD=' + os.getenv('IPL_GTFS_DB_POSTGREST_PASSWORD'),
        ],
        'container_kwargs': {
            #   "auto_remove": True # auto_remove currently results in error
            'volumes': [
                os.path.join(os.getenv('IPL_GTFS_IMPORTER_HOST_GTFS_OUTPUT_DIR'), ':/var/gtfs/:rw'),
                os.path.join(os.getenv('IPL_GTFS_IMPORTER_HOST_CUSTOM_SCRIPTS_DIR'), ':/etc/gtfs'),
            ]
        },
    },
    name='import_op',
)


@op
def reload_pgbouncer_databases(import_op):
    client = docker.from_env()
    # Temporarilly return ipl-pgbouncer-1 as default container name if not configured otherwise
    container_name = os.getenv('IPL_GTFS_PGBOUNCER_CONTAINER', 'ipl-pgbouncer-1')
    # TODO check for existanc and log warning if not
    # if container_name == None:
    #    logging.warn('Will not reload pgbouncer databases, as IPL_GTFS_PGBOUNCER_CONTAINER is unset')
    #    return
    container = client.containers.get(container_name)
    if container:
        container.exec_run('/reload-pgbouncer-databases.sh')
    else:
        logging.warn(
            f'Will not reload pgbouncer databases, as IPL_GTFS_PGBOUNCER_CONTAINER {container_name} is not found'
        )


@graph_asset(
    group_name='gtfs',
    auto_materialize_policy=AutoMaterializePolicy.eager(),
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60 * 24, cron_schedule='0 1 * * *'),
)
def gtfs():
    """
    Downloads, cleans, and imports the gtfs data
    """
    return reload_pgbouncer_databases(import_op())
