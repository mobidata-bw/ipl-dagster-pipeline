# mypy: disable-error-code="operator,arg-type"
# We ignore operator and arg-type warnings, as failing if env vars are unset is explicitly intended
import os
import os.path
import warnings

from dagster import (
    AutoMaterializePolicy,
    FreshnessPolicy,
    graph_asset,
)
from dagster_docker import docker_container_op

import_op = docker_container_op.configured(
    {
        'image': {'env': 'IPL_GTFS_IMPORTER_IMAGE'},
        'networks': ['ipl_ipl'],
        'env_vars': [
            'PGHOST=gtfs-db',
            'PGUSER=' + os.getenv('IPL_GTFS_DB_POSTGRES_USER'),
            'PGPASSWORD=' + os.getenv('IPL_GTFS_DB_POSTGRES_PASSWORD'),
            'PGDATABASE=' + os.getenv('IPL_GTFS_DB_POSTGRES_DB'),
            'GTFS_DOWNLOAD_URL',  # no var name mapping required
            'GTFS_IMPORTER_DB_PREFIX=' + os.getenv('IPL_GTFS_DB_POSTGRES_DB_PREFIX'),
            'GTFS_IMPORTER_DSN_FILE=/var/gtfs/pgbouncer-dsn.txt',
            'GTFS_TMP_DIR=/var/gtfs',
            'POSTGREST_USER=' + os.getenv('IPL_GTFS_DB_POSTGREST_USER'),
            'POSTGREST_PASSWORD=' + os.getenv('IPL_GTFS_DB_POSTGREST_PASSWORD'),
            'GTFS_DOWNLOAD_USER_AGENT',
        ],
        'container_kwargs': {
            #   "auto_remove": True # auto_remove currently results in error
            'volumes': [
                os.path.join(os.getenv('IPL_ROOT_DIR'), 'var/gtfs/:/var/gtfs/:rw'),
                os.path.join(os.getenv('IPL_ROOT_DIR'), 'etc/gtfs:/etc/gtfs'),
            ]
        },
    },
    name='import_op',
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
    return import_op()
