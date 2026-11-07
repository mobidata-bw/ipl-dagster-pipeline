# Copyright 2023 Holger Bruch (hb@mfdz.de)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# mypy: disable-error-code="operator,arg-type"
# We ignore operator and arg-type warnings, as failing if env vars are unset is explicitly intended

import logging
import os
import os.path

import docker
from dagster import AutomationCondition, graph_asset, op
from dagster_docker import docker_container_op

logger = logging.getLogger(__name__)


def make_gtfs_import_op(
    *,
    op_name,
    gtfs_importer_schema,
    gtfs_download_url,
    gtfs_download_user_agent,
    gtfstidy_before_import=False,
    custom_env_vars=[],
    cpu_shares=512,
    extra_hosts={},
):
    return docker_container_op.configured(
        {
            # Note: We mirror ipl-orchestration's docker-compose.yaml here, the env vars & mounts should be kept in sync with it.
            'image': {'env': 'IPL_GTFS_IMPORTER_IMAGE'},
            'networks': [os.getenv('IPL_GTFS_IMPORTER_NETWORK')],
            'env_vars': [
                # todo: connect via pgbouncer?
                'PGHOST=gtfs-db',
                'PGPORT=5432',
                'PGUSER=' + os.getenv('IPL_GTFS_DB_POSTGRES_USER'),
                'PGPASSWORD=' + os.getenv('IPL_GTFS_DB_POSTGRES_PASSWORD'),
                'PGDATABASE=' + os.getenv('IPL_GTFS_DB_POSTGRES_DB'),
                'GTFS_IMPORTER_SCHEMA=' + gtfs_importer_schema,
                'GTFS_DOWNLOAD_URL=' + gtfs_download_url,
                'GTFS_DOWNLOAD_USER_AGENT=' + gtfs_download_user_agent,
                'GTFS_TMP_DIR=/var/gtfs',
                'GTFSTIDY_BEFORE_IMPORT=' + gtfstidy_before_import,
            ]
            + custom_env_vars,
            'container_kwargs': {
                # # > Remove the container when it has finished running. Default: False.
                # 'auto_remove': True,
                'volumes': [
                    os.getenv('IPL_MAIN_GTFS_IMPORTER_HOST_GTFS_OUTPUT_DIR') + ':/var/gtfs/:rw',
                    os.getenv('IPL_MAIN_GTFS_IMPORTER_HOST_CUSTOM_SCRIPTS_DIR') + ':/etc/gtfs',
                    # todo: explain why
                    os.path.join(os.getenv('IPL_MAIN_GTFS_IMPORTER_HOST_CUSTOM_SCRIPTS_DIR'), 'download.sh')
                    + ':/importer/download.sh',
                ],
                # > CPU shares (relative weight).
                # from https://docs.docker.com/config/containers/resource_constraints/#configure-the-default-cfs-scheduler:
                # > --cpu-shares – Set this flag to a value greater or less than the default of 1024 to increase or reduce the container's weight, and give it access to a greater or lesser proportion of the host machine's CPU cycles. This is only enforced when CPU cycles are constrained. When plenty of CPU cycles are available, all containers use as much CPU as they need. In that way, this is a soft limit. --cpu-shares doesn't prevent containers from being scheduled in Swarm mode. It prioritizes container CPU resources for the available CPU cycles. It doesn't guarantee or reserve any specific CPU access.
                'cpu_shares': cpu_shares,
                'extra_hosts': extra_hosts,
            },
        },
        name=op_name,
    )


main_gtfs_import = make_gtfs_import_op(
    op_name='main_gtfs_import',
    gtfs_importer_schema=os.getenv('IPL_MAIN_GTFS_DB_POSTGRES_SCHEMA'),
    gtfs_download_url=os.getenv('IPL_MAIN_GTFS_IMPORTER_GTFS_DOWNLOAD_URL'),
    gtfs_download_user_agent=os.getenv('IPL_MAIN_GTFS_IMPORTER_GTFS_DOWNLOAD_USER_AGENT'),
    gtfstidy_before_import=os.getenv('IPL_MAIN_GTFS_IMPORTER_GTFSTIDY_BEFORE_IMPORT', 'false'),
    custom_env_vars=[
        'POSTGREST_USER=' + os.getenv('IPL_MAIN_GTFS_DB_POSTGREST_USER'),
        'POSTGREST_PASSWORD=' + os.getenv('IPL_MAIN_GTFS_DB_POSTGREST_PASSWORD'),
        'POSTGREST_STATEMENT_COST_LIMIT=' + os.getenv('IPL_MAIN_GTFS_API_COST_LIMIT'),
    ],
    extra_hosts=(
        {os.getenv('IPL_MAIN_GTFS_IMPORTER_EXTRA_HOST_HOSTNAME'): os.getenv('IPL_MAIN_GTFS_IMPORTER_EXTRA_HOST_IP')}
        if os.getenv('IPL_MAIN_GTFS_IMPORTER_EXTRA_HOST_HOSTNAME') and os.getenv('IPL_MAIN_GTFS_IMPORTER_EXTRA_HOST_IP')
        else {}
    ),
)


@op
def reload_main_gtfs_api_config(main_gtfs_import):
    # Note: We mirror ipl-orchestration's `import-new-gtfs` Make target here.
    client = docker.from_env()

    container_name = os.getenv('IPL_MAIN_GTFS_API_CONTAINER_NAME')
    if container_name == None:
        raise Exception(
            "Failed to reload gtfs-api's config because $IPL_MAIN_GTFS_API_CONTAINER_NAME is unset or empty.",
        )
    container = client.containers.get(container_name)
    if container == None:
        raise Exception(
            f'Failed to reload gtfs-api\'s config because there is no container "{container_name}" (from $IPL_MAIN_GTFS_API_CONTAINER_NAME).',
        )

    # > To reload the configuration via signal, send a SIGUSR2 signal to the server process.
    # Note: We assume here that the PostgREST process is the container's root/only process.
    container.kill(signal='USR2')


@graph_asset(
    group_name='main_gtfs',
    automation_condition=(
        AutomationCondition.on_cron('0 1 * * *') & ~AutomationCondition.in_progress() | AutomationCondition.eager()
    ),
)
def main_gtfs():
    """
    Downloads, cleans, and imports MobiData-BW's main (as in: all regular PT) GTFS feed
    """
    return reload_main_gtfs_api_config(main_gtfs_import())
