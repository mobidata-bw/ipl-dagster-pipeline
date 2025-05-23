# Copyright 2024 Holger Bruch (hb@mfdz.de), Ernesto Ruge (ernesto.ruge@binary-butterfly.de)
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

import os

from dagster import (
    AssetExecutionContext,
    AutomationCondition,
    PipesSubprocessClient,
    asset,
)

SCRIPT_DIR = os.getenv('SCRIPT_DIR', './scripts/')


def _env_vars_map(env_vars: list[str]) -> dict[str, str]:
    env = {}
    for env_var in env_vars:
        value = os.getenv(env_var)
        if value is None:
            raise ValueError(f'Environment variable {env_var} is required but not undefined')
        env[env_var] = value

    return env


@asset(
    compute_kind='shell',
    group_name='webcams',
    automation_condition=AutomationCondition.on_cron('* * * * *') & ~AutomationCondition.in_progress(),
)
# explicitly no typing '-> Sequence[PipesExecutionResult]' due to https://github.com/dagster-io/dagster/issues/25490
def webcam_images(context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient):
    """
    Downloads webcam images via lftp.
    """
    env = _env_vars_map(
        [
            'IPL_WEBCAM_USER',
            'IPL_WEBCAM_PASSWORD',
            'IPL_WEBCAM_SERVER',
            'IPL_WEBCAM_KEEP_DAYS',
            'IPL_WEBCAM_WORKER',
        ],
    )
    return pipes_subprocess_client.run(
        command=['bash', 'download_webcams.sh'], context=context, cwd=SCRIPT_DIR, env=env
    ).get_results()
