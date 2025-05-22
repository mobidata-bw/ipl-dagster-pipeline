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
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AutomationCondition,
    PipesSubprocessClient,
    asset,
)

from pipeline.sources import WebcamWorker
from pipeline.sources.webcam_worker import WebcamWorkerConfig


def _get_env_var(env_var: str) -> str:
    value = os.getenv(env_var)

    if value is None:
        raise ValueError(f'Environment variable {env_var} is required but not undefined')

    return value


@asset(
    compute_kind='shell',
    group_name='webcams',
    automation_condition=AutomationCondition.on_cron('* * * * *') & ~AutomationCondition.in_progress(),
)
# explicitly no typing '-> Sequence[PipesExecutionResult]' due to https://github.com/dagster-io/dagster/issues/25490
def webcam_images(context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient):
    """
    Downloads webcam via WebcamWorker
    """
    worker = WebcamWorker(
        pipes_subprocess_client=pipes_subprocess_client,
        context=context,
        config=WebcamWorkerConfig(
            host=_get_env_var('IPL_WEBCAM_SERVER'),
            user=_get_env_var('IPL_WEBCAM_USER'),
            password=_get_env_var('IPL_WEBCAM_PASSWORD'),
            worker_count=int(_get_env_var('IPL_WEBCAM_WORKER')),
            keep_days=int(_get_env_var('IPL_WEBCAM_KEEP_DAYS')),
            image_path=Path(_get_env_var('IPL_WEBCAM_IMAGE_PATH')),
            symlink_path=Path(_get_env_var('IPL_WEBCAM_SYMLINK_PATH')),
        ),
    )
    worker.run()
