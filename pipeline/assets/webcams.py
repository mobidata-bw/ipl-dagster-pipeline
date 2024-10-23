import os
import random
import warnings
from typing import Sequence

from dagster import (
    AssetExecutionContext,
    AutoMaterializePolicy,
    ExperimentalWarning,
    FreshnessPolicy,
    PipesExecutionResult,
    PipesSubprocessClient,
    asset,
)

SCRIPT_DIR = os.getenv('SCRIPT_DIR', './scripts/')


@asset(
    compute_kind='shell',
    group_name='webcams',
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=2, cron_schedule='* * * * *'),
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def webcam_images(
    context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient
) -> Sequence['PipesExecutionResult']:
    """
    Downloads webcam images via lftp.
    """

    return pipes_subprocess_client.run(
        command=['bash', 'download_webcams.sh'], context=context, cwd=SCRIPT_DIR
    ).get_results()
