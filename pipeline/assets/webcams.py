import os
import random
import warnings

from dagster import (
    AssetExecutionContext,
    AutoMaterializePolicy,
    ExperimentalWarning,
    FreshnessPolicy,
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
# explicitly no typing '-> Sequence[PipesExecutionResult]' due to https://github.com/dagster-io/dagster/issues/25490
def webcam_images(context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient):
    """
    Downloads webcam images via lftp.
    """

    return pipes_subprocess_client.run(
        command=['bash', 'download_webcams.sh'], context=context, cwd=SCRIPT_DIR
    ).get_results()
