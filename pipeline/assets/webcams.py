import os
import random
import warnings

from dagster import (
    AssetExecutionContext,
    AutoMaterializePolicy,
    ExperimentalWarning,
    FreshnessPolicy,
    asset,
)
from dagster_shell import execute_shell_command

SCRIPT_DIR = os.getenv('SCRIPT_DIR', './scripts/')


@asset(
    compute_kind='shell',
    group_name='webcams',
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=2, cron_schedule='* * * * *'),
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def webcam_images(context: AssetExecutionContext) -> None:
    """
    Downloads webcam images via lftp.
    """
    (logs, exit_code) = execute_shell_command(
        'bash download_webcams.sh', cwd=SCRIPT_DIR, output_logging='STREAM', log=context.log
    )

    if exit_code != 0:
        raise RuntimeError(f'Downloading webcam images failed with error code {exit_code}')
