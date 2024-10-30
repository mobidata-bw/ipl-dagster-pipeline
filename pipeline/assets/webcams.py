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
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=2, cron_schedule='* * * * *'),
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
# explicitly no typing '-> Sequence[PipesExecutionResult]' due to https://github.com/dagster-io/dagster/issues/25490
def webcam_images(context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient):
    """
    Downloads webcam images via lftp.
    """
    env = _env_vars_map(['IPL_WEBCAM_USER', 'IPL_WEBCAM_PASSWORD', 'IPL_WEBCAM_SERVER', 'WEBCAM_KEEP_DAYS'])
    return pipes_subprocess_client.run(
        command=['bash', 'download_webcams.sh'], context=context, cwd=SCRIPT_DIR, env=env
    ).get_results()
