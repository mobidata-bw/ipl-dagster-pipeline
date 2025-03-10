import logging
import socket
import time
from typing import Any

# Dagster Patch ---------------------------------------------------
import dagster._check as check

# / Dagster Patch ---------------------------------------------------
from dagster import DagsterRunStatus
from dagster_graphql import DagsterGraphQLClient, DagsterGraphQLClientError
from requests.exceptions import ConnectionError

logging.basicConfig()
logger = logging.getLogger(__file__.rsplit('/', 1)[1])
logger.setLevel(logging.INFO)

client = DagsterGraphQLClient('localhost', port_number=3000)

GET_RUNS_QUERY = '''
query RunsQuery ($filter: RunsFilter) {
  runsOrError(
    filter: $filter
  ) {
    __typename
    ... on Runs {
      results {
        runId
        jobName
        status
        runConfigYaml
        startTime
        endTime
      }
    }
  }
}
'''

# Dagster Patch  (see also https://github.com/dagster-io/dagster/pull/28339)  -----

TERMINATE_RUNS_JOB_MUTATION = '''
mutation GraphQLClientTerminateRuns($runIds: [String!]! $terminatePolicy: TerminateRunPolicy) {
  terminateRuns(runIds: $runIds, terminatePolicy: $terminatePolicy) {
    __typename
    ... on TerminateRunsResult {
      terminateRunResults {
        __typename
        ... on TerminateRunSuccess {
          run  {
            runId
          }
        }
        ... on TerminateRunFailure {
          message
        }
        ... on RunNotFoundError {
          runId
          message
        }
        ... on UnauthorizedError {
          message
        }
        ... on PythonError {
          message
          stack
        }
      }
    }
  }
}
'''


def terminate_runs(self, run_ids: list[str], force: bool = False):
    """Terminates a list of pipeline runs. This method it is useful when you would like to stop a list of pipeline runs
    based on a external event.

    Args:
        run_ids (List[str]): The list run ids of the pipeline runs to terminate
    """
    check.list_param(run_ids, 'run_ids', of_type=str)

    res_data: dict[str, dict[str, Any]] = self._execute(
        TERMINATE_RUNS_JOB_MUTATION,
        {'runIds': run_ids, 'terminatePolicy': 'MARK_AS_CANCELED_IMMEDIATELY' if force else 'SAFE_TERMINATE'},
    )

    query_result: dict[str, Any] = res_data['terminateRuns']
    run_query_result: list[dict[str, Any]] = query_result['terminateRunResults']

    errors = []
    for run_result in run_query_result:
        if run_result['__typename'] == 'TerminateRunSuccess':
            continue
        if run_result['__typename'] == 'RunNotFoundError':
            errors.append(('RunNotFoundError', run_result['message']))
        else:
            errors.append((run_result['__typename'], run_result['message']))

    if errors:
        if len(errors) < len(run_ids):
            raise DagsterGraphQLClientError('TerminateRunsError', f'Some runs could not be terminated: {errors}')
        if len(errors) == len(run_ids):
            raise DagsterGraphQLClientError('TerminateRunsError', f'All run terminations failed: {errors}')


# Patch DagsterGraphQLClient.terminate_runs to support force option
DagsterGraphQLClient.terminate_runs = terminate_runs  # type: ignore[method-assign]

# / End of dagster patch --------------------------


def get_run_ids_of_runs(status: list[str], timeout: int = 20) -> list[str]:
    variables = {'filter': {'statuses': status}}

    start_time = time.perf_counter()
    while True:
        try:
            response = client._execute(GET_RUNS_QUERY, variables)
            return [run['runId'] for run in response['runsOrError']['results']]
        except DagsterGraphQLClientError as ex:
            if isinstance(ex.__cause__, ConnectionError):
                time.sleep(0.1)
                if time.perf_counter() - start_time >= timeout:
                    raise TimeoutError('Waited too long for the Dagster Webserver startup') from ex
            else:
                raise


run_ids = get_run_ids_of_runs(['STARTED', 'STARTING'])

if len(run_ids) > 0:
    logger.info(f'Terminating runs {run_ids}')

    client.terminate_runs(run_ids, True)  # type: ignore[call-arg]
else:
    logger.info('No run in state STARTED or STARTING')
