import logging
import socket
import time

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
    client.terminate_runs(run_ids)
else:
    logger.info('No run in state STARTED or STARTING')
