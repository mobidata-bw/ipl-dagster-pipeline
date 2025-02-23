#!/bin/bash

set -eo pipefail
set -x

echo "Starting terminate_starting_and_started_runs in background to terminate orphaned ones."
python /opt/dagster/app/scripts/terminate_starting_and_started_runs.py &

# Hand off to the CMD
exec "$@"
