# Copyright 2025 Ernesto Ruge (ernesto.ruge@binary-butterfly.de)
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

from subprocess import PIPE, Popen  # noqa: S404
from typing import Sequence
from unittest.mock import Mock

import pytest
from dagster import AssetExecutionContext, PipesSubprocessClient
from dagster._core.errors import DagsterPipesExecutionError
from dagster._core.pipes import PipesClientCompletedInvocation


@pytest.fixture
def stripped_pipes_subprocess_client() -> Mock:
    """
    Removes all Dagster related code and just runs the subprocess.
    TODO: atm not used, as I don't know how to get stdout. Might be removed permanently.
    """
    mock = Mock(PipesSubprocessClient())

    def run(command: str | Sequence[str], **kwargs) -> PipesClientCompletedInvocation:
        process = Popen(command, stdout=PIPE, stderr=PIPE)  # noqa: S603
        out, err = process.communicate()
        if process.returncode != 0:
            raise DagsterPipesExecutionError(
                f'External execution process failed with code {process.returncode}',
            )
        return Mock(PipesClientCompletedInvocation)

    mock.run.side_effect = run

    return mock


@pytest.fixture
def mocked_asset_execution_context() -> Mock:
    mock = Mock(AssetExecutionContext)
    mock.log.info.return_value = None
    mock.log.error.return_value = None
    return mock
