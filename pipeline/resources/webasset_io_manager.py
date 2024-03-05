# Copyright 2023 Holger Bruch (hb@mfdz.de)
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

import gzip
import json
import os
import shutil
import tempfile
from typing import Any, Optional, Union

from dagster import (
    ConfigurableIOManager,
    InputContext,
    OutputContext,
)


# in followinng line, ignore mypy Metaclass conflict, see https://github.com/dagster-io/dagster/issues/17443
class JsonWebAssetIOManager(ConfigurableIOManager):  # type: ignore[misc]
    """This IOManager will take in a stream and store it in a folder for web publishing.
    To avoid delivering files during write, the file is written first to a temp destination
    and later on moved to the final destination.

    In case create_precompressed is specified, a gzipped version of the
    file is created in the same path.
    """

    destination_directory: str = ''
    create_precompressed: bool = True

    @property
    def _config(self) -> dict[str, Any]:
        return self.dict()

    def _path_and_filename(self, context: Union[InputContext, OutputContext]):
        path = os.path.join(self.destination_directory, *context.asset_key.path[:-1])
        filename = f'{context.asset_key.path[-1]}.json'

        return path, filename

    def handle_output(self, context: OutputContext, json_dict: dict):
        (destination_path, filename) = self._path_and_filename(context)

        tmpfilename = os.path.join(destination_path, filename + '.tmp')
        finalfilename = os.path.join(destination_path, filename)

        if not os.path.exists(destination_path):
            os.makedirs(destination_path)

        with open(tmpfilename, 'w') as file:
            json.dump(json_dict, file)

        if self.create_precompressed:
            with open(tmpfilename, 'rb') as f_in, gzip.open(tmpfilename + '.gz', 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
                os.replace(tmpfilename + '.gz', finalfilename + '.gz')

        os.replace(tmpfilename, finalfilename)

    def load_input(self, context: InputContext) -> dict:
        (source_path, filename) = self._path_and_filename(context)
        with open(os.path.join(source_path, filename), 'r') as source:
            return json.load(source)
