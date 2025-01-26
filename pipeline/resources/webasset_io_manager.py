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

import json
from pathlib import Path
from typing import Any, Union

from dagster import (
    ConfigurableIOManager,
    InputContext,
    OutputContext,
)

from pipeline.util.urllib import store_with_tmp_and_gzipped


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
    file_suffix: str = '.json'

    @property
    def _config(self) -> dict[str, Any]:
        return self.dict()

    def _path_and_filename(self, context: Union[InputContext, OutputContext]):
        return Path(self.destination_directory).joinpath(*context.asset_key.path).with_suffix(self.file_suffix)

    def handle_output(self, context: OutputContext, json_dict: dict):
        final_filename = self._path_and_filename(context)

        def store_json(tmp_filename):
            with tmp_filename.open('w') as file:
                json.dump(json_dict, file)

            return True

        store_with_tmp_and_gzipped(final_filename, store_json, self.create_precompressed)

    def load_input(self, context: InputContext) -> dict:
        filename = self._path_and_filename(context)
        with filename.open('r') as source:
            return json.load(source)
