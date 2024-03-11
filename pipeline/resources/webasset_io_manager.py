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
from pathlib import Path

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
    file_suffix: str = '.json'

    @property
    def _config(self) -> dict[str, Any]:
        return self.dict()

    def _path_and_filename(self, context: Union[InputContext, OutputContext]):
        path = Path(self.destination_directory).joinpath(*context.asset_key.path[:-1])
        filename = f'{context.asset_key.path[-1]}{self.file_suffix}'

        return path, filename

    def handle_output(self, context: OutputContext, json_dict: dict):
        (destination_path, filename) = self._path_and_filename(context)

        final_filename = destination_path / filename
        tmp_filename = final_filename.parent / (final_filename.name + '.tmp')
        
        if not final_filename.parent.exists():
            final_filename.parent(final_filename)

        with tmp_filename.open('w') as file:
            json.dump(json_dict, file)

        gzip_tmp_filename = tmp_filename.parent / (tmp_filename.name + '.gz')  
        gzip_final_filename =  final_filename.parent / (final_filename.name + '.gz')
        if self.create_precompressed:
            with tmp_filename.open('rb') as f_in, gzip.open(gzip_tmp_filename, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        # move temporary files to destination files
        tmp_filename.replace(final_filename)
        if self.create_precompressed:
            gzip_tmp_filename.replace(gzip_final_filename)

    def load_input(self, context: InputContext) -> dict:
        (source_path, filename) = self._path_and_filename(context)
        with (source_path / filename).open('r') as source:
            return json.load(source)
