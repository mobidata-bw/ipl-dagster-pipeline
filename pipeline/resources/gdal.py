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

from typing import Optional

from dagster import ConfigurableResource
from osgeo import ogr
from osgeo_utils.samples import ogr2ogr


# need mypy to ignore following line due to https://github.com/dagster-io/dagster/issues/17443
class Ogr2OgrResource(ConfigurableResource):  # type: ignore
    username: str = 'postgres'  # noqa: S105
    password: str = 'postgres'  # noqa: S105
    host: str = 'localhost'
    port: int = 5432
    database: Optional[str]

    def import_file(self, file_to_import: str, pg_use_copy=True, schema: str = 'public'):
        connect_string = self._connect_string()
        schema_param = f'SCHEMA={schema}'
        args = [
            'ogr2ogr',
            '-f',
            'PostgreSQL',
            connect_string,
            '-lco',
            schema_param,
            '-overwrite',
        ]

        if pg_use_copy:
            args.extend(['--config', 'PG_USE_COPY', 'YES'])

        args.append(file_to_import)
        ogr.UseExceptions()
        exit_code = ogr2ogr.main(args)
        if exit_code != 0:
            raise Exception(f'Could not import {file_to_import}')
        return

    def _connect_string(self):
        database = self.database if self.database else self.username
        return f'PG:user={self.username} password={self.password} host={self.host} port={self.port} dbname={database}'
