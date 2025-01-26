from typing import Optional

from dagster import (
    ConfigurableResource,
)
from osgeo import ogr
from osgeo_utils.samples import ogr2ogr


# need mypy to ignore following line due to https://github.com/dagster-io/dagster/issues/17443
class Ogr2OgrResource(ConfigurableResource):  # type: ignore
    username: str = 'postgres'
    password: str = 'postgres'
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
