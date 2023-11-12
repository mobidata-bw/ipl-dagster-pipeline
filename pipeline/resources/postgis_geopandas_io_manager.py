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

import csv
from contextlib import contextmanager
from io import StringIO
from typing import Any, Dict, Iterator, Optional, Sequence, cast

import geopandas
import pandas
from dagster import (
    ConfigurableIOManager,
    InputContext,
    OutputContext,
)
from psycopg2.errors import UndefinedTable
from sqlalchemy import create_engine
from sqlalchemy.engine import URL, Connection

# TODO figure out appropriate SQL injection mechanism while allowing dynamic table/column provision


@contextmanager
def connect_postgresql(config, schema='public') -> Iterator[Connection]:
    url = URL.create(
        'postgresql+psycopg2',
        username=config['user'],
        password=config['password'],
        host=config['host'],
        port=config['port'],
        database=config['database'],
    )
    conn = None
    try:
        conn = create_engine(url).connect()
        yield conn
    finally:
        if conn:
            conn.close()


class PostgreSQLPandasIOManager(ConfigurableIOManager):
    """This IOManager will take in a pandas dataframe and store it in postgresql."""

    host: Optional[str] = 'localhost'
    port: Optional[int] = 5432
    user: Optional[str] = 'postgres'
    password: Optional[str]
    database: Optional[str]
    chunksize: Optional[int] = 10000

    @property
    def _config(self) -> Dict[str, Any]:
        return self.dict()

    def handle_output(self, context: OutputContext, obj: pandas.DataFrame):
        schema, table = self._get_schema_table(context.asset_key)

        if isinstance(obj, pandas.DataFrame):
            row_count = len(obj)
            context.log.info(f'Row count: {row_count}, partion: {context.partition_key}')
            with connect_postgresql(config=self._config) as con:
                self._create_schema_if_not_exists(schema, con)
                # just recreate table with empty frame (obj[:0]) and load later via copy_from
                obj[:0].to_sql(
                    con=con,
                    name=table,
                    index=True,
                    schema=schema,
                    if_exists='replace',
                )
                obj_without_index = obj.reset_index()
                sio = StringIO()
                writer = csv.writer(sio, delimiter='\t')
                writer.writerows(obj_without_index.values)
                sio.seek(0)
                c = con.connection.cursor()
                c.copy_expert(f"COPY {schema}.{table} FROM STDIN WITH (FORMAT csv, DELIMITER '\t', NULL 'nan')", sio)  # type: ignore[attr-defined]
                con.connection.commit()
        elif obj is None:
            self.delete_asset(context)
        else:
            raise Exception(f'Outputs of type {type(obj)} not supported.')

    def load_input(self, context: InputContext) -> geopandas.GeoDataFrame:
        schema, table = self._get_schema_table(context.asset_key)
        with connect_postgresql(config=self._config) as con:
            columns = (context.metadata or {}).get('columns')
            return self._load_input(con, table, schema, columns, context)

    def _get_partition_expr(self, context: OutputContext) -> str:
        output_context_metadata = context.metadata or {}
        partition_expr = output_context_metadata.get('partition_expr')
        if partition_expr is None:
            raise ValueError(
                f"Asset '{context.asset_key}' has partitions, but no 'partition_expr'"
                " metadata value, so we don't know what column it's partitioned on. To"
                " specify a column, set this metadata value. E.g."
                ' @asset(metadata={"partition_expr": "your_partition_column"}).'
            )
        return cast(str, partition_expr)

    def delete_asset(self, context: OutputContext):
        schema, table = self._get_schema_table(context.asset_key)
        if context.has_partition_key:
            # add additional column (name? for now just partition)
            # to the frame and initialize with partition_name
            # (name could become part of metadata, or deduced from partiton def)
            partition_col_name = self._get_partition_expr(context)
            partition_key = context.partition_key
            # We leave other partions untouched, but need to delete data from this
            with connect_postgresql(config=self._config) as con:
                self._delete_partition(schema, table, partition_col_name, partition_key, con)
        else:
            raise Exception('Deletion of not-partitioned assets not yet supported.')

    def _delete_partition(self, schema, table, partition_col_name, partition_key, con):
        try:
            c = con.connection.cursor()
            c.execute(f"DELETE FROM {schema}.{table} WHERE {partition_col_name}='{partition_key}'")
        except UndefinedTable:
            # TODO log debug info, asset did not exist, so nothing to
            con.connection.rollback()
            pass

    def _load_input(
        self, con: Connection, table: str, schema: str, columns: Optional[Sequence[str]], context: InputContext
    ) -> pandas.DataFrame:
        return pandas.read_sql(
            sql=self._get_select_statement(
                table,
                schema,
                columns,
            ),
            con=con,
        )

    def _create_schema_if_not_exists(self, schema, con):
        with con.connection.cursor() as c:
            c.execute(f'CREATE SCHEMA IF NOT EXISTS {schema}')

    def _get_schema_table(self, asset_key):
        return asset_key.path[-2] if len(asset_key.path) > 1 else 'public', asset_key.path[-1]

    def _get_select_statement(
        self,
        table: str,
        schema: str,
        columns: Optional[Sequence[str]],
    ):
        col_str = ', '.join(columns) if columns else '*'
        return f'SELECT {col_str} FROM {schema}.{table}'


class PostGISGeoPandasIOManager(PostgreSQLPandasIOManager):
    """This IOManager will take in a geopandas dataframe and store it in postgis."""

    def handle_output(self, context: OutputContext, obj: geopandas.GeoDataFrame):
        schema, table = self._get_schema_table(context.asset_key)

        if isinstance(obj, geopandas.GeoDataFrame):
            len(obj)
            with connect_postgresql(config=self._config) as con:
                if context.has_partition_key:
                    # add additional column (name? for now just partition)
                    # to the frame and initialize with partition_name
                    # (name could become part of metadata, ob may be contained already)
                    partition_col_name = self._get_partition_expr(context)
                    partition_key = context.partition_key
                    obj[partition_col_name] = partition_key

                    # We leave other partions untouched, but need to delete data from this
                    # partition before we append again.
                    if_exists_action = 'append'
                    self._delete_partition(schema, table, partition_col_name, partition_key, con)
                else:
                    # All data can be replaced (e.g. deleted before insertion).
                    # geopandas will take care of this.
                    if_exists_action = 'replace'

                self._create_schema_if_not_exists(schema, con)
                obj.to_postgis(con=con, name=table, schema=schema, if_exists=if_exists_action, chunksize=self.chunksize)
        else:
            super().handle_output(context, obj)

    def _load_input(
        self, con: Connection, table: str, schema: str, columns: Optional[Sequence[str]], context: InputContext
    ) -> geopandas.GeoDataFrame:
        return geopandas.read_postgis(
            sql=self._get_select_statement(
                table,
                schema,
                columns,
            ),
            geom_col=(context.metadata or {}).get('geom_col', 'geometry'),
            con=con,
        )
