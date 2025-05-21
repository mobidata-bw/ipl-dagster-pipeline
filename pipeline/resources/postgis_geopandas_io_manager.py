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

import re
from contextlib import closing, contextmanager
from io import StringIO
from typing import Any, Dict, Iterator, Optional, Sequence, cast

import geopandas
import pandas
from dagster import ConfigurableIOManager, InputContext, OutputContext
from psycopg2.errors import UndefinedTable
from sqlalchemy import create_engine
from sqlalchemy.engine import URL, Connection


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


# need mypy to ignore following line due to https://github.com/dagster-io/dagster/issues/17443
class PostgreSQLPandasIOManager(ConfigurableIOManager):  # type: ignore
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

    def _assert_sql_safety(self, *expressions: str) -> None:
        r"""
        Raises a ValueError in case an expression contains characters other than
        word or number chars or is of lenght 0 (^[\w\d]+$).
        """
        for expression in expressions:
            if not re.match(r'^[\w\d]+$', expression):
                raise ValueError(f'Unexpected sql identifier {expression}')

    def handle_output(self, context: OutputContext, obj: pandas.DataFrame):
        schema, table = self._get_schema_table(context.asset_key)

        if isinstance(obj, pandas.DataFrame):
            with connect_postgresql(config=self._config) as con:
                self._create_schema_if_not_exists(schema, con)
                table_exists = self._has_table(con, schema, table)
                if table_exists:
                    self._truncate_table(schema, table, con)
                    if not self._has_primary_key(con, schema, table):
                        # table has no primary key yet, create primary key
                        self._create_primary_key(schema, table, obj.index.names, con)
                else:
                    # create table with empty frame (obj[:0]) and load later via copy_from
                    obj[:0].to_sql(
                        con=con,
                        name=table,
                        index=True,
                        schema=schema,
                        if_exists='replace',
                    )
                    # table was just created, create primary key (to_sql doesn't create these,
                    # though index=True suggests this)
                    self._create_primary_key(schema, table, obj.index.names, con)
                obj.reset_index()
                sio = StringIO()
                obj.to_csv(sio, sep='\t', na_rep='', header=False)
                sio.seek(0)
                with closing(con.connection.cursor()) as c:
                    # ignore mypy attribute check, as postgres cursor has custom extension to DBAPICursor: copy_expert
                    c.copy_expert(f"COPY {schema}.{table} FROM STDIN WITH (FORMAT csv, DELIMITER '\t')", sio)  # type: ignore[attr-defined]
                    con.connection.commit()
                context.add_output_metadata({'num_rows': len(obj), 'table_name': f'{schema}.{table}'})
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
                ' specify a column, set this metadata value. E.g.'
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

    def _delete_partition(self, schema: str, table: str, partition_col_name: str, partition_key: str, con: Connection):
        try:
            self._assert_sql_safety(schema, table, partition_col_name, partition_key)
            with closing(con.connection.cursor()) as c:
                c.execute(f"DELETE FROM {schema}.{table} WHERE {partition_col_name}='{partition_key}'")
        except UndefinedTable:
            # TODO log debug info, asset did not exist, so nothing to
            pass

    def _truncate_table(self, schema: str, table: str, con: Connection):
        try:
            self._assert_sql_safety(schema, table)
            with closing(con.connection.cursor()) as c:
                c.execute(f'TRUNCATE TABLE {schema}.{table}')
        except UndefinedTable:
            # TODO log debug info, asset did not exist, so nothing to
            pass

    def _create_primary_key(self, schema: str, table: str, keys: list[str], con: Connection):
        with closing(con.connection.cursor()) as c:
            self._assert_sql_safety(schema, table, *keys)
            c.execute(f'ALTER TABLE {schema}.{table} ADD PRIMARY KEY ({",".join(keys)})')

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

    def _create_schema_if_not_exists(self, schema: str, con: Connection):
        with closing(con.connection.cursor()) as c:
            self._assert_sql_safety(schema)
            c.execute(f'CREATE SCHEMA IF NOT EXISTS {schema}')

    def _get_schema_table(self, asset_key):
        return asset_key.path[-2] if len(asset_key.path) > 1 else 'public', asset_key.path[-1]

    def _get_select_statement(
        self,
        table: str,
        schema: str,
        columns: Optional[Sequence[str]],
    ):
        columns = columns or []
        self._assert_sql_safety(schema, table, *columns)
        col_str = ', '.join(columns) if columns else '*'
        return f'SELECT {col_str} FROM {schema}.{table}'

    def _has_table(self, con: Connection, schema: str, table: str):
        with closing(con.connection.cursor()) as c:
            c.execute(
                'SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = %s AND table_name = %s',
                (schema, table),
            )
            fetch_result = c.fetchone()
            if fetch_result and fetch_result[0] == 1:
                return True
            return False

    def _has_primary_key(self, con: Connection, schema: str, table: str):
        with closing(con.connection.cursor()) as c:
            c.execute(
                """SELECT i.indexrelid::regclass
                  FROM pg_index i
                  JOIN pg_class c ON c.oid = i.indrelid
                 WHERE c.relnamespace::regnamespace::varchar = %s
                   AND c.relname = %s AND i.indisprimary ='t'""",
                (schema, table),
            )
            fetch_result = c.fetchone()
            return fetch_result is not None


# need mypy to ignore following line due to https://github.com/dagster-io/dagster/issues/17443
class PostGISGeoPandasIOManager(PostgreSQLPandasIOManager):  # type: ignore
    """This IOManager will take in a geopandas dataframe and store it in postgis."""

    def handle_output(self, context: OutputContext, obj: geopandas.GeoDataFrame):
        schema, table = self._get_schema_table(context.asset_key)

        if isinstance(obj, geopandas.GeoDataFrame):
            with connect_postgresql(config=self._config) as con:
                self._create_schema_if_not_exists(schema, con)
                table_exists = self._has_table(con, schema, table)
                if table_exists:
                    if context.has_partition_key:
                        # add additional column (name? for now just partition)
                        # to the frame and initialize with partition_name
                        # (name could become part of metadata, ob may be contained already)
                        partition_col_name = self._get_partition_expr(context)
                        partition_key = context.partition_key
                        obj[partition_col_name] = partition_key

                        # We leave other partitions untouched, but need to delete data from this
                        # partition before we append again.
                        self._delete_partition(schema, table, partition_col_name, partition_key, con)
                    else:
                        # All data can be replaced (i.e. truncated before insertion).
                        # geopandas will take care of this.
                        self._truncate_table(schema, table, con)
                # while writing standard pandas.DataFrames to sql tables is database agnostic and performed
                # internally via SQLAlchemy, writing a GeoDataFrame requires explicitly using to_postgis. See
                # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_sql.html and
                # https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.to_postgis.html
                obj.to_postgis(
                    con=con, name=table, index=True, schema=schema, if_exists='append', chunksize=self.chunksize
                )
                if not table_exists or not self._has_primary_key(con, schema, table):
                    # table was just created or has no primary key,
                    # create primary key (to_postgis doesn't create these,
                    # though index=True suggests this)
                    self._create_primary_key(schema, table, obj.index.names, con)
                con.connection.commit()
                context.add_output_metadata({'num_rows': len(obj), 'table_name': f'{schema}.{table}'})
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
