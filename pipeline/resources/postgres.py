# Copyright 2026 Holger Bruch (hb@mfdz.de)
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

import dagster as dg
import psycopg2


class PostgresResource(dg.ConfigurableResource):
    host: str
    port: int = 5432
    database: str
    user: str
    password: str

    def get_connection(self):
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
        )

    def execute_if_exists_else(self, tablename, exists_statements, else_statements, schemaname='public'):
        exists = False
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT EXISTS (
                        SELECT 1
                        FROM pg_matviews
                        WHERE schemaname = '{schemaname}'
                          AND matviewname = '{tablename}'
                    )
                    """)
                exists = cur.fetchone()[0]
                statements_to_execute = exists_statements if exists else else_statements
                for statement in statements_to_execute:
                    cur.execute(statement)

            conn.commit()
