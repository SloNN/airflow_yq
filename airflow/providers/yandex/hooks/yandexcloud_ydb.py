# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations


from airflow.providers.yandex.hooks.yandex import YandexCloudBaseHook
from airflow.exceptions import AirflowException
import ydb
import time
import posixpath
from datetime import timedelta, datetime

class YDBHook(YandexCloudBaseHook):
    """
    A base hook for Yandex.Cloud Data Proc.

    :param yandex_conn_id: The connection ID to use when fetching connection info.
    """

    def __init__(self, endpoint:str, database:str, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.endpoint = endpoint
        self.database = database
        self.query_id: str | None = None

    def bulk_upsert(self, table, column_types: dict[str,ydb.PrimitiveType.Uint64], values: list[dict[str:Any]]) -> None:
        with ydb.Driver(endpoint=self.endpoint, database=self.database, credentials=ydb.AccessTokenCredentials(self.get_iam_token())) as driver:
            driver.wait(timeout=5, fail_fast=True)

            with ydb.SessionPool(driver) as pool:
                query_column_types = ydb.BulkUpsertColumns()

                for name, type in column_types.items():
                    query_column_types.add_column(name, ydb.OptionalType(type))

                driver.table_client.bulk_upsert( posixpath.join(self.database, table), values, query_column_types)



