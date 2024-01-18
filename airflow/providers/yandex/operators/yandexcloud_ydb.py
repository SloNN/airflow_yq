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

import warnings
from dataclasses import dataclass
from typing import TYPE_CHECKING, Sequence
from datetime import timedelta
from airflow.configuration import conf
import pandas as pd
import ydb


from airflow.exceptions import AirflowException
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.yandex.hooks.yandexcloud_ydb import YDBHook
from airflow.models.baseoperator import BaseOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context

class YDBBulkUpsertOperator(BaseOperator):

    ui_color = "#ededed"

    template_fields: Sequence[str] = ("templates_dict")
    template_fields_renderers = {"templates_dict": "json"}


    def __init__(
        self,
        *,
        endpoint: str,
        database: str,
        table: str,
        column_types: dict[str,ydb.PrimitiveType]| None = None,
        values: list[dict[str:Any]]| None = None,
        folder_id: str | None = None,
        connection_id: str | None = None,
        public_ssh_key: str | None = None,
        service_account_id: str | None = None,
        templates_dict: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.endpoint = endpoint
        self.database = database
        self.column_types = column_types
        self.values = values
        self.table = table
        self.folder_id = folder_id
        self.connection_id = connection_id
        self.public_ssh_key = public_ssh_key
        self.service_account_id = service_account_id
        self.templates_dict = templates_dict

        if templates_dict is not None and "column_types" in templates_dict and "column_values" in templates_dict:
            ct2 = {}
            for t,v in templates_dict["column_types"].items():
                ct2[t] = eval(v)
            self.column_types = ct2

            self.values = templates_dict["column_values"]

        print(f"templates_dict={self.templates_dict}")

        self.hook: YDBHook | None = None

    def execute(self, context: Context) -> None:
        self.hook = YDBHook(
            endpoint = self.endpoint,
            database = self.database,
            yandex_conn_id=self.connection_id,
            default_folder_id=self.folder_id,
            default_public_ssh_key=self.public_ssh_key,
            default_service_account_id=self.service_account_id
        )

        print(f"templates_dict={self.templates_dict}")
        print(f"column_types={self.column_types}")
        print(f"values={self.values}")
        self.hook.bulk_upsert(self.table, self.column_types, self.values)
