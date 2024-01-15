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

import asyncio
from typing import TYPE_CHECKING, Any, AsyncIterator

from airflow.providers.yandex.hooks.yandexcloud_yq import YQHook, QueryType
from airflow.triggers.base import BaseTrigger, TriggerEvent
import traceback

if TYPE_CHECKING:
    from datetime import timedelta


class YQQueryStatusTrigger(BaseTrigger):

    def __init__(
        self,
        poll_interval: float,
        query_id: str,
        yandex_conn_id: str,
    ):
        super().__init__()
        self.poll_interval = poll_interval
        self.query_id = query_id
        self.yandex_conn_id = yandex_conn_id

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.yandex.triggers.yandexcloud_yq.YQQueryStatusTrigger",
            {
                "poll_interval": self.poll_interval,
                "query_id": self.query_id,
                "yandex_conn_id": self.yandex_conn_id
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        try:
            while True:
                status = await self.get_query_status(self.query_id)
                if status not in ["RUNNING", "PENDING"]:
                    break
                await asyncio.sleep(self.poll_interval)

            yield TriggerEvent(
                {
                    "status": status,
                    "query_id": self.query_id,
                    "yandex_conn_id": self.yandex_conn_id
                }
                )
        except Exception as e:
            message = f"{str(e)} trace={traceback.format_exc()}"
            yield TriggerEvent({"status": "error", "message": message})

    async def get_query_status(self, query_id: str) -> dict[str, Any]:
        """Return True if the SQL query is still running otherwise return False."""
        hook = YQHook(
            yandex_conn_id=self.yandex_conn_id
        )
        return await hook.get_query_status_async(query_id)

    def _set_context(self, context):
        pass
