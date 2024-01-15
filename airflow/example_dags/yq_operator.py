#
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
"""Example DAG demonstrating the usage of the BashOperator."""
from __future__ import annotations

import datetime

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.yandex.operators.yandexcloud_yq import YQExecuteQueryOperator
from airflow.operators.python import PythonOperator
# import airflow.providers.yandex.operators.yandexcloud_dataproc

with DAG(
    dag_id="yq_operator",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60)
) as dag:
    run_this_last = EmptyOperator(
        task_id="run_this_last",
    )

    # # [START howto_operator_bash]
    # run_this = BashOperator(
    #     task_id="run_after_loop",
    #     bash_command="echo 1",
    # )
    # # [END howto_operator_bash]

    # run_this >> run_this_last

    # # [END howto_operator_bash_template]
    # also_run_this >> run_this_last

    def process_result(**kwargs):
        ti = kwargs['ti']
        result = ti.xcom_pull(task_ids='samplequery')
        print(result)

        df = YQExecuteQueryOperator.to_dataframe(result)
        print(df)


    yq_operator2 = YQExecuteQueryOperator(task_id="samplequery2", sql="select 22 as d, 33 as t")
    yq_operator2 >> run_this_last

    yq_operator3 = YQExecuteQueryOperator(task_id="samplequery3", sql="select 33 as d, 44 as t")
    yq_operator3 >> run_this_last

    yq_operator4 = YQExecuteQueryOperator(task_id="samplequery4", sql="select 33 as d, 44 as t")
    yq_operator4 >> run_this_last


    yq_operator = YQExecuteQueryOperator(task_id="samplequery", sql="select 1")
    yq_operator >> yq_operator2


    process_result_task = PythonOperator(
            task_id='process_result',
            python_callable=process_result,
            provide_context=True)

    yq_operator >> process_result_task




    # # [START howto_operator_bash_skip]
    # this_will_skip = BashOperator(
    #     task_id="this_will_skip",
    #     bash_command='echo "hello world"; exit 99;',
    #     dag=dag,
    # )
    # # [END howto_operator_bash_skip]
    # this_will_skip >> run_this_last

if __name__ == "__main__":
    dag.test()
