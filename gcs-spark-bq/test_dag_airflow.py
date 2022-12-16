################
#
# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# author: cdamien 2022
#
################

import os
from airflow.models import Variable
from datetime import datetime
from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (DataprocCreateBatchOperator,DataprocGetBatchOperator)
from datetime import datetime
from airflow.utils.dates import days_ago
import string
import random # define the random module
S = 10  # number of characters in the string.
# call random.choices() string module to find the string in Uppercase + numeric data.
ran = ''.join(random.choices(string.digits, k = S))

###update parameters here
PROJECT_ID = "dataplatformdaydemo"
REGION = "us-west1"
subnet="default"
#phs_server="phs" 
code_bucket="spark_source_code"
bq_dataset="raw_zone"
service_account= "182543661919-compute@developer.gserviceaccount.com"

dag_name= "test_batch_serverless"
###


BATCH_ID = "teststreaming"+str(ran)

etl_url = "gs://"+code_bucket+"/prep_data2bq.py"

BATCH_CONFIG1 = {
    "pyspark_batch": {
        "main_python_file_uri": etl_url,
        "args":[
          PROJECT_ID,
          bq_dataset,
          code_bucket,
          dag_name
        ],
        "jar_file_uris": [
      "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.22.2.jar"
    ]
    },
    "environment_config":{
        "execution_config":{
            "service_account": service_account,
            "subnetwork_uri": subnet
            },
        # "peripherals_config": {
          #  "spark_history_server_config": {
          #      "dataproc_cluster": f"projects/{PROJECT_ID}/regions/{REGION}/clusters/{phs_server}"
          #      }
          #  },
        },
}

with models.DAG(
    "serverless_batch_test",
    schedule_interval=None,
    start_date = days_ago(2),
    catchup=False,
) as dag_serverless_batch:
    # [START how_to_cloud_dataproc_create_batch_operator]
    create_serverless_batch1 = DataprocCreateBatchOperator(
        task_id="etl_serverless",
        project_id=PROJECT_ID,
        region=REGION,
        batch=BATCH_CONFIG1,
        batch_id=BATCH_ID)

# [END how_to_cloud_dataproc_create_batch_operator]

create_serverless_batch1
