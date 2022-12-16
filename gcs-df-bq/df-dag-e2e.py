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

import datetime

from airflow import models
from airflow.providers.apache.beam.operators.beam import (
    BeamRunJavaPipelineOperator,
    BeamRunPythonPipelineOperator,
)

#dataflow related
from airflow.providers.google.cloud.hooks.dataflow import DataflowJobStatus
from airflow.providers.google.cloud.operators.dataflow import (
    CheckJobRunning,
    DataflowTemplatedJobStartOperator,
)

# dataflow related
from airflow.providers.google.cloud.sensors.dataflow import (
    DataflowJobAutoScalingEventsSensor,
    DataflowJobMessagesSensor,
    DataflowJobMetricsSensor,
    DataflowJobStatusSensor,
)

#gcs related
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
    GCSSynchronizeBucketsOperator,
)

from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor


#gcs move file
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
#from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

# BigQuery related
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)


from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago




# parameters
PROJECT_ID = "dataplatformdaydemo"
REGION = "us-central1"

BUCKET_NAME_SRC = "gs://demo-settings/composer_sensed_file/in"
OBJECT_1 = "employees.csv" #file to listen to
BUCKET_NAME_DST = "gs://demo-settings/composer_sensed_file"

DATASET = "composer_demo"
TABLE_1 = "df_results"
TABLE_2 = "aggr_results"

default_args = {
    # Tell airflow to start one day ago, so that it runs as soon as you upload it
    "start_date": days_ago(1),
    "project_id": PROJECT_ID,
    "region": REGION,

}

# start of the DAG
with models.DAG(
    "e2e_dataflow_bq",
    default_args=default_args,
    schedule_interval=datetime.timedelta(days=1),  # Override to match your needs
) as dag:
    
    
    # -- check if a file is in the expected gcs bucket
    gcs_object_exists = GCSObjectExistenceSensor(
        bucket=BUCKET_NAME_SRC,
        object=OBJECT_1,
        mode='poke',
        task_id="gcs_object_exists_task",
    )


    # -- start the dataflow job
    df_start_job = BeamRunPythonPipelineOperator(
        task_id="start_df_job_sync",
        runner="DataflowRunner",
        py_file="gs://demo-settings/composer_sensed_file/df_code/gcs2bq.py",
        py_options=[],
        pipeline_options={
            'input':"gs://demo-settings/composer_sensed_file/in/employees.csv",
            'output': "dataplatformdaydemo:composer_demo.df_results",
            "autoscaling_algorithm": "THROUGHPUT_BASED",
            "num_workers": 1,
            "temp_location":"gs://demo-settings/df_tmp",
        },
        py_requirements=['apache-beam[gcp]==2.41.0'],
        py_interpreter='python3',
        py_system_site_packages=False,
        dataflow_config={
            "job_name": "gcs2bq_composer",
            "location": 'us-central1',
            "wait_until_finished": True, # I m not waiting for the end of the job
        },
    )



    #--perform an aggregation in Bigquery (ELT) and push the result of the query in a different table
    bq_etl_aggregation = BigQueryInsertJobOperator(
            task_id="execute_aggregation_query",
            configuration={
                "query": {
                    "query": f"SELECT SUM(SALARY) AS total FROM {DATASET}.{TABLE_1}",
                    "useLegacySql": False,
                    "destinationTable": {
                        "projectId": PROJECT_ID,
                        "datasetId": DATASET,
                        "tableId": TABLE_2,
                    },
                }
            },
            location='US',
    )

    #-- move the file to backup
    gcs_move_processed_file = GCSToGCSOperator(
        task_id="move_file_backup",
        source_bucket=BUCKET_NAME_SRC,
        source_object=OBJECT_1,
        destination_bucket=BUCKET_NAME_DST,
        destination_object="backup_" + OBJECT_1,
        move_object=True,
)



    gcs_object_exists >> df_start_job >> bq_etl_aggregation >> gcs_move_processed_file 