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

#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType
import sys
import pyspark.sql.functions as F

#Building a Spark session to be able to perform read/write operations to and from BigQuery
spark = SparkSession.builder.appName('serverless-pyspark-test').config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.22.2.jar')\
.enableHiveSupport().getOrCreate()

#Reading the arguments and storing them in variables
project_name=sys.argv[1]
dataset_name=sys.argv[2]
bucket_name=sys.argv[3]

#Reading network traffic source data for cleanup and preprocessing
malicious_df = spark.read.csv("gs://"+bucket_name+"/traffic_dataset.csv", header=True, inferSchema=True)
malicious_df.show(5)
malicious_df.printSchema()

# Feature summary of network traffic data
malicious_df.describe().show()

# Deduping network traffic data to prevent skewness in data
malicious_df = malicious_df.dropDuplicates()

#Filtering out features to keep only relevant features
malicious_df=malicious_df.select('count',
'protocol_type',
'logged_in',
'dst_host_same_srv_rate',
'srv_diff_host_rate',
'service',
'dst_host_same_src_port_rate',
'srv_serror_rate',
'same_srv_rate',
'dst_host_srv_count',
'hot',
'dst_host_rerror_rate',
'serror_rate',
'dst_host_srv_rerror_rate',
'dst_host_count',
'srv_count',
'dst_host_serror_rate',
'wrong_fragment',
'dst_host_srv_diff_host_rate',
'dst_host_diff_srv_rate',
'src_bytes',
'num_compromised',
'rerror_rate',
'dst_host_srv_serror_rate',
'srv_rerror_rate',
'dst_bytes',
'flag',
'is_guest_login',
'duration',
'diff_srv_rate',
'num_root',
'num_shells',
'class')

malicious_df.printSchema()

#Writing the clean data into a BigQuery table
spark.conf.set("parentProject", project_name)
spark.conf.set("temporaryGcsBucket", bucket_name)
malicious_df.write.format('bigquery') .mode("overwrite").option('table', project_name+':'+dataset_name+'.test_data') .save()

malicious_df.show(20)
print('Job Completed Successfully!')
