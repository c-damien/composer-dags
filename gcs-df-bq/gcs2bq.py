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

import json
import logging
import time
import traceback
import argparse

# Beam and interactive Beam imports
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.fileio import WriteToFiles

#from apache_beam.runners.interactive.interactive_runner import InteractiveRunner
#import apache_beam.runners.interactive.interactive_beam as ib
#from apache_beam.runners import DataflowRunner

from apache_beam import window

def ingest_to_bq(known_args, options):
    
 
    #options = PipelineOptions(
    #    streaming=True,
    #    project=project,
    #    region=region,
    #    staging_location="gs://streaming-demo-dpd/tmp/staging",
    #    temp_location="gs://streaming-demo-dpd/tmp/temp"
    #)
    logging.info("\n Reading from" + known_args.input)
    logging.info("\n Writting to" + known_args.output)


    with beam.Pipeline(options=options) as p: #p = beam.Pipeline(DataflowRunner() , options=options) #DataflowRunner() InteractiveRunner()
    
      
      pb_data = (p      | "Read file" >> beam.io.ReadFromText('gs://demo-settings/composer_sensed_file/in/employees.csv')
                        | 'Split records' >> beam.Map(lambda x: x.split(','))
                        | 'Prepare Data' >> beam.Map(lambda x: {"EMPLOYEE_ID": int(x[0]), "FIRST_NAME": x[1], "LAST_NAME": x[2], "EMAIL": x[3], "PHONE_NUMBER": x[4], "ROLE": x[5], "SALARY": int(x[6])}) 
                        | 'write to bq' >> beam.io.WriteToBigQuery(table="dataplatformdaydemo:composer_demo.df_results", schema='EMPLOYEE_ID:INT64,FIRST_NAME:string,LAST_NAME:string,EMAIL:string,PHONE_NUMBER:string,ROLE:string,SALARY:INT64',
                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)    
      )

def get_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True, help='Input file in GCS')
    parser.add_argument('--output', required=True, help='Output table in BQ')
    return parser.parse_known_args(argv)


def run(argv=None):
  known_args, pipeline_args = get_args(argv)
  options = PipelineOptions(pipeline_args)
  
  try:
    ingest_to_bq(known_args, options)
    logging.info("\n pipeline is running \n")
  except (KeyboardInterrupt, SystemExit):
    raise
  except:
    logging.info("\n pipeline Exception raised")
    traceback.print_exc()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()