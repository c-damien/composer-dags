# Simple end to end example of a Cloud Composer v2 DAG that integrates GCS, dataflow and BigQuery


## Process overall
1. Listen to a GCS bucket for a new file  ( in this case the employees.csv)
2. Launch a Dataflow pipeline that:
2.1 Read from the GCS bucket the new CSV file
2.2 Parse it and prepare it to be ingested in BigQuery
2.3 Push it in a table in BigQuery
3. Execute a query in BigQuery to perform some aggregation after data have been loaded to demonstrate how an ELT pipeline could be orchestrated
4. Move the processed GCS file to a different bucket

## Files
|Filename|Where to store|description|
| ------------- |:-------------:| -----:|
|df-dag-e2e.py |DAG folder of composer, available from your composer instance|Whole pipeline (need to be updated to reflect the different paths: dataflow pipeline, buckets)|
|employees.csv |GCS bucket shared with Dataflow Service account | Test sample raw CSV data|
|gcs2bq.py| In a GCS bucket accessible by Dataflow | Dataflow pipeline to ingest data in BigQuery (need to be updated with the right path to employees.csv and bigquery table)|
|launch_remote.sh| Next to gcs2bq.py| Shell script to launch in standalone the dataflow pipeline (test purpose)|

## Settings to adjust
Most settings can be adjusted by going into the main composer dag `df-dag-e2e.py` and change the settings in the `parameters` section 

