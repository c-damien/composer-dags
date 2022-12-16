# composer-dags
Examples of dags for GCP Cloud Composer v2 (Managed Airflow).

sub directory | Description |
--- | --- 
[gcs-df-bq](https://github.com/c-damien/composer-dags/tree/main/gcs-df-bq)| DAG that listen for a new CSV file in GCS then launch a dataflow pipeline to read and prepare the file to ingest into BigQuery then launch a query to perform an ELT type of data aggregation|

