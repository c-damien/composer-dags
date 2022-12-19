# composer dag examples
Examples of dags for GCP Cloud Composer v2 (Managed Airflow).

sub directory | products| Description |
--- | --- | --- 
[gcs-df-bq](https://github.com/c-damien/composer-dags/tree/main/gcs-df-bq) | Composer, GCS, Dataflow, BigQuery | DAG that listen for a new CSV file in GCS then launch a dataflow pipeline to read and prepare the file to ingest into BigQuery then launch a query to perform an ELT type of data aggregation |
[gcs-spark-bq](https://github.com/c-damien/composer-dags/tree/main/gcs-spark-bq) | Composer, GCS, Spark, BigQuery | Dag launch a spark serverless (Dataproc) batch job and push the data in `traffic_dataset.csv` to BigQuery table |



