from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="gcs_to_bigquery_load",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    start = EmptyOperator(task_id="start")

    load_csv_to_bq = GCSToBigQueryOperator(
        task_id="load_csv_to_bq",
        bucket="bkt-demo-33",                  # Bucket name as a string
        source_objects=["customer.csv"],       # File in the bucket
        destination_project_dataset_table="project-b7277fe2-8bfe-4e91-8a2.composer.customer",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        autodetect=True,
        gcp_conn_id="google_cloud_default",    # Optional if using default connection
    )

    end = EmptyOperator(task_id="end")

    end << load_csv_to_bq << start