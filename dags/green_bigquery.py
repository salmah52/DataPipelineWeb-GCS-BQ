import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from web.operators.WebToGCSHK import WebToGCSHKOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")



DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2019, 1, 1),
    "email": [os.getenv("ALERT_EMAIL", "")],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
ENDPOINT = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
SERVICE = "green"
OBJECT = SERVICE+'_tripdata_{{ dag_run.logical_date.strftime(\'%Y-%m\') }}.csv'
SOURCE_OBJECT = SERVICE+'_tripdata_{{ dag_run.logical_date.strftime(\'%Y-%m\') }}.csv'
DATASET="green_taxi_09"
DATETIME_COLUMN = "lpep_pickup_datetime"
FILE_FORMAT= "CSV"


with DAG(
    dag_id="Load-Green-Taxi-Data-Web-To-GCS-To-BQ",
    description="Job to move data from website to Google Cloud Storage and then from GCS to BigQuery",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 2 * *",
    max_active_runs=1,
    catchup=True,
    tags=["Website-to-GCS-Bucket-to-BQ"],
) as dag:
    start = EmptyOperator(task_id="start")

    download_to_gcs= WebToGCSHKOperator(
        task_id="download_to_gcs",
        endpoint=ENDPOINT,
        destination_path=OBJECT,
        destination_bucket=BUCKET,
        service=SERVICE,
    )
    load_gcs_to_bigquery = GCSToBigQueryOperator(  #gs://taxi-trips009/green/green_tripdata_2019-01.csv
    task_id="load_gcs_to_bigquery",     # URI gs://taxi-trips009/green/green_tripdata_2019-01.csv.gz.csv; 2538)
    bucket=f"{BUCKET}",
    source_objects=[f"{SERVICE}/{OBJECT}"],
    destination_project_dataset_table=f"{PROJECT_ID}.{DATASET}.{SERVICE}_tripdata",
    autodetect=True,
    write_disposition="WRITE_TRUNCATE",
    source_format="CSV",
    max_bad_records=9000000  # Correct argument name
)


    end = EmptyOperator(task_id="end")

    start >> download_to_gcs >> load_gcs_to_bigquery >> end