from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

from constants import GCS_BUCKET, BIG_QUERY_CONN_ID, GOOGLE_CLOUD_DEFAULT

AIRFLOW = 'airflow'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

with DAG('load_cmp_records', schedule_interval=None, default_args=default_args) as dag:
    load_finwire_staging = GoogleCloudStorageToBigQueryOperator(
        task_id='load_finwire_files_to_staging',
        bucket=GCS_BUCKET,
        source_objects=['historical/FINWIRE*'],
        schema_fields=[
            {"name": "row", "type": "STRING", "mode": "REQUIRED"}],
        field_delimiter=',',
        destination_project_dataset_table='staging.finwire',
        write_disposition='WRITE_TRUNCATE',
        autodetect=False,
        bigquery_conn_id=BIG_QUERY_CONN_ID,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_DEFAULT,
        ignore_unknown_values=False
    )