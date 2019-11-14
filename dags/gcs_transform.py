from datetime import datetime

from airflow import models
from airflow.contrib.operators.gcs_download_operator import GoogleCloudStorageDownloadOperator
from airflow.operators.python_operator import PythonOperator

from transformations import transform_account

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

with models.DAG("gcs_transform", default_args=default_args, schedule_interval=None) as dag:
    download_file = GoogleCloudStorageDownloadOperator(
        task_id="download_file",
        bucket='tpc-di_data',
        object='Batch2/Account.txt',
        filename='account_download.txt',
        google_cloud_storage_conn_id='google_cloud_default'
    )

    transform_file = PythonOperator(
        task_id='run_script',
        python_callable=transform_account.main
    )

    download_file >> transform_file
