from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

from common_tasks import load_prospect_file_to_staging
from constants import GOOGLE_CLOUD_DEFAULT, BIG_QUERY_CONN_ID, GCS_BUCKET
from utils import insert_overwrite, insert_if_empty, execute_sql, \
    reset_table

AIRFLOW = 'airflow'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}
# Can run independently
with DAG('load_customer_account', schedule_interval=None, default_args=default_args) as dag:
    load_customer_management_staging = GoogleCloudStorageToBigQueryOperator(
        task_id='load_customer_management_file_to_staging',
        bucket=GCS_BUCKET,
        source_objects=['historical/CustomerMgmt.json'],
        source_format='NEWLINE_DELIMITED_JSON',
        destination_project_dataset_table='staging.customer_management',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        bigquery_conn_id=BIG_QUERY_CONN_ID,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_DEFAULT,
        ignore_unknown_values=False
    )

    load_customer_from_customer_management = insert_overwrite(
        task_id='load_customer_from_customer_management',
        sql_file_path='queries'
                      '/load_customer_records_from_customer_management.sql',
        destination_table='staging.customer_historical')

    load_account_historical_from_customer_management = insert_overwrite(
        task_id='load_account_historical_from_customer_management',
        sql_file_path='queries'
                      '/load_account_records_from_customer_management.sql',
        destination_table='staging.account_historical')

    load_dim_prospect_from_staging_historical = insert_if_empty(
        task_id='load_dim_prospect_from_staging_historical',
        sql_file_path='queries/load_prospect_historical_to_dim_prospect.sql',
        destination_table='master.prospect')

    load_dim_customer_from_staging_customer_historical = insert_if_empty(
        task_id='load_dim_customer_from_staging_customer_historical',
        sql_file_path='queries/load_dim_customer_from_staging_customer_historical.sql',
        destination_table='master.dim_customer')

    load_dim_account_from_staging_account_historical = insert_if_empty(
        task_id='load_dim_account_from_staging_account_historical',
        sql_file_path='queries/load_dim_account_from_staging_account_historical.sql',
        destination_table='master.dim_account')

    process_error_customer_historical_records = execute_sql(
        task_id='process_error_customer_historical_records',
        sql_file_path="queries/process_customer_historical_error.sql")

    recreate_prospect = reset_table('prospect')
    recreate_dim_account = reset_table('dim_account')
    recreate_dim_customer = reset_table('dim_customer')

    load_prospect_file_to_staging = load_prospect_file_to_staging(incremental=False)

    [load_customer_management_staging, load_prospect_file_to_staging]

    load_customer_management_staging >> [load_customer_from_customer_management,
                                         load_account_historical_from_customer_management]

    [load_customer_from_customer_management,
     load_prospect_file_to_staging] >> recreate_prospect >> load_dim_prospect_from_staging_historical

    load_customer_from_customer_management >> process_error_customer_historical_records

    [load_customer_from_customer_management,
     load_dim_prospect_from_staging_historical] >> recreate_dim_customer >> load_dim_customer_from_staging_customer_historical

    [load_account_historical_from_customer_management,
     load_dim_customer_from_staging_customer_historical] >> recreate_dim_account >> load_dim_account_from_staging_account_historical
