from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

from constants import GCS_BUCKET, BIG_QUERY_CONN_ID, GOOGLE_CLOUD_DEFAULT
from utils import insert_overwrite, execute_sql, insert_if_empty

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

    load_cmp_records_staging = insert_overwrite(task_id='extract_cmp_records_from_finwire',
                                                sql_file_path='queries/transform_finwire_to_cmp.sql',
                                                destination_table='staging.cmp_records')

    load_dim_company_from_cmp_records = insert_if_empty(task_id='load_dim_company_from_cmp_records',
                                                        sql_file_path='queries/load_cmp_records_to_dim_company.sql',
                                                        destination_table='master.dim_company')

    process_error_cmp_records = execute_sql(task_id='process_cmp_records_error',
                                            sql_file_path="queries/process_cmp_records_error.sql")

    load_sec_records_staging = insert_overwrite(task_id='extract_sec_records_from_finwire',
                                                sql_file_path='queries/transform_finwire_to_sec.sql',
                                                destination_table='staging.sec_records')

    load_dim_security_from_sec_records = insert_if_empty(task_id='load_dim_security_from_sec_records',
                                                         sql_file_path='queries/load_sec_records_to_dim_security.sql',
                                                         destination_table='master.dim_security')

    load_fin_records_staging = insert_overwrite(task_id='extract_fin_records_from_finwire',
                                                sql_file_path='queries/transform_finwire_to_fin.sql',
                                                destination_table='staging.fin_records')

    load_dim_finance_from_fin_records = insert_if_empty(task_id='load_dim_finance_from_fin_records',
                                                        sql_file_path='queries/load_fin_records_to_financial.sql',
                                                        destination_table='master.financial')

    load_cmp_records_staging >> [process_error_cmp_records, load_dim_company_from_cmp_records]
    load_finwire_staging >> [load_cmp_records_staging, load_sec_records_staging, load_fin_records_staging]
    [load_dim_company_from_cmp_records, load_sec_records_staging] >> load_dim_security_from_sec_records
    [load_dim_company_from_cmp_records, load_fin_records_staging] >> load_dim_finance_from_fin_records
