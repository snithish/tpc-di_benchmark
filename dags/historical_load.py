from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

GOOGLE_CLOUD_DEFAULT = 'google_cloud_default'

BIG_QUERY_CONN_ID = 'bigquery_default'

GCS_BUCKET = 'tpc-di_data'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

with DAG('historical_load', schedule_interval=None, default_args=default_args) as dag:
    load_date_file_to_staging = GoogleCloudStorageToBigQueryOperator(
        task_id='load_date_to_staging',
        bucket=GCS_BUCKET,
        source_objects=['Batch1/Date.txt'],
        schema_fields=[{"name": "SK_DateID", "type": "INT64", "mode": "REQUIRED"},
                       {"name": "DateValue", "type": "DATE", "mode": "REQUIRED"},
                       {"name": "DateDesc", "type": "STRING", "mode": "REQUIRED"},
                       {"name": "CalendarYearID", "type": "INT64", "mode": "REQUIRED"},
                       {"name": "CalendarYearDesc", "type": "STRING", "mode": "REQUIRED"},
                       {"name": "CalendarQtrID", "type": "INT64", "mode": "REQUIRED"},
                       {"name": "CalendarQtrDesc", "type": "STRING", "mode": "REQUIRED"},
                       {"name": "CalendarMonthID", "type": "INT64", "mode": "REQUIRED"},
                       {"name": "CalendarMonthDesc", "type": "STRING", "mode": "REQUIRED"},
                       {"name": "CalendarWeekID", "type": "INT64", "mode": "REQUIRED"},
                       {"name": "CalendarWeekDesc", "type": "STRING", "mode": "REQUIRED"},
                       {"name": "DayOfWeekNum", "type": "INT64", "mode": "REQUIRED"},
                       {"name": "DayOfWeekDesc", "type": "STRING", "mode": "REQUIRED"},
                       {"name": "FiscalYearID", "type": "INT64", "mode": "REQUIRED"},
                       {"name": "FiscalYearDesc", "type": "STRING", "mode": "REQUIRED"},
                       {"name": "FiscalQtrID", "type": "INT64", "mode": "REQUIRED"},
                       {"name": "FiscalQtrDesc", "type": "STRING", "mode": "REQUIRED"},
                       {"name": "HolidayFlag", "type": "BOOLEAN", "mode": "NULLABLE"}],
        field_delimiter='|',
        destination_project_dataset_table='staging.date',
        write_disposition='WRITE_TRUNCATE',
        autodetect=False,
        bigquery_conn_id=BIG_QUERY_CONN_ID,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_DEFAULT,
        ignore_unknown_values=False
    )
