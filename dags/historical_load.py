from datetime import datetime
from typing import List, Dict

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


def construct_gcs_to_bq_operator(task_id: str, source_objects: List[str], schema_fields: List[Dict],
                                 destination_project_dataset_table: str) -> GoogleCloudStorageToBigQueryOperator:
    return GoogleCloudStorageToBigQueryOperator(
        task_id=task_id,
        bucket=GCS_BUCKET,
        source_objects=source_objects,
        schema_fields=schema_fields,
        field_delimiter='|',
        destination_project_dataset_table=destination_project_dataset_table,
        write_disposition='WRITE_TRUNCATE',
        autodetect=False,
        bigquery_conn_id=BIG_QUERY_CONN_ID,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_DEFAULT,
        ignore_unknown_values=False
    )


with DAG('historical_load', schedule_interval=None, default_args=default_args) as dag:
    load_date_file_to_master = construct_gcs_to_bq_operator('load_date_to_staging', ['Batch1/Date.txt'], [
        {"name": "SK_DateID", "type": "INT64", "mode": "REQUIRED"},
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
        {"name": "HolidayFlag", "type": "BOOLEAN", "mode": "NULLABLE"}], 'master.date')

    load_time_file_to_master = construct_gcs_to_bq_operator('load_time_to_staging', ['Batch1/Time.txt'], [
        {"name": "INT64imeID", "type": "INT64", "mode": "REQUIRED"},
        {"name": "TimeValue", "type": "STRING", "mode": "REQUIRED"},
        {"name": "HourID", "type": "INT64", "mode": "REQUIRED"},
        {"name": "HourDesc", "type": "STRING", "mode": "REQUIRED"},
        {"name": "MinuteID", "type": "INT64", "mode": "REQUIRED"},
        {"name": "MinuteDesc", "type": "STRING", "mode": "REQUIRED"},
        {"name": "SecondID", "type": "INT64", "mode": "REQUIRED"},
        {"name": "SecondDesc", "type": "STRING", "mode": "REQUIRED"},
        {"name": "MarketHoursFlag", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "OfficeHoursFlag", "type": "BOOLEAN", "mode": "NULLABLE"}], 'master.time')

    load_industry_file_to_master = construct_gcs_to_bq_operator('load_industry_to_master', ['Batch1/Industry.txt'], [
        {"name": "IN_ID", "type": "STRING", "mode": "REQUIRED"},
        {"name": "IN_NAME", "type": "STRING", "mode": "REQUIRED"},
        {"name": "IN_SC_ID", "type": "STRING", "mode": "REQUIRED"}], 'master.industry')

    load_status_type_file_to_master = construct_gcs_to_bq_operator('load_status_type_to_master',
                                                                   ['Batch1/StatusType.txt'], [
                                                                       {"name": "ST_ID", "type": "STRING",
                                                                        "mode": "REQUIRED"},
                                                                       {"name": "ST_NAME", "type": "STRING",
                                                                        "mode": "REQUIRED"}], 'master.status_type')

    load_tax_rate_file_to_master = construct_gcs_to_bq_operator('load_tax_rate_to_master',
                                                                ['Batch1/TaxRate.txt'], [
                                                                    {"name": "TX_ID", "type": "STRING",
                                                                     "mode": "REQUIRED"},
                                                                    {"name": "TX_NAME", "type": "STRING",
                                                                     "mode": "REQUIRED"},
                                                                    {"name": "TX_RATE", "type": "NUMERIC",
                                                                     "mode": "REQUIRED"}], 'master.tax_rate')
