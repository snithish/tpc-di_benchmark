from datetime import datetime

from airflow import DAG

from constants import CSV_EXTENSION
from utils import construct_gcs_to_bq_operator, get_file_path, insert_overwrite, reset_table

AIRFLOW = 'airflow'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

with DAG('historical_load', schedule_interval=None, default_args=default_args) as dag:
    load_date_file_to_master = construct_gcs_to_bq_operator('load_date_to_master', get_file_path(False, 'Date'), [
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
        {"name": "HolidayFlag", "type": "BOOLEAN", "mode": "NULLABLE"}], 'master.dim_date')

    load_time_file_to_master = construct_gcs_to_bq_operator('load_time_to_master', get_file_path(False, 'Time'), [
        {"name": "SK_TimeID", "type": "INT64", "mode": "REQUIRED"},
        {"name": "TimeValue", "type": "STRING", "mode": "REQUIRED"},
        {"name": "HourID", "type": "INT64", "mode": "REQUIRED"},
        {"name": "HourDesc", "type": "STRING", "mode": "REQUIRED"},
        {"name": "MinuteID", "type": "INT64", "mode": "REQUIRED"},
        {"name": "MinuteDesc", "type": "STRING", "mode": "REQUIRED"},
        {"name": "SecondID", "type": "INT64", "mode": "REQUIRED"},
        {"name": "SecondDesc", "type": "STRING", "mode": "REQUIRED"},
        {"name": "MarketHoursFlag", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "OfficeHoursFlag", "type": "BOOLEAN", "mode": "NULLABLE"}], 'master.dim_time')

    load_industry_file_to_master = construct_gcs_to_bq_operator('load_industry_to_master',
                                                                get_file_path(False, 'Industry'), [
                                                                    {"name": "IN_ID", "type": "STRING",
                                                                     "mode": "REQUIRED"},
                                                                    {"name": "IN_NAME", "type": "STRING",
                                                                     "mode": "REQUIRED"},
                                                                    {"name": "IN_SC_ID", "type": "STRING",
                                                                     "mode": "REQUIRED"}], 'master.industry')

    load_status_type_file_to_master = construct_gcs_to_bq_operator('load_status_type_to_master',
                                                                   get_file_path(False, 'StatusType'), [
                                                                       {"name": "ST_ID", "type": "STRING",
                                                                        "mode": "REQUIRED"},
                                                                       {"name": "ST_NAME", "type": "STRING",
                                                                        "mode": "REQUIRED"}], 'master.status_type')

    load_tax_rate_file_to_master = construct_gcs_to_bq_operator('load_tax_rate_to_master',
                                                                get_file_path(False, 'TaxRate'), [
                                                                    {"name": "TX_ID", "type": "STRING",
                                                                     "mode": "REQUIRED"},
                                                                    {"name": "TX_NAME", "type": "STRING",
                                                                     "mode": "REQUIRED"},
                                                                    {"name": "TX_RATE", "type": "NUMERIC",
                                                                     "mode": "REQUIRED"}], 'master.tax_rate')

    load_trade_type_file_to_master = construct_gcs_to_bq_operator('load_trade_type_to_master',
                                                                  get_file_path(False, 'TradeType'), [
                                                                      {"name": "TT_ID", "type": "STRING",
                                                                       "mode": "REQUIRED"},
                                                                      {"name": "TT_NAME", "type": "STRING",
                                                                       "mode": "REQUIRED"},
                                                                      {"name": "TT_IS_SELL", "type": "INT64",
                                                                       "mode": "REQUIRED"},
                                                                      {"name": "TT_IS_MRKT", "type": "INT64",
                                                                       "mode": "REQUIRED"}], 'master.trade_type')

    load_hr_file_to_staging = construct_gcs_to_bq_operator('load_hr_to_staging',
                                                           get_file_path(False, 'HR', CSV_EXTENSION), [
                                                               {"name": "EmployeeID", "type": "INTEGER",
                                                                "mode": "REQUIRED"},
                                                               {"name": "ManagerID", "type": "INTEGER",
                                                                "mode": "REQUIRED"},
                                                               {"name": "EmployeeFirstName", "type": "STRING",
                                                                "mode": "REQUIRED"},
                                                               {"name": "EmployeeLastName", "type": "STRING",
                                                                "mode": "REQUIRED"},
                                                               {"name": "EmployeeMI", "type": "STRING",
                                                                "mode": "NULLABLE"},
                                                               {"name": "EmployeeJobCode", "type": "INTEGER",
                                                                "mode": "NULLABLE"},
                                                               {"name": "EmployeeBranch", "type": "STRING",
                                                                "mode": "NULLABLE"},
                                                               {"name": "EmployeeOffice", "type": "STRING",
                                                                "mode": "NULLABLE"},
                                                               {"name": "EmployeePhone", "type": "STRING",
                                                                "mode": "NULLABLE"}], 'staging.hr')
    transform_hr_to_broker = insert_overwrite(task_id='transform_hr_to_broker',
                                              sql_file_path='queries/transform_load_dim_broker.sql',
                                              destination_table='master.dim_broker')

    load_batch_date_from_file = construct_gcs_to_bq_operator('load_batch_date_from_file',
                                                             get_file_path(False, 'BatchDate'), [
                                                                 {"name": "BatchDate", "type": "DATE",
                                                                  "mode": "REQUIRED"}
                                                             ], 'staging.batch_date')

    reset_batch_number = reset_table(table_name='batch_number')

    [load_date_file_to_master, load_hr_file_to_staging] >> transform_hr_to_broker
