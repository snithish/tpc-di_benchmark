from datetime import datetime

from airflow import DAG

from utils import construct_gcs_to_bq_operator

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
    load_date_file_to_master = construct_gcs_to_bq_operator('load_date_to_master', ['Batch1/Date.txt'], [
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

    load_time_file_to_master = construct_gcs_to_bq_operator('load_time_to_master', ['Batch1/Time.txt'], [
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

    load_trade_type_file_to_master = construct_gcs_to_bq_operator('load_trade_type_to_master',
                                                                  ['Batch1/TradeType.txt'], [
                                                                      {"name": "TT_ID", "type": "STRING",
                                                                       "mode": "REQUIRED"},
                                                                      {"name": "TT_NAME", "type": "STRING",
                                                                       "mode": "REQUIRED"},
                                                                      {"name": "TT_IS_SELL", "type": "INT64",
                                                                       "mode": "REQUIRED"},
                                                                      {"name": "TT_IS_MRKT", "type": "INT64",
                                                                       "mode": "REQUIRED"}], 'master.trade_type')
