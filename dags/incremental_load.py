from datetime import datetime

from airflow import DAG

from utils import construct_gcs_to_bq_operator, get_file_path

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

with DAG('incremental_load', schedule_interval=None, default_args=default_args) as dag:


    load_daily_market_file_to_staging = construct_gcs_to_bq_operator('load_daily_market_to_staging',
                                                                     get_file_path(True, 'DailyMarket'), [
                                                                         {"name": "CDC_FLAG", "type": "STRING",
                                                                          "mode": "REQUIRED"},
                                                                         {"name": "CDC_DSN", "type": "INTEGER",
                                                                          "mode": "REQUIRED"},
                                                                         {"name": "DM_DATE", "type": "DATE",
                                                                          "mode": "REQUIRED"},
                                                                         {"name": "DM_S_SYMB", "type": "STRING",
                                                                          "mode": "REQUIRED"},
                                                                         {"name": "DM_CLOSE", "type": "NUMERIC",
                                                                          "mode": "REQUIRED"},
                                                                         {"name": "DM_HIGH", "type": "NUMERIC",
                                                                          "mode": "REQUIRED"},
                                                                         {"name": "DM_LOW", "type": "NUMERIC",
                                                                          "mode": "REQUIRED"},
                                                                         {"name": "DM_VOL", "type": "INTEGER",
                                                                          "mode": "REQUIRED"}],
                                                                     'staging.daily_market')


    load_watch_history_file_to_staging = construct_gcs_to_bq_operator('load_watch_history_to_staging',
                                                                      get_file_path(True, 'WatchHistory'), [
                                                                          {"name": "CDC_FLAG", "type": "STRING",
                                                                           "mode": "REQUIRED"},
                                                                          {"name": "CDC_DSN", "type": "INTEGER",
                                                                           "mode": "REQUIRED"},
                                                                          {"name": "W_C_ID", "type": "INTEGER",
                                                                           "mode": "REQUIRED"},
                                                                          {"name": "W_S_SYMB", "type": "STRING",
                                                                           "mode": "REQUIRED"},
                                                                          {"name": "W_DTS", "type": "DATETIME",
                                                                           "mode": "REQUIRED"},
                                                                          {"name": "W_ACTION", "type": "STRING",
                                                                           "mode": "NULLABLE"}],
                                                                      'staging.watch_history')
