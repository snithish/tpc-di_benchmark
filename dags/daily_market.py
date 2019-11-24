from datetime import datetime

from airflow import DAG

from utils import construct_gcs_to_bq_operator, get_file_path, insert_overwrite

AIRFLOW = 'airflow'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

with DAG('daily_market', schedule_interval=None, default_args=default_args) as dag:
    # load_daily_market_to_staging = construct_gcs_to_bq_operator('load_daily_market_to_staging',
    #                                                             get_file_path(False, 'DailyMarket'),
    #                                                             [{"name": "DM_DATE", "type": "DATE",
    #                                                               "mode": "REQUIRED"},
    #                                                              {"name": "DM_S_SYMB", "type": "STRING",
    #                                                               "mode": "REQUIRED"},
    #                                                              {"name": "DM_CLOSE", "type": "NUMERIC",
    #                                                               "mode": "REQUIRED"},
    #                                                              {"name": "DM_HIGH", "type": "NUMERIC",
    #                                                               "mode": "REQUIRED"},
    #                                                              {"name": "DM_LOW", "type": "NUMERIC",
    #                                                               "mode": "REQUIRED"},
    #                                                              {"name": "DM_VOL", "type": "INTEGER",
    #                                                               "mode": "REQUIRED"}],
    #                                                             'staging.daily_market_historical')

    insert_overwrite(task_id='transform_to_52_week_stats',
                     sql_file_path='queries/transform_daily_market_historical_52_week.sql',
                     destination_table='staging.daily_market_historical_transformed')


