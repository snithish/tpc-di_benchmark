from datetime import datetime

from airflow import DAG

from utils import construct_gcs_to_bq_operator, get_file_path, reset_table, insert_if_empty, insert_overwrite

AIRFLOW = 'airflow'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

with DAG('load_facts_historical', schedule_interval=None, default_args=default_args) as dag:
    load_cash_transactions_to_staging = construct_gcs_to_bq_operator('load_cash_transactions_to_staging',
                                                                     get_file_path(False, 'CashTransaction'),
                                                                     [{"name": "CT_CA_ID", "type": "INTEGER",
                                                                       "mode": "REQUIRED"},
                                                                      {"name": "CT_DTS", "type": "DATETIME",
                                                                       "mode": "REQUIRED"},
                                                                      {"name": "CT_AMT", "type": "FLOAT",
                                                                       "mode": "REQUIRED"},
                                                                      {"name": "CT_NAME", "type": "STRING",
                                                                       "mode": "REQUIRED"}],
                                                                     'staging.cash_transaction_historical')
    recreate_fact_cash_balances = reset_table('fact_cash_balances')

    load_fact_cash_balances_from_staging_history = insert_if_empty(
        task_id="load_fact_cash_balances_from_staging_history",
        sql_file_path='queries/load_cash_balances_historical_to_dim_cash_balances.sql',
        destination_table='master.fact_cash_balances')

    load_holding_history_historical_to_staging = construct_gcs_to_bq_operator(
        'load_holding_history_historical_to_staging',
        get_file_path(False, 'HoldingHistory'),
        [{"name": "HH_H_T_ID", "type": "INTEGER",
          "mode": "REQUIRED"},
         {"name": "HH_T_ID", "type": "INTEGER",
          "mode": "REQUIRED"},
         {"name": "HH_BEFORE_QTY", "type": "INTEGER",
          "mode": "REQUIRED"},
         {"name": "HH_AFTER_QTY", "type": "INTEGER",
          "mode": "REQUIRED"}],
        'staging.holding_history_historical')

    recreate_fact_holdings = reset_table('fact_holdings')

    load_fact_holding_from_staging_history = insert_if_empty(
        task_id="load_fact_holding_from_staging_history",
        sql_file_path='queries/load_holdings_historical_to_fact_holdings.sql',
        destination_table='master.fact_holdings')

    load_watch_history_historical_to_staging = construct_gcs_to_bq_operator(
        'load_watch_history_historical_to_staging',
        get_file_path(False, 'WatchHistory'),
        [{"name": "W_C_ID", "type": "INTEGER", "mode": "REQUIRED"},
         {"name": "W_S_SYMB", "type": "STRING", "mode": "REQUIRED"},
         {"name": "W_DTS", "type": "DATETIME", "mode": "REQUIRED"},
         {"name": "W_ACTION", "type": "STRING", "mode": "REQUIRED"}],
        'staging.watch_history_historical')

    recreate_fact_watches = reset_table('fact_watches')

    load_fact_watches_from_staging_watch_history = insert_if_empty(
        task_id="load_fact_watches_from_staging_watch_history",
        sql_file_path='queries/load_watch_history_historical_to_fact_watches.sql',
        destination_table='master.fact_watches')

    load_daily_market_to_staging = construct_gcs_to_bq_operator('load_daily_market_to_staging',
                                                                get_file_path(False, 'DailyMarket'),
                                                                [{"name": "DM_DATE", "type": "DATE",
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
                                                                'staging.daily_market_historical')

    create_intermediary_table_daily_market = insert_overwrite(task_id='transform_to_52_week_stats',
                                                              sql_file_path='queries/transform_daily_market_historical_52_week.sql',
                                                              destination_table='staging.daily_market_historical_transformed')

    recreate_fact_market_history = reset_table('fact_market_history')

    load_fact_market_history_from_staging_market_history_transformed = insert_if_empty(
        task_id="load_fact_market_history_from_staging_market_history_transformed",
        sql_file_path='queries/load_fact_market_history_from_historical.sql',
        destination_table='master.fact_market_history')

load_cash_transactions_to_staging >> recreate_fact_cash_balances >> load_fact_cash_balances_from_staging_history
load_holding_history_historical_to_staging >> recreate_fact_holdings >> load_fact_holding_from_staging_history
load_watch_history_historical_to_staging >> recreate_fact_watches >> load_fact_watches_from_staging_watch_history
load_daily_market_to_staging >> create_intermediary_table_daily_market >> recreate_fact_market_history >> load_fact_market_history_from_staging_market_history_transformed
