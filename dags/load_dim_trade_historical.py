from datetime import datetime

from airflow import DAG

from utils import construct_gcs_to_bq_operator, get_file_path

AIRFLOW = 'airflow'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

with DAG('load_dim_trade_historical', schedule_interval=None, default_args=default_args) as dag:
    load_trade_to_staging = construct_gcs_to_bq_operator('load_trade_to_staging',
                                                         get_file_path(False, 'Trade'), [
                                                             {"name": "T_ID", "type": "INTEGER",
                                                              "mode": "REQUIRED"},
                                                             {"name": "T_DTS", "type": "DATETIME",
                                                              "mode": "REQUIRED"},
                                                             {"name": "T_ST_ID", "type": "STRING",
                                                              "mode": "REQUIRED"},
                                                             {"name": "T_TT_ID", "type": "STRING",
                                                              "mode": "REQUIRED"},
                                                             {"name": "T_IS_CASH", "type": "BOOLEAN",
                                                              "mode": "NULLABLE"},
                                                             {"name": "T_S_SYMB", "type": "STRING",
                                                              "mode": "REQUIRED"},
                                                             {"name": "T_QTY", "type": "INTEGER",
                                                              "mode": "NULLABLE"},
                                                             {"name": "T_BID_PRICE", "type": "NUMERIC",
                                                              "mode": "NULLABLE"},
                                                             {"name": "T_CA_ID", "type": "INTEGER",
                                                              "mode": "REQUIRED"},
                                                             {"name": "T_EXEC_NAME", "type": "STRING",
                                                              "mode": "REQUIRED"},
                                                             {"name": "T_TRADE_PRICE", "type": "NUMERIC",
                                                              "mode": "NULLABLE"},
                                                             {"name": "T_CHRG", "type": "NUMERIC",
                                                              "mode": "NULLABLE"},
                                                             {"name": "T_COMM", "type": "NUMERIC",
                                                              "mode": "NULLABLE"},
                                                             {"name": "T_TAX", "type": "NUMERIC",
                                                              "mode": "NULLABLE"}], 'staging.trade_historical')

    load_trade_history_to_staging = construct_gcs_to_bq_operator('load_trade_history_to_staging',
                                                                 get_file_path(False, 'TradeHistory'), [
                                                                     {"name": "TH_T_ID", "type": "INTEGER",
                                                                      "mode": "REQUIRED"},
                                                                     {"name": "TH_DTS", "type": "DATETIME",
                                                                      "mode": "REQUIRED"},
                                                                     {"name": "TH_ST_ID", "type": "STRING",
                                                                      "mode": "REQUIRED"}],
                                                                 'staging.trade_history_historical')
