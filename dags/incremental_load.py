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
    load_account_file_to_staging = construct_gcs_to_bq_operator('load_account_to_staging',
                                                                get_file_path(True, 'Account'), [
                                                                    {"name": "CDC_FLAG", "type": "STRING",
                                                                     "mode": "REQUIRED"},
                                                                    {"name": "CDC_DSN", "type": "INT64",
                                                                     "mode": "REQUIRED"},
                                                                    {"name": "CA_ID", "type": "INT64",
                                                                     "mode": "REQUIRED"},
                                                                    {"name": "CA_B_ID", "type": "INT64",
                                                                     "mode": "REQUIRED"},
                                                                    {"name": "CA_C_ID", "type": "INT64",
                                                                     "mode": "REQUIRED"},
                                                                    {"name": "CA_NAME", "type": "STRING",
                                                                     "mode": "NULLABLE"},
                                                                    {"name": "CA_TAX_ST", "type": "INT64",
                                                                     "mode": "REQUIRED"},
                                                                    {"name": "CA_ST_ID", "type": "STRING",
                                                                     "mode": "REQUIRED"}], 'staging.account')
    load_cash_transaction_file_to_staging = construct_gcs_to_bq_operator('load_cash_transaction_to_staging',
                                                                         get_file_path(True, 'CashTransaction'), [
                                                                             {"name": "CDC_FLAG", "type": "STRING",
                                                                              "mode": "REQUIRED"},
                                                                             {"name": "CDC_DSN", "type": "INTEGER",
                                                                              "mode": "REQUIRED"},
                                                                             {"name": "CT_CA_ID", "type": "INTEGER",
                                                                              "mode": "REQUIRED"},
                                                                             {"name": "CT_DTS", "type": "DATETIME",
                                                                              "mode": "REQUIRED"},
                                                                             {"name": "CT_AMT", "type": "FLOAT",
                                                                              "mode": "REQUIRED"},
                                                                             {"name": "CT_NAME", "type": "STRING",
                                                                              "mode": "REQUIRED"}],
                                                                         'staging.cash_transaction')
