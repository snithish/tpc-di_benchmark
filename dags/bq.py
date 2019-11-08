from datetime import timedelta, datetime

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
}

with DAG('v1_8_bigquery', schedule_interval=timedelta(days=1),
         default_args=default_args) as dag:
    bq_extract_one_day = BigQueryOperator(
        task_id='values_in_account',
        bql='queries/select_account.sql',
        bigquery_conn_id='bigquery_default',
        use_legacy_sql=False,
        location='US'
    )