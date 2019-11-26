from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from common_tasks import load_prospect_file_to_staging
from utils import construct_gcs_to_bq_operator, get_file_path, execute_sql, reset_table, insert_if_empty

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

with DAG('proper_incremental_load', schedule_interval=None, default_args=default_args) as dag:
    load_customer_file_to_staging = construct_gcs_to_bq_operator('load_customer_to_staging',
                                                                 get_file_path(True, 'Customer'), [
                                                                     {"name": "CDC_FLAG", "type": "STRING",
                                                                      "mode": "REQUIRED"},
                                                                     {"name": "CDC_DSN", "type": "INTEGER",
                                                                      "mode": "REQUIRED"},
                                                                     {"name": "C_ID", "type": "INTEGER",
                                                                      "mode": "REQUIRED"},
                                                                     {"name": "C_TAX_ID", "type": "STRING",
                                                                      "mode": "REQUIRED"},
                                                                     {"name": "C_ST_ID", "type": "STRING",
                                                                      "mode": "NULLABLE"},
                                                                     {"name": "C_L_NAME", "type": "STRING",
                                                                      "mode": "REQUIRED"},
                                                                     {"name": "C_F_NAME", "type": "STRING",
                                                                      "mode": "REQUIRED"},
                                                                     {"name": "C_M_NAME", "type": "STRING",
                                                                      "mode": "NULLABLE"},
                                                                     {"name": "C_GNDR", "type": "STRING",
                                                                      "mode": "NULLABLE"},
                                                                     {"name": "C_TIER", "type": "INTEGER",
                                                                      "mode": "NULLABLE"},
                                                                     {"name": "C_DOB", "type": "DATE",
                                                                      "mode": "REQUIRED"},
                                                                     {"name": "C_ADLINE1", "type": "STRING",
                                                                      "mode": "REQUIRED"},
                                                                     {"name": "C_ADLINE2", "type": "STRING",
                                                                      "mode": "NULLABLE"},
                                                                     {"name": "C_ZIPCODE", "type": "STRING",
                                                                      "mode": "REQUIRED"},
                                                                     {"name": "C_CITY", "type": "STRING",
                                                                      "mode": "REQUIRED"},
                                                                     {"name": "C_STATE_PRO", "type": "STRING",
                                                                      "mode": "REQUIRED"},
                                                                     {"name": "C_CTRY", "type": "STRING",
                                                                      "mode": "NULLABLE"},
                                                                     {"name": "C_CTRY_1", "type": "STRING",
                                                                      "mode": "NULLABLE"},
                                                                     {"name": "C_AREA_1", "type": "STRING",
                                                                      "mode": "NULLABLE"},
                                                                     {"name": "C_LOCAL_1", "type": "STRING",
                                                                      "mode": "NULLABLE"},
                                                                     {"name": "C_EXT_1", "type": "STRING",
                                                                      "mode": "NULLABLE"},
                                                                     {"name": "C_CTRY_2", "type": "STRING",
                                                                      "mode": "NULLABLE"},
                                                                     {"name": "C_AREA_2", "type": "STRING",
                                                                      "mode": "NULLABLE"},
                                                                     {"name": "C_LOCAL_2", "type": "STRING",
                                                                      "mode": "NULLABLE"},
                                                                     {"name": "C_EXT_2", "type": "STRING",
                                                                      "mode": "NULLABLE"},
                                                                     {"name": "C_CTRY_3", "type": "STRING",
                                                                      "mode": "NULLABLE"},
                                                                     {"name": "C_AREA_3", "type": "STRING",
                                                                      "mode": "NULLABLE"},
                                                                     {"name": "C_LOCAL_3", "type": "STRING",
                                                                      "mode": "NULLABLE"},
                                                                     {"name": "C_EXT_3", "type": "STRING",
                                                                      "mode": "NULLABLE"},
                                                                     {"name": "C_EMAIL_1", "type": "STRING",
                                                                      "mode": "NULLABLE"},
                                                                     {"name": "C_EMAIL_2", "type": "STRING",
                                                                      "mode": "NULLABLE"},
                                                                     {"name": "C_LCL_TX_ID", "type": "STRING",
                                                                      "mode": "REQUIRED"},
                                                                     {"name": "C_NAT_TX_ID", "type": "STRING",
                                                                      "mode": "REQUIRED"}],
                                                                 'staging.customer')

    load_batch_date_from_file = construct_gcs_to_bq_operator('load_batch_date_from_file',
                                                             get_file_path(True, 'BatchDate'), [
                                                                 {"name": "BatchDate", "type": "DATE",
                                                                  "mode": "REQUIRED"}
                                                             ], 'staging.batch_date')

    update_batch_id = execute_sql(task_id='increment_batch_id', sql_file_path='queries/increment_batch.sql')

    recreate_dim_customer_schema_staging = reset_table(table_name='staging_dim_customer')

    load_staging_dim_customer_from_staging_customer = insert_if_empty(
        task_id='load_staging_dim_customer_from_staging_customer',
        sql_file_path='queries/incremental/load_customer_staging_to_staging_dim_customer.sql',
        destination_table='staging.dim_customer')

    merge_master_dim_customer_with_staging_dim_customer = execute_sql(
        task_id="merge_master_dim_customer_with_staging_dim_customer",
        sql_file_path='queries/incremental/merge_staging_dim_customer_with_master_dim_customer.sql')

    prospect_file_to_staging = load_prospect_file_to_staging(True)

    merge_master_prospect_with_staging_prospect = execute_sql(
        task_id="merge_master_prospect_with_staging_prospect",
        sql_file_path='queries/incremental/merge_staging_prospect_to_master_prospect.sql')

    update_customer_status_prospect = DummyOperator(task_id="update_customer_status_prospect")

    # All Customer related tasks
    # Prospect has to be populated before staging_dim_customer computation for MarketingNameplate
    merge_master_prospect_with_staging_prospect >> load_staging_dim_customer_from_staging_customer
    [load_batch_date_from_file, update_batch_id,
     load_customer_file_to_staging] >> recreate_dim_customer_schema_staging
    recreate_dim_customer_schema_staging >> load_staging_dim_customer_from_staging_customer
    load_staging_dim_customer_from_staging_customer >> merge_master_dim_customer_with_staging_dim_customer
    # End Customer related tasks

    # Prospect Related tasks
    [load_batch_date_from_file, update_batch_id,
     prospect_file_to_staging] >> merge_master_prospect_with_staging_prospect
    [merge_master_prospect_with_staging_prospect,
     merge_master_dim_customer_with_staging_dim_customer] >> update_customer_status_prospect
