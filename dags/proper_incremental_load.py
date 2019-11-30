from datetime import datetime

from airflow import DAG

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

    update_customer_status_prospect = execute_sql(
        task_id="update_customer_status_prospect",
        sql_file_path='queries/incremental/update_is_customer_flag_prospect.sql')

    add_history_tracking_record_for_customer_in_account = execute_sql(
        task_id="add_history_tracking_record_for_customer_in_account",
        sql_file_path='queries/incremental/resync_dim_account_with_new_customer_sk.sql')

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

    merge_master_dim_account_with_staging_account = execute_sql(
        task_id="merge_master_dim_account_with_staging_account",
        sql_file_path='queries/incremental/merge_staging_account_with_master_dim_account.sql')

    load_trade_file_to_staging = construct_gcs_to_bq_operator('load_trade_to_staging',
                                                              get_file_path(True, 'Trade'), [
                                                                  {"name": "CDC_FLAG", "type": "STRING",
                                                                   "mode": "REQUIRED"},
                                                                  {"name": "CDC_DSN", "type": "INTEGER",
                                                                   "mode": "REQUIRED"},
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
                                                                   "mode": "NULLABLE"}],
                                                              'staging.trade')

    merge_master_dim_trade_with_staging_trade = execute_sql(
        task_id="merge_master_dim_trade_with_staging_trade",
        sql_file_path='queries/incremental/merge_staging_trade_with_master_dim_trade.sql')

    # Facts
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

    merge_master_fact_cash_balances_with_staging_cash_transactions = execute_sql(
        task_id="merge_master_fact_cash_balances_with_staging_cash_transactions",
        sql_file_path='queries/incremental/merge_staging_cash_transaction_with_master_fact_cash_balances.sql')

    load_holding_history_file_to_staging = construct_gcs_to_bq_operator('load_holding_history_to_staging',
                                                                        get_file_path(True, 'HoldingHistory'), [
                                                                            {"name": "CDC_FLAG", "type": "STRING",
                                                                             "mode": "REQUIRED"},
                                                                            {"name": "CDC_DSN", "type": "INTEGER",
                                                                             "mode": "REQUIRED"},
                                                                            {"name": "HH_H_T_ID", "type": "INTEGER",
                                                                             "mode": "REQUIRED"},
                                                                            {"name": "HH_T_ID", "type": "INTEGER",
                                                                             "mode": "REQUIRED"},
                                                                            {"name": "HH_BEFORE_QTY", "type": "INTEGER",
                                                                             "mode": "REQUIRED"},
                                                                            {"name": "HH_AFTER_QTY", "type": "INTEGER",
                                                                             "mode": "REQUIRED"}],
                                                                        'staging.holding_history')

    merge_master_fact_holdings_with_staging_holding_history = execute_sql(
        task_id="merge_master_fact_holdings_with_staging_holding_history",
        sql_file_path='queries/incremental/merge_staging_holding_history_with_master_fact_holdings.sql')

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

    merge_master_fact_watches_with_staging_watch_history = execute_sql(
        task_id="merge_master_fact_watches_with_staging_watch_history",
        sql_file_path='queries/incremental/merge_staging_watch_history_with_master_fact_watches.sql')

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

    merge_master_fact_market_history_with_staging_daily_market = execute_sql(
        task_id="merge_master_fact_market_history_with_staging_daily_market",
        sql_file_path='queries/incremental/append_incremental_market_history.sql')

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

    # Account Related tasks
    [load_batch_date_from_file, update_batch_id,
     load_account_file_to_staging] >> merge_master_dim_account_with_staging_account
    merge_master_dim_customer_with_staging_dim_customer >> add_history_tracking_record_for_customer_in_account
    add_history_tracking_record_for_customer_in_account >> merge_master_dim_account_with_staging_account

    # Trade Related tasks
    [load_batch_date_from_file, update_batch_id,
     load_trade_file_to_staging] >> merge_master_dim_trade_with_staging_trade
    merge_master_dim_account_with_staging_account >> merge_master_dim_trade_with_staging_trade

    # Fact Cash Balances Related tasks
    [load_batch_date_from_file, update_batch_id,
     load_cash_transaction_file_to_staging] >> merge_master_fact_cash_balances_with_staging_cash_transactions
    merge_master_dim_account_with_staging_account >> merge_master_fact_cash_balances_with_staging_cash_transactions

    # Fact Holding Related tasks
    [load_batch_date_from_file, update_batch_id,
     load_holding_history_file_to_staging] >> merge_master_fact_holdings_with_staging_holding_history
    merge_master_dim_trade_with_staging_trade >> merge_master_fact_holdings_with_staging_holding_history

    # Fact Watches Related tasks
    [load_batch_date_from_file, update_batch_id,
     load_watch_history_file_to_staging] >> merge_master_fact_watches_with_staging_watch_history
    merge_master_dim_customer_with_staging_dim_customer >> merge_master_fact_watches_with_staging_watch_history

    # Fact Market History Related tasks

    [load_batch_date_from_file, update_batch_id,
     load_daily_market_file_to_staging] >> merge_master_fact_market_history_with_staging_daily_market
