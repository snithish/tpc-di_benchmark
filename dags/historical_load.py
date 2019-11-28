from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.dummy_operator import DummyOperator

from common_tasks import load_prospect_file_to_staging
from constants import CSV_EXTENSION, GCS_BUCKET, BIG_QUERY_CONN_ID, GOOGLE_CLOUD_DEFAULT
from utils import construct_gcs_to_bq_operator, get_file_path, insert_overwrite, reset_table, insert_if_empty, \
    execute_sql

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

    reference_data_load_complete = DummyOperator(task_id='reference_data_load_complete')

    [load_date_file_to_master, load_hr_file_to_staging] >> transform_hr_to_broker

    [load_date_file_to_master, load_time_file_to_master, load_industry_file_to_master, load_status_type_file_to_master,
     load_tax_rate_file_to_master, load_trade_type_file_to_master, transform_hr_to_broker, load_batch_date_from_file,
     reset_batch_number] >> reference_data_load_complete

    # Reference data loading done

    # Load FINWIRE to master dimensions
    load_finwire_staging = GoogleCloudStorageToBigQueryOperator(
        task_id='load_finwire_files_to_staging',
        bucket=GCS_BUCKET,
        source_objects=['historical/FINWIRE*'],
        schema_fields=[
            {"name": "row", "type": "STRING", "mode": "REQUIRED"}],
        field_delimiter=',',
        destination_project_dataset_table='staging.finwire',
        write_disposition='WRITE_TRUNCATE',
        autodetect=False,
        bigquery_conn_id=BIG_QUERY_CONN_ID,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_DEFAULT,
        ignore_unknown_values=False
    )

    load_cmp_records_staging = insert_overwrite(task_id='extract_cmp_records_from_finwire',
                                                sql_file_path='queries/transform_finwire_to_cmp.sql',
                                                destination_table='staging.cmp_records')

    load_dim_company_from_cmp_records = insert_if_empty(task_id='load_dim_company_from_cmp_records',
                                                        sql_file_path='queries/load_cmp_records_to_dim_company.sql',
                                                        destination_table='master.dim_company')

    process_error_cmp_records = execute_sql(task_id='process_cmp_records_error',
                                            sql_file_path="queries/process_cmp_records_error.sql")

    load_sec_records_staging = insert_overwrite(task_id='extract_sec_records_from_finwire',
                                                sql_file_path='queries/transform_finwire_to_sec.sql',
                                                destination_table='staging.sec_records')

    load_dim_security_from_sec_records = insert_if_empty(task_id='load_dim_security_from_sec_records',
                                                         sql_file_path='queries/load_sec_records_to_dim_security.sql',
                                                         destination_table='master.dim_security')

    load_fin_records_staging = insert_overwrite(task_id='extract_fin_records_from_finwire',
                                                sql_file_path='queries/transform_finwire_to_fin.sql',
                                                destination_table='staging.fin_records')

    load_dim_finance_from_fin_records = insert_if_empty(task_id='load_dim_finance_from_fin_records',
                                                        sql_file_path='queries/load_fin_records_to_financial.sql',
                                                        destination_table='master.financial')

    recreate_dim_company = reset_table('dim_company')
    recreate_dim_security = reset_table('dim_security')
    recreate_financial = reset_table('financial')

    finwire_data_load_complete = DummyOperator(task_id='finwire_data_load_complete')

    reference_data_load_complete >> load_finwire_staging
    load_cmp_records_staging >> recreate_dim_company >> load_dim_company_from_cmp_records
    load_cmp_records_staging >> [process_error_cmp_records, load_dim_company_from_cmp_records]
    load_finwire_staging >> [load_cmp_records_staging, load_sec_records_staging, load_fin_records_staging]
    [load_dim_company_from_cmp_records,
     load_sec_records_staging] >> recreate_dim_security >> load_dim_security_from_sec_records
    [load_dim_company_from_cmp_records,
     load_fin_records_staging] >> recreate_financial >> load_dim_finance_from_fin_records

    [load_dim_company_from_cmp_records, load_dim_security_from_sec_records,
     load_dim_finance_from_fin_records] >> finwire_data_load_complete

    # FINWIRE data loading done

    # Load Customer Management to master dimensions

    load_customer_management_staging = GoogleCloudStorageToBigQueryOperator(
        task_id='load_customer_management_file_to_staging',
        bucket=GCS_BUCKET,
        source_objects=['historical/CustomerMgmt.json'],
        source_format='NEWLINE_DELIMITED_JSON',
        destination_project_dataset_table='staging.customer_management',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        bigquery_conn_id=BIG_QUERY_CONN_ID,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_DEFAULT,
        ignore_unknown_values=False
    )

    load_customer_from_customer_management = insert_overwrite(
        task_id='load_customer_from_customer_management',
        sql_file_path='queries'
                      '/load_customer_records_from_customer_management.sql',
        destination_table='staging.customer_historical')

    load_account_historical_from_customer_management = insert_overwrite(
        task_id='load_account_historical_from_customer_management',
        sql_file_path='queries'
                      '/load_account_records_from_customer_management.sql',
        destination_table='staging.account_historical')

    load_dim_prospect_from_staging_historical = insert_if_empty(
        task_id='load_dim_prospect_from_staging_historical',
        sql_file_path='queries/load_prospect_historical_to_dim_prospect.sql',
        destination_table='master.prospect')

    load_dim_customer_from_staging_customer_historical = insert_if_empty(
        task_id='load_dim_customer_from_staging_customer_historical',
        sql_file_path='queries/load_dim_customer_from_staging_customer_historical.sql',
        destination_table='master.dim_customer')

    load_dim_account_from_staging_account_historical = insert_if_empty(
        task_id='load_dim_account_from_staging_account_historical',
        sql_file_path='queries/load_dim_account_from_staging_account_historical.sql',
        destination_table='master.dim_account')

    process_error_customer_historical_records = execute_sql(
        task_id='process_error_customer_historical_records',
        sql_file_path="queries/process_customer_historical_error.sql")

    recreate_prospect = reset_table('prospect')
    recreate_dim_account = reset_table('dim_account')
    recreate_dim_customer = reset_table('dim_customer')

    load_prospect_file_to_staging = load_prospect_file_to_staging(incremental=False)

    customer_account_data_load_complete = DummyOperator(task_id='customer_account_data_load_complete')

    reference_data_load_complete >> [load_customer_management_staging, load_prospect_file_to_staging]

    load_customer_management_staging >> [load_customer_from_customer_management,
                                         load_account_historical_from_customer_management]

    [load_customer_from_customer_management,
     load_prospect_file_to_staging] >> recreate_prospect >> load_dim_prospect_from_staging_historical

    load_customer_from_customer_management >> process_error_customer_historical_records

    [load_customer_from_customer_management,
     load_dim_prospect_from_staging_historical] >> recreate_dim_customer >> load_dim_customer_from_staging_customer_historical

    [load_account_historical_from_customer_management,
     load_dim_customer_from_staging_customer_historical] >> recreate_dim_account >> load_dim_account_from_staging_account_historical

    [load_dim_account_from_staging_account_historical,
     load_dim_customer_from_staging_customer_historical,
     load_dim_prospect_from_staging_historical] >> customer_account_data_load_complete

    # Customer Management done

    # Trade loading to master dimension

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

    recreate_dim_trade = reset_table('dim_trade')

    load_dim_trade_from_historical = insert_if_empty(task_id="load_dim_trade_from_historical",
                                                     sql_file_path="queries/load_trade_historical_to_dim_trade.sql",
                                                     destination_table="master.dim_trade")

    trade_data_load_complete = DummyOperator(task_id='trade_data_load_complete')

    [customer_account_data_load_complete, finwire_data_load_complete, load_trade_history_to_staging,
     load_trade_to_staging] >> recreate_dim_trade >> load_dim_trade_from_historical >> trade_data_load_complete

    dimension_loading_complete = DummyOperator(task_id='dimension_loading_complete')

    [customer_account_data_load_complete, finwire_data_load_complete,
     trade_data_load_complete] >> dimension_loading_complete

    # All dimension loading is complete

    # Start Fact Loading

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

    dimension_loading_complete >> load_cash_transactions_to_staging >> recreate_fact_cash_balances >> load_fact_cash_balances_from_staging_history
    dimension_loading_complete >> load_holding_history_historical_to_staging >> recreate_fact_holdings >> load_fact_holding_from_staging_history
    dimension_loading_complete >> load_watch_history_historical_to_staging >> recreate_fact_watches >> load_fact_watches_from_staging_watch_history
    dimension_loading_complete >> load_daily_market_to_staging >> create_intermediary_table_daily_market >> recreate_fact_market_history >> load_fact_market_history_from_staging_market_history_transformed
