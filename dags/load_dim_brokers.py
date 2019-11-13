from datetime import datetime

from airflow import DAG

from constants import CSV_EXTENSION
from utils import construct_gcs_to_bq_operator, get_file_path, execute_sql

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

with DAG('load_dim_brokers', schedule_interval=None, default_args=default_args) as dag:
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
    transform_hr_to_broker = execute_sql('transform_hr_to_broker', 'queries/transform_load_dim_broker.sql')

    load_hr_file_to_staging >> transform_hr_to_broker
