from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

from utils import construct_gcs_to_bq_operator, get_file_path, CSV_EXTENSION


def load_prospect_file_to_staging(incremental: bool) -> GoogleCloudStorageToBigQueryOperator:
    return construct_gcs_to_bq_operator('load_prospect_historical_to_staging',
                                 get_file_path(incremental, 'Prospect', CSV_EXTENSION), [
                                     {"name": "AgencyID", "type": "STRING",
                                      "mode": "REQUIRED"},
                                     {"name": "LastName", "type": "STRING",
                                      "mode": "REQUIRED"},
                                     {"name": "FirstName", "type": "STRING",
                                      "mode": "REQUIRED"},
                                     {"name": "MiddleInitial", "type": "STRING",
                                      "mode": "NULLABLE"},
                                     {"name": "Gender", "type": "STRING",
                                      "mode": "NULLABLE"},
                                     {"name": "AddressLine1", "type": "STRING",
                                      "mode": "NULLABLE"},
                                     {"name": "AddressLine2", "type": "STRING",
                                      "mode": "NULLABLE"},
                                     {"name": "PostalCode", "type": "STRING",
                                      "mode": "NULLABLE"},
                                     {"name": "City", "type": "STRING",
                                      "mode": "REQUIRED"},
                                     {"name": "State", "type": "STRING",
                                      "mode": "REQUIRED"},
                                     {"name": "Country", "type": "STRING",
                                      "mode": "NULLABLE"},
                                     {"name": "Phone", "type": "STRING",
                                      "mode": "NULLABLE"},
                                     {"name": "Income", "type": "INTEGER",
                                      "mode": "NULLABLE"},
                                     {"name": "NumberCars", "type": "INTEGER",
                                      "mode": "NULLABLE"},
                                     {"name": "NumberChildren", "type": "INTEGER",
                                      "mode": "NULLABLE"},
                                     {"name": "MaritalStatus", "type": "STRING",
                                      "mode": "NULLABLE"},
                                     {"name": "Age", "type": "INTEGER",
                                      "mode": "NULLABLE"},
                                     {"name": "CreditRating", "type": "INTEGER",
                                      "mode": "NULLABLE"},
                                     {"name": "OwnOrRentFlag", "type": "STRING",
                                      "mode": "NULLABLE"},
                                     {"name": "Employer", "type": "STRING",
                                      "mode": "NULLABLE"},
                                     {"name": "NumberCreditCards", "type": "INTEGER",
                                      "mode": "NULLABLE"},
                                     {"name": "NetWorth", "type": "INTEGER",
                                      "mode": "NULLABLE"}], 'staging.prospect')
