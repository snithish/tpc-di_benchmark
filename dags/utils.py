from constants import *

from typing import List, Dict

from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator


def construct_gcs_to_bq_operator(task_id: str, source_objects: List[str], schema_fields: List[Dict],
                                 destination_project_dataset_table: str) -> GoogleCloudStorageToBigQueryOperator:
    return GoogleCloudStorageToBigQueryOperator(
        task_id=task_id,
        bucket=GCS_BUCKET,
        source_objects=source_objects,
        schema_fields=schema_fields,
        field_delimiter='|',
        destination_project_dataset_table=destination_project_dataset_table,
        write_disposition='WRITE_TRUNCATE',
        autodetect=False,
        bigquery_conn_id=BIG_QUERY_CONN_ID,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_DEFAULT,
        ignore_unknown_values=False
    )


def get_file_path(incremental: bool, filename: str, extension: str = 'txt') -> List[str]:
    folder_path = "incremental"
    if not incremental:
        folder_path = "history"

    return ["{}/{}.{}".format(folder_path, filename, extension)]
