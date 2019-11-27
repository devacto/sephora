import datetime
import os

from airflow import models
from airflow.contrib.operators import bigquery_get_data
from airflow.contrib.operators import bigquery_operator
from airflow.contrib.operators import bigquery_to_gcs
from airflow.operators import bash_operator
from airflow.operators import email_operator
from airflow.operators import dummy_operator
from airflow.utils import trigger_rule
from datetime import timedelta
from google.cloud import storage

bq_raw_dataset_name = 'raw'
bq_tmp_dataset_name = 'tmp'
bq_final_dataset_name = 'final'

gcp_project = 'sephora-sde-test'
gcs_bucket = 'vwib_sephora_sde_test'
email = 'hi@victor.codes'

# returns a dictionary with file name and the contents.
def read_sql_from_gcs(folder_name, bucket_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    files = bucket.list_blobs(prefix=folder_name + '/')
    files_list = [file.name for file in files if '.' in file.name]
    result = {}
    for f in files_list:
        blob = bucket.get_blob(f)
        content = blob.download_as_string()
        result[f] = content
    return result

today = datetime.datetime.now()
yesterday = today - timedelta(1)

default_dag_args = {
    'start_date': yesterday,
    'email': email,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'project_id': gcp_project
}

with models.DAG(
        'product_table',
        schedule_interval=datetime.timedelta(hours=1),
        default_args=default_dag_args) as dag:

    bq_make_raw_dataset = bash_operator.BashOperator(
        task_id='make_bq_raw_dataset',
        bash_command='bq --location=asia-southeast1 ls {} || bq --location=asia-southeast1 mk {}'.format(
            bq_raw_dataset_name, bq_raw_dataset_name))

    raw_sql_files = read_sql_from_gcs(bq_raw_dataset_name, gcs_bucket)

    bq_start_making_raw_tables = dummy_operator.DummyOperator(
        task_id='start_making_raw_tables'
    )

    bq_end_making_raw_tables = dummy_operator.DummyOperator(
        task_id='end_making_raw_tables'
    )

    for filename in raw_sql_files:
        sql_statement = raw_sql_files[filename].decode()
        table_name = filename.replace('.sql', '')
        table_name = table_name.replace('raw/', '')
        bq_make_raw_tables = bigquery_operator.BigQueryOperator(
            task_id='make_raw_table_{}'.format(table_name),
            sql=sql_statement,
            use_legacy_sql=False,
            location='asia-southeast1'
        )
        bq_start_making_raw_tables >> bq_make_raw_tables
        bq_make_raw_tables >> bq_end_making_raw_tables

    bq_make_raw_dataset >> bq_start_making_raw_tables

    tmp_sql_files = read_sql_from_gcs(bq_tmp_dataset_name, gcs_bucket)

    bq_make_tmp_dataset = bash_operator.BashOperator(
        task_id='make_bq_tmp_dataset',
        bash_command='bq --location=asia-southeast1 ls {} || bq --location=asia-southeast1 mk {}'.format(
            bq_tmp_dataset_name, bq_tmp_dataset_name
        )
    )

    bq_start_making_tmp_tables = dummy_operator.DummyOperator(
        task_id='start_making_tmp_tables'
    )

    bq_end_making_tmp_tables = dummy_operator.DummyOperator(
        task_id='end_making_tmp_tables'
    )

    for f in tmp_sql_files:
        sql_statement = tmp_sql_files[f].decode()
        table_name = f.replace('.sql', '')
        table_name = table_name.replace('tmp/', '')
        bq_make_tmp_tables = bigquery_operator.BigQueryOperator(
            task_id='make_tmp_table_{}'.format(table_name),
            sql=sql_statement,
            use_legacy_sql=False,
            location='asia-southeast1',
            destination_dataset_table=gcp_project + '.tmp.' + table_name,
            write_disposition='WRITE_TRUNCATE'
        )
        bq_start_making_tmp_tables >> bq_make_tmp_tables
        bq_make_tmp_tables >> bq_end_making_tmp_tables

    bq_make_tmp_dataset >> bq_start_making_tmp_tables

    # Connect this to the previous set of tasks for the raw dataset.
    bq_end_making_raw_tables >> bq_make_tmp_dataset

    final_sql_files = read_sql_from_gcs(bq_final_dataset_name, gcs_bucket)

    bq_make_final_dataset = bash_operator.BashOperator(
        task_id='make_bq_final_dataset',
        bash_command='bq --location=asia-southeast1 ls {} || bq --location=asia-southeast1 mk {}'.format(
            bq_final_dataset_name, bq_final_dataset_name
        )
    )

    bq_start_making_final_tables = dummy_operator.DummyOperator(
        task_id='start_making_final_tables'
    )

    bq_end_making_final_tables = dummy_operator.DummyOperator(
        task_id='end_making_final_tables'
    )

    for f in final_sql_files:
        sql_statement = final_sql_files[f].decode()
        table_name = f.replace('.sql', '')
        table_name = table_name.replace('final/', '')
        bq_make_final_tables = bigquery_operator.BigQueryOperator(
            task_id='make_final_table_{}'.format(table_name),
            sql=sql_statement,
            use_legacy_sql=False,
            location='asia-southeast1',
            destination_dataset_table=gcp_project + '.final.' + table_name,
            write_disposition='WRITE_TRUNCATE'
        )
        bq_start_making_final_tables >> bq_make_final_tables
        bq_make_final_tables >> bq_end_making_final_tables

    bq_make_final_dataset >> bq_start_making_final_tables

    # Connect this to the previous set of tasks for the tmp dataset.
    bq_end_making_tmp_tables >> bq_make_final_dataset
