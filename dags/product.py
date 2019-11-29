import datetime
import os
import yaml

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


# creates an array of dependencies list, for example
# [['tmp/item_purchase_prices.sql', 'tmp/variants.sql'],
#  ['tmp/variant_images.sql', 'tmp/variants.sql']]
# that translates to
# 'tmp/item_purchase_prices.sql' >> 'tmp/variants.sql'
# 'tmp/variant_images.sql' >> 'tmp/variants.sql'
def dependency_arrays(sql_statements_list, dep_dictionary):
    start_making = 'start_making'
    end_making = 'end_making'
    roots = []
    leafs = []
    all_files = []
    all_depends_on = []
    all_items = []
    dependencies = []

    d = dep_dictionary
    for item in d:
        if item['file'] not in all_items:
            all_items.append(item['file'])
        if item['file'] not in all_files:
            all_files.append(item['file'])
        if item['depends_on'] not in all_items:
            all_items.append(item['depends_on'])
        if item['depends_on'] not in all_depends_on:
            all_depends_on.append(item['depends_on'])
        dependency = [item['depends_on'], item['file']]
        dependencies.append(dependency)

    roots = [item for item in all_depends_on if item not in all_files]
    leafs = [item for item in all_files if item not in all_depends_on]

    for i in roots:
        dependency = [start_making, i]
        dependencies.append(dependency)

    for i in leafs:
        dependency = [i, end_making]
        dependencies.append(dependency)

    files_list = sql_statements_list
    for i in files_list:
        if i not in all_items:
            d1 = [start_making, i]
            dependencies.append(d1)
            d2 = [i, end_making]
            dependencies.append(d2)

    return dependencies


# create dependencies dictionary from yml file, for example
# [{'file': 'tmp/variants.sql', 'depends_on': 'tmp/item_purchase_prices.sql'},
#  {'file': 'tmp/variants.sql', 'depends_on': 'tmp/variant_images.sql'}]
def dependencies_dictionary(bucket_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob('dependencies.yml')
    s = blob.download_as_string()
    deps = yaml.load(s)
    d_dict = deps['dependencies']
    return d_dict


# returns a dictionary with file name and BigQueryOperator, for example:
# {'tmp/variants.sql': <BigQueryOperatorObject>}
# given dictionary of filename and sql statements as inputs
def create_bq_operator_dict_from(sql_statements_dictionary):
    d = sql_statements_dictionary
    result = dict((k, '') for k in d)
    # iterating through the keys.
    for k in d:
        tid = k.replace('.sql', '')
        tid = tid.replace('tmp/', '')
        b = bigquery_operator.BigQueryOperator(
            task_id='make_tmp_table_{}'.format(tid),
            sql=d[k],
            use_legacy_sql=False,
            location='asia-southeast1',
            destination_dataset_table=gcp_project + '.tmp.' + tid,
            write_disposition='WRITE_TRUNCATE'
        )
        result[k] = b
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

    # dict of filenames and sqls
    tmp_sql_files = read_sql_from_gcs(bq_tmp_dataset_name, gcs_bucket)

    # dict of filenames and BigQueryOperator
    bq_operators = create_bq_operator_dict_from(tmp_sql_files)

    # add the dummy operators into the dictionary
    bq_operators['start_making'] = bq_start_making_tmp_tables
    bq_operators['end_making'] = bq_end_making_tmp_tables

    # dict from dependencies.yml file
    deps_dict = dependencies_dictionary(gcs_bucket)

    # returns a list of files in the directory
    # ['tmp/variants.sql', 'tmp/products.sql']
    tmp_files_list = [keys for keys in tmp_sql_files.keys()]

    dependencies_list = dependency_arrays(tmp_files_list, deps_dict)
    
    for d in dependencies_list:
        # t1 is task one
        # t2 is task two
        # t1 >> t2
        t1 = bq_operators[d[0]]
        t2 = bq_operators[d[1]]
        t1 >> t2

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
