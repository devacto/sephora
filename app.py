import json

from flask import Flask
from flask import request
from flask import jsonify
from auth import make_iap_request
from google.cloud import bigquery

app = Flask(__name__)

def trigger_dag(data, context=None):
    client_id = '42483088270-ft3gbqtelgd11koguhu3vgpe3v0upj2e.apps.googleusercontent.com'
    dag_name = 'product_table'
    webserver_url = (
        'https://b9a698264010f93e8-tp.appspot.com/api/experimental/dags/'
        + dag_name
        + '/dag_runs'
    )
    make_iap_request(webserver_url, client_id, method='POST', json=data)

def create_insert_statement(table, fields, values):
    fields_str = ', '.join(fields)
    values_str = ', '.join(values)
    result = 'INSERT INTO `raw.{}` ({}) VALUES ({});'.format(table, fields_str, values_str)
    return result

def sql_statement_from_args(args, table_name):
    data = args
    fields = []
    values = []
    for k, v in data.items():
        fields.append(k)
        values.append('{}'.format(v))
    query = create_insert_statement(table_name, fields, values)
    return query


@app.route('/run', methods=['POST'])
def trigger_airflow_dag():
    response = 'Running product_table DAG.'
    trigger_dag({})
    return jsonify({'status': response})


@app.route('/<table_name>', methods=['POST'])
def add_new_record(table_name):
    status = 200
    query = sql_statement_from_args(request.args, table_name)
    message = 'Success'

    try:
        client = bigquery.Client()
        query_job = client.query(query)
        _ = query_job.result()
    except Exception as e:
        raw_message = str(e).split('\n')[0]
        message = raw_message[4:]
        status = int(raw_message[:3])

    return jsonify({'http_status': '{}'.format(status), 'message': message, 
        'query': query}), status


if __name__ == '__main__':
    app.run(debug=False, use_reloader=False)
