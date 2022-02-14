from datetime import datetime
import boto3
import requests
from collections.abc import Mapping
import os
from dateutil.parser import parse
import pyarrow as pa
import pyarrow.parquet as pq
import time
import s3fs
import json

def is_date(string, fuzzy=False):
    """
    Return whether the string can be interpreted as a date.

    :param string: str, string to check for date
    :param fuzzy: bool, ignore unknown tokens in string if True
    """
    try: 
        parse(string, fuzzy=fuzzy)
        return True
    except ValueError:
        return False
    
    
def infer_schema(columns, values):
    data_types = []

    for key, val in enumerate(values):
        if val is None:
            data_types.append((columns[key], pa.null()))
        elif isinstance(val, int):
            data_types.append((columns[key], pa.int32()))
        elif isinstance(val, float):
            data_types.append((columns[key], pa.float32()))
        else:
            data_types.append((columns[key], pa.string()))

    return data_types
    
def transform_data(json_data):

    cols = []
    row_data = []

    for k, v in json_data.items():
        for kk, vv in v.items():
            if not isinstance(json_data[k][kk], Mapping):
                cols.append(kk)
                row_data.append(vv)
            else:
                for sub_key, value in json_data[k][kk].items():
                    cols.append(sub_key)
                    row_data.append(value)
                    
    return cols, row_data


def get_secret():
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(
        SecretId='tdf_test/api_key'
    )

def call_api():
    key = get_secret()
    url = 'http://api.weatherapi.com/v1/current.json?key={}&q=-37.504136, 145.744302&aqi=no'.format(key)
    response = requests.get(url)
    return response

def handler(event, context):
    
    s3 = s3fs.S3FileSystem()

    test_status = False
    retries = 3
    iterations = 0

    s3_bucket = os.getenv('destination_bucket')

    while not test_status and iterations < retries:
        response = call_api()
        if response.status_code != 200:
            print(f'API not responding, status code: {response.status_code}')        
            time.sleep(3)
        else:
            test_status = True
            print('Response OK. Continuing.')
        iterations += 1

    if not test_status:
        print(f'API did not respond. Will retry at next scheduled interval.')
        
    try:
        json_obj = response.json()
        s3_key = f's3://{s3_bucket}/raw/{datetime.now().year}/{datetime.now().month}/{datetime.now().day}/{datetime.now().hour}.json'

        json_text = json.dumps(json_obj)

        with s3.open(s3_key, 'w') as f:
            json.dump(json_text, f)
        print(f'API response saved to s3://{s3_bucket}/raw/')

    except ValueError:
        print('API response does not contain properly formed JSON. Exiting.')

    columns, values = transform_data(json_obj)
    parquet_schema = infer_schema(columns, values)

    parquet_data = [pa.array([values[k]]) for k,v in enumerate(columns)]

    batch = pa.RecordBatch.from_arrays(
        parquet_data,
        schema = pa.schema(parquet_schema)
    )

    table = pa.Table.from_batches([batch])

    s3_key = f's3://{s3_bucket}/curated/{datetime.now().year}/{datetime.now().month}/{datetime.now().day}/{datetime.now().hour}/weather.parquet'

    with s3.open(s3_key, 'wb') as f:
        pq.write_table(table, f)
        
    print(f'Parquet file saved to s3://{s3_bucket}/curated/')