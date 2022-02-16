"""
Lambda function to gather data from an API.

It stores the raw data in S3, infers the schema, convert to parquet and then saves to s3 in new place.
"""

import boto3
from collections.abc import Mapping
from datetime import datetime, timezone
from dateutil.parser import parse
import json
import os
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import s3fs
import time


def is_date(string, fuzzy=False) -> bool:
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
    
    
def infer_schema(columns, values) -> list:
    """Checks data types of values in a list and infers the parquet datatypes"""

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
    

def transform_data(json_data) -> (list, list):
    """Flattens a json object down to a list"""

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


def get_local_datetime() -> datetime:
    """The function returns a datetime object with the AEST timezone"""

    import pytz
    now = datetime.now(pytz.timezone('Australia/Melbourne'))
    return now


def save_curated_data(s3_client, table, dt, s3_bucket) -> None:
    """Saves the curated parquet version of the data in S3"""

    s3_key = f's3://{s3_bucket}/curated/{dt.year}/{dt.month}/{dt.day}/{dt.hour}/weather.parquet'

    with s3_client.open(s3_key, 'wb') as f:
        pq.write_table(table, f)
        
    print(f'Parquet file saved to s3://{s3_bucket}/curated/')


def generate_parquet_table(json_obj) -> pa.Table:
    """Converts the original JSON data into Parquet format for improved queryability"""

    columns, values = transform_data(json_obj)
    parquet_schema = infer_schema(columns, values)

    parquet_data = [pa.array([values[k]]) for k,v in enumerate(columns)]

    batch = pa.RecordBatch.from_arrays(
        parquet_data,
        schema = pa.schema(parquet_schema)
    )

    table = pa.Table.from_batches([batch])

    return table


def handler(event, context) -> dict:
    """Handler function used to run the code for AWS Labmda.

        event: -> Returned with status
        context: -> Not utilised
    """
    return_obj = {"event": event, "status": "SUCCEEDED"}

    s3_client = s3fs.S3FileSystem()

    raw_bucket = os.getenv('raw_bucket')
    curated_bucket = os.getenv('curated_bucket')

    dt = get_local_datetime()

    with s3_client.open(event['s3_key'], 'rb') as f:
        try:
             json_obj = json.loads(json.loads(f.read()))
        except:
            return_obj['status'] = "FAILED"
            return return_obj

    table = generate_parquet_table(json_obj)

    save_curated_data(s3_client, table, dt, curated_bucket)

    return return_obj
