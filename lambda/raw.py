"""
Lambda function to gather data from an API.

It stores the raw data in S3, infers the schema, convert to parquet and then saves to s3 in new place.
"""

import boto3
from datetime import datetime, timezone
import json
import os
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
    
    
def get_local_datetime() -> datetime:
    """The function returns a datetime object with the AEST timezone"""

    import pytz
    now = datetime.now(pytz.timezone('Australia/Melbourne'))
    return now


def get_secret() -> json:
    """Retrives the API ket from AWS Secrets manager"""
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(
        SecretId='my_tdf_test/api_key'
    )
    return response['SecretString']


def save_raw_data(s3_client, response, dt, s3_bucket) -> json:
    """Saves the raw data direct from the API into S3 in case there is an issue processing the later steps"""

    try:
        json_obj = response.json()
        s3_key = f's3://{s3_bucket}/raw/{dt.year}/{dt.month}/{dt.day}/{dt.hour}.json'

        json_text = json.dumps(json_obj)

        with s3_client.open(s3_key, 'w') as f:
            json.dump(json_text, f)
        print(f'Raw API response saved to s3://{s3_bucket}/raw/')

    except ValueError:
        print('API response does not contain properly formed JSON. Exiting.')

    return json_obj, s3_key


def call_api():
    """Function that calls the API"""

    key = get_secret()
    url = 'http://api.weatherapi.com/v1/current.json?key={}&q=-37.504136, 145.744302&aqi=no'.format(key)
    response = requests.get(url)
    return response


def handler(event, context) -> dict:
    """Handler function used to run the code for AWS Labmda.

        event: -> Returned with status and s3_key
        context: -> Not utilised
    """
    return_obj = {"event": event, "status": "SUCCEEDED"}

    s3_client = s3fs.S3FileSystem()

    test_status = False
    retries = 3
    iterations = 0

    raw_bucket = os.getenv('raw_bucket')

    dt = get_local_datetime()

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
        return_obj['status'] = "FAILED"
        
    json_obj, s3_key = save_raw_data(s3_client, response, dt, raw_bucket)
    return_obj['s3_key'] = s3_key

    return return_obj
