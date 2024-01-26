import json
from urllib.parse import unquote_plus
import boto3

s3_client = boto3.client('s3')

def get_subdirectories(client, bucket, prefix, delimiter='/'):
    paginator = client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter=delimiter)

    subdirectories = set()
    for page in page_iterator:
        subdirs = page.get('CommonPrefixes')
        if subdirs:
            for subdir in subdirs:
                subdir_name = subdir.get('Prefix')
                if subdir_name:
                    clean_name = unquote_plus(subdir_name)
                    subdirectories.add(clean_name.rstrip(delimiter))
    return list(subdirectories)


def lambda_handler(event, context=None):
    query_params = event.get('queryStringParameters')
    cityname = query_params.get('city') if query_params else None

    if not cityname:
        return {
            'statusCode': 400,
            'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Credentials': 'true',
                },
            'error': 'City not specified'
        }

    try:
        bucket_name = 'imagestoreserverless'
        base_prefix = f'data/{cityname}/'

        # Get the first level of subdirectories (dates)
        dates = get_subdirectories(s3_client, bucket_name, base_prefix)

        # Organize the subdirectories by dates
        organized_subdirs = {}
        for date_dir in dates:
            date_prefix = f"{date_dir}/"
            subdirs = get_subdirectories(s3_client, bucket_name, date_prefix)
            subdirs = [subdir.split('/')[-1] for subdir in subdirs]
            organized_subdirs[date_dir.split('/')[-1]] = subdirs
        return_json = {
            'statusCode': 200,
            'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Credentials': 'true',
                },
            'body': json.dumps(organized_subdirs)
        }
    except Exception as e:
        return_json = {
            'statusCode': 500,
            'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Credentials': 'true',
                },
            'error': f'An error occurred: {e}'
        }
    
    print('returning:')
    print(json.dumps(return_json))
    return return_json