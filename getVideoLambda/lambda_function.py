import json
from urllib.parse import unquote_plus
from test import test_event
import boto3

s3_client = boto3.client(
    service_name='s3',
    region_name='us-east-1'
)

def get_images(client, bucket, prefix, delimiter='/'):
    paginator = client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter=delimiter)

    images = set()
    for page in page_iterator:
        objs = page.get('Contents')
        if objs:
            for obj in objs:
                obj_name = obj.get('Key')
                if obj_name:
                    clean_name = unquote_plus(obj_name)
                    images.add(clean_name.rstrip(delimiter))
    return list(images)

# Lambda handler function
def lambda_handler(event, context):
    query_params = event.get('queryStringParameters')
    city = query_params.get('city') if query_params else None
    date = query_params.get('date') if query_params else None
    hour = query_params.get('hour') if query_params else None

    base_url = f"https://imagestoreserverless.s3.amazonaws.com/"

    # new
    bucket_name = "imagestoreserverless"
    image_urls = get_images(s3_client, bucket_name, f'data/{city}/{date}/{hour}/')
    image_urls = [base_url + url for url in image_urls]
    image_urls.sort(key=lambda x: int(x.split('/')[-1].split('.')[0].split('_')[-1]))


    plot_urls = get_images(s3_client, bucket_name, f'data_plots/{city}/{date}/{hour}_plots/')
    plot_urls = [base_url + url for url in plot_urls]

    if not image_urls:
        return {
            'statusCode': 404,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Credentials': 'true'
            },
            'body': f"Could not find data for {city}, {date}, {hour}"
        }

    response_data = {
        "images": image_urls,
        "plots": plot_urls
    }

    return_json = {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': 'true'
        },
        'body': json.dumps(response_data)
    }
    return return_json

# Test the function by invoking it locally
result = lambda_handler(test_event, None)

# Print the returned result
print(json.dumps(result['body'], indent=2))