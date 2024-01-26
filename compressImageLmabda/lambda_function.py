import json
import urllib.parse
import boto3
from PIL import Image
import io
from datetime import datetime

print('Loading function')

s3 = boto3.client('s3')


def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    download_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    # 'upload/cityname_2023-01-01T11:20:32.png'
    try:
        # get object and save as Image
        response = s3.get_object(Bucket=bucket, Key=download_key)
        image = Image.open(response['Body'])
        size = image.size
        
        # compress image
        resized = image.resize((int(size[0]/2), int(size[1]/2)))
        
        # save to in memory file
        in_mem_file = io.BytesIO()
        resized.save(in_mem_file, format=image.format)
        in_mem_file.seek(0)
        
        # upload object
        # !!!!! DANGER !!!!! never upload to upload/* from this lambda
        cityname, timestamp = download_key.split('/')[1].split('.')[0].split('_')
        dt = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S')
        upload_key = f"temp/{cityname}/{dt.year}-{dt.month}-{dt.day}/{dt.hour}/{dt.minute}.png"
        s3.put_object(Bucket='imagestoreserverless', Key=upload_key, Body=in_mem_file)
        
        # delete object
        s3.delete_object(Bucket=bucket, Key=download_key)

        print(f'successfully compressed and saved image to {upload_key}')
        return {
            'statusCode': 200,
            'message': f'successfully compressed and saved image to {upload_key}'
        }
    except Exception as e:
        print(e)
        return {
            'statusCode': 400,
            'message': f'Error: {e}'
        }
              
