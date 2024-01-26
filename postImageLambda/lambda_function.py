import psycopg2
import os
from datetime import datetime
import boto3
import base64
import jwt
from jwt.exceptions import DecodeError

# Lambda handler function
def lambda_handler(event, context):
    bucket_name = "imagestoreserverless"
    region_name = "us-east-1"

    s3 = boto3.resource('s3', region_name=region_name)

    # Establish a connection to the database
    conn = psycopg2.connect(
        dbname=os.environ['DB_NAME'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD'],
        host=os.environ['DB_HOST'],
        port=os.environ['DB_PORT']
    )

    try:
        # Extract event data
        cityname = event.get('cityname')
        timestamp = event.get('timestamp', datetime.utcnow().isoformat())
        air_pressure = event.get('air_pressure')
        temperature = event.get('temperature')
        humidity = event.get('humidity')
        image_decoded = base64.b64decode(event.get('image'))
        token = event.get('token')

        # Verify the token, if it is invalid, an exception will be raised
        decode = jwt.decode(token, os.environ['SECRET_TOKEN'], algorithms=['HS256'])
        if decode['token'] != 'valid':
            raise DecodeError("Invalid token")
        print(decode)

        # Upload file to S3
        file_name = f"upload/{cityname}_{timestamp}.png"
        s3.Bucket(bucket_name).put_object(Key=file_name, Body=image_decoded)

        # Insert data into the 'weatherdata' table
        insert_sql = "INSERT INTO weatherdata (cityname, timestamp, air_pressure, humidity, temperature) VALUES (%s, %s, %s, %s, %s)"
        with conn.cursor() as cur:
            cur.execute(insert_sql, (cityname, timestamp, air_pressure, humidity, temperature))
            conn.commit()

        # Return a success response
        return {
            'statusCode': 200,
            'body': f"Successfully inserted weather data for {cityname}, saved file to {file_name}"
        }
    except DecodeError as e:
        # Return an error response
        return {
            'statusCode': 401,
            'body': f"Invalid token"
        }
    except Exception as e:
        # Return an error response
        return {
            'statusCode': 500,
            'body': f"Failed to insert weather data for {cityname} at {timestamp}. Error: {str(e)}"
        }
    finally:
        # Close the database connection
        conn.close()
