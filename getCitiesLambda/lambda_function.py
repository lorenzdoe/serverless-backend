import json
import os
import psycopg2
import redis


def fetch_distinct_citynames():
    print('try fetching from redis')
    
    r = redis.Redis(
        host=os.environ['REDIS_HOST'],
        port=15743,
        password=os.environ['REDIS_PASSWORD'])
    
    res = r.get('citynames')
    if res:
        print('cache hit')
        res = res.decode('utf-8').split(',')
        return res
    
    print('try fetching from db')
    # # Establish a connection to the database
    conn = psycopg2.connect(
        dbname=os.environ['DB_NAME'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD'],
        host=os.environ['DB_HOST'],
        port=os.environ['DB_PORT']
    )

    # SQL statement to execute
    select_sql = "SELECT DISTINCT cityname FROM weatherdata"

    # Execute the SQL statement
    with conn.cursor() as cur:
        cur.execute(select_sql)
        rows = cur.fetchall()
        citynames = [row[0] for row in rows]
    conn.close()

    # set citynames with expiration of 600 seconds (10 minutes)
    r.set('citynames', ','.join(citynames), ex=600)
    print('cache miss')

    return citynames

def lambda_handler(event, context):
    try:
        citynames = fetch_distinct_citynames()
        response = {
            'statusCode': 200,
            'body': citynames
        }
    except Exception as e:
        print(e)
        response = {
            'statusCode': 500,
            'body': 'something went wrong'
        }
    return response