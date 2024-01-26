import psycopg2
import boto3
import io
import os
import matplotlib.pyplot as plt
from urllib.parse import unquote_plus

s3_client = boto3.client(
    service_name='s3',
    region_name='us-east-1')

# Establish a connection to the database
conn = psycopg2.connect(
    dbname=os.environ['DB_NAME'],
    user=os.environ['DB_USER'],
    password=os.environ['DB_PASSWORD'],
    host=os.environ['DB_HOST'],
    port=os.environ['DB_PORT']
)


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

def get_plot_data(new_weather_data):
    plot_data = dict()
    for city, date, hour in new_weather_data:
        # Get data from the database
        cur = conn.cursor()
        # my date looks like this: 2024-01-30
        year, month, day = date.split('-')
        query = f"""
        SELECT air_pressure, humidity, temperature FROM weatherdata
                WHERE cityname='{city}' AND
                    EXTRACT(YEAR FROM timestamp)={year} AND
                    EXTRACT(MONTH FROM timestamp)={month} AND
                    EXTRACT(DAY FROM timestamp)={day} AND
                    EXTRACT(HOUR FROM timestamp)={hour}
                    ORDER BY timestamp;
        """
        cur.execute(query)
        data = cur.fetchall()
        cur.close()
        air_pressure_data = [row[0] for row in data]
        humidity_data = [row[1] for row in data]
        temperature_data = [row[2] for row in data]
        plot_data[(city, date, hour)] = (air_pressure_data, humidity_data, temperature_data)
    print(plot_data)
    return plot_data


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

# creates plot and returns buffer
def create_plot(title, xlabel, ylabel, data, color):
    fig_width, fig_height = 5, 3.5
    dpi = 100
    fig, ax = plt.subplots(figsize=(fig_width, fig_height))
    ax.plot(range(1, len(data) + 1), data, color=color)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    plt.tight_layout()

    # Save the plot to a buffer
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=dpi)
    buf.seek(0)
    return buf

def create_and_save_plots(plot_data):
    for (city, date, hour), (air_pressure_data, humidity_data, temperature_data) in plot_data.items():
        # Create the plots
        air_pressure_plot = create_plot('Air Pressure', 'Time', 'Air Pressure', air_pressure_data, 'blue')
        humidity_plot = create_plot('Humidity', 'Time', 'Humidity', humidity_data, 'red')
        temperature_plot = create_plot('Temperature', 'Time', 'Temperature', temperature_data, 'green')

        print('created plots')

        plot_path = f'data_plots/{city}/{date}/{hour}_plots'
        # Save the plots to S3
        s3_client.put_object(Bucket='imagestoreserverless', Key=f'{plot_path}/1.png', Body=air_pressure_plot)
        s3_client.put_object(Bucket='imagestoreserverless', Key=f'{plot_path}/2.png', Body=humidity_plot)
        s3_client.put_object(Bucket='imagestoreserverless', Key=f'{plot_path}/3.png', Body=temperature_plot)
        
        print('saved plots')

def lambda_handler(event, context):
    bucket_name = "imagestoreserverless"

    # [(city, date, hour), ...]
    new_weather_data = []
    try:
        # Get the first level of subdirectories (cities)
        cities_paths = get_subdirectories(s3_client, bucket_name, 'temp/')
        cities = [city.split('/')[-1] for city in cities_paths]
        
        # Get the second level of subdirectories (dates)
        for city in cities:
            dates_paths = get_subdirectories(s3_client, bucket_name, f'temp/{city}/')
            dates = [date.split('/')[-1] for date in dates_paths]
            
            # Get the third level of subdirectories (hours)
            for date in dates:
                hours_paths = get_subdirectories(s3_client, bucket_name, f'temp/{city}/{date}/')
                hours = [hour.split('/')[-1] for hour in hours_paths]

                # Get the fourth level - the images and move them to the data directory
                for hour in hours:
                    images_paths = get_images(s3_client, bucket_name, f'temp/{city}/{date}/{hour}/')
                    images = [image.split('/')[-1] for image in images_paths]

                    # Add the data to the new_weather_data list for plot generation
                    new_weather_data.append((city, date, hour))
                    
                    # move the images to the data directory
                    for image in images:
                        s3_client.copy_object(Bucket=bucket_name, CopySource=f'{bucket_name}/temp/{city}/{date}/{hour}/{image}', Key=f'data/{city}/{date}/{hour}/{image}')
                        s3_client.delete_object(Bucket=bucket_name, Key=f'temp/{city}/{date}/{hour}/{image}')
                        print(f'deleted and moved temp/{city}/{date}/{hour}/{image}')

        # Generate and save the plots
        if new_weather_data:
            # {('cityname', 'date', 'hour'): ([air_pressure], [humidity], [temperature]), ...}
            plot_data = get_plot_data(new_weather_data)
            create_and_save_plots(plot_data)
        
        return {
            'statusCode': 200,
            'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Credentials': 'true',
                },
            'body': 'success'
        }

    except Exception as e:
        print(e)
        return {
            'statusCode': 500,
            'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Credentials': 'true',
                },
            'error': 'Internal Server Error'
        }