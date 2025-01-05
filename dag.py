import boto3
import json
import csv
import io
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime

# AWS Credentials
aws_access_key_id=''
aws_secret_access_key=''
AWS_REGION = 'us-east-1'
S3_BUCKET = 'spotify-s3-uq'
INPUT_KEY = 'raw_data/to_process/spotify_raw_2025-01-05 18:54:13.191250.json'
OUTPUT_KEY = 'processed/transformed_data.csv'

# S3 Client Initialization
s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=AWS_REGION
)

def extract_data(**kwargs):
    """Extract JSON data from S3."""
    response = s3_client.get_object(Bucket=S3_BUCKET, Key=INPUT_KEY)
    data = json.loads(response['Body'].read().decode('utf-8'))
    logging.info("Data extracted successfully.")
    # Push the data to XCom
    kwargs['ti'].xcom_push(key='raw_data', value=data)

def transform_data(**kwargs):
    """Transform JSON data into a CSV format."""
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_data', key='raw_data')  # Pull the data from XCom

    csv_buffer = io.StringIO()
    csv_writer = csv.writer(csv_buffer)

    # Write header
    csv_writer.writerow(['Playlist Name', 'Playlist ID', 'Owner Name', 'Total Tracks', 'Image URL'])

    # Write rows
    for playlist in data['items']:
        playlist_name = playlist.get('name', 'N/A')
        playlist_id = playlist.get('id', 'N/A')
        owner_name = playlist.get('owner', {}).get('display_name', 'N/A')
        total_tracks = playlist.get('tracks', {}).get('total', 0)
        image_url = playlist.get('images', [{}])[0].get('url', 'N/A')
        csv_writer.writerow([playlist_name, playlist_id, owner_name, total_tracks, image_url])
    
    logging.info("Data transformed successfully.")
    csv_buffer.seek(0)
    # Push transformed data to XCom
    ti.xcom_push(key='csv_data', value=csv_buffer.getvalue())

def load_data(**kwargs):
    """Load the transformed data back into S3."""
    ti = kwargs['ti']
    csv_data = ti.xcom_pull(task_ids='transform_data', key='csv_data')  # Pull the transformed data from XCom

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=OUTPUT_KEY,
        Body=csv_data,
        ContentType='text/csv'
    )
    logging.info(f"Transformed data loaded to s3://{S3_BUCKET}/{OUTPUT_KEY}")

# Define the Airflow DAG
dag = DAG(
    'spotify_etl_pipeline',
    description='Extract, Transform, and Load Spotify Data using S3',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False
)

# Task 1: Extract
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,  # Ensure that we pass context to the task
    dag=dag
)

# Task 2: Transform
transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,  # Ensure that we pass context to the task
    dag=dag
)

# Task 3: Load
load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,  # Ensure that we pass context to the task
    dag=dag
)

# DAG Execution Flow
extract_task >> transform_task >> load_task
