from datetime import datetime, timedelta
from airflow import DAG
from  airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

default_args = {
    'owner': 'airflow',
    'retry_delay': timedelta(minutes=5),
    'retries': 5
}

with DAG(
    default_args=default_args,
    dag_id='dag_with_s3_v02',
    start_date=datetime(2025, 7, 1),
    schedule='@daily',    
) as dag:
    task1 = S3KeySensor(
        task_id='s3_key_sensor',
        bucket_name='learn-airflow-bucket',
        bucket_key='data.csv',
        aws_conn_id='aws_conn_test',
        mode='poke',
        poke_interval=5,
        timeout=30,
    )
    
    task1