from datetime import datetime, timedelta
from airflow import DAG
from tempfile import NamedTemporaryFile

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import csv
import logging

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def postgres_to_s3(**kwargs):
    ds_nodash = kwargs['ds_nodash']
    next_ds_nodash = (datetime.strptime(ds_nodash, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")
    # step 1: query data from postgresql db and save into text file
    hook = PostgresHook(postgres_conn_id='postgres_localhost')
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM orders where date <= '{next_ds_nodash}' and date >= '{ds_nodash}'")
    
    with NamedTemporaryFile(mode='w', suffix=f"{ds_nodash}") as f:
    # with open(f"dags/get_orders_{ds_nodash}.txt", "w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([desc[0] for desc in cursor.description])  # write header
        csv_writer.writerows(cursor)  # write data
        f.flush()
        cursor.close()
        conn.close()
        logging.info(f"Data exported to get_orders_{ds_nodash}.txt")
    
    # step 2: upload the text file to s3 bucket
        s3_hook = S3Hook(aws_conn_id='aws_conn_test')
        s3_hook.load_file(
            filename=f.name,
            key=f"orders/{ds_nodash}.txt",
            bucket_name='learn-airflow-bucket',
            replace=True
        )
        logging.info("Orders file %s has been pushed to S3!", f.name)

with DAG(
    dag_id='dag_with_postgres_hooks_v04',
    default_args=default_args,
    description='A DAG with a Postgres operator',
    start_date=datetime(2025, 7, 1),
    schedule='0 0 * * *',  # This cron expression means the DAG will run daily at midnight
) as dag:
 
    task1 = PythonOperator(
        task_id='postgres_to_s3_task',
        python_callable=postgres_to_s3
    )
    
    task1