from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import csv
import logging

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def postgres_to_s3():
    # step 1: query data from postgresql db and save into text file
    hook = PostgresHook(postgres_conn_id='postgres_localhost')
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM orders where date <= '20220501'")
    with open("dags/get_orders.txt", "w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([desc[0] for desc in cursor.description])  # write header
        csv_writer.writerows(cursor)  # write data
    cursor.close()
    conn.close()
    logging.info("Data exported to get_orders.txt")
    # step 2: upload the text file to s3 bucket

with DAG(
    dag_id='dag_with_postgres_hooks_v01',
    default_args=default_args,
    description='A DAG with a Postgres operator',
    start_date=datetime(2025, 7, 1),
    schedule='0 0 * * *',  # This cron expression means the DAG will run daily at midnight
) as dag:
 
    task1 = PythonOperator(
        task_id='postgres_to_s3_task',
        python_callable=postgres_to_s3,
    )
    
    task1