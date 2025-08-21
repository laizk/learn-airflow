from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}  

with DAG(
    default_args=default_args,
    dag_id='dag_with_cron_expression_v03',
    description='A DAG with a cron expression schedule',
    start_date=datetime(2025, 7, 1),
    schedule='0 3 * * Tue',  # This cron expression means the DAG will run daily at midnight
    catchup=True
    
) as dag:
    task1 = BashOperator(
        task_id='task1',
        bash_command='echo this is a DAG with a cron expression schedule'
    )

    task1
    