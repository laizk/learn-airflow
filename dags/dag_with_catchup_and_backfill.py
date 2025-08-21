from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='dag_with_catchup_and_backfill_v02',
    description='A simple DAG for demonstration purposes',
    start_date=datetime(2025, 8, 10, 1),
    schedule='@daily',
    default_args=default_args,
    catchup=False
) as dag:
    task1 = BashOperator(
        task_id='task1',
        bash_command='echo this is dag with catchup and backfill'
    )
    
    task1
