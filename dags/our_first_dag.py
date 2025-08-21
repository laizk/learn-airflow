from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='our_first_dag2',
    description='A simple DAG for demonstration purposes',
    start_date=datetime(2021, 7, 29, 2),
    schedule='@daily',
    default_args=default_args,
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command='echo hello world, this is the first task'
    )
    
    task2 = BashOperator(
        task_id='second_task',
        bash_command='echo this is the second task, it runs after the first task',
    )
    
    task1.set_downstream(task2)

