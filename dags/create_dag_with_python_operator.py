from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def greet(name, age):
    print(f"Hello, I'm {name} and I am {age} years old!")

with DAG(
    default_args=default_args,
    dag_id='our_dag_with_python_operator_v02',
    description='A simple DAG with a Python operator',
    start_date=datetime(2021, 7, 29, 2),
    schedule='@daily',
    ) as dag:
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet,
        op_kwargs={'name': 'John', 'age': 30},
    )
    
    task1