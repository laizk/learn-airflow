from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id='dag_with_taskflow_api_v01',
    default_args=default_args,
    start_date=datetime(2021, 7, 29, 2),
    schedule='@daily',
)
def hello_world_etl():
    
    @task()
    def get_name():
        return "David"
    
    @task()
    def get_age():
        return 25
    
    @task()
    def greet(first_name, age):
        print(f"Hello, I'm {first_name} and I am {age} years old!")
        
    name = get_name()
    age = get_age()
    greet(name, age)
    
greet_dag = hello_world_etl()