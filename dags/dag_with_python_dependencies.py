from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'retry_delay': timedelta(minutes=5),
    'retries': 5
    }

def get_sklearn():
    import sklearn
    print(f"scikit-learn version: {sklearn.__version__}")
    
def get_matplotlib():
    import matplotlib
    print(f"matplotlib version: {matplotlib.__version__}")

with DAG(
    default_args=default_args,
    dag_id='dag_with_python_dependencies_v02',
    start_date=datetime(2025, 7, 1),
    schedule='@daily',
) as dag:
    
    get_sklearn_task = PythonOperator(
        task_id='get_sklearn',
        python_callable=get_sklearn
    )
    
    get_matplotlib_task = PythonOperator(
        task_id='get_matplotlib',
        python_callable=get_matplotlib
    )
    
get_sklearn_task >> get_matplotlib_task