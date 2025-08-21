from datetime import datetime, timedelta

from airflow import DAG
# from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='dag_with_postgres_operator_v01',
    default_args=default_args,
    description='A DAG with a Postgres operator',
    start_date=datetime(2025, 7, 1),
    schedule='0 0 * * *',  # This cron expression means the DAG will run daily at midnight
) as dag:
    task1 = SQLExecuteQueryOperator(
        task_id='create_postgres_table',
        conn_id='postgres_localhost',
        sql="""
            CREATE TABLE IF NOT EXISTS dag_runs (
                dt date,
                dag_id character varying,
                primary key (dt, dag_id)
            )
        """
    )
    
    task1