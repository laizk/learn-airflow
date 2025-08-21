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
    dag_id='dag_with_postgres_operator_v03',
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
    
    task2 = SQLExecuteQueryOperator(
        task_id='insert_into_table',
        conn_id='postgres_localhost',
        sql="""
            INSERT INTO dag_runs (dt, dag_id)
            VALUES ('{{ ds }}', '{{ dag.dag_id }}')
        """
    )
    
    task3 = SQLExecuteQueryOperator(
        task_id='delete_data_from_table',
        conn_id='postgres_localhost',
        sql="""
            DELETE FROM dag_runs
            WHERE dt = '{{ ds }}' AND dag_id = '{{ dag.dag_id }}';
        """
    )    
    
    task1 >> task3 >> task2