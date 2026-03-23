# pipelines/dags/test_connections.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

def test_postgres():
    hook = PostgresHook(postgres_conn_id='rideflow_postgres')
    records = hook.get_records("SELECT version();")
    print(f"PostgreSQL version: {records}")

with DAG(
    'test_connections',
    start_date=datetime(2026, 3, 21),
    schedule=None,
    catchup=False
) as dag:
    test_task = PythonOperator(
        task_id='test_postgres_connection',
        python_callable=test_postgres
    )