from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}


def py_task():
    print('PYTHON TASK OF AIRFLOW')


dag = DAG(
    dag_id="airflow_kt_dag_02",
    start_date=datetime(2021, 7, 1),
    schedule_interval="@daily",
    tags=["Airflow KT"],
    default_args=default_args
)

first_task = PythonOperator(
    task_id="first_task",
    dag=dag,
    python_callable=py_task
)

second_task = PythonOperator(
    task_id="second_task",
    dag=dag,
    python_callable=py_task
)
