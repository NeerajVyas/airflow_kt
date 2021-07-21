from airflow import DAG

from datetime import datetime, timedelta

# default arguments - Common attributes of your task
default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
        dag_id="airflow_kt_dag_01",
        start_date=datetime(2021, 7, 1),
        schedule_interval="@daily",
        tags=["Airflow KT"],
        default_args=default_args,
        catchup=False
) as dag:
    None
