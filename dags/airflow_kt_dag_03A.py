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

# {"test":"value"}


def py_task(**context):
    runtime_conf = context['dag_run'].conf
    print('PYTHON TASK OF AIRFLOW', runtime_conf['test'])


with DAG(
        dag_id="airflow_kt_dag_03A",
        start_date=datetime(2021, 7, 1),
        schedule_interval="@daily",
        tags=["Airflow KT"],
        default_args=default_args,
        catchup=False
) as dag:

    first_task = PythonOperator(
            task_id="first_task",
            python_callable=py_task
    )

    second_task = PythonOperator(
        task_id="second_task",
        python_callable=py_task
    )

    third_task = PythonOperator(
        task_id="third_task",
        python_callable=py_task
    )

    fourth_task = PythonOperator(
        task_id="fourth_task",
        python_callable=py_task
    )

    # first_task >> second_task
    # second_task << first_task
    # second_task.set_upstream(first_task)
    # first_task.set_downstream(second_task)
