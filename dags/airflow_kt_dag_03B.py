from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models.baseoperator import chain, cross_downstream

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


with DAG(
        dag_id="airflow_kt_dag_03B",
        start_date=datetime(2021, 7, 1),
        schedule_interval="@daily",
        tags=["Airflow KT"],
        default_args=default_args,
        catchup=False
) as dag:

    zeroth_task = PythonOperator(
            task_id="zeroth_task",
            python_callable=py_task
    )

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

    fifth_task = PythonOperator(
        task_id="fifth_task",
        python_callable=py_task
    )

    # zeroth_task >> first_task >> second_task >> third_task >> fourth_task >> fifth_task
    # chain(zeroth_task, first_task, second_task, third_task, fourth_task, fifth_task)

    # zeroth_task >> first_task >> [second_task, third_task, fourth_task] >> fifth_task

    # Below one doesn't work, gives parsing error
    # [first_task, second_task] >> [third_task, fourth_task] >> fifth_task

    zeroth_task >> [first_task, second_task]
    cross_downstream([first_task, second_task], [third_task, fourth_task])
    [third_task, fourth_task] >> fifth_task
