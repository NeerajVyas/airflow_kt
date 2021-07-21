
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.models import Variable

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}


def set_task():
    Variable.set("my_key", "airflow_kt")


def get_task():
    print(Variable.get("my_key"))


"""
{
"login": "my_login",
"password": "my_password",
"config": {
    "role": "admin"
    }
}
"""


def get_json_variable():
    from airflow.models import Variable
    settings = Variable.get("settings", deserialize_json=True)
    # And be able to access the values like in a dictionary
    print(settings['login'])
    print(settings['config']['role'])


with DAG(
        dag_id="airflow_kt_dag_03C",
        start_date=datetime(2021, 7, 1),
        schedule_interval="@daily",
        tags=["Airflow KT"],
        default_args=default_args,
        catchup=False
) as dag:

    set_task = PythonOperator(
            task_id="set_task",
            python_callable=set_task
    )

    get_task = PythonOperator(
        task_id="get_task",
        python_callable=get_task
    )

    bash_task = BashOperator(
        task_id="bash_task",
        bash_command="echo  {{ var.value.my_key }}"
    )

    get_json_variable = PythonOperator(
        task_id="get_json_variable",
        python_callable=get_json_variable
    )

    set_task >> get_task >> bash_task >> get_json_variable

