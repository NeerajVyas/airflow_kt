import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 0,
    "retry_delay": timedelta(minutes=5)
}


def country_exits(**context):
    ti = context['ti']
    table_name = ti.xcom_pull(task_ids='fetch_data', key='table_name')
    fetched_country = ti.xcom_pull(task_ids='fetch_data', key='country')
    request = f"SELECT * FROM {table_name} WHERE country = '{fetched_country}'"
    pg_hook = PostgresHook(
        postgres_conn_id="country_postgres",
        schema="airflow"
    )

    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    sources = cursor.fetchall()

    if sources:
        return 'skip_insertion'
    else:
        print('ALREADY EXISTS, hence skipping', sources, request)
        return 'insert_data'


def display_countries(**context):
    ti = context['ti']
    table_name = ti.xcom_pull(task_ids='fetch_data', key='table_name')
    request = f"SELECT * FROM {table_name}"
    pg_hook = PostgresHook(
        postgres_conn_id="country_postgres",
        schema="airflow"
    )

    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    sources = cursor.fetchall()
    for source in sources:
        print(f" COUNTRY - {source[0]}, COUNTRY_CODE {source[1]}")


def fetch_data(**context: dict):

    dag_params = {
        "table_name": "random"
    }

    runtime_conf = context['dag_run'].conf
    if runtime_conf:
        dag_params["table_name"] = runtime_conf["table_name"]

    data = requests.get("https://random-data-api.com/api/address/random_address").json()

    ti = context['ti']
    ti.xcom_push(key="table_name", value=dag_params["table_name"])
    ti.xcom_push(key="country", value=data["country"].replace("'", ""))
    ti.xcom_push(key="country_code", value=data["country_code"].replace("'", ""))


with DAG(
        dag_id="airflow_kt_dag_08",
        start_date=datetime(2021, 7, 1),
        schedule_interval=None,
        tags=["Airflow KT"],
        default_args=default_args,
        catchup=False
) as dag:

    fetch_data = PythonOperator(
            task_id="fetch_data",
            python_callable=fetch_data,
            provide_context=True
    )

    create_country_table = PostgresOperator(
        task_id="create_country_table",
        postgres_conn_id="country_postgres",
        sql="""
            CREATE TABLE IF NOT EXISTS {{ ti.xcom_pull(task_ids='fetch_data', key='table_name') }} (
                country VARCHAR NOT NULL,
                country_code VARCHAR NOT NULL
            );
          """
    )

    country_exits = BranchPythonOperator(
        task_id='country_exits',
        python_callable=country_exits
    )

    insert_data = PostgresOperator(
        task_id="insert_data",
        postgres_conn_id="country_postgres",
        sql="""
            INSERT INTO {{ ti.xcom_pull(task_ids='fetch_data', key='table_name') }}
            VALUES (
                '{{ ti.xcom_pull(task_ids='fetch_data', key='country') }}',
                '{{ ti.xcom_pull(task_ids='fetch_data', key='country_code') }}'
            );
        """
    )

    skip_insertion = DummyOperator(
        task_id='skip_insertion'
    )

    display_countries = PythonOperator(
            task_id="display_countries",
            python_callable=display_countries,
            provide_context=True,
            trigger_rule='none_failed_or_skipped'
    )

    fetch_data >> create_country_table >> country_exits
    country_exits >> insert_data >> display_countries
    country_exits >> skip_insertion >> display_countries

