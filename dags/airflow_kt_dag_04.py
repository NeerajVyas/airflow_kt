from airflow import DAG
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


with DAG(
        dag_id="airflow_kt_dag_04",
        start_date=datetime(2021, 7, 1),
        schedule_interval=None,
        tags=["Airflow KT"],
        default_args=default_args,
        catchup=False
) as dag:

    create_country_table = PostgresOperator(
        task_id="create_country_table",
        postgres_conn_id="country_postgres",
        sql="""
            CREATE TABLE IF NOT EXISTS random (
                country VARCHAR NOT NULL,
                country_code VARCHAR NOT NULL
            );
          """
    )

    insert_data = PostgresOperator(
        task_id="insert_data",
        postgres_conn_id="country_postgres",
        sql="""
            INSERT INTO random
            VALUES (
                'INDIA',
                'IN'
            );
        """
    )

    create_country_table >> insert_data

