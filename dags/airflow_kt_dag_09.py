from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.subdag import SubDagOperator
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


def load_subdag(parent_dag_name, child_dag_name, args):
    dag_subdag = DAG(
        dag_id='{0}.{1}'.format(parent_dag_name, child_dag_name),
        default_args=args,
        start_date=datetime(2021, 7, 1),
        schedule_interval="@daily",
    )
    with dag_subdag:
        for i in range(5):
            t = DummyOperator(
                task_id='load_subdag_{0}'.format(i),
                default_args=args,
                dag=dag_subdag,
            )

    return dag_subdag

def py_task():
    print('PYTHON TASK OF AIRFLOW')


with DAG(
        dag_id="airflow_kt_dag_09",
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

    load_tasks = SubDagOperator(
        task_id="load_tasks",
        subdag=load_subdag(
            parent_dag_name="airflow_kt_dag_09",
            child_dag_name="load_tasks",
            args=default_args
        ),
        default_args=default_args,
        dag=dag,
    )

    zeroth_task >> load_tasks >> first_task


