# Import DAG class from airflow package
from airflow import DAG

# Instantiation of smallest DAG with no task and minimum parameter
with DAG(
		dag_id='airflow_kt_dag_00',
		tags=["Airflow KT"]
) as dag:
	None
