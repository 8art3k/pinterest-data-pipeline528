from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime, timedelta

notebook_task = {
    'notebook_path': '/Workspace/Users/malek01@hotmail.co.uk/pinterest_data',
}

default_args = {
    'owner': '262542bdae36',  
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,  
    'retry_delay': timedelta(minutes=2) 
}

with DAG('262542bdae36_dag', 
    start_date=datetime(2025, 1, 30), 
    schedule_interval='@daily', 
    catchup=False,
    default_args=default_args
    ) as dag:

    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='submit_run',
        databricks_conn_id='databricks_default', 
        existing_cluster_id='1108-162752-8okw8dgg',
        notebook_task=notebook_task 
    )
    opr_submit_run
