from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import sys
import os

sys.path.append('/opt/airflow/scripts/python')
from fetch_season_logs import save_gamelog

default_args = {
    'owner': 'ian',
    'start_date': datetime(2025, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='gamelog_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    fetch_data = PythonOperator(
        task_id='run_data_fetcher',
        python_callable=save_gamelog
    )
    
    trigger_permission_fixer = TriggerDagRunOperator(
        task_id='trigger_perm',
        trigger_dag_id='fix_permissions_dag'
    )
    
    fetch_data >> trigger_permission_fixer

