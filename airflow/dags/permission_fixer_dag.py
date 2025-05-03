from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

default_args = {
    'owner': 'ian',
    'start_date': datetime(2025, 1, 1),
    'retries': 0
}

with DAG(
    dag_id='fix_permissions_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    fix_permissions = BashOperator(
        task_id='fix_permissions',
        bash_command="bash /opt/airflow/scripts/bash/permissions.sh "
    )



