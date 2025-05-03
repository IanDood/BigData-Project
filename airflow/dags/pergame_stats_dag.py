from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
import sys

sys.path.append('/opt/airflow/scripts/python')
from spark.pergame_stats_spark import pergame_stats

default_args = {
    'owner': 'ian',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='nba_stats_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    pergame_stats_table = """
    CREATE TABLE IF NOT EXISTS pergame_stats (
        season VARCHAR(255),
        ppg FLOAT,
        rpg FLOAT,
        apg FLOAT,
        spg FLOAT,
        bpg FLOAT,
        "3pt_pct" FLOAT
    );
    """

    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='postgres_bigdata',
        sql=pergame_stats_table
    )

    run_average_stats = PythonOperator(
        task_id='run_spark_nba_stats',
        python_callable=pergame_stats
    )

    create_table >> run_average_stats

