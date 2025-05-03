from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
import sys

sys.path.append('/opt/airflow/scripts/python')
from spark.zscore_stats_spark import zscore_rdd

default_args = {
    'owner': 'ian',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='nba_zscore_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    zscore_stats_table = """
    CREATE TABLE IF NOT EXISTS zscore_stats (
        season VARCHAR(255),
        ppg FLOAT,
        rpg FLOAT,
        apg FLOAT,
        spg FLOAT,
        bpg FLOAT,
        "3pt_pct" FLOAT,
        ppg_zscore FLOAT,
        rpg_zscore FLOAT,
        apg_zscore FLOAT,
        spg_zscore FLOAT,
        bpg_zscore FLOAT,
        "3pt_pct_zscore" FLOAT
    );
    """

    create_table = SQLExecuteQueryOperator(
        task_id='create_zscore_table',
        conn_id='postgres_bigdata',
        sql=zscore_stats_table
    )

    run_zscore_stats = PythonOperator(
        task_id='run_spark_zscore_stats',
        python_callable=zscore_rdd
    )

    create_table >> run_zscore_stats

