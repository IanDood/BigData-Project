from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import sys
import os

sys.path.append('/opt/airflow/scripts/python')
from fetch_news import save_nba_news
from spark.wordcount_spark import wordcount_job

default_args = {
    'owner': 'ian',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='news_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    scrape_news = PythonOperator(
        task_id='scrape_nba_news',
        python_callable=save_nba_news
    )

    trigger_permission_fixer = TriggerDagRunOperator(
        task_id='trigger_perm',
        trigger_dag_id='fix_permissions_dag'
    )
    
    wordcount_tables = """CREATE TABLE IF NOT EXISTS weekly_nba_wordcount (
        word VARCHAR(255),
        count INT,
        run_date DATE
    );
    """
    
    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='postgres_bigdata', 
        sql=wordcount_tables
    )

    run_wordcount = PythonOperator(
        task_id='run_spark_word_count',
        python_callable=wordcount_job
    )

    scrape_news >> trigger_permission_fixer >> create_table >> run_wordcount

