from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, current_date 
from datetime import datetime

def wordcount_job():
    # Configuration
    POSTGRES_URL = "jdbc:postgresql://postgres:5432/bigdata"
    POSTGRES_TABLE = "weekly_nba_wordcount"
    POSTGRES_USER = "ian"
    POSTGRES_PASSWORD = "project"
    POSTGRES_DRIVER_PATH = "/opt/airflow/jar/postgresql-42.7.3.jar"

    # Input file path using your style
    INPUT_FILE = f"/opt/airflow/nba_data/news-logs/nba_news_{datetime.now().strftime('%Y%m%d')}.txt"

    # Start Spark Session
    spark = SparkSession.builder \
        .appName("NBA Word Count Upload to Postgres") \
        .config("spark.jars", POSTGRES_DRIVER_PATH) \
        .getOrCreate()

    # Word Count
    text_df = spark.read.text(INPUT_FILE)

    word_count_df = text_df.select(
        explode(split(text_df['value'], r'\W+')).alias('word')
    ).filter(
        "length(word) > 0"
    ).groupBy(
        "word"
    ).count()

    word_count_df = word_count_df.withColumn("run_date", current_date())

    # Write to PostgreSQL
    word_count_df.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", POSTGRES_TABLE) \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    # Exit Session
    spark.stop()

