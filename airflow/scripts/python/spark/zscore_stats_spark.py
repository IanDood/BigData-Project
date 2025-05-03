from pyspark.sql import SparkSession
import math

def zscore_rdd():
    # Configuration
    POSTGRES_URL = "jdbc:postgresql://postgres:5432/bigdata"
    POSTGRES_INPUT_TABLE = "pergame_stats"
    POSTGRES_OUTPUT_TABLE = "zscore_stats"
    POSTGRES_USER = "ian"
    POSTGRES_PASSWORD = "project"
    POSTGRES_DRIVER_PATH = "/opt/airflow/jar/postgresql-42.7.3.jar"

    # Start Spark session
    spark = SparkSession.builder \
        .appName("Z-ScoreStats-RDD") \
        .config("spark.jars", POSTGRES_DRIVER_PATH) \
        .getOrCreate()

    # Read data from PostgreSQL
    df = spark.read.jdbc(
        url=POSTGRES_URL,
        table=POSTGRES_INPUT_TABLE,
        properties={
            "user": POSTGRES_USER,
            "password": POSTGRES_PASSWORD,
            "driver": "org.postgresql.Driver"
        }
    )

    # Convert to RDD and exclude the 'career' row
    stats_rdd = df.rdd.filter(lambda row: row['season'] != 'career')

    # Extract values for computation
    stat_list = ['ppg', 'rpg', 'apg', 'spg', 'bpg', '3pt_pct']
    stat_indices = {name: idx for idx, name in enumerate(stat_list, start=1)}  # +1 because season is index 0

    data = stats_rdd.map(lambda row: (
        row['season'],
        float(row['ppg']),
        float(row['rpg']),
        float(row['apg']),
        float(row['spg']),
        float(row['bpg']),
        float(row['3pt_pct'])
    )).cache()

    # Compute means
    collected_data = data.collect()
    stat_columns = list(zip(*[row[1:] for row in collected_data]))  # Transpose to column-wise
    means = [sum(col) / len(col) for col in stat_columns]
    stddevs = [math.sqrt(sum((x - means[i]) ** 2 for x in col) / len(col)) for i, col in enumerate(stat_columns)]

    # Compute z-scores
    def add_zscores(row):
        season = row[0]
        stats = row[1:]
        zscores = [(val - means[i]) / stddevs[i] if stddevs[i] != 0 else 0.0 for i, val in enumerate(stats)]
        return (season, *stats, *zscores)

    result_rdd = data.map(add_zscores)

    # Define output column names
    schema = ['season'] + stat_list + [f"{s}_zscore" for s in stat_list]
    result_df = result_rdd.toDF(schema)

    # Save to PostgreSQL
    result_df.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", POSTGRES_OUTPUT_TABLE) \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    spark.stop()

