from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract
from pyspark.sql.types import FloatType

def pergame_stats():
    # Configuration
    POSTGRES_URL = "jdbc:postgresql://postgres:5432/bigdata"
    POSTGRES_TABLE = "pergame_stats"
    POSTGRES_USER = "ian"
    POSTGRES_PASSWORD = "project"
    POSTGRES_DRIVER_PATH = "/opt/airflow/jar/postgresql-42.7.3.jar"

    # Create Spark session
    spark = SparkSession.builder \
        .appName("NBAStatsMapReduce") \
        .config("spark.jars", POSTGRES_DRIVER_PATH) \
        .getOrCreate()

    # Read all CSVs
    df = spark.read.csv("/opt/airflow/nba_data/gamelogs/*_Season.csv", header=True, inferSchema=True)
    df = df.withColumn("source_file", input_file_name())
    df = df.withColumn("season", regexp_extract("source_file", r"([0-9]{4}-[0-9]{2})", 1))

    # RDD for aggregation
    rdd = df.rdd

    season_averages = rdd.map(lambda row: (
        row["season"],
        (
            float(row["PTS"]), float(row["REB"]), float(row["AST"]),
            float(row["STL"]), float(row["BLK"]),
            float(row["FG3M"]), float(row["FG3A"]),
            1   # Game count
        )
    ))

    # Aggregate per season
    season_reduced = season_averages.reduceByKey(lambda a, b: tuple(a[i] + b[i] for i in range(len(a))))

    # Per-season averages
    season_final_averages = season_reduced.mapValues(lambda vals: {
        "ppg": vals[0] / vals[7],
        "rpg": vals[1] / vals[7],
        "apg": vals[2] / vals[7],
        "spg": vals[3] / vals[7],
        "bpg": vals[4] / vals[7],
        "3pt_pct": vals[5] / vals[6] if vals[6] != 0 else 0.0
    })

    # Convert to DataFrame
    season_result_df = season_final_averages.map(lambda x: (x[0], *x[1].values())) \
        .toDF(["season", "ppg", "rpg", "apg", "spg", "bpg", "3pt_pct"])

    # ===== Career Row Construction =====
    total_stats = season_averages.map(lambda x: x[1]) \
        .reduce(lambda a, b: tuple(a[i] + b[i] for i in range(len(a))))

    total_games = total_stats[7]

    career_averages_dict = {
        "ppg": total_stats[0] / total_games,
        "rpg": total_stats[1] / total_games,
        "apg": total_stats[2] / total_games,
        "spg": total_stats[3] / total_games,
        "bpg": total_stats[4] / total_games,
        "3pt_pct": total_stats[5] / total_stats[6] if total_stats[6] != 0 else 0.0
    }

    # One-row DataFrame for "career"
    career_result_df = spark.createDataFrame(
        [("career", *career_averages_dict.values())],
        ["season", "ppg", "rpg", "apg", "spg", "bpg", "3pt_pct"]
    )

    # Combine season and career
    combined_df = season_result_df.union(career_result_df)

    # Write to PostgreSQL
    combined_df.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", POSTGRES_TABLE) \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    spark.stop()



