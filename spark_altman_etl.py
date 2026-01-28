from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, when, lit, round, avg, stddev, abs, rank, desc
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.sql.window import Window

# --- CONFIGURATION CONSTANTS ---
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "financial_reports_stream"
SHUFFLE_PARTITIONS = "5"  # Low partition count for local testing efficiency

def get_spark_session(app_name: str) -> SparkSession:
    """
    Initializes and returns the Spark Session with necessary configurations.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def get_schema() -> StructType:
    """
    Defines the schema for the incoming JSON data from Kafka.
    Matches the CSV structure provided by the ingestion layer.
    """
    return StructType([
        StructField("ticker", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("total_assets", FloatType(), True),
        StructField("current_assets", FloatType(), True),
        StructField("current_liabilities", FloatType(), True),
        StructField("total_liabilities", FloatType(), True),
        StructField("retained_earnings", FloatType(), True),
        StructField("ebit", FloatType(), True),
        StructField("market_cap", FloatType(), True),
        StructField("total_revenue", FloatType(), True)
    ])

def read_kafka_stream(spark: SparkSession) -> DataFrame:
    """
    Reads the raw stream from Kafka and parses the JSON payload.
    """
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON using the defined schema
    parsed_stream = raw_stream.select(
        from_json(col("value").cast("string"), get_schema()).alias("data")
    ).select("data.*")
    
    return parsed_stream

def calculate_altman_features(df: DataFrame) -> DataFrame:
    """
    Feature Engineering Step 1:
    Calculates the 5 coefficients (X1 - X5) required for the Altman Z-Score model.
    """
    return df.withColumn(
        "working_capital", col("current_assets") - col("current_liabilities")
    ).withColumn(
        "X1", col("working_capital") / col("total_assets")
    ).withColumn(
        "X2", col("retained_earnings") / col("total_assets")
    ).withColumn(
        "X3", col("ebit") / col("total_assets")
    ).withColumn(
        "X4", col("market_cap") / col("total_liabilities")
    ).withColumn(
        "X5", col("total_revenue") / col("total_assets")
    )

def compute_z_score(df: DataFrame) -> DataFrame:
    """
    Feature Engineering Step 2:
    Applies the linear Altman Formula (1968) to compute the Z-Score.
    Formula: Z = 1.2(X1) + 1.4(X2) + 3.3(X3) + 0.6(X4) + 1.0(X5)
    """
    return df.withColumn(
        "Z_Score", 
        round(
            (lit(1.2) * col("X1")) + 
            (lit(1.4) * col("X2")) + 
            (lit(3.3) * col("X3")) + 
            (lit(0.6) * col("X4")) + 
            (lit(1.0) * col("X5")), 
            2
        )
    )

def enrich_with_analytics(df: DataFrame) -> DataFrame:
    """
    Advanced Analytics Step:
    Calculates dynamic sector averages, standard deviations, and assigns Health Zones.
    """
    stats_window = Window.partitionBy("year")

    # 1. Calculate Statistics (Avg & StdDev) per Year
    df_stats = df \
        .withColumn("Yearly_Avg_Z", round(avg("Z_Score").over(stats_window), 2)) \
        .withColumn("Yearly_StdDev", round(stddev("Z_Score").over(stats_window), 2)) \
        .na.fill(0.0, ["Yearly_StdDev"])

    # 2. Apply Logic: Benchmarking, Anomaly Detection, and Classification
    return df_stats.withColumn(
        "Performance", 
        when(col("Z_Score") > col("Yearly_Avg_Z"), "Outperforming")
        .otherwise("Underperforming")
    ).withColumn(
        "Is_Anomaly",
        when(abs(col("Z_Score") - col("Yearly_Avg_Z")) > (lit(2) * col("Yearly_StdDev")), "YES")
        .otherwise("No")
    ).withColumn(
        "Health_Zone",
        when(col("Z_Score") > 2.99, "Safe (Green)")
        .when((col("Z_Score") >= 1.81) & (col("Z_Score") <= 2.99), "Grey (Caution)")
        .otherwise("Distress (Red)")
    )

def apply_top5_ranking(df: DataFrame) -> DataFrame:
    """
    Ranking Step:
    Assigns a rank to each company based on Z-Score within its Year and keeps only Top 5.
    """
    rank_window = Window.partitionBy("year").orderBy(desc("Z_Score"))
    
    return df.withColumn("Rank", rank().over(rank_window)) \
             .filter(col("Rank") <= 5)

def format_final_output(df: DataFrame) -> DataFrame:
    """
    Selects and orders the final columns for the presentation layer.
    """
    return df.select(
        "year", "Rank", "ticker", "Z_Score", "Health_Zone", 
        "Performance", "Is_Anomaly", "Yearly_Avg_Z"
    ).orderBy("year", "Rank")

def write_to_console(df: DataFrame):
    """
    Writes the streaming DataFrame to the console.
    Uses 'complete' mode to support dynamic re-ranking (Leaderboard logic).
    """
    query = df.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()
    
    query.awaitTermination()

# --- MAIN EXECUTION FLOW ---
if __name__ == "__main__":
    print("Starting Financial Health Analysis System...")
    
    # 1. Init
    spark = get_spark_session("Altman_Z_Score_Modular_System")
    
    # 2. Ingest
    stream_df = read_kafka_stream(spark)
    
    # 3. Process (ETL & Logic)
    features_df = calculate_altman_features(stream_df)
    scored_df = compute_z_score(features_df)
    
    # 4. Analyze (Big Data Capabilities)
    enriched_df = enrich_with_analytics(scored_df)
    
    # 5. Rank (Leaderboard)
    top5_df = apply_top5_ranking(enriched_df)
    
    # 6. Format
    final_df = format_final_output(top5_df)
    
    # 7. Output
    write_to_console(final_df)