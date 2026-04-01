"""
Streaming ETL Consumer for Altman Z-Score and Z'-Score.
Features:
- Dual-Scoring: Evaluates both Original and Prime formulas simultaneously.
- Data Quality Gate (DLQ): Filters invalid raw data and writes to native logger.
- Anti-Poisoning: Sanitizes NaNs and prevents Division by Zero in financial math.
- Silver/Gold Materialization: Calculates global market averages and Top-5 Leaderboards.
"""

import os
import shutil
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, when, lit, round, isnan, avg, stddev, abs, upper
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

# Import the native logger from your project structure
from src.raw_features.logger import log_message

# --- CONFIGURATION CONSTANTS ---
KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = "raw_features" 
APP_NAME = "Altman_Dual_Scoring_ETL"
SILVER_STORAGE_PATH = "local_storage/silver_scores"

def enrich_with_analytics(df: DataFrame) -> DataFrame:
    """
    Advanced Analytics Step:
    Calculates dynamic averages, standard deviations across the batch,
    then assigns performance and anomaly flags based on Z_Score_Prime.
    Z_Score_Prime is used because it is always available (does not require Market Cap).
    """
    stats_window = Window.partitionBy()

    # 1. Calculate Statistics (Avg & StdDev) for Z_Score_Prime
    df_stats = df \
        .withColumn("Yearly_Avg_Z", round(avg("Z_Score_Prime").over(stats_window), 2)) \
        .withColumn("Yearly_StdDev", round(stddev("Z_Score_Prime").over(stats_window), 2)) \
        .na.fill(0.0, ["Yearly_StdDev"])

    # 2. Apply Logic: Benchmarking, Anomaly Detection
    return df_stats.withColumn(
        "Performance",
        when(col("Z_Score_Prime") > col("Yearly_Avg_Z"), "Outperforming")
        .otherwise("Underperforming")
    ).withColumn(
        "Is_anomaly",
        when(abs(col("Z_Score_Prime") - col("Yearly_Avg_Z")) > (lit(2) * col("Yearly_StdDev")), "Yes")
        .otherwise("No")
    ).drop("Yearly_Avg_Z", "Yearly_StdDev")

def clean_silver_storage():
    """
    Cleans the local Parquet storage directory before starting the stream.
    Ensures a fresh start for the demonstration.
    """
    if os.path.exists(SILVER_STORAGE_PATH):
        shutil.rmtree(SILVER_STORAGE_PATH)
        log_message(f"Cleared previous state at {SILVER_STORAGE_PATH}", APP_NAME, "INFO")

def get_schema() -> StructType:
    return StructType([
        StructField("ticker", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("common_stock_units", FloatType(), True),
        StructField("current_assets", FloatType(), True),
        StructField("current_liabilities", FloatType(), True),
        StructField("short_term_debt", FloatType(), True),
        StructField("long_term_debt", FloatType(), True),
        StructField("retained_earnings", FloatType(), True),
        StructField("stockholders_equity", FloatType(), True),
        StructField("total_assets", FloatType(), True),
        StructField("net_income", FloatType(), True),
        StructField("interest_expense", FloatType(), True),
        StructField("tax_expense", FloatType(), True),
        StructField("total_revenue", FloatType(), True),
        StructField("market_cap", FloatType(), True),
    ])

def data_quality_gate(df: DataFrame) -> DataFrame:
    """
    Evaluates incoming raw data for completeness before applying math.
    Flags records with nulls/NaNs in critical baseline fields as invalid.
    """
    critical_fields = [
        "current_assets", "current_liabilities", "total_assets", 
        "retained_earnings", "stockholders_equity", "net_income", "total_revenue"
    ]
    
    is_valid_cond = col("ticker").isNotNull() & col("year").isNotNull()
    
    for field in critical_fields:
        is_valid_cond = is_valid_cond & col(field).isNotNull() & ~isnan(col(field))
        
    # Total Assets must be > 0 to prevent base Division By Zero
    is_valid_cond = is_valid_cond & (col("total_assets") > 0)
    
    return df.withColumn("is_valid_record", is_valid_cond)

def preprocess_and_engineer_features(df: DataFrame) -> DataFrame:
    """
    Sanitizes values and computes derived financial metrics.
    Unit normalization (millions/thousands → absolute values) is applied upstream
    on the producer side (normalize_units_before_kafka in combined_metrics.py)
    before records are published to Kafka, so values arrive already in absolute form.

    Market-cap handling:
      - If market_cap is present and positive, it is used for Z_Original.
      - If market_cap is absent/invalid, Z_Original will be skipped (null)
        and only Z_Prime will be calculated.
    """
    # 1. Sanitize NaN/null financial fields → 0.0 (no unit scaling needed here)
    fields_to_sanitize = [
        "common_stock_units", "current_assets", "current_liabilities",
        "short_term_debt", "long_term_debt", "retained_earnings",
        "stockholders_equity", "total_assets", "net_income",
        "interest_expense", "tax_expense", "total_revenue"
    ]
    for field in fields_to_sanitize:
        df = df.withColumn(field,
            when(col(field).isNull() | isnan(col(field)), lit(0.0))
            .otherwise(col(field))
        )

    # 2. Sanitize market_cap: keep only positive values; null/NaN/<=0 → null
    df = df.withColumn(
        "market_cap",
        when(
            col("market_cap").isNull() | isnan(col("market_cap")) | (col("market_cap") <= 0),
            lit(None).cast("float")
        ).otherwise(col("market_cap"))
    )

    # 3. Feature Engineering
    df = df.withColumn("total_liabilities", col("current_liabilities") + col("long_term_debt") + col("short_term_debt"))
    df = df.withColumn("ebit", col("net_income") + col("interest_expense") + col("tax_expense"))

    return df

def calculate_dual_z_scores(df: DataFrame) -> DataFrame:
    """
    Computes Z-Score formulas using safe division. 
    Applies final Anti-Poison filters to drop lingering NaNs.
    """
    # 1. Standard Ratios
    df = df.withColumn("X1", (col("current_assets") - col("current_liabilities")) / col("total_assets")) \
           .withColumn("X2", col("retained_earnings") / col("total_assets")) \
           .withColumn("X3", col("ebit") / col("total_assets")) \
           .withColumn("X5", col("total_revenue") / col("total_assets"))

    # 2. Safe Division for X4 (Handles companies with exactly 0 total liabilities)
    df = df.withColumn("X4_Original", 
        when(col("total_liabilities") > 0, col("market_cap") / col("total_liabilities"))
        .otherwise(col("market_cap"))
    )
    df = df.withColumn("X4_Prime", 
        when(col("total_liabilities") > 0, col("stockholders_equity") / col("total_liabilities"))
        .otherwise(col("stockholders_equity"))
    )

    # 3. Calculate Scores
    df = df.withColumn(
        "Z_Score_Original",
        round((lit(1.2) * col("X1")) + (lit(1.4) * col("X2")) + (lit(3.3) * col("X3")) + (lit(0.6) * col("X4_Original")) + (lit(1.0) * col("X5")), 2)
    )

    df = df.withColumn(
        "Z_Score_Prime",
        round((lit(0.717) * col("X1")) + (lit(0.847) * col("X2")) + (lit(3.107) * col("X3")) + (lit(0.420) * col("X4_Prime")) + (lit(0.998) * col("X5")), 2)
    )

    # 4. Assign Risk Zones
    df = df.withColumn(
        "Health_Zone_Original",
        when(col("Z_Score_Original").isNull(), lit("N/A - No Market Cap"))
        .when(col("Z_Score_Original") >= 2.99, "Safe (Green)")
        .when(col("Z_Score_Original") <= 1.81, "Distress (Red)")
        .otherwise("Grey (Caution)")
    )

    df = df.withColumn(
        "Health_Zone_Prime",
        when(col("Z_Score_Prime") >= 2.90, "Safe (Green)")
        .when(col("Z_Score_Prime") <= 1.23, "Distress (Red)")
        .otherwise("Grey (Caution)")
    )

    # Clean intermediate logic and apply Final Anti-Poison Filter
    df = df.drop("X1", "X2", "X3", "X5", "X4_Original", "X4_Prime")
    return df.filter(~isnan(col("Z_Score_Prime")))

def process_micro_batch(batch_df: DataFrame, batch_id: int):
    """
    Executes for every micro-batch. Routes invalid records to DLQ logs 
    and appends valid records to Silver storage to update Gold dashboards.
    """
    record_count = batch_df.count()
    if record_count == 0:
        return

    log_message(f"[Batch {batch_id}] Intercepted {record_count} raw records from Kafka.", APP_NAME, "INFO")

    # --- ROUTING: DATA QUALITY GATE ---
    bad_df = batch_df.filter(col("is_valid_record") == False)
    good_df = batch_df.filter(col("is_valid_record") == True).drop("is_valid_record")

    bad_count = bad_df.count()
    good_count = good_df.count()

    # --- DEAD LETTER LOGGING (Handling Corrupted Data) ---
    if bad_count > 0:
        log_message(f"[Batch {batch_id}] DATA QUALITY ALERT: Dropping {bad_count} invalid records.", APP_NAME, "WARNING")
        corrupted_rows = bad_df.select("ticker", "year").collect()
        for row in corrupted_rows:
            log_message(f"[Batch {batch_id}] REJECTED: Ticker '{row.ticker}' for Year '{row.year}'. Reason: Missing/NaN critical financial fields or Total Assets <= 0.", APP_NAME, "WARNING")

    # --- MARKET CAP DATA QUALITY WARNING ---
    if good_count > 0:
        no_mcap_df = good_df.filter(col("Z_Score_Original").isNull())
        no_mcap_count = no_mcap_df.count()
        if no_mcap_count > 0:
            log_message(
                f"[Batch {batch_id}] MARKET CAP WARNING: {no_mcap_count} record(s) have no valid Market Cap. "
                f"Z_Original skipped; only Z_Prime calculated.",
                APP_NAME, "WARNING"
            )
            no_mcap_rows = no_mcap_df.select("ticker", "year").collect()
            for row in no_mcap_rows:
                log_message(
                    f"[Batch {batch_id}] NO MARKET CAP: Ticker '{row.ticker}' Year '{row.year}' → Z_Original=N/A, Z_Prime only.",
                    APP_NAME, "WARNING"
                )

    # --- DASHBOARD GENERATION (Handling Clean Data) ---
    if good_count > 0:
        log_message(f"[Batch {batch_id}] Proceeding with {good_count} valid records.", APP_NAME, "INFO")

        # Enrich with performance and anomaly analytics
        enriched_df = enrich_with_analytics(good_df)

        print(f"\n=======================================================")
        print(f"📥 [BATCH {batch_id}] NEW VALID STREAM DATA")
        print(f"=======================================================")
        enriched_df.select(
            upper(col("ticker")).alias("Company"),
            "year",
            "Z_Score_Original", "Health_Zone_Original",
            "Z_Score_Prime", "Health_Zone_Prime",
            "Performance", "Is_anomaly"
        ).show(truncate=False)
        
        # Write to Silver Storage (keep original ticker column for downstream joins)
        enriched_df.write.mode("append").parquet(SILVER_STORAGE_PATH)
        
        # Read Full Historical Dataset for Gold Layer Dashboards
        spark_session = enriched_df.sparkSession
        history_df = spark_session.read.parquet(SILVER_STORAGE_PATH)
        history_df.createOrReplaceTempView("historical_scores")

        # 1. Market Averages
        print(f"\n📊 [BATCH {batch_id}] GLOBAL MARKET AVERAGES BY YEAR")
        spark_session.sql("""
            SELECT year,
                   COUNT(ticker) as total_companies_analyzed,
                   ROUND(AVG(Z_Score_Prime), 2) as market_avg_z_prime,
                   ROUND(AVG(Z_Score_Original), 2) as market_avg_z_original
            FROM historical_scores
            GROUP BY year
            ORDER BY year DESC
        """).show(truncate=False)

        # 2. Leaderboards
        print(f"\n🏆 [BATCH {batch_id}] TOP-5 LEADERBOARD (By Modified Z'-Score)")
        spark_session.sql("""
            WITH RankedScores AS (
                SELECT year, UPPER(ticker) as Company, Z_Score_Prime, Health_Zone_Prime,
                       Performance, Is_anomaly,
                       RANK() OVER (PARTITION BY year ORDER BY Z_Score_Prime DESC) as rank
                FROM historical_scores
            )
            SELECT year, rank, Company, Z_Score_Prime, Health_Zone_Prime, Performance, Is_anomaly
            FROM RankedScores
            WHERE rank <= 5
            ORDER BY year DESC, rank ASC
        """).show(n=5, truncate=False)
        
        log_message(f"[Batch {batch_id}] Dashboards successfully recalculated.", APP_NAME, "INFO")

if __name__ == "__main__":
    try:
        log_message(f"Initializing {APP_NAME}...", APP_NAME, "INFO")
        clean_silver_storage()

        spark = SparkSession.builder \
            .appName(APP_NAME) \
            .master("local[*]") \
            .getOrCreate()
            
        spark.sparkContext.setLogLevel("WARN")

        # Read Stream (Using 'latest' for Live Demo)
        raw_stream = spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("subscribe", INPUT_TOPIC) \
            .option("startingOffsets", "latest") \
            .load()

        parsed_stream = raw_stream.select(
            from_json(col("value").cast("string"), get_schema()).alias("data")
        ).select("data.*")
        
        # Execute Pipeline
        validated_stream = data_quality_gate(parsed_stream)
        clean_stream = preprocess_and_engineer_features(validated_stream)
        scored_stream = calculate_dual_z_scores(clean_stream)

        log_message("Starting continuous stream and dashboard engine...", APP_NAME, "INFO")

        # Start Stream and execute micro-batches
        query = scored_stream.writeStream \
            .outputMode("append") \
            .foreachBatch(process_micro_batch) \
            .start()
        
        query.awaitTermination()

    except Exception as e:
        log_message(f"Fatal error: {str(e)}", APP_NAME, "ERROR")
        raise e