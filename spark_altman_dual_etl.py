"""
Streaming ETL Consumer for Altman Z-Score (Original) and Z'-Score (Prime).

Pipeline stages:
  1. Kafka ingestion  → parse JSON into structured DataFrame
  2. Data Quality Gate → reject records with missing critical fields (DLQ logging)
  3. Preprocessing     → sanitize NaN/null values, engineer derived features (EBIT, total_liabilities)
  4. Dual Z-Score calc → compute both Original (market-cap) and Prime (book-value) scores
  5. Analytics enrich  → add Performance (vs. batch avg) and Anomaly Detection (>2σ)
  6. Silver persist    → upsert scored records to Parquet by (ticker, year) key (local_storage/silver_scores/)
  7. Gold dashboards   → compute market averages and Top-5 leaderboard from Silver data

Market Cap handling:
  - If market_cap is present and positive → Z_Score_Original is calculated.
  - If market_cap is absent/invalid       → Z_Score_Original = null, only Z'-Score Prime is computed.
"""

import os
import shutil
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, when, lit, round, isnan, avg, stddev, abs, upper, lower
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

def upsert_to_silver(new_df: DataFrame, spark_session: SparkSession, batch_id: int) -> DataFrame:
    """
    Upsert (merge) new records into the Silver Parquet storage.
    
    Composite key: (ticker, year).
    - If a record with the same (ticker, year) already exists in Parquet,
      it is replaced with the new incoming record (UPDATE).
    - If no matching record exists, the new record is inserted (INSERT).
    
    Returns the full merged DataFrame (existing unchanged + new/updated).
    """
    has_parquet_files = (
        os.path.exists(SILVER_STORAGE_PATH)
        and any(f.endswith(".parquet") for f in os.listdir(SILVER_STORAGE_PATH))
    )

    SILVER_TMP_PATH = SILVER_STORAGE_PATH + "_tmp"

    if has_parquet_files:
        existing_df = spark_session.read.parquet(SILVER_STORAGE_PATH)
        # Normalize tickers in existing data to lowercase.
        # Old Parquet files may contain uppercase tickers from runs before
        # the case-normalization fix was applied.
        existing_df = existing_df.withColumn("ticker", lower(col("ticker")))
        existing_count = existing_df.count()

        # Identify which new records match existing (ticker, year) — these are UPDATEs
        new_keys = new_df.select("ticker", "year")
        updated_df = existing_df.join(new_keys, on=["ticker", "year"], how="inner")
        updated_count = updated_df.count()
        inserted_count = new_df.count() - updated_count

        # Keep only existing records that are NOT being overwritten
        unchanged_df = existing_df.join(new_keys, on=["ticker", "year"], how="left_anti")

        # Merge: unchanged old records + all new records (updates + inserts)
        merged_df = unchanged_df.unionByName(new_df, allowMissingColumns=True)

        log_message(
            f"[Batch {batch_id}] UPSERT: {existing_count} existing records in Silver. "
            f"Incoming {new_df.count()}: {updated_count} UPDATED, {inserted_count} INSERTED.",
            APP_NAME, "INFO"
        )

        if updated_count > 0:
            updated_rows = updated_df.select("ticker", "year").collect()
            for row in updated_rows:
                log_message(
                    f"[Batch {batch_id}] UPDATED: Ticker '{row.ticker}' Year {row.year} — replaced with fresh stream data.",
                    APP_NAME, "INFO"
                )
    else:
        merged_df = new_df
        log_message(
            f"[Batch {batch_id}] UPSERT: No existing Silver data. "
            f"Inserting all {new_df.count()} records as new.",
            APP_NAME, "INFO"
        )

    # Write to a temporary path first, then atomically swap.
    # Writing directly to SILVER_STORAGE_PATH with mode("overwrite") would delete
    # the source Parquet files while the lazy merged_df still references them.
    if os.path.exists(SILVER_TMP_PATH):
        shutil.rmtree(SILVER_TMP_PATH)
    merged_df.write.mode("overwrite").parquet(SILVER_TMP_PATH)

    # Swap: remove old Silver, rename tmp → Silver
    if os.path.exists(SILVER_STORAGE_PATH):
        shutil.rmtree(SILVER_STORAGE_PATH)
    os.rename(SILVER_TMP_PATH, SILVER_STORAGE_PATH)

    # Read from the freshly written path for downstream use
    return spark_session.read.parquet(SILVER_STORAGE_PATH)

def get_schema() -> StructType:
    """
    Defines the expected JSON schema for incoming Kafka messages.
    Fields arrive pre-normalized (absolute values) from the producer side.
    The 'market_cap' field is optional — null/missing triggers Z'-Prime-only mode.
    """
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
        "retained_earnings", "stockholders_equity", "net_income", "total_revenue",
        "current_liabilities", "long_term_debt", "short_term_debt",
        "interest_expense", "tax_expense"
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
    Computes both Z-Score Original and Z'-Score Prime using safe division.

    Z_Score_Original uses market_cap for X4 — if market_cap is null,
    X4_Original propagates null, making Z_Score_Original null as well.
    Z_Score_Prime always uses stockholders_equity (book value) for X4.

    Applies final Anti-Poison filter to drop rows with NaN Z_Score_Prime.
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
        print(f"[BATCH {batch_id}] NEW VALID STREAM DATA")
        print(f"=======================================================")
        enriched_df.select(
            upper(col("ticker")).alias("Company"),
            "year",
            "Z_Score_Original", "Health_Zone_Original",
            "Z_Score_Prime", "Health_Zone_Prime",
            "Performance", "Is_anomaly"
        ).show(n=50, truncate=False)
        
        # Upsert to Silver Storage — merge with existing Parquet by (ticker, year)
        spark_session = enriched_df.sparkSession
        history_df = upsert_to_silver(enriched_df, spark_session, batch_id)
        
        # Use the full merged dataset for Gold Layer Dashboards
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
        # NOTE: clean_silver_storage() is intentionally NOT called here.
        # Silver Parquet data persists across runs so the upsert logic in
        # process_micro_batch() can detect existing (ticker, year) records
        # and update them instead of creating duplicates.
        # Call clean_silver_storage() manually if a full reset is needed.

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
        
        # Normalize ticker to lowercase for consistent upsert keys.
        # The manual parser sends lowercase tickers; the API producer now does too,
        # but this guard ensures any future producer also matches.
        parsed_stream = parsed_stream.withColumn("ticker", lower(col("ticker")))
        
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