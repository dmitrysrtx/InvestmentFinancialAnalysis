from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, lit, round
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

# --- CONFIG ---
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "financial_reports_stream"

# --- INIT SPARK ---
spark = SparkSession.builder \
    .appName("Altman_Z_Score_Analysis") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "5") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# --- SCHEMA ---
# Alligned with the JSON structure from Kafka
schema = StructType([
    StructField("ticker", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("total_assets", FloatType(), True),
    StructField("current_assets", FloatType(), True),
    StructField("current_liabilities", FloatType(), True),
    StructField("total_liabilities", FloatType(), True),
    StructField("retained_earnings", FloatType(), True),  # Important for Altman
    StructField("ebit", FloatType(), True),               # Earnings Before Interest & Taxes
    StructField("market_cap", FloatType(), True),         # Market Value of Equity
    StructField("total_revenue", FloatType(), True)       # Sales
])

# --- 1. READ STREAM FROM KAFKA ---
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", INPUT_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# Парсинг JSON
json_stream = raw_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# --- 2. CALCULATE ALTMAN COEFFICIENTS (X1 - X5) ---
# X1 = Working Capital / Total Assets
# Working Capital = Current Assets - Current Liabilities
df_coeffs = json_stream.withColumn(
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

# --- 3. CALCULATE Z-SCORE ---
# Formula: Z = 1.2*X1 + 1.4*X2 + 3.3*X3 + 0.6*X4 + 1.0*X5
df_z_score = df_coeffs.withColumn(
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

# --- 4. ASSIGN HEALTH RATING (ZONES) ---
# Safe Zone: Z > 2.99
# Grey Zone: 1.81 < Z < 2.99
# Distress Zone: Z < 1.81
df_rated = df_z_score.withColumn(
    "Health_Zone",
    when(col("Z_Score") > 2.99, "Safe (Green)")
    .when((col("Z_Score") >= 1.81) & (col("Z_Score") <= 2.99), "Grey (Caution)")
    .otherwise("Distress (Red)")
)

# Choose relevant columns for output
final_output = df_rated.select(
    "ticker", "year", "Z_Score", "Health_Zone", "X1", "X2", "X3", "X4", "X5"
)

# --- 5. OUTPUT TO CONSOLE (FOR DEBUGGING) ---
query = final_output.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()