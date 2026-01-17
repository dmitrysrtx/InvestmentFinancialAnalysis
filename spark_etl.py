from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lead, when
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.sql.window import Window

# --- SPARK INIT ---
# We use the standard SparkSession builder
spark = SparkSession.builder \
    .appName("Financial_ETL_Project") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# --- SCHEMA DEFINITION ---
# Must match the JSON sent by the producer
schema = StructType([
    StructField("ticker", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("report_date", StringType(), True),
    StructField("total_revenue", FloatType(), True),
    StructField("net_income", FloatType(), True),
    StructField("current_assets", FloatType(), True),
    StructField("current_liabilities", FloatType(), True),
    StructField("total_assets", FloatType(), True),
    StructField("total_liabilities", FloatType(), True),
    StructField("stockholders_equity", FloatType(), True),
    StructField("interest_expense", FloatType(), True),
    StructField("ebit", FloatType(), True),
    StructField("close_price", FloatType(), True)
])

# --- 1. READ FROM KAFKA ---
# We use 'read' (Batch) instead of 'readStream' here to allow 
# complex Window functions (looking at future prices) for training data generation.
print("Reading data from Kafka...")
df_raw = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "financial_reports_stream") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON payload (value is binary in Kafka)
df_parsed = df_raw.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# --- 2. FEATURE ENGINEERING (Formulas from your Image) ---
print("Calculating Financial Ratios...")

# Avoid division by zero issues
df_features = df_parsed.na.fill(0.0)

df_ratios = df_features \
    .withColumn("current_ratio", col("current_assets") / col("current_liabilities")) \
    .withColumn("net_profit_margin", col("net_income") / col("total_revenue")) \
    .withColumn("roe", col("net_income") / col("stockholders_equity")) \
    .withColumn("roa", col("net_income") / col("total_assets")) \
    .withColumn("debt_to_equity", col("total_liabilities") / col("stockholders_equity")) \
    .withColumn("debt_to_assets", col("total_liabilities") / col("total_assets"))

# --- 3. CREATE TARGET LABEL (Supervised Learning) ---
# Logic: Compare current year's price with next year's price.
# We define a Window partitioned by Ticker and ordered by Year.
windowSpec = Window.partitionBy("ticker").orderBy("year")

# Look ahead 1 year to get the future price
df_labeled = df_ratios.withColumn("next_year_price", lead("close_price", 1).over(windowSpec))

# Calculate Percentage Return
df_final = df_labeled.withColumn("yearly_return", 
                                 (col("next_year_price") - col("close_price")) / col("close_price"))

# DEFINE 'HEALTHY' (Label = 1):
# If the stock grew by more than 5% (0.05) next year, we consider the current report "Healthy".
df_dataset = df_final.withColumn("label", when(col("yearly_return") > 0.05, 1.0).otherwise(0.0))

# Remove rows where we don't have next year's data (the most recent year)
df_dataset = df_dataset.filter(col("next_year_price").isNotNull())

# Clean up infinite values created by division by zero
df_dataset = df_dataset.na.drop()

print("Preview of Training Data:")
df_dataset.select("ticker", "year", "roe", "debt_to_equity", "label").show(5)

# --- 4. SAVE TO STORAGE ---
# Save as Parquet for the ML model to read efficiently
output_path = "financial_training_data.parquet"
df_dataset.write.mode("overwrite").parquet(output_path)
print(f"Data saved to {output_path}")

spark.stop()