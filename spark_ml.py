from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# --- SPARK INIT ---
spark = SparkSession.builder \
    .appName("Financial_Prediction_ML") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# --- 1. LOAD DATA ---
print("Loading processed data...")
df = spark.read.parquet("financial_training_data.parquet")

# --- 2. PREPARE FEATURES ---
# These input columns match the formulas from your image
input_cols = [
    "current_ratio", 
    "net_profit_margin", 
    "roe", 
    "roa", 
    "debt_to_equity",
    "debt_to_assets"
]

# Combine all columns into a single vector column named "features"
assembler = VectorAssembler(inputCols=input_cols, outputCol="features_raw")
df_vector = assembler.transform(df)

# Standardize features (Scale them so large numbers don't dominate)
scaler = StandardScaler(inputCol="features_raw", outputCol="features")
scaler_model = scaler.fit(df_vector)
df_ready = scaler_model.transform(df_vector)

# Select only relevant columns
data = df_ready.select("ticker", "year", "features", "label")

# --- 3. TRAIN/TEST SPLIT ---
# Use the most recent year for testing
print("Splitting data by year...")
max_year = data.select(max("year")).first()[0]
print(f"Using year {max_year} for testing.")

train_data = data.filter(col("year") < max_year)
test_data = data.filter(col("year") == max_year)

print(f"Training on {train_data.count()} records.")
print(f"Testing on {test_data.count()} records.")

# --- 4. MODEL TRAINING (Random Forest) ---
# We use Random Forest because it works well with tabular financial data
rf = RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=50, maxDepth=5)
rf_model = rf.fit(train_data)

# --- 5. EVALUATION ---
predictions = rf_model.transform(test_data)

print("\n--- Predictions Preview ---")
predictions.select("ticker", "year", "label", "prediction", "probability").show(5)

evaluator = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy")

accuracy = evaluator.evaluate(predictions)
print(f"\nModel Accuracy: {accuracy*100:.2f}%")

# Show which financial ratio was most important
print("\n--- Feature Importance ---")
importances = rf_model.featureImportances
for i, col_name in enumerate(input_cols):
    print(f"{col_name}: {importances[i]:.4f}")

spark.stop()