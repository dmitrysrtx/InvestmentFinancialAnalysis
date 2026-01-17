# Financial Health Prediction System using Apache Spark & Kafka

## 📌 Project Overview
This project is a semester assignment for the **Apache Spark** course. It demonstrates a complete Data Engineering and Machine Learning pipeline that simulates the processing of financial reports to predict the investment potential of technology companies.

The system ingests historical financial data (simulating a real-time stream), processes it using **Apache Spark**, calculates key financial ratios (Fundamental Analysis), and trains a **Random Forest** model to classify companies as "Healthy" (Buy) or "Unhealthy" (Avoid).

## 🚀 Architecture
The pipeline consists of three main components:

1.  **Data Ingestion (Producer):**
    * Fetches historical financial data (Balance Sheet, Income Statement) using `yfinance`.
    * Streams data as JSON events into an **Apache Kafka** topic (`financial_reports_stream`).
2.  **ETL & Processing (Spark Core/SQL):**
    * Consumes data from Kafka.
    * Performs **Feature Engineering**: Calculates financial ratios (ROE, ROA, Current Ratio, etc.).
    * Performs **Data Labeling**: Compares the stock price at the time of the report with the price *one year later*. If the return is > 5%, the label is set to `1` (Healthy).
    * Saves the processed dataset to Parquet format.
3.  **Machine Learning (Spark MLlib):**
    * Loads the Parquet data.
    * Trains a **Random Forest Classifier**.
    * Evaluates the model's accuracy in predicting future company performance based on financial ratios.

## 🛠️ Prerequisites

* **Python 3.8+**
* **Apache Kafka**
* **Apache Spark 3.x**
* **Java 8/11** (Required for Spark/Kafka)

### Python Dependencies
```bash
pip install yfinance kafka-python pyspark numpy pandas