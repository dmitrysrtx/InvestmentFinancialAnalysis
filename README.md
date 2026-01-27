# Real-Time Financial Health Analysis (Altman Z-Score)

==============================================================================
DESCRIPTION
==============================================================================
This project implements a distributed system for assessing the financial health 
of companies using the **Altman Z-Score** model. 

Instead of training a "black box" Machine Learning model, we implement a 
deterministic financial formula within **Apache Spark Structured Streaming**. 
The system processes simulated real-time streams of financial reports (10-K), 
calculates the Z-Score coefficients on-the-fly, and classifies companies into 
Safe, Grey, or Distress zones.

**Architecture:**
1. **Data Source (Student 1):** Raw PDF/HTML reports parsed into structured CSV.
2. **Stream Producer (Python):** Simulates real-time arrival of reports via Kafka.
3. **Analysis Engine (Spark):** Consumes stream, computes Z-Score, and assigns ratings.

==============================================================================
THE ALTMAN Z-SCORE MODEL
==============================================================================
The system calculates the following coefficients based on raw financial data:

* **X1:** Working Capital / Total Assets
* **X2:** Retained Earnings / Total Assets
* **X3:** EBIT / Total Assets
* **X4:** Market Value of Equity / Total Liabilities
* **X5:** Sales / Total Assets

**Formula:** `Z = 1.2(X1) + 1.4(X2) + 3.3(X3) + 0.6(X4) + 1.0(X5)`

**Classification Zones:**
* 🟢 **Safe Zone:** Z > 2.99
* 🟡 **Grey Zone:** 1.81 < Z < 2.99
* 🔴 **Distress Zone:** Z < 1.81

==============================================================================
PREREQUISITES
==============================================================================
1.  **Environment:** Linux / VLAB (College Environment).
2.  **Kafka Location:** `/usr/local/kafka/kafka_2.13-3.2.1`
3.  **Python Libraries:** `kafka-python`, `pyspark`.

==============================================================================
EXECUTION INSTRUCTIONS
==============================================================================

STEP 0: CONFIGURE AND START KAFKA
---------------------------------
1.  Navigate to the Kafka directory:
    cd /usr/local/kafka/kafka_2.13-3.2.1

2.  FIX CONFIGURATION (One-time setup):
    sed -i 's|#listeners=PLAINTEXT://:9092|listeners=PLAINTEXT://:9092|' config/server.properties

3.  Start Zookeeper (Terminal 1):
    bin/zookeeper-server-start.sh config/zookeeper.properties

4.  Start Kafka Server (Terminal 2):
    bin/kafka-server-start.sh config/server.properties
    (Wait 15 seconds for initialization)

5.  Create the Topic (Terminal 3):
    bin/kafka-topics.sh --create --topic financial_reports_stream --bootstrap-server localhost:9092

------------------------------------------------------------------------------

STEP 1: RUN DATA PRODUCER (TERMINAL 3)
--------------------------------------
This script reads the cleaned CSV dataset (prepared by Student 1) and streams 
it to Kafka to simulate real-time reporting events.

1.  Navigate to your project folder:
    cd /path/to/project

2.  Run the producer:
    python3 producer.py

    > Output: "Sent: AAPL - 2022", "Sent: MSFT - 2023"...

------------------------------------------------------------------------------

STEP 2: RUN SPARK ALTMAN ANALYSIS (TERMINAL 4)
----------------------------------------------
This is the main analysis job. It reads from Kafka, calculates the 5 coefficients, 
applies the Z-Score formula, and outputs the Health Rating.

1.  Run the Spark job (ensure the package version matches your environment):
    
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 spark_altman.py

    > **Output:** The console will display a real-time updating table:
    > +------+----+-------+-------------+-----+-----+
    > |ticker|year|Z_Score|Health_Zone  |X1   |X2   |
    > +------+----+-------+-------------+-----+-----+
    > |AAPL  |2023|4.56   |Safe (Green) |0.4  |0.5  |
    > |ZOMB  |2023|1.10   |Distress (Red)|-0.2 |...  |
    > +------+----+-------+-------------+-----+-----+

==============================================================================
AUTHORS
==============================================================================
* **Student 1:** Raw Data Collection & PDF Parsing (ETL to CSV)
* **Student 2:** Kafka Streaming, Spark Implementation & Z-Score Analysis
* **Student 3:** Literature Review & Methodology Selection