# Real-Time Financial Health Analysis & Leaderboard

==============================================================================
DESCRIPTION
==============================================================================
This project implements a distributed Big Data system for assessing the financial 
health of companies using the **Altman Z-Score** model. 

The system simulates a real-time stream of financial reports (10-K), calculates 
Z-Scores on-the-fly, benchmarks companies against the market, and maintains a 
**Real-Time Top 5 Leaderboard** of the healthiest companies per year.

**Key Features:**
1. **Deterministic Financial Modeling:** Implementation of the Altman Z-Score formula (1968).
2. **Dynamic Benchmarking:** Comparing individual performance vs. real-time sector averages.
3. **Anomaly Detection:** Identifying statistical outliers (>2σ from the mean).
4. **Live Leaderboard:** A constantly updating "Top 5" list using Spark Window Functions.

==============================================================================
ARCHITECTURE & ROLES
==============================================================================
* **Student 1 (ETL):** Collects unstructured PDF/HTML reports, parses them using Spark Batch, 
  and persists clean data to CSV.
* **Student 2 (Streaming):** Implements the Kafka Producer and the Spark Structured 
  Streaming job (Z-Score calculation, Ranking, and Analytics).
* **Student 3 (Research):** Methodology selection and Literature Review.

==============================================================================
THE ALTMAN Z-SCORE MODEL
==============================================================================
**Formula:** `Z = 1.2(X1) + 1.4(X2) + 3.3(X3) + 0.6(X4) + 1.0(X5)`

**Variables:**
* **X1:** Working Capital / Total Assets
* **X2:** Retained Earnings / Total Assets
* **X3:** EBIT / Total Assets
* **X4:** Market Value of Equity / Total Liabilities
* **X5:** Sales / Total Assets

**Zones:**
* 🟢 **Safe:** Z > 2.99
* 🟡 **Grey:** 1.81 < Z < 2.99
* 🔴 **Distress:** Z < 1.81

==============================================================================
EXECUTION INSTRUCTIONS
==============================================================================

STEP 0: CONFIGURE AND START KAFKA
---------------------------------
1.  Navigate to Kafka directory:
    cd /usr/local/kafka/kafka_2.13-3.2.1

2.  Fix Configuration (One-time setup):
    sed -i 's|#listeners=PLAINTEXT://:9092|listeners=PLAINTEXT://:9092|' config/server.properties

3.  Start Zookeeper (Terminal 1):
    bin/zookeeper-server-start.sh config/zookeeper.properties

4.  Start Kafka Server (Terminal 2):
    bin/kafka-server-start.sh config/server.properties

5.  Create Topic (Terminal 3):
    bin/kafka-topics.sh --create --topic financial_reports_stream --bootstrap-server localhost:9092

------------------------------------------------------------------------------

STEP 1: RUN DATA PRODUCER (TERMINAL 3)
--------------------------------------
Simulates real-time report ingestion from the cleaned CSV dataset.

1.  Navigate to project folder:
    cd /path/to/project

2.  Run producer:
    python3 producer.py

------------------------------------------------------------------------------

STEP 2: RUN SPARK ANALYTICS ENGINE (TERMINAL 4)
-----------------------------------------------
This job computes the Z-Scores and maintains the Top 5 Leaderboard.
**Note:** This job runs in 'Complete' mode to handle dynamic re-ranking of companies.

1.  Submit the Spark job:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 spark_altman_etl.py

    > **Expected Output:**
    > +----+----+------+-------+-------------+---------------+----------+
    > |year|Rank|ticker|Z_Score|Health_Zone  |Performance    |Is_Anomaly|
    > +----+----+------+-------+-------------+---------------+----------+
    > |2023|1   |NVDA  |15.40  |Safe (Green) |Outperforming  |YES       |
    > |2023|2   |MSFT  |8.20   |Safe (Green) |Outperforming  |No        |
    > |2023|3   |AAPL  |7.55   |Safe (Green) |Outperforming  |No        |
    > ...
    > +----+----+------+-------+-------------+---------------+----------+

==============================================================================
TROUBLESHOOTING
==============================================================================
* **Why 'Complete' Mode?** We use `outputMode("complete")` because ranking (Top 5) is a relative calculation. 
  When a new high-performing company arrives, it shifts the rank of all others, 
  requiring a refresh of the entire leaderboard table.