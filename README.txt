# Financial Health Prediction System (Apache Spark & Kafka)

==============================================================================
DESCRIPTION
==============================================================================
This project implements a simulated real-time Financial Health Prediction system
   using Apache Spark Structured Streaming, Kafka, and Machine Learning.
The system ingests historical financial data (simulating a real-time stream),
   processes it using **Apache Spark**, calculates key financial ratios
   (Fundamental Analysis), and trains a **Random Forest** model to classify
   companies as "Healthy" (Buy) or "Unhealthy" (Avoid).

==============================================================================
ARCHITECTURE
==============================================================================
The pipeline consists of three main components:

1.  **Data Ingestion (Producer):**
    * Fetches historical financial data (Balance Sheet, Income Statement) using `yfinance`.
    * Streams data as JSON events into an **Apache Kafka** topic (`financial_reports_stream`).
2.  **ETL & Processing (Spark Core/SQL):**
    * Consumes data from Kafka.
    * Performs **Feature Engineering**: Calculates financial ratios (ROE, ROA, Current Ratio, etc.).
    * Performs **Data Labeling**: Compares the stock price at the time of the report with the
      price *one year later*. If the return is > 5%, the label is set to `1` (Healthy).
    * Saves the processed dataset to Parquet format.
3.  **Machine Learning (Spark MLlib):**
    * Loads the Parquet data.
    * Trains a **Random Forest Classifier**.
    * Evaluates the model's accuracy in predicting future company performance based on financial ratios.

==============================================================================
PREREQUISITES
==============================================================================
1. Environment: Linux / VLAB (College Environment).
2. Kafka installed at: /usr/local/kafka/kafka_2.13-3.2.1
### Python Dependencies
```bash
pip install yfinance kafka-python pyspark numpy pandas
pip install --upgrade kafka-python


==============================================================================
EXECUTION INSTRUCTIONS
==============================================================================

STEP 0: CONFIGURE AND START KAFKA
---------------------------------
Before running the application, you must configure Kafka to listen on the 
correct port and start the services.

1. Navigate to the Kafka directory:
   cd /usr/local/kafka/kafka_2.13-3.2.1

2. FIX CONFIGURATION (One-time setup):
   Run the following command to uncomment the listener configuration. 
   This ensures Kafka listens on port 9092 and avoids connection errors.

   sed -i 's|#listeners=PLAINTEXT://:9092|listeners=PLAINTEXT://:9092|' config/server.properties

3. Start Zookeeper (Terminal 1):
   bin/zookeeper-server-start.sh config/zookeeper.properties

4. Start Kafka Server (Terminal 2):
   bin/kafka-server-start.sh config/server.properties

   > Wait about 15 seconds for the server to fully initialize.

5. Create the Topic (Terminal 3):
   bin/kafka-topics.sh --create --topic financial_reports_stream --bootstrap-server localhost:9092

------------------------------------------------------------------------------

STEP 1: RUN DATA PRODUCER (TERMINAL 3)
--------------------------------------
This script acts as the source of data. It fetches financial reports and pushes 
them to the Kafka topic 'financial_reports_stream'.

1. Navigate to your project folder (where you saved the python scripts):
   cd /path/to/your/project/folder

2. Run the producer script:
   python3 producer.py

   > Output: You should see logs indicating data is being sent:
   > "Sent: AAPL - 2022"
   > "Sent: MSFT - 2021"
   > ...
   > "--- Data Ingestion Complete ---"

------------------------------------------------------------------------------

STEP 2: RUN SPARK ETL JOB (TERMINAL 4)
--------------------------------------
This step processes the stream, calculates financial ratios, and creates the 
training dataset.

1. Navigate to your project folder.

2. Run the Spark ETL job using spark-submit.
   Note: We explicitly include the 'spark-sql-kafka' package compatible with 
   Spark 3.2.1 (the version used in the VLAB environment).

   PYSPARK_DRIVER_PYTHON=python spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 spark_etl.py

   > Wait for the process to finish.
   > Success Indicator: A folder named 'financial_training_data.parquet' will 
   > be created in your directory, and the console will print "Data saved".
   Output ex.:
      26/01/19 11:11:27 WARN Utils: Your hostname, Ubuntu26BD-13 resolves to a loopback address: 127.0.1.1; using 192.168.90.46 instead (on interface ens160)
      26/01/19 11:11:27 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
      :: loading settings :: url = jar:file:/usr/local/spark/spark-3.3.0-bin-hadoop3/jars/ivy-2.5.0.jar!/org/apache/ivy/core/settings/ivysettings.xml
      Ivy Default Cache set to: /home/linuxu/.ivy2/cache
      The jars for the packages stored in: /home/linuxu/.ivy2/jars
      org.apache.spark#spark-sql-kafka-0-10_2.12 added as a dependency
      :: resolving dependencies :: org.apache.spark#spark-submit-parent-3d6ca3a9-f650-4193-9a75-b948b6287047;1.0
            confs: [default]
            found org.apache.spark#spark-sql-kafka-0-10_2.12;3.2.1 in central
            found org.apache.spark#spark-token-provider-kafka-0-10_2.12;3.2.1 in central
            found org.apache.kafka#kafka-clients;2.8.0 in central
            found org.lz4#lz4-java;1.7.1 in central
            found org.xerial.snappy#snappy-java;1.1.8.4 in central
            found org.slf4j#slf4j-api;1.7.30 in central
            found org.apache.hadoop#hadoop-client-runtime;3.3.1 in central
            found org.spark-project.spark#unused;1.0.0 in central
            found org.apache.hadoop#hadoop-client-api;3.3.1 in central
            found org.apache.htrace#htrace-core4;4.1.0-incubating in central
            found commons-logging#commons-logging;1.1.3 in central
            found com.google.code.findbugs#jsr305;3.0.0 in central
            found org.apache.commons#commons-pool2;2.6.2 in central
      :: resolution report :: resolve 628ms :: artifacts dl 22ms
            :: modules in use:
            com.google.code.findbugs#jsr305;3.0.0 from central in [default]
            commons-logging#commons-logging;1.1.3 from central in [default]
            org.apache.commons#commons-pool2;2.6.2 from central in [default]
            org.apache.hadoop#hadoop-client-api;3.3.1 from central in [default]
            org.apache.hadoop#hadoop-client-runtime;3.3.1 from central in [default]
            org.apache.htrace#htrace-core4;4.1.0-incubating from central in [default]
            org.apache.kafka#kafka-clients;2.8.0 from central in [default]
            org.apache.spark#spark-sql-kafka-0-10_2.12;3.2.1 from central in [default]
            org.apache.spark#spark-token-provider-kafka-0-10_2.12;3.2.1 from central in [default]
            org.lz4#lz4-java;1.7.1 from central in [default]
            org.slf4j#slf4j-api;1.7.30 from central in [default]
            org.spark-project.spark#unused;1.0.0 from central in [default]
            org.xerial.snappy#snappy-java;1.1.8.4 from central in [default]
            ---------------------------------------------------------------------
            |                  |            modules            ||   artifacts   |
            |       conf       | number| search|dwnlded|evicted|| number|dwnlded|
            ---------------------------------------------------------------------
            |      default     |   13  |   0   |   0   |   0   ||   13  |   0   |
            ---------------------------------------------------------------------
      :: retrieving :: org.apache.spark#spark-submit-parent-3d6ca3a9-f650-4193-9a75-b948b6287047
            confs: [default]
            0 artifacts copied, 13 already retrieved (0kB/11ms)
      26/01/19 11:11:28 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
      26/01/19 11:11:29 INFO SparkContext: Running Spark version 3.3.0
      26/01/19 11:11:29 INFO ResourceUtils: ==============================================================
      26/01/19 11:11:29 INFO ResourceUtils: No custom resources configured for spark.driver.
      26/01/19 11:11:29 INFO ResourceUtils: ==============================================================
      26/01/19 11:11:29 INFO SparkContext: Submitted application: Financial_ETL_Project
      26/01/19 11:11:29 INFO ResourceProfile: Default ResourceProfile created, executor resources: Map(cores -> name: cores, amount: 1, script: , vendor: , memory -> name: memory, amount: 1024, script: , vendor: , offHeap -> name: offHeap, amount: 0, script: , vendor: ), task resources: Map(cpus -> name: cpus, amount: 1.0)
      26/01/19 11:11:29 INFO ResourceProfile: Limiting resource is cpu
      26/01/19 11:11:29 INFO ResourceProfileManager: Added ResourceProfile id: 0
      26/01/19 11:11:30 INFO SecurityManager: Changing view acls to: linuxu
      26/01/19 11:11:30 INFO SecurityManager: Changing modify acls to: linuxu
      26/01/19 11:11:30 INFO SecurityManager: Changing view acls groups to: 
      26/01/19 11:11:30 INFO SecurityManager: Changing modify acls groups to: 
      26/01/19 11:11:30 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(linuxu); groups with view permissions: Set(); users  with modify permissions: Set(linuxu); groups with modify permissions: Set()
      26/01/19 11:11:30 INFO Utils: Successfully started service 'sparkDriver' on port 35735.
      26/01/19 11:11:30 INFO SparkEnv: Registering MapOutputTracker
      26/01/19 11:11:30 INFO SparkEnv: Registering BlockManagerMaster
      26/01/19 11:11:30 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
      26/01/19 11:11:30 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
      26/01/19 11:11:30 INFO SparkEnv: Registering BlockManagerMasterHeartbeat
      26/01/19 11:11:30 INFO DiskBlockManager: Created local directory at /tmp/blockmgr-18090512-9eae-46c9-b4ba-6fd53d320f95
      26/01/19 11:11:30 INFO MemoryStore: MemoryStore started with capacity 434.4 MiB
      26/01/19 11:11:30 INFO SparkEnv: Registering OutputCommitCoordinator
      26/01/19 11:11:31 INFO Utils: Successfully started service 'SparkUI' on port 4040.
      26/01/19 11:11:31 INFO SparkContext: Added JAR file:///home/linuxu/.ivy2/jars/org.apache.spark_spark-sql-kafka-0-10_2.12-3.2.1.jar at spark://192.168.90.46:35735/jars/org.apache.spark_spark-sql-kafka-0-10_2.12-3.2.1.jar with timestamp 1768813889831
      26/01/19 11:11:31 INFO SparkContext: Added JAR file:///home/linuxu/.ivy2/jars/org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.2.1.jar at spark://192.168.90.46:35735/jars/org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.2.1.jar with timestamp 1768813889831
      26/01/19 11:11:31 INFO SparkContext: Added JAR file:///home/linuxu/.ivy2/jars/org.apache.kafka_kafka-clients-2.8.0.jar at spark://192.168.90.46:35735/jars/org.apache.kafka_kafka-clients-2.8.0.jar with timestamp 1768813889831
      26/01/19 11:11:31 INFO SparkContext: Added JAR file:///home/linuxu/.ivy2/jars/com.google.code.findbugs_jsr305-3.0.0.jar at spark://192.168.90.46:35735/jars/com.google.code.findbugs_jsr305-3.0.0.jar with timestamp 1768813889831
      26/01/19 11:11:31 INFO SparkContext: Added JAR file:///home/linuxu/.ivy2/jars/org.apache.commons_commons-pool2-2.6.2.jar at spark://192.168.90.46:35735/jars/org.apache.commons_commons-pool2-2.6.2.jar with timestamp 1768813889831
      26/01/19 11:11:31 INFO SparkContext: Added JAR file:///home/linuxu/.ivy2/jars/org.spark-project.spark_unused-1.0.0.jar at spark://192.168.90.46:35735/jars/org.spark-project.spark_unused-1.0.0.jar with timestamp 1768813889831
      26/01/19 11:11:31 INFO SparkContext: Added JAR file:///home/linuxu/.ivy2/jars/org.apache.hadoop_hadoop-client-runtime-3.3.1.jar at spark://192.168.90.46:35735/jars/org.apache.hadoop_hadoop-client-runtime-3.3.1.jar with timestamp 1768813889831
      26/01/19 11:11:31 INFO SparkContext: Added JAR file:///home/linuxu/.ivy2/jars/org.lz4_lz4-java-1.7.1.jar at spark://192.168.90.46:35735/jars/org.lz4_lz4-java-1.7.1.jar with timestamp 1768813889831
      26/01/19 11:11:31 INFO SparkContext: Added JAR file:///home/linuxu/.ivy2/jars/org.xerial.snappy_snappy-java-1.1.8.4.jar at spark://192.168.90.46:35735/jars/org.xerial.snappy_snappy-java-1.1.8.4.jar with timestamp 1768813889831
      26/01/19 11:11:31 INFO SparkContext: Added JAR file:///home/linuxu/.ivy2/jars/org.slf4j_slf4j-api-1.7.30.jar at spark://192.168.90.46:35735/jars/org.slf4j_slf4j-api-1.7.30.jar with timestamp 1768813889831
      26/01/19 11:11:31 INFO SparkContext: Added JAR file:///home/linuxu/.ivy2/jars/org.apache.hadoop_hadoop-client-api-3.3.1.jar at spark://192.168.90.46:35735/jars/org.apache.hadoop_hadoop-client-api-3.3.1.jar with timestamp 1768813889831
      26/01/19 11:11:31 INFO SparkContext: Added JAR file:///home/linuxu/.ivy2/jars/org.apache.htrace_htrace-core4-4.1.0-incubating.jar at spark://192.168.90.46:35735/jars/org.apache.htrace_htrace-core4-4.1.0-incubating.jar with timestamp 1768813889831
      26/01/19 11:11:31 INFO SparkContext: Added JAR file:///home/linuxu/.ivy2/jars/commons-logging_commons-logging-1.1.3.jar at spark://192.168.90.46:35735/jars/commons-logging_commons-logging-1.1.3.jar with timestamp 1768813889831
      26/01/19 11:11:31 INFO SparkContext: Added file file:///home/linuxu/.ivy2/jars/org.apache.spark_spark-sql-kafka-0-10_2.12-3.2.1.jar at file:///home/linuxu/.ivy2/jars/org.apache.spark_spark-sql-kafka-0-10_2.12-3.2.1.jar with timestamp 1768813889831
      26/01/19 11:11:31 INFO Utils: Copying /home/linuxu/.ivy2/jars/org.apache.spark_spark-sql-kafka-0-10_2.12-3.2.1.jar to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.apache.spark_spark-sql-kafka-0-10_2.12-3.2.1.jar
      26/01/19 11:11:31 INFO SparkContext: Added file file:///home/linuxu/.ivy2/jars/org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.2.1.jar at file:///home/linuxu/.ivy2/jars/org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.2.1.jar with timestamp 1768813889831
      26/01/19 11:11:31 INFO Utils: Copying /home/linuxu/.ivy2/jars/org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.2.1.jar to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.2.1.jar
      26/01/19 11:11:31 INFO SparkContext: Added file file:///home/linuxu/.ivy2/jars/org.apache.kafka_kafka-clients-2.8.0.jar at file:///home/linuxu/.ivy2/jars/org.apache.kafka_kafka-clients-2.8.0.jar with timestamp 1768813889831
      26/01/19 11:11:31 INFO Utils: Copying /home/linuxu/.ivy2/jars/org.apache.kafka_kafka-clients-2.8.0.jar to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.apache.kafka_kafka-clients-2.8.0.jar
      26/01/19 11:11:31 INFO SparkContext: Added file file:///home/linuxu/.ivy2/jars/com.google.code.findbugs_jsr305-3.0.0.jar at file:///home/linuxu/.ivy2/jars/com.google.code.findbugs_jsr305-3.0.0.jar with timestamp 1768813889831
      26/01/19 11:11:31 INFO Utils: Copying /home/linuxu/.ivy2/jars/com.google.code.findbugs_jsr305-3.0.0.jar to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/com.google.code.findbugs_jsr305-3.0.0.jar
      26/01/19 11:11:31 INFO SparkContext: Added file file:///home/linuxu/.ivy2/jars/org.apache.commons_commons-pool2-2.6.2.jar at file:///home/linuxu/.ivy2/jars/org.apache.commons_commons-pool2-2.6.2.jar with timestamp 1768813889831
      26/01/19 11:11:31 INFO Utils: Copying /home/linuxu/.ivy2/jars/org.apache.commons_commons-pool2-2.6.2.jar to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.apache.commons_commons-pool2-2.6.2.jar
      26/01/19 11:11:31 INFO SparkContext: Added file file:///home/linuxu/.ivy2/jars/org.spark-project.spark_unused-1.0.0.jar at file:///home/linuxu/.ivy2/jars/org.spark-project.spark_unused-1.0.0.jar with timestamp 1768813889831
      26/01/19 11:11:31 INFO Utils: Copying /home/linuxu/.ivy2/jars/org.spark-project.spark_unused-1.0.0.jar to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.spark-project.spark_unused-1.0.0.jar
      26/01/19 11:11:31 INFO SparkContext: Added file file:///home/linuxu/.ivy2/jars/org.apache.hadoop_hadoop-client-runtime-3.3.1.jar at file:///home/linuxu/.ivy2/jars/org.apache.hadoop_hadoop-client-runtime-3.3.1.jar with timestamp 1768813889831
      26/01/19 11:11:31 INFO Utils: Copying /home/linuxu/.ivy2/jars/org.apache.hadoop_hadoop-client-runtime-3.3.1.jar to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.apache.hadoop_hadoop-client-runtime-3.3.1.jar
      26/01/19 11:11:31 INFO SparkContext: Added file file:///home/linuxu/.ivy2/jars/org.lz4_lz4-java-1.7.1.jar at file:///home/linuxu/.ivy2/jars/org.lz4_lz4-java-1.7.1.jar with timestamp 1768813889831
      26/01/19 11:11:31 INFO Utils: Copying /home/linuxu/.ivy2/jars/org.lz4_lz4-java-1.7.1.jar to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.lz4_lz4-java-1.7.1.jar
      26/01/19 11:11:31 INFO SparkContext: Added file file:///home/linuxu/.ivy2/jars/org.xerial.snappy_snappy-java-1.1.8.4.jar at file:///home/linuxu/.ivy2/jars/org.xerial.snappy_snappy-java-1.1.8.4.jar with timestamp 1768813889831
      26/01/19 11:11:31 INFO Utils: Copying /home/linuxu/.ivy2/jars/org.xerial.snappy_snappy-java-1.1.8.4.jar to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.xerial.snappy_snappy-java-1.1.8.4.jar
      26/01/19 11:11:31 INFO SparkContext: Added file file:///home/linuxu/.ivy2/jars/org.slf4j_slf4j-api-1.7.30.jar at file:///home/linuxu/.ivy2/jars/org.slf4j_slf4j-api-1.7.30.jar with timestamp 1768813889831
      26/01/19 11:11:31 INFO Utils: Copying /home/linuxu/.ivy2/jars/org.slf4j_slf4j-api-1.7.30.jar to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.slf4j_slf4j-api-1.7.30.jar
      26/01/19 11:11:31 INFO SparkContext: Added file file:///home/linuxu/.ivy2/jars/org.apache.hadoop_hadoop-client-api-3.3.1.jar at file:///home/linuxu/.ivy2/jars/org.apache.hadoop_hadoop-client-api-3.3.1.jar with timestamp 1768813889831
      26/01/19 11:11:31 INFO Utils: Copying /home/linuxu/.ivy2/jars/org.apache.hadoop_hadoop-client-api-3.3.1.jar to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.apache.hadoop_hadoop-client-api-3.3.1.jar
      26/01/19 11:11:31 INFO SparkContext: Added file file:///home/linuxu/.ivy2/jars/org.apache.htrace_htrace-core4-4.1.0-incubating.jar at file:///home/linuxu/.ivy2/jars/org.apache.htrace_htrace-core4-4.1.0-incubating.jar with timestamp 1768813889831
      26/01/19 11:11:31 INFO Utils: Copying /home/linuxu/.ivy2/jars/org.apache.htrace_htrace-core4-4.1.0-incubating.jar to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.apache.htrace_htrace-core4-4.1.0-incubating.jar
      26/01/19 11:11:31 INFO SparkContext: Added file file:///home/linuxu/.ivy2/jars/commons-logging_commons-logging-1.1.3.jar at file:///home/linuxu/.ivy2/jars/commons-logging_commons-logging-1.1.3.jar with timestamp 1768813889831
      26/01/19 11:11:31 INFO Utils: Copying /home/linuxu/.ivy2/jars/commons-logging_commons-logging-1.1.3.jar to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/commons-logging_commons-logging-1.1.3.jar
      26/01/19 11:11:31 INFO Executor: Starting executor ID driver on host 192.168.90.46
      26/01/19 11:11:31 INFO Executor: Starting executor with user classpath (userClassPathFirst = false): ''
      26/01/19 11:11:31 INFO Executor: Fetching file:///home/linuxu/.ivy2/jars/org.apache.spark_spark-sql-kafka-0-10_2.12-3.2.1.jar with timestamp 1768813889831
      26/01/19 11:11:31 INFO Utils: /home/linuxu/.ivy2/jars/org.apache.spark_spark-sql-kafka-0-10_2.12-3.2.1.jar has been previously copied to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.apache.spark_spark-sql-kafka-0-10_2.12-3.2.1.jar
      26/01/19 11:11:31 INFO Executor: Fetching file:///home/linuxu/.ivy2/jars/commons-logging_commons-logging-1.1.3.jar with timestamp 1768813889831
      26/01/19 11:11:31 INFO Utils: /home/linuxu/.ivy2/jars/commons-logging_commons-logging-1.1.3.jar has been previously copied to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/commons-logging_commons-logging-1.1.3.jar
      26/01/19 11:11:31 INFO Executor: Fetching file:///home/linuxu/.ivy2/jars/org.apache.commons_commons-pool2-2.6.2.jar with timestamp 1768813889831
      26/01/19 11:11:31 INFO Utils: /home/linuxu/.ivy2/jars/org.apache.commons_commons-pool2-2.6.2.jar has been previously copied to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.apache.commons_commons-pool2-2.6.2.jar
      26/01/19 11:11:31 INFO Executor: Fetching file:///home/linuxu/.ivy2/jars/org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.2.1.jar with timestamp 1768813889831
      26/01/19 11:11:31 INFO Utils: /home/linuxu/.ivy2/jars/org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.2.1.jar has been previously copied to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.2.1.jar
      26/01/19 11:11:31 INFO Executor: Fetching file:///home/linuxu/.ivy2/jars/org.apache.hadoop_hadoop-client-api-3.3.1.jar with timestamp 1768813889831
      26/01/19 11:11:31 INFO Utils: /home/linuxu/.ivy2/jars/org.apache.hadoop_hadoop-client-api-3.3.1.jar has been previously copied to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.apache.hadoop_hadoop-client-api-3.3.1.jar
      26/01/19 11:11:31 INFO Executor: Fetching file:///home/linuxu/.ivy2/jars/org.spark-project.spark_unused-1.0.0.jar with timestamp 1768813889831
      26/01/19 11:11:31 INFO Utils: /home/linuxu/.ivy2/jars/org.spark-project.spark_unused-1.0.0.jar has been previously copied to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.spark-project.spark_unused-1.0.0.jar
      26/01/19 11:11:31 INFO Executor: Fetching file:///home/linuxu/.ivy2/jars/org.apache.htrace_htrace-core4-4.1.0-incubating.jar with timestamp 1768813889831
      26/01/19 11:11:31 INFO Utils: /home/linuxu/.ivy2/jars/org.apache.htrace_htrace-core4-4.1.0-incubating.jar has been previously copied to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.apache.htrace_htrace-core4-4.1.0-incubating.jar
      26/01/19 11:11:31 INFO Executor: Fetching file:///home/linuxu/.ivy2/jars/com.google.code.findbugs_jsr305-3.0.0.jar with timestamp 1768813889831
      26/01/19 11:11:31 INFO Utils: /home/linuxu/.ivy2/jars/com.google.code.findbugs_jsr305-3.0.0.jar has been previously copied to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/com.google.code.findbugs_jsr305-3.0.0.jar
      26/01/19 11:11:31 INFO Executor: Fetching file:///home/linuxu/.ivy2/jars/org.apache.kafka_kafka-clients-2.8.0.jar with timestamp 1768813889831
      26/01/19 11:11:31 INFO Utils: /home/linuxu/.ivy2/jars/org.apache.kafka_kafka-clients-2.8.0.jar has been previously copied to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.apache.kafka_kafka-clients-2.8.0.jar
      26/01/19 11:11:31 INFO Executor: Fetching file:///home/linuxu/.ivy2/jars/org.lz4_lz4-java-1.7.1.jar with timestamp 1768813889831
      26/01/19 11:11:31 INFO Utils: /home/linuxu/.ivy2/jars/org.lz4_lz4-java-1.7.1.jar has been previously copied to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.lz4_lz4-java-1.7.1.jar
      26/01/19 11:11:31 INFO Executor: Fetching file:///home/linuxu/.ivy2/jars/org.xerial.snappy_snappy-java-1.1.8.4.jar with timestamp 1768813889831
      26/01/19 11:11:31 INFO Utils: /home/linuxu/.ivy2/jars/org.xerial.snappy_snappy-java-1.1.8.4.jar has been previously copied to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.xerial.snappy_snappy-java-1.1.8.4.jar
      26/01/19 11:11:31 INFO Executor: Fetching file:///home/linuxu/.ivy2/jars/org.slf4j_slf4j-api-1.7.30.jar with timestamp 1768813889831
      26/01/19 11:11:31 INFO Utils: /home/linuxu/.ivy2/jars/org.slf4j_slf4j-api-1.7.30.jar has been previously copied to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.slf4j_slf4j-api-1.7.30.jar
      26/01/19 11:11:31 INFO Executor: Fetching file:///home/linuxu/.ivy2/jars/org.apache.hadoop_hadoop-client-runtime-3.3.1.jar with timestamp 1768813889831
      26/01/19 11:11:31 INFO Utils: /home/linuxu/.ivy2/jars/org.apache.hadoop_hadoop-client-runtime-3.3.1.jar has been previously copied to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.apache.hadoop_hadoop-client-runtime-3.3.1.jar
      26/01/19 11:11:31 INFO Executor: Fetching spark://192.168.90.46:35735/jars/commons-logging_commons-logging-1.1.3.jar with timestamp 1768813889831
      26/01/19 11:11:31 INFO TransportClientFactory: Successfully created connection to /192.168.90.46:35735 after 42 ms (0 ms spent in bootstraps)
      26/01/19 11:11:31 INFO Utils: Fetching spark://192.168.90.46:35735/jars/commons-logging_commons-logging-1.1.3.jar to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/fetchFileTemp18405941741263121346.tmp
      26/01/19 11:11:31 INFO Utils: /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/fetchFileTemp18405941741263121346.tmp has been previously copied to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/commons-logging_commons-logging-1.1.3.jar
      26/01/19 11:11:31 INFO Executor: Adding file:/tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/commons-logging_commons-logging-1.1.3.jar to class loader
      26/01/19 11:11:31 INFO Executor: Fetching spark://192.168.90.46:35735/jars/org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.2.1.jar with timestamp 1768813889831
      26/01/19 11:11:31 INFO Utils: Fetching spark://192.168.90.46:35735/jars/org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.2.1.jar to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/fetchFileTemp1857360379074540162.tmp
      26/01/19 11:11:31 INFO Utils: /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/fetchFileTemp1857360379074540162.tmp has been previously copied to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.2.1.jar
      26/01/19 11:11:31 INFO Executor: Adding file:/tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.2.1.jar to class loader
      26/01/19 11:11:31 INFO Executor: Fetching spark://192.168.90.46:35735/jars/org.apache.spark_spark-sql-kafka-0-10_2.12-3.2.1.jar with timestamp 1768813889831
      26/01/19 11:11:31 INFO Utils: Fetching spark://192.168.90.46:35735/jars/org.apache.spark_spark-sql-kafka-0-10_2.12-3.2.1.jar to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/fetchFileTemp11271216676221022889.tmp
      26/01/19 11:11:31 INFO Utils: /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/fetchFileTemp11271216676221022889.tmp has been previously copied to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.apache.spark_spark-sql-kafka-0-10_2.12-3.2.1.jar
      26/01/19 11:11:31 INFO Executor: Adding file:/tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.apache.spark_spark-sql-kafka-0-10_2.12-3.2.1.jar to class loader
      26/01/19 11:11:31 INFO Executor: Fetching spark://192.168.90.46:35735/jars/org.apache.htrace_htrace-core4-4.1.0-incubating.jar with timestamp 1768813889831
      26/01/19 11:11:31 INFO Utils: Fetching spark://192.168.90.46:35735/jars/org.apache.htrace_htrace-core4-4.1.0-incubating.jar to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/fetchFileTemp4111720319125642649.tmp
      26/01/19 11:11:32 INFO Utils: /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/fetchFileTemp4111720319125642649.tmp has been previously copied to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.apache.htrace_htrace-core4-4.1.0-incubating.jar
      26/01/19 11:11:32 INFO Executor: Adding file:/tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.apache.htrace_htrace-core4-4.1.0-incubating.jar to class loader
      26/01/19 11:11:32 INFO Executor: Fetching spark://192.168.90.46:35735/jars/org.apache.hadoop_hadoop-client-runtime-3.3.1.jar with timestamp 1768813889831
      26/01/19 11:11:32 INFO Utils: Fetching spark://192.168.90.46:35735/jars/org.apache.hadoop_hadoop-client-runtime-3.3.1.jar to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/fetchFileTemp18196466467920197847.tmp
      26/01/19 11:11:32 INFO Utils: /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/fetchFileTemp18196466467920197847.tmp has been previously copied to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.apache.hadoop_hadoop-client-runtime-3.3.1.jar
      26/01/19 11:11:32 INFO Executor: Adding file:/tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.apache.hadoop_hadoop-client-runtime-3.3.1.jar to class loader
      26/01/19 11:11:32 INFO Executor: Fetching spark://192.168.90.46:35735/jars/org.slf4j_slf4j-api-1.7.30.jar with timestamp 1768813889831
      26/01/19 11:11:32 INFO Utils: Fetching spark://192.168.90.46:35735/jars/org.slf4j_slf4j-api-1.7.30.jar to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/fetchFileTemp14449367949005996031.tmp
      26/01/19 11:11:32 INFO Utils: /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/fetchFileTemp14449367949005996031.tmp has been previously copied to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.slf4j_slf4j-api-1.7.30.jar
      26/01/19 11:11:32 INFO Executor: Adding file:/tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.slf4j_slf4j-api-1.7.30.jar to class loader
      26/01/19 11:11:32 INFO Executor: Fetching spark://192.168.90.46:35735/jars/com.google.code.findbugs_jsr305-3.0.0.jar with timestamp 1768813889831
      26/01/19 11:11:32 INFO Utils: Fetching spark://192.168.90.46:35735/jars/com.google.code.findbugs_jsr305-3.0.0.jar to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/fetchFileTemp15362041747190203323.tmp
      26/01/19 11:11:32 INFO Utils: /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/fetchFileTemp15362041747190203323.tmp has been previously copied to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/com.google.code.findbugs_jsr305-3.0.0.jar
      26/01/19 11:11:32 INFO Executor: Adding file:/tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/com.google.code.findbugs_jsr305-3.0.0.jar to class loader
      26/01/19 11:11:32 INFO Executor: Fetching spark://192.168.90.46:35735/jars/org.spark-project.spark_unused-1.0.0.jar with timestamp 1768813889831
      26/01/19 11:11:32 INFO Utils: Fetching spark://192.168.90.46:35735/jars/org.spark-project.spark_unused-1.0.0.jar to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/fetchFileTemp6251727083274744122.tmp
      26/01/19 11:11:32 INFO Utils: /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/fetchFileTemp6251727083274744122.tmp has been previously copied to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.spark-project.spark_unused-1.0.0.jar
      26/01/19 11:11:32 INFO Executor: Adding file:/tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.spark-project.spark_unused-1.0.0.jar to class loader
      26/01/19 11:11:32 INFO Executor: Fetching spark://192.168.90.46:35735/jars/org.apache.hadoop_hadoop-client-api-3.3.1.jar with timestamp 1768813889831
      26/01/19 11:11:32 INFO Utils: Fetching spark://192.168.90.46:35735/jars/org.apache.hadoop_hadoop-client-api-3.3.1.jar to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/fetchFileTemp13058783912047133649.tmp
      26/01/19 11:11:32 INFO Utils: /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/fetchFileTemp13058783912047133649.tmp has been previously copied to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.apache.hadoop_hadoop-client-api-3.3.1.jar
      26/01/19 11:11:32 INFO Executor: Adding file:/tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.apache.hadoop_hadoop-client-api-3.3.1.jar to class loader
      26/01/19 11:11:32 INFO Executor: Fetching spark://192.168.90.46:35735/jars/org.lz4_lz4-java-1.7.1.jar with timestamp 1768813889831
      26/01/19 11:11:32 INFO Utils: Fetching spark://192.168.90.46:35735/jars/org.lz4_lz4-java-1.7.1.jar to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/fetchFileTemp12764928344995423010.tmp
      26/01/19 11:11:32 INFO Utils: /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/fetchFileTemp12764928344995423010.tmp has been previously copied to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.lz4_lz4-java-1.7.1.jar
      26/01/19 11:11:32 INFO Executor: Adding file:/tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.lz4_lz4-java-1.7.1.jar to class loader
      26/01/19 11:11:32 INFO Executor: Fetching spark://192.168.90.46:35735/jars/org.xerial.snappy_snappy-java-1.1.8.4.jar with timestamp 1768813889831
      26/01/19 11:11:32 INFO Utils: Fetching spark://192.168.90.46:35735/jars/org.xerial.snappy_snappy-java-1.1.8.4.jar to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/fetchFileTemp4015163967774268544.tmp
      26/01/19 11:11:32 INFO Utils: /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/fetchFileTemp4015163967774268544.tmp has been previously copied to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.xerial.snappy_snappy-java-1.1.8.4.jar
      26/01/19 11:11:32 INFO Executor: Adding file:/tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.xerial.snappy_snappy-java-1.1.8.4.jar to class loader
      26/01/19 11:11:32 INFO Executor: Fetching spark://192.168.90.46:35735/jars/org.apache.kafka_kafka-clients-2.8.0.jar with timestamp 1768813889831
      26/01/19 11:11:32 INFO Utils: Fetching spark://192.168.90.46:35735/jars/org.apache.kafka_kafka-clients-2.8.0.jar to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/fetchFileTemp9695693561266847454.tmp
      26/01/19 11:11:32 INFO Utils: /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/fetchFileTemp9695693561266847454.tmp has been previously copied to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.apache.kafka_kafka-clients-2.8.0.jar
      26/01/19 11:11:32 INFO Executor: Adding file:/tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.apache.kafka_kafka-clients-2.8.0.jar to class loader
      26/01/19 11:11:32 INFO Executor: Fetching spark://192.168.90.46:35735/jars/org.apache.commons_commons-pool2-2.6.2.jar with timestamp 1768813889831
      26/01/19 11:11:32 INFO Utils: Fetching spark://192.168.90.46:35735/jars/org.apache.commons_commons-pool2-2.6.2.jar to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/fetchFileTemp10874596565152592826.tmp
      26/01/19 11:11:32 INFO Utils: /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/fetchFileTemp10874596565152592826.tmp has been previously copied to /tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.apache.commons_commons-pool2-2.6.2.jar
      26/01/19 11:11:32 INFO Executor: Adding file:/tmp/spark-d7e34ce0-d0d7-4a1d-946b-a4951bdbddcc/userFiles-d87861c5-d5f5-4112-b074-7e0020f0590d/org.apache.commons_commons-pool2-2.6.2.jar to class loader
      26/01/19 11:11:32 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 33355.
      26/01/19 11:11:32 INFO NettyBlockTransferService: Server created on 192.168.90.46:33355
      26/01/19 11:11:32 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
      26/01/19 11:11:32 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, 192.168.90.46, 33355, None)
      26/01/19 11:11:32 INFO BlockManagerMasterEndpoint: Registering block manager 192.168.90.46:33355 with 434.4 MiB RAM, BlockManagerId(driver, 192.168.90.46, 33355, None)
      26/01/19 11:11:32 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, 192.168.90.46, 33355, None)
      26/01/19 11:11:32 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, 192.168.90.46, 33355, None)
      Reading data from Kafka...
      Calculating Financial Ratios...
      Preview of Training Data:
      +------+----+--------------------+-------------------+-----+
      |ticker|year|                 roe|     debt_to_equity|label|
      +------+----+--------------------+-------------------+-----+
      |   AMD|2021|  0.4217686940100302| 0.6565292669091467|  0.0|
      |   AMD|2022|0.024109589435656477|0.23433790571628793|  1.0|
      |   AMD|2023|0.015279467334598316|  0.214574532512824|  1.0|
      |  AMZN|2021| 0.24133966120335798| 2.0420557242078954|  0.0|
      |  AMZN|2022|-0.01863834604196...| 2.1680739109221845|  0.0|
      +------+----+--------------------+-------------------+-----+
      only showing top 5 rows

      Data saved to financial_training_data.parquet

------------------------------------------------------------------------------

STEP 3: TRAIN MACHINE LEARNING MODEL (TERMINAL 4)
-------------------------------------------------
Once the data is prepared (Step 2 is complete), run the Machine Learning script.

1. Run the ML training script:
   PYSPARK_DRIVER_PYTHON=python spark-submit spark_ml.py

   > Output:
   > 1. Predictions Preview (Table showing labels and probabilities)
   > 2. Model Accuracy (e.g., "Model Accuracy: 85.00%")
   > 3. Feature Importance Table
   Ex.:
      26/01/19 11:13:41 WARN Utils: Your hostname, Ubuntu26BD-13 resolves to a loopback address: 127.0.1.1; using 192.168.90.46 instead (on interface ens160)
      26/01/19 11:13:41 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
      26/01/19 11:13:42 INFO SparkContext: Running Spark version 3.3.0
      26/01/19 11:13:42 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
      26/01/19 11:13:42 INFO ResourceUtils: ==============================================================
      26/01/19 11:13:42 INFO ResourceUtils: No custom resources configured for spark.driver.
      26/01/19 11:13:42 INFO ResourceUtils: ==============================================================
      26/01/19 11:13:42 INFO SparkContext: Submitted application: Financial_Prediction_ML
      26/01/19 11:13:42 INFO ResourceProfile: Default ResourceProfile created, executor resources: Map(cores -> name: cores, amount: 1, script: , vendor: , memory -> name: memory, amount: 1024, script: , vendor: , offHeap -> name: offHeap, amount: 0, script: , vendor: ), task resources: Map(cpus -> name: cpus, amount: 1.0)
      26/01/19 11:13:42 INFO ResourceProfile: Limiting resource is cpu
      26/01/19 11:13:42 INFO ResourceProfileManager: Added ResourceProfile id: 0
      26/01/19 11:13:43 INFO SecurityManager: Changing view acls to: linuxu
      26/01/19 11:13:43 INFO SecurityManager: Changing modify acls to: linuxu
      26/01/19 11:13:43 INFO SecurityManager: Changing view acls groups to: 
      26/01/19 11:13:43 INFO SecurityManager: Changing modify acls groups to: 
      26/01/19 11:13:43 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(linuxu); groups with view permissions: Set(); users  with modify permissions: Set(linuxu); groups with modify permissions: Set()
      26/01/19 11:13:43 INFO Utils: Successfully started service 'sparkDriver' on port 34189.
      26/01/19 11:13:43 INFO SparkEnv: Registering MapOutputTracker
      26/01/19 11:13:43 INFO SparkEnv: Registering BlockManagerMaster
      26/01/19 11:13:43 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
      26/01/19 11:13:43 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
      26/01/19 11:13:43 INFO SparkEnv: Registering BlockManagerMasterHeartbeat
      26/01/19 11:13:43 INFO DiskBlockManager: Created local directory at /tmp/blockmgr-411dd1ed-a4b0-4aea-8c34-c90ef00b24fb
      26/01/19 11:13:43 INFO MemoryStore: MemoryStore started with capacity 434.4 MiB
      26/01/19 11:13:43 INFO SparkEnv: Registering OutputCommitCoordinator
      26/01/19 11:13:43 INFO Utils: Successfully started service 'SparkUI' on port 4040.
      26/01/19 11:13:44 INFO Executor: Starting executor ID driver on host 192.168.90.46
      26/01/19 11:13:44 INFO Executor: Starting executor with user classpath (userClassPathFirst = false): ''
      26/01/19 11:13:44 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 38583.
      26/01/19 11:13:44 INFO NettyBlockTransferService: Server created on 192.168.90.46:38583
      26/01/19 11:13:44 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
      26/01/19 11:13:44 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, 192.168.90.46, 38583, None)
      26/01/19 11:13:44 INFO BlockManagerMasterEndpoint: Registering block manager 192.168.90.46:38583 with 434.4 MiB RAM, BlockManagerId(driver, 192.168.90.46, 38583, None)
      26/01/19 11:13:44 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, 192.168.90.46, 38583, None)
      26/01/19 11:13:44 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, 192.168.90.46, 38583, None)
      Loading processed data...
      Training on 21 records.
      Testing on 6 records.

      --- Predictions Preview ---
      +------+----+-----+----------+--------------------+
      |ticker|year|label|prediction|         probability|
      +------+----+-----+----------+--------------------+
      |   AMD|2023|  1.0|       1.0|         [0.18,0.82]|
      |  CSCO|2022|  1.0|       0.0|[0.66047619047619...|
      |  CSCO|2024|  1.0|       1.0|[0.45714285714285...|
      |   IBM|2022|  1.0|       0.0|[0.67714285714285...|
      |  MSFT|2023|  1.0|       1.0|         [0.31,0.69]|
      +------+----+-----+----------+--------------------+
      only showing top 5 rows


      Model Accuracy: 50.00%

      --- Feature Importance ---
      current_ratio: 0.1765
      net_profit_margin: 0.1324
      roe: 0.1467
      roa: 0.1789
      debt_to_equity: 0.2050
      debt_to_assets: 0.1605

==============================================================================
TROUBLESHOOTING
==============================================================================
* "Connection Refused": 
  Ensure you ran the 'sed' command in STEP 0 and that Zookeeper/Kafka are running.

* "Path does not exist" or "AnalysisException": 
  Make sure STEP 2 finished successfully and created the 'financial_training_data.parquet' folder 
  before running STEP 3.

* Package Errors:
  If spark-submit fails to find the Kafka package, ensure you have internet access 
  to download the JARs, or ask the instructor for the local JAR path.