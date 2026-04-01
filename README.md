# Real-Time Financial Health Analysis & Leaderboard

==============================================================================
DESCRIPTION
==============================================================================
This project implements a distributed Big Data system for assessing the financial 
health of companies using **dual Altman Z-Score models**: the Original Z-Score (1968) 
and the Modified Z'-Score (1983).

The system ingests financial reports (10-K) via two independent pipelines — manual 
HTML parsing and Yahoo Finance API — calculates both Z-Scores on-the-fly, benchmarks 
companies against the market, and maintains a **Real-Time Top 5 Leaderboard** of the 
healthiest companies per year.

All scored data is persisted to **Parquet** (Silver layer) for historical analysis 
and Gold-layer dashboard generation.

**Key Features:**
1. **Dual Financial Modeling:** Simultaneous calculation of Z-Score Original (market-cap) and Z'-Score Prime (book-value).
2. **Dynamic Benchmarking:** Comparing individual Z'-Score performance vs. real-time batch averages.
3. **Anomaly Detection:** Identifying statistical outliers (>2σ from the mean Z'-Score).
4. **Live Leaderboard:** A constantly updating "Top 5" list using Spark Window Functions.
5. **Data Quality Gate (DLQ):** Filters and logs invalid/incomplete financial records.
6. **Parquet Silver/Gold Architecture:** Durable storage for historical trend analysis.

==============================================================================
ARCHITECTURE & DATA FLOW
==============================================================================

```
  ┌─────────────────────┐     ┌─────────┐     ┌──────────────────────────────┐
  │  DATA PRODUCERS      │────▶│  KAFKA  │────▶│  SPARK STREAMING CONSUMER    │
  │                      │     │  Topic  │     │  (spark_altman_dual_etl.py)  │
  │  1. Manual Parser    │     └─────────┘     │                              │
  │     (10-K HTML)      │                     │  Data Quality Gate           │
  │                      │                     │  ↓                           │
  │  2. API Producer     │                     │  Preprocess & Engineer       │
  │     (yfinance)       │                     │  ↓                           │
  └─────────────────────┘                     │  Calculate Dual Z-Scores     │
                                               │  ↓                           │
                                               │  Enrich (Performance,        │
                                               │          Anomaly Detection)  │
                                               │  ↓                           │
                                               │  Silver Storage (Parquet)    │
                                               │  ↓                           │
                                               │  Gold Dashboards             │
                                               │  (Averages, Leaderboard)     │
                                               └──────────────────────────────┘
```

**Two Independent Producer Pipelines:**
* **Manual Parser** (`raw_features_spark_publisher.py`): Extracts metrics from raw 10-K HTML filings stored locally.
* **API Producer** (`src/raw_features/produser_api.py`): Fetches financial data via Yahoo Finance API with market cap and stock price.

==============================================================================
THE ALTMAN Z-SCORE MODELS
==============================================================================

### Z-Score Original (1968) — For Public Companies

Requires **Market Capitalization** (market value of equity).

**Formula:** `Z = 1.2(X1) + 1.4(X2) + 3.3(X3) + 0.6(X4) + 1.0(X5)`

| Variable | Formula | Description |
|----------|---------|-------------|
| **X1** | Working Capital / Total Assets | Liquidity measure |
| **X2** | Retained Earnings / Total Assets | Cumulative profitability |
| **X3** | EBIT / Total Assets | Operating efficiency |
| **X4** | **Market Value of Equity** / Total Liabilities | Market confidence |
| **X5** | Sales / Total Assets | Asset utilization |

**Risk Zones:**
* 🟢 **Safe:** Z > 2.99
* 🟡 **Grey:** 1.81 < Z < 2.99
* 🔴 **Distress:** Z < 1.81

> **Note:** If Market Cap data is unavailable for a record, Z-Score Original is 
> marked as `N/A` and only Z'-Score Prime is calculated.

---

### Z'-Score Prime (1983) — For Private / Non-Market Companies

Uses **Book Value of Equity** (Stockholders' Equity) instead of Market Cap.

**Formula:** `Z' = 0.717(X1) + 0.847(X2) + 3.107(X3) + 0.420(X4) + 0.998(X5)`

| Variable | Formula | Description |
|----------|---------|-------------|
| **X1** | Working Capital / Total Assets | Liquidity measure |
| **X2** | Retained Earnings / Total Assets | Cumulative profitability |
| **X3** | EBIT / Total Assets | Operating efficiency |
| **X4** | **Stockholders' Equity** / Total Liabilities | Solvency (book value) |
| **X5** | Sales / Total Assets | Asset utilization |

**Risk Zones:**
* 🟢 **Safe:** Z' > 2.90
* 🟡 **Grey:** 1.23 < Z' < 2.90
* 🔴 **Distress:** Z' < 1.23

==============================================================================
ANALYTICS: PERFORMANCE & ANOMALY DETECTION
==============================================================================

After Z-Score calculation, each record is enriched with two analytics columns:

### Performance
Compares a company's Z'-Score against the **batch average**:
* **Outperforming** — Z'-Score is above the average
* **Underperforming** — Z'-Score is at or below the average

### Is_Anomaly
Flags companies whose Z'-Score deviates more than **2 standard deviations** 
from the batch mean:
* **Yes** — Statistical outlier (|Z' - μ| > 2σ)
* **No** — Within normal distribution range

> Both metrics use Z'-Score Prime as the base since it is always available 
> (does not depend on Market Cap availability).

==============================================================================
DATA STORAGE: PARQUET SILVER/GOLD ARCHITECTURE
==============================================================================

The system uses a **Lakehouse-style** storage pattern with Apache Parquet:

* **Silver Layer** (`local_storage/silver_scores/`): Append-only Parquet files 
  containing all scored records with full financial data, dual Z-Scores, Health Zones, 
  Performance, and Anomaly flags.

* **Gold Layer** (computed on read): Aggregated dashboards generated from Silver data:
  - **Market Averages by Year** — Mean Z'-Score and Z-Score across all analyzed companies.
  - **Top-5 Leaderboard** — Best-performing companies ranked by Z'-Score per year.

Silver storage is cleared on each application restart to ensure a clean demonstration state.

==============================================================================
RAW_FEATURES EXTRACTOR (10-K HTML → KAFKA JSON)
==============================================================================
`raw_features_spark_publisher.py` is the extraction job that converts raw 10-K HTML
files into normalized financial metrics and publishes them to Kafka.

**What it does:**
* Reads filings from `assets/filings_10k/<ticker>/filing-YYYY-*.htm(l)`.
* Extracts key metrics from Consolidated Balance Sheet and Consolidated Cash Flow tables:
  `common_stock_units`, `current_assets`, `current_liabilities`, `short_term_debt`,
  `long_term_debt`, `stockholders_equity`, `total_assets`, `net_income`,
  `interest_expense`, `tax_expense`, `retained_earnings`, `total_revenue`.
* Merges filing-level metrics into one consolidated payload per company (from fiscal year 2015+).
* Normalizes units (millions/thousands → absolute values) before publishing.
* Publishes one JSON message per company-year to the configured Kafka topic.

Operate the extractor through the `Makefile` component target:

```bash
make raw_features process
make raw_features process company=aapl
make raw_features do_export
make raw_features do_import
```

Use `make help` for configurable variables (`RAW_FEATURES_SPARK_PUBLISHER_*`) and defaults.
Runtime logs are written to `logs/raw_features_spark_publisher.log`.

**How it achieves this (implementation details):**
The job enumerates company filings by filename (`filing-YYYY-*`) and filters by fiscal
year threshold. It parallelizes file processing with Spark RDD partitions, then applies
table-selection heuristics (keyword markers, exclusions, and year-density scoring) to
choose the best Balance Sheet and Cash Flow tables from each HTML filing. Metric extractors
in `src/raw_features/*_rules.py` parse values by year and validate required fields.
Finally, `combine_metrics` merges all filing-level frames per company, keeps the most useful
non-fallback values, preserves units in column labels, and serializes one JSON payload that
is produced to Kafka with the company ticker as message key.

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

5.  Create Topics (Terminal 3):
    bin/kafka-topics.sh --create --topic raw_features --bootstrap-server localhost:9092
    bin/kafka-topics.sh --create --topic financial_reports_stream --bootstrap-server localhost:9092

------------------------------------------------------------------------------

STEP 1: RUN DATA PRODUCER (TERMINAL 3)
--------------------------------------
Choose one of the two producer pipelines:

**Option A — Manual Parser (10-K HTML filings):**
```bash
make raw_features process
make raw_features process company=aapl
```

**Option B — API Producer (Yahoo Finance):**
```bash
make run-producer start_year=2015 end_year=2022
make run-producer ticker=AAPL start_year=2020 end_year=2022
```

------------------------------------------------------------------------------

STEP 2: RUN SPARK DUAL SCORING ENGINE (TERMINAL 4)
---------------------------------------------------
This job computes both Z-Score Original and Z'-Score Prime, enriches with 
Performance/Anomaly analytics, and maintains the Top-5 Leaderboard.

Uses `outputMode("append")` with `foreachBatch` for micro-batch processing 
and Parquet-based historical aggregation.

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 spark_altman_dual_etl.py
```

> **Expected Output:**
> ```
> +-------+----+----------------+---------------------+---------------+------------------+--------------+----------+
> |Company|year|Z_Score_Original|Health_Zone_Original  |Z_Score_Prime  |Health_Zone_Prime |Performance   |Is_Anomaly|
> +-------+----+----------------+---------------------+---------------+------------------+--------------+----------+
> |NVDA   |2023|15.40           |Safe (Green)         |11.20          |Safe (Green)      |Outperforming |No        |
> |MSFT   |2023|8.20            |Safe (Green)         |6.85           |Safe (Green)      |Outperforming |No        |
> |AAPL   |2023|7.55            |Safe (Green)         |5.90           |Safe (Green)      |Outperforming |No        |
> |IBM    |2023|N/A             |N/A - No Market Cap  |2.10           |Grey (Caution)    |Underperforming|No       |
> ...
> +-------+----+----------------+---------------------+---------------+------------------+--------------+----------+
> ```

==============================================================================
TROUBLESHOOTING
==============================================================================
* **Why `append` mode with `foreachBatch`?** We use `outputMode("append")` because each 
  micro-batch is processed independently. Historical aggregation (market averages, 
  leaderboards) is achieved by reading back all accumulated Silver Parquet data 
  within each batch handler, avoiding the limitations of complete mode.

* **Missing Z_Score_Original values?** Records without valid Market Cap data will have 
  `Z_Score_Original = null` and `Health_Zone_Original = "N/A - No Market Cap"`. 
  The Z'-Score Prime is always calculated using Book Value of Equity instead.

* **Silver storage reset:** The `local_storage/silver_scores/` directory is cleared 
  on each application restart to ensure a clean demonstration state.
