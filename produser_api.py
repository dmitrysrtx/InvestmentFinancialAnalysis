"""Kafka producer for financial reports (Yahoo Finance API).

Architecture
------------
Mirrors the manual parser (raw_features_spark_publisher.py):

1. **Workers** — each ticker is processed by a separate Spark RDD partition
   (``fetch_financials_worker``).  The worker fetches balance-sheet, income-
   statement, and price data via ``yfinance`` and returns a list of
   ``(ticker, year, json_payload)`` tuples.

2. **Driver** — collects all worker results, flattens them into a single
   year-first sorted list, and publishes every record to Kafka in one burst
   so the downstream Spark Structured Streaming consumer sees exactly **one
   micro-batch**.

Modes
-----
1. Year-range mode (default / batch):
   Publish all annual records for every ticker in TICKERS that fall within
   [START_YEAR, END_YEAR].  Both bounds are inclusive.
   Controlled via env vars START_YEAR and END_YEAR (or Makefile variables
   start_year / end_year).

2. Per-document mode (single-ticker):
   Pass a single ticker via the env var TARGET_TICKER (or Makefile variable
   ``ticker``).  Only that company's records are published; year-range
   filtering still applies.

Examples (via Makefile)
-----------------------
  make run-producer start_year=2015 end_year=2022
  make run-producer start_year=2023 end_year=2025
  make run-producer ticker=AAPL start_year=2020 end_year=2022
"""

from __future__ import annotations

import os
import sys
import json
import math
from typing import Dict, List, Optional, Tuple

import yfinance as yf

from pyspark.sql import SparkSession

# --- CONFIGURATION ---
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'raw_features'
APP_NAME = 'ProducerAPI'

# Default list of tech companies to analyze
TICKERS = ['AAOI', 'AAPL', 'ACIW', 'ACLS', 'ACMR', 'ACN', 'ADBE', 'ADI', 'ADP', 'ADSK', 'AGYS', 'AKAM', 'AMAT', 'AMBA', 'AMD']

DEFAULT_START_YEAR = 2015
DEFAULT_END_YEAR = None  # None means no upper bound


# ---------------------------------------------------------------------------
# Configuration helpers
# ---------------------------------------------------------------------------

def _parse_year_env(name: str, default) -> Optional[int]:
    """Parse an integer year from an env var; return *default* if unset."""
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        print(f"ERROR: {name} must be a 4-digit year, got '{raw}'")
        sys.exit(1)


def get_year_range():
    """Return (start_year: int, end_year: int | None) from env vars."""
    start_year = _parse_year_env("START_YEAR", DEFAULT_START_YEAR)
    end_year = _parse_year_env("END_YEAR", DEFAULT_END_YEAR)
    if end_year is not None and end_year < start_year:
        print(f"ERROR: END_YEAR ({end_year}) must be >= START_YEAR ({start_year})")
        sys.exit(1)
    return start_year, end_year


# ---------------------------------------------------------------------------
# Worker function — executed on Spark executors
# ---------------------------------------------------------------------------

def fetch_financials_worker(
    partition_items: list,
) -> List[Tuple[str, int, str]]:
    """Spark worker: fetch financial data for each ticker in the partition.

    *partition_items* is an iterable of ``(ticker, start_year, end_year)``
    tuples.  Returns a list of ``(ticker, year, json_payload)`` tuples — one
    per fiscal year that passes the year-range filter.

    The function is intentionally a module-level function so that Spark can
    serialize and ship it to executors.
    """
    import yfinance as yf  # imported inside worker for serialisation safety

    results: List[Tuple[str, int, str]] = []

    for ticker, start_year, end_year in partition_items:
        try:
            stock = yf.Ticker(ticker)
            balance_sheet = stock.balance_sheet
            financials = stock.financials
            history = stock.history(period="max")

            if balance_sheet.empty or financials.empty:
                continue

            dates = financials.columns
            for date in dates:
                try:
                    year = date.year

                    if year < start_year:
                        continue
                    if end_year is not None and year > end_year:
                        continue

                    date_str = str(date.date())

                    def get_val(df, key):
                        try:
                            return float(df.loc[key, date])
                        except KeyError:
                            return 0.0

                    # Stock price at report date (or yearly average fallback)
                    close_price = 0.0
                    if date_str in history.index:
                        close_price = float(history.loc[date_str]['Close'])
                    else:
                        yearly_data = history[history.index.year == year]
                        if not yearly_data.empty:
                            close_price = float(yearly_data['Close'].mean())

                    common_stock_units = get_val(balance_sheet, 'Ordinary Shares Number')
                    market_cap = close_price * common_stock_units

                    data = {
                        # --- Metadata ---
                        'ticker': ticker.lower(),
                        'year': year,
                        # --- Income Statement ---
                        'total_revenue': get_val(financials, 'Total Revenue'),
                        'net_income': get_val(financials, 'Net Income'),
                        'ebit': get_val(financials, 'EBIT'),
                        'interest_expense': get_val(financials, 'Interest Expense'),
                        'tax_expense': get_val(financials, 'Tax Provision'),
                        # --- Balance Sheet ---
                        'current_assets': get_val(balance_sheet, 'Current Assets'),
                        'current_liabilities': get_val(balance_sheet, 'Current Liabilities'),
                        'total_assets': get_val(balance_sheet, 'Total Assets'),
                        'total_liabilities': get_val(balance_sheet, 'Total Liabilities Net Minority Interest'),
                        'stockholders_equity': get_val(balance_sheet, 'Stockholders Equity'),
                        'retained_earnings': get_val(balance_sheet, 'Retained Earnings'),
                        'short_term_debt': get_val(balance_sheet, 'Current Debt'),
                        'long_term_debt': get_val(balance_sheet, 'Long Term Debt'),
                        'common_stock_units': get_val(balance_sheet, 'Ordinary Shares Number'),
                        # --- Market Data ---
                        'close_price': close_price,
                        'market_cap': market_cap,
                    }
                    # Sanitize NaN/Inf → None before JSON serialization.
                    # json.dumps(float('nan')) produces invalid JSON ("NaN")
                    # that Spark's from_json cannot parse, causing the entire
                    # record to be null and rejected by the Data Quality Gate.
                    sanitized_data = {
                        k: (None if isinstance(v, float) and (math.isnan(v) or math.isinf(v)) else v)
                        for k, v in data.items()
                    }
                    results.append((ticker, year, json.dumps(sanitized_data)))
                except Exception as e:
                    # Skip individual year on error; don't abort the whole ticker
                    pass

        except Exception:
            # yfinance network / API error — skip ticker
            pass

    return results


# ---------------------------------------------------------------------------
# Driver-side: year-first ordering (mirrors build_year_first_publish_records)
# ---------------------------------------------------------------------------

def build_year_first_publish_records(
    raw_records: List[Tuple[str, int, str]],
) -> List[Tuple[int, str, str]]:
    """Re-order collected worker results into (year, ticker, json) tuples
    sorted by year ascending, then ticker ascending — identical to the
    ordering strategy in raw_features_spark_publisher.py."""

    rows_by_year: Dict[int, List[Tuple[str, str]]] = {}
    for ticker, year, payload_json in raw_records:
        rows_by_year.setdefault(year, []).append((ticker, payload_json))

    ordered: List[Tuple[int, str, str]] = []
    for year in sorted(rows_by_year.keys()):
        for ticker, payload_json in sorted(rows_by_year[year], key=lambda t: t[0]):
            ordered.append((year, ticker, payload_json))
    return ordered


# ---------------------------------------------------------------------------
# Kafka setup
# ---------------------------------------------------------------------------

def setup_kafka_producer():
    """Create a confluent_kafka Producer."""
    kafka_host = os.getenv("PRODUCER_API_KAFKA_HOST", "").strip()
    kafka_port = os.getenv("PRODUCER_API_KAFKA_PORT", "").strip()
    kafka_topic = os.getenv("PRODUCER_API_KAFKA_TOPIC", "").strip() or TOPIC_NAME

    if kafka_host and kafka_port:
        try:
            int(kafka_port)
        except ValueError:
            print(f"ERROR: PRODUCER_API_KAFKA_PORT must be a valid integer, got '{kafka_port}'")
            sys.exit(1)
        bootstrap_servers = f"{kafka_host}:{kafka_port}"
    else:
        bootstrap_servers = KAFKA_BROKER

    try:
        from confluent_kafka import Producer
    except ImportError:
        print("ERROR: Missing dependency confluent_kafka. Install it (pip install confluent-kafka).")
        sys.exit(1)

    producer = Producer({"bootstrap.servers": bootstrap_servers})
    return producer, kafka_topic


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    start_year, end_year = get_year_range()

    # Per-document (single-ticker) mode
    target_ticker = os.getenv("TARGET_TICKER", "").strip().upper()
    tickers_to_process = [target_ticker] if target_ticker else TICKERS

    year_range_label = (
        f"{start_year}–{end_year}" if end_year is not None
        else f"{start_year}–present"
    )

    if target_ticker:
        print(f"--- Starting Data Ingestion (single ticker: {target_ticker}, years: {year_range_label}) ---")
    else:
        print(f"--- Starting Data Ingestion (all tickers, years: {year_range_label}) ---")

    # ------------------------------------------------------------------
    # STEP 1: Spark workers — fetch data in parallel (one ticker per task)
    # ------------------------------------------------------------------
    spark = SparkSession.builder \
        .appName(APP_NAME) \
        .master("local[*]") \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    # Build partition input: each element is (ticker, start_year, end_year)
    worker_inputs = [(t, start_year, end_year) for t in tickers_to_process]
    num_slices = max(1, min(len(worker_inputs), sc.defaultParallelism or len(worker_inputs)))

    print(f"Distributing {len(worker_inputs)} ticker(s) across {num_slices} Spark worker(s)...")

    rdd = sc.parallelize(worker_inputs, num_slices)
    # mapPartitions gives each worker an iterator over its slice of tickers
    raw_records: List[Tuple[str, int, str]] = (
        rdd.mapPartitions(lambda part: fetch_financials_worker(list(part)))
           .collect()
    )

    print(f"Workers returned {len(raw_records)} total year-records across {len(tickers_to_process)} ticker(s).")

    if not raw_records:
        print("WARNING: No records fetched. Nothing to publish.")
        spark.stop()
        sys.exit(0)

    # ------------------------------------------------------------------
    # STEP 2: Driver — flatten by year, sort year-first
    # ------------------------------------------------------------------
    publish_records = build_year_first_publish_records(raw_records)

    print(
        f"Publishing {len(publish_records)} records to Kafka in year-first order "
        f"(years {publish_records[0][0]}–{publish_records[-1][0]})..."
    )

    # ------------------------------------------------------------------
    # STEP 3: Publish all records to Kafka in one burst
    # ------------------------------------------------------------------
    kafka_producer, kafka_topic = setup_kafka_producer()

    for year, ticker, serialized_payload in publish_records:
        kafka_producer.produce(
            kafka_topic,
            key=ticker.lower(),
            value=serialized_payload,
        )
        kafka_producer.poll(0)
        print(
            f"Published metrics for fiscal year {year} "
            f"-- company '{ticker}' to kafka topic '{kafka_topic}'"
        )

    kafka_producer.flush()

    print(
        f"--- Data Ingestion Complete: {len(publish_records)} records "
        f"published to '{kafka_topic}' ---"
    )

    spark.stop()
