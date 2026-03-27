"""Kafka producer for financial reports.

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

import os
import sys
import time
import json
import yfinance as yf
from kafka import KafkaProducer

# --- CONFIGURATION ---
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'financial_reports_stream'

# Default list of tech companies to analyze
TICKERS = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'INTC', 'NVDA', 'CSCO', 'ORCL', 'IBM', 'AMD']

DEFAULT_START_YEAR = 2015
DEFAULT_END_YEAR = None  # None means no upper bound


def _parse_year_env(name: str, default) -> int | None:
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


def get_financials(ticker: str, start_year: int, end_year: int | None):
    """Fetch financial data for *ticker* filtered to [start_year, end_year].

    Returns a list of dicts (one per qualifying fiscal year).
    """
    print(f"Fetching data for {ticker}...")
    try:
        stock = yf.Ticker(ticker)

        # Fetch Balance Sheet and Income Statement
        balance_sheet = stock.balance_sheet
        financials = stock.financials
        # Fetch history for price labels
        history = stock.history(period="max")

        if balance_sheet.empty or financials.empty:
            return []

        data_points = []
        dates = financials.columns

        for date in dates:
            try:
                year = date.year

                # --- Year-range filter ---
                if year < start_year:
                    continue
                if end_year is not None and year > end_year:
                    continue

                date_str = str(date.date())

                # Helper to safely extract value from DF
                def get_val(df, key):
                    try:
                        return float(df.loc[key, date])
                    except KeyError:
                        return 0.0

                # Get stock price at the time of the report
                close_price = 0.0
                if date_str in history.index:
                    close_price = float(history.loc[date_str]['Close'])
                else:
                    # Fallback: average of that year
                    yearly_data = history[history.index.year == year]
                    if not yearly_data.empty:
                        close_price = float(yearly_data['Close'].mean())

                # Construct the data object
                record = {
                    'ticker': ticker,
                    'year': year,
                    'report_date': date_str,
                    # --- Raw Values for Ratios ---
                    'total_revenue': get_val(financials, 'Total Revenue'),
                    'net_income': get_val(financials, 'Net Income'),
                    'current_assets': get_val(balance_sheet, 'Current Assets'),
                    'current_liabilities': get_val(balance_sheet, 'Current Liabilities'),
                    'total_assets': get_val(balance_sheet, 'Total Assets'),
                    'total_liabilities': get_val(balance_sheet, 'Total Liabilities Net Minority Interest'),
                    'stockholders_equity': get_val(balance_sheet, 'Stockholders Equity'),
                    'interest_expense': get_val(financials, 'Interest Expense'),
                    'ebit': get_val(financials, 'EBIT'),
                    # --- Price for Labeling ---
                    'close_price': close_price
                }
                data_points.append(record)
            except Exception as e:
                print(f"Skipping year {date.year} for {ticker}: {e}")

        return data_points

    except Exception as e:
        print(f"Failed to fetch {ticker}: {e}")
        return []


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

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    for ticker in tickers_to_process:
        reports = get_financials(ticker, start_year, end_year)
        for report in reports:
            producer.send(TOPIC_NAME, value=report)
            print(f"Sent: {ticker} - {report['year']}")
            # Simulate real-time streaming delay
            time.sleep(0.2)

    producer.flush()
    print("--- Data Ingestion Complete ---")
