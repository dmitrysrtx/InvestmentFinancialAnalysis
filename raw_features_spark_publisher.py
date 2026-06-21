"""Goal: examine original 10-K reports in assets/filings_10k and publish .parquet
objects to Kafka as RAW_FEATURE DataFrames (see constants.py).
"""

from __future__ import annotations

import os
import re
import json

from typing import Dict, List, Optional, Tuple

from pyspark.sql import SparkSession

from src.raw_features.combined_metrics import (
  combine_metrics,
  normalize_units_before_kafka,
  parse_metric_column,
)
from src.raw_features.consolidated_balance_sheet import (
  extract_metrics as extract_balance_sheet_metrics,
  read_raw_balance_sheet
)
from src.raw_features.consolidated_cashflow_statements import (
  extract_metrics as extract_cashflow_metrics,
  read_raw_cashflow_statements
)
from src.raw_features.constants import (
  BALANCE_SHEET_ERR_TEMPLATE,
  CASHFLOW_ERR_TEMPLATE,
  RAW_TABLES_DIR
)
from src.raw_features.logger import log_message

MAIN_PROCESS_NAME = "RawFeaturesSparkPublisher"
JOB_KEY_SEPARATOR = "@"
DEFAULT_START_YEAR = 2015
DEFAULT_END_YEAR = None  # None means no upper bound

def get_year_range() -> tuple:
  """Return (start_year: int, end_year: int | None) from env vars START_YEAR / END_YEAR.

  START_YEAR defaults to DEFAULT_START_YEAR (2015).
  END_YEAR defaults to None (no upper bound).
  """
  raw_start = os.getenv("START_YEAR", "").strip()
  raw_end = os.getenv("END_YEAR", "").strip()

  if raw_start:
    try:
      start_year = int(raw_start)
    except ValueError as e:
      raise ValueError(
        f"START_YEAR must be a 4-digit year, got '{raw_start}'"
      ) from e
  else:
    start_year = DEFAULT_START_YEAR

  if raw_end:
    try:
      end_year = int(raw_end)
    except ValueError as e:
      raise ValueError(
        f"END_YEAR must be a 4-digit year, got '{raw_end}'"
      ) from e
  else:
    end_year = DEFAULT_END_YEAR

  if end_year is not None and end_year < start_year:
    raise ValueError(
      f"END_YEAR ({end_year}) must be >= START_YEAR ({start_year})"
    )

  return start_year, end_year

# ---------------------------------------------------------------------------
# Legacy helper kept for backward-compat (used by list_company_filings)
# ---------------------------------------------------------------------------
def get_fiscal_year_threshold() -> int:
  start_year, _ = get_year_range()
  return start_year

def get_required_env_var(name: str) -> str:
  value = os.getenv(name)
  if value is None or value.strip() == "":
    raise RuntimeError(f"Missing required env var {name}")
  return value.strip()

def setup_kafka_channel():
  kafka_host = get_required_env_var("RAW_FEATURES_SPARK_PUBLISHER_KAFKA_HOST")
  kafka_port_raw = get_required_env_var("RAW_FEATURES_SPARK_PUBLISHER_KAFKA_PORT")
  kafka_channel = get_required_env_var("RAW_FEATURES_SPARK_PUBLISHER_TARGET_KAFKA_CHANNEL")
  try:
    kafka_port = int(kafka_port_raw)
  except ValueError as e:
    raise RuntimeError(
      "RAW_FEATURES_SPARK_PUBLISHER_KAFKA_PORT must be a valid integer port"
    ) from e
  try:
    from confluent_kafka import Producer
  except ImportError as e:
    raise RuntimeError(
      "Missing dependency confluent_kafka. Install it (e.g. pip install confluent-kafka)."
    ) from e
  producer = Producer({"bootstrap.servers": f"{kafka_host}:{kafka_port}"})
  return producer, kafka_channel

def extract_fiscal_year(filename: str) -> Optional[str]:
  match = re.match(r"^filing-(\d{4})-", filename)
  if not match:
    return None
  return match.group(1)

def build_job_key(company: str, fiscal_year: str) -> str:
  return f"{company}{JOB_KEY_SEPARATOR}{fiscal_year}"

def parse_job_key(job_key: str) -> Tuple[str, str]:
  parts = job_key.split(JOB_KEY_SEPARATOR, 1)
  if len(parts) != 2:
    return job_key, "unknown"
  return parts[0], parts[1]

def list_company_filings(
  base_dir: str,
  target_company: str,
  start_year: Optional[int] = None,
  end_year: Optional[int] = None,
) -> List[Tuple[str, str]]:
  """List filings for *target_company* within [start_year, end_year].

  start_year defaults to DEFAULT_START_YEAR when None.
  end_year=None means no upper bound.
  """
  if start_year is None:
    start_year = DEFAULT_START_YEAR
  company = target_company.lower().strip()
  company_dir = os.path.join(base_dir, company)
  filings: List[Tuple[str, str]] = []

  if not os.path.isdir(company_dir):
    return filings

  for filename in sorted(os.listdir(company_dir)):
    fiscal_year = extract_fiscal_year(filename)
    if fiscal_year is None:
      continue
    fy_int = int(fiscal_year)
    if fy_int < start_year:
      continue
    if end_year is not None and fy_int > end_year:
      continue
    job_key = build_job_key(company, fiscal_year)
    filings.append((job_key, os.path.join(company_dir, filename)))

  return filings

def clean_raw_html_tables(output_dir: str) -> None:
  for root, _dirs, files in os.walk(output_dir):
    for filename in files:
      if filename.endswith(".html"):
        try:
          os.remove(os.path.join(root, filename))
        except OSError:
          continue

def list_all_company_filings(
  base_dir: str,
  start_year: Optional[int] = None,
  end_year: Optional[int] = None,
) -> List[Tuple[str, str]]:
  filings: List[Tuple[str, str]] = []
  for dirname in sorted(os.listdir(base_dir)):
    company_dir = os.path.join(base_dir, dirname)
    if not os.path.isdir(company_dir):
      continue
    filings.extend(list_company_filings(base_dir, dirname, start_year=start_year, end_year=end_year))
  return filings

def build_year_first_publish_records(
  combined_metrics: List[Tuple[str, str]]
) -> List[Tuple[int, str, str]]:
  rows_by_year: Dict[int, List[Tuple[str, str]]] = {}

  for company, serialized_metrics in combined_metrics:
    try:
      payload = json.loads(serialized_metrics)
    except json.JSONDecodeError:
      continue

    ticker = str(payload.get("ticker", payload.get("ticket", company))).lower().strip()
    metrics_by_year: Dict[int, Dict[str, object]] = {}

    for metric_name, metric_values in payload.items():
      if metric_name in ("ticker", "ticket"):
        continue
      if not isinstance(metric_values, dict):
        continue
      for column_label, metric_value in metric_values.items():
        year, unit = parse_metric_column(column_label)
        if year is None:
          continue
        row = metrics_by_year.setdefault(year, {"ticker": ticker, "year": year})
        row[metric_name] = metric_value
        if unit:
          units = row.setdefault("units", {})
          if isinstance(units, dict) and metric_name not in units:
            units[metric_name] = unit

    for year, year_payload in metrics_by_year.items():
      units = year_payload.setdefault("units", {})
      if not isinstance(units, dict):
        units = {}
        year_payload["units"] = units
      rows_by_year.setdefault(year, []).append((ticker, year_payload))

  # --- Spark driver stage: normalise units → absolute values before Kafka ---
  # Group all per-company records, apply normalize_units_before_kafka, then
  # flatten back into the year-keyed structure for ordered publishing.
  records_by_company: Dict[str, List[dict]] = {}
  for year, ticker_payloads in rows_by_year.items():
    for ticker, payload in ticker_payloads:
      records_by_company.setdefault(ticker, []).append(payload)

  normalised_rows_by_year: Dict[int, List[Tuple[str, str]]] = {}
  for ticker, company_records in records_by_company.items():
    normalised = normalize_units_before_kafka(company_records)
    for record in normalised:
      year = record["year"]
      normalised_rows_by_year.setdefault(year, []).append(
        (ticker, json.dumps(record))
      )
  rows_by_year = normalised_rows_by_year

  ordered_records: List[Tuple[int, str, str]] = []
  for year in sorted(rows_by_year.keys()):
    for ticker, serialized_payload in sorted(rows_by_year[year], key=lambda item: item[0]):
      ordered_records.append((year, ticker, serialized_payload))
  return ordered_records

def main() -> None:
  start_year, end_year = get_year_range()
  assets_dir = get_required_env_var("RAW_FEATURES_SPARK_PUBLISHER_ASSETS")
  if not os.path.isabs(assets_dir):
    raise RuntimeError(
      "RAW_FEATURES_SPARK_PUBLISHER_ASSETS must be an absolute path"
    )
  if not os.path.isdir(assets_dir):
    raise RuntimeError(
      "RAW_FEATURES_SPARK_PUBLISHER_ASSETS does not exist or is not a directory: "
      f"{assets_dir}"
    )

  year_range_label = (
    "%s–%s" % (start_year, end_year) if end_year is not None
    else "%s–present" % start_year
  )

  target_company = os.getenv("RAW_FEATURES_SPARK_PUBLISHER_TARGET_COMPANY", "").strip()
  kafka_producer, kafka_channel = setup_kafka_channel()
  if target_company:
    log_message(
      "Single target company mode enabled; processing company '%s' only (years: %s)" % (
        target_company.upper(),
        year_range_label
      )
    )
    filings = list_company_filings(assets_dir, target_company, start_year=start_year, end_year=end_year)
  else:
    log_message(
      "Full process mode enabled; processing all companies under assets path (years: %s)" % (
        year_range_label
      )
    )
    filings = list_all_company_filings(assets_dir, start_year=start_year, end_year=end_year)
  
  if not filings:
    if target_company:
      log_message(
        "Company %s -- no filings to process in year range %s" % (
          target_company.upper(),
          year_range_label
        ),
        "ERROR"
      )
    else:
      log_message(
        "No filings found under %s in year range %s" % (
          assets_dir,
          year_range_label
        ),
        "ERROR"
      )
    return

  spark = SparkSession.builder.appName(MAIN_PROCESS_NAME).getOrCreate()
  sc = spark.sparkContext
  log_message(f"SparkSession started for {MAIN_PROCESS_NAME}")
  num_slices = max(1, min(len(filings), sc.defaultParallelism or len(filings)))
  if target_company:
    log_message(
      "Will process %s 10-K filings for company %s with %s spark workers "
      "(year range: %s)" % (
        str(len(filings)),
        target_company.upper(),
        str(num_slices),
        year_range_label
      )
    )
  else:
    log_message(
      "Will process %s 10-K filings across all companies with %s spark workers "
      "(year range: %s, assets path: %s)" % (
        str(len(filings)),
        str(num_slices),
        year_range_label,
        assets_dir
      )
    )
  filings_rdd = sc.parallelize(filings, num_slices)
  balance_sheet = filings_rdd.mapPartitions(read_raw_balance_sheet).collect()
  abort_execution = False
  
  for filing_key, worker_output in balance_sheet:
    if BALANCE_SHEET_ERR_TEMPLATE in worker_output:
      company, fiscal_year = parse_job_key(filing_key)
      log_message(
        "Fiscal Year %s -- company %s -- failed to read Consolidated Balance Sheet -- %s" % (
          fiscal_year,
          company.upper(),
          worker_output
        )
      )
      abort_execution = True

  if abort_execution:
    return
  
  log_message(f"Read {len(balance_sheet)} Consolidated Balance Sheet in total")
  balance_sheet_by_company = {filing_key: table for filing_key, table in balance_sheet}
  rdd = sc.parallelize(balance_sheet, num_slices)
  balance_sheet_metrics = rdd.mapPartitions(extract_balance_sheet_metrics).collect()

  for filing_key, worker_output in balance_sheet_metrics:
    if BALANCE_SHEET_ERR_TEMPLATE in worker_output:
      company, fiscal_year = parse_job_key(filing_key)
      log_message(
        "Fiscal Year %s -- company %s -- failed to analyze Consolidated Balance Sheet -- %s" % (
          fiscal_year,
          company.upper(),
          worker_output
        )
      )
      log_message(f"Balance Sheet: {balance_sheet_by_company.get(filing_key)}")
      abort_execution = True

  if abort_execution:
    return

  log_message(f"Analyzed {len(balance_sheet_metrics)} Consolidated Balance Sheet in total")
  filings_rdd = sc.parallelize(filings, num_slices)
  cashflow_statements = filings_rdd.mapPartitions(read_raw_cashflow_statements).collect()
  abort_execution = False

  for filing_key, worker_output in cashflow_statements:
    if CASHFLOW_ERR_TEMPLATE in worker_output:
      company, fiscal_year = parse_job_key(filing_key)
      log_message(
        "Fiscal Year %s -- company %s -- failed to read Consolidated Cash Flow Statements -- %s" % (
          fiscal_year,
          company.upper(),
          worker_output
        )
      )
      abort_execution = True

  if abort_execution:
    return

  log_message(f"Read {len(cashflow_statements)} Consolidated Cash Flow Statements in total")
  cashflow_statements_by_company = {filing_key: table for filing_key, table in cashflow_statements}
  cashflow_rdd = sc.parallelize(cashflow_statements, num_slices)
  cashflow_metrics = cashflow_rdd.mapPartitions(extract_cashflow_metrics).collect()

  for filing_key, worker_output in cashflow_metrics:
    if CASHFLOW_ERR_TEMPLATE in worker_output:
      company, fiscal_year = parse_job_key(filing_key)
      log_message(
        "Fiscal Year %s -- company %s -- failed to analyze Consolidated Cash Flow Statements -- %s" % (
          fiscal_year,
          company.upper(),
          worker_output
        )
      )
      log_message(f"Cash Flow Statements: {cashflow_statements_by_company.get(filing_key)}")
      abort_execution = True

  if abort_execution:
    return

  log_message(f"Analyzed {len(cashflow_metrics)} Consolidated Cash Flow Statements in total")
  try:
    combined_metrics = combine_metrics(
      balance_sheet_metrics=balance_sheet_metrics,
      cashflow_metrics=cashflow_metrics,
      fiscal_year_threshold=start_year,
      fiscal_year_end=end_year,
      job_key_separator=JOB_KEY_SEPARATOR
    )
  except ValueError as e:
    log_message(str(e), "ERROR")
    return

  publish_records = build_year_first_publish_records(combined_metrics)
  if not publish_records:
    log_message("No yearly records were generated from combined metrics", "ERROR")
    return

  log_message(
    "Publishing %s yearly raw-feature records to kafka channel '%s' in year-first order" % (
      str(len(publish_records)),
      kafka_channel
    )
  )

  current_year = None
  for year, company, serialized_metrics in publish_records:
    if current_year is not None and year != current_year:
      kafka_producer.flush()
    current_year = year
    kafka_producer.produce(kafka_channel, key=company, value=serialized_metrics)
    kafka_producer.poll(0)
    log_message(
      "Published metrics for fiscal year %s -- company '%s' to kafka channel '%s'" % (
        str(year),
        company.upper(),
        kafka_channel
      )
    )

  kafka_producer.flush()
  
  spark.stop()

if __name__ == "__main__":
  main()
