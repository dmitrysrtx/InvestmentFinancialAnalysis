RAW_FEATURES_SPARK_PUBLISHER_ROOT ?= $(CURDIR)
RAW_FEATURES_SPARK_PUBLISHER_ASSETS ?= $(RAW_FEATURES_SPARK_PUBLISHER_ROOT)/assets/filings_10k
RAW_FEATURES_SPARK_PUBLISHER_ASSETS_EXPORT ?= $(RAW_FEATURES_SPARK_PUBLISHER_ROOT)/assets_export.zip
RAW_FEATURES_SPARK_PUBLISHER_KAFKA_HOST ?= localhost
RAW_FEATURES_SPARK_PUBLISHER_KAFKA_PORT ?= 9092
RAW_FEATURES_SPARK_PUBLISHER_TARGET_KAFKA_CHANNEL ?= raw_features
PYTHON ?= /home/linuxu/anaconda3/bin/python
company ?=
# Year-range variables (used by run-producer and process targets)
start_year ?=
end_year ?=
# Per-document (single-ticker) variable for run-producer
ticker ?=
assets_dir := $(RAW_FEATURES_SPARK_PUBLISHER_ASSETS)
publisher_script := $(RAW_FEATURES_SPARK_PUBLISHER_ROOT)/raw_features_spark_publisher.py
supported_component := raw_features
selected_component := $(firstword $(MAKECMDGOALS))

.DEFAULT_GOAL := help

.PHONY: help raw_features process do_export do_import run-altman-etl run-altman-zprime-etl run-producer

help:
	@echo "Usage: make <target> [options]"
	@echo ""
	@echo "Targets:"
	@echo "  raw_features            Supported component. See actions below."
	@echo "  run-altman-etl          Run spark_altman_etl.py."
	@echo "  run-altman-zprime-etl   Run spark_altman_zprime_etl.py."
	@echo "  run-producer_api        Run producer_api.py (Kafka producer)."
	@echo ""
	@echo "Actions for 'raw_features':"
	@echo "  process                 Run raw_features_spark_publisher.py."
	@echo "                          Optional: company=<ticker>   (single-company mode)."
	@echo "                          Optional: start_year=<YYYY>  (default: 2015)."
	@echo "                          Optional: end_year=<YYYY>    (default: no upper bound)."
	@echo "  do_export               Zip assets dir to assets export archive."
	@echo "  do_import               Import assets archive unless assets dir is non-empty."
	@echo ""
	@echo "Options (override with make VAR=value):"
	@echo "  RAW_FEATURES_SPARK_PUBLISHER_ROOT                  default: $(RAW_FEATURES_SPARK_PUBLISHER_ROOT)"
	@echo "  RAW_FEATURES_SPARK_PUBLISHER_ASSETS                default: $(RAW_FEATURES_SPARK_PUBLISHER_ASSETS)"
	@echo "  RAW_FEATURES_SPARK_PUBLISHER_ASSETS_EXPORT         default: $(RAW_FEATURES_SPARK_PUBLISHER_ASSETS_EXPORT)"
	@echo "  RAW_FEATURES_SPARK_PUBLISHER_KAFKA_HOST            default: $(RAW_FEATURES_SPARK_PUBLISHER_KAFKA_HOST)"
	@echo "  RAW_FEATURES_SPARK_PUBLISHER_KAFKA_PORT            default: $(RAW_FEATURES_SPARK_PUBLISHER_KAFKA_PORT)"
	@echo "  RAW_FEATURES_SPARK_PUBLISHER_TARGET_KAFKA_CHANNEL  default: $(RAW_FEATURES_SPARK_PUBLISHER_TARGET_KAFKA_CHANNEL)"
	@echo "  PYTHON                                             default: $(PYTHON)"
	@echo "  start_year                                         default: (unset → 2015)"
	@echo "  end_year                                           default: (unset → no upper bound)"
	@echo "  ticker                                             default: (unset → all tickers, run-producer only)"
	@echo ""
	@echo "Examples:"
	@echo "  make raw_features process"
	@echo "  make raw_features process company=aaoi"
	@echo "  make raw_features process start_year=2015 end_year=2022"
	@echo "  make raw_features process start_year=2023 end_year=2025"
	@echo "  make raw_features process company=aaoi start_year=2018 end_year=2022"
	@echo "  make raw_features do_export"
	@echo "  make raw_features do_import"
	@echo "  make run-altman-etl"
	@echo "  make run-altman-dual-etl"
	@echo "  make run-producer_api"
	@echo "  make run-producer_api start_year=2015 end_year=2022"
	@echo "  make run-producer_api start_year=2023 end_year=2025"
	@echo "  make run-producer_api ticker=AAPL start_year=2020 end_year=2022"

raw_features:
	@if [ "$(selected_component)" != "$(supported_component)" ]; then \
		echo "ERROR: unsupported component '$(selected_component)'. Supported component: $(supported_component)"; \
		exit 1; \
	fi
	@if [ "$(words $(MAKECMDGOALS))" -eq 1 ]; then \
		echo "INFO: component '$(supported_component)' selected. Choose one action: process | do_export | do_import"; \
	fi

process: raw_features
	@if [ -z "$(RAW_FEATURES_SPARK_PUBLISHER_ROOT)" ]; then \
		echo "ERROR: RAW_FEATURES_SPARK_PUBLISHER_ROOT is required"; \
		exit 1; \
	fi
	@if [ ! -f "$(publisher_script)" ]; then \
		echo "ERROR: publisher script not found: $(publisher_script)"; \
		exit 1; \
	fi
	@if [ -n "$(company)" ]; then \
		echo "INFO: process mode single company=$(company)"; \
		cd "$(RAW_FEATURES_SPARK_PUBLISHER_ROOT)" && \
		RAW_FEATURES_SPARK_PUBLISHER_ROOT="$(RAW_FEATURES_SPARK_PUBLISHER_ROOT)" \
		RAW_FEATURES_SPARK_PUBLISHER_KAFKA_HOST="$(RAW_FEATURES_SPARK_PUBLISHER_KAFKA_HOST)" \
		RAW_FEATURES_SPARK_PUBLISHER_KAFKA_PORT="$(RAW_FEATURES_SPARK_PUBLISHER_KAFKA_PORT)" \
		RAW_FEATURES_SPARK_PUBLISHER_TARGET_KAFKA_CHANNEL="$(RAW_FEATURES_SPARK_PUBLISHER_TARGET_KAFKA_CHANNEL)" \
		RAW_FEATURES_SPARK_PUBLISHER_ASSETS="$(RAW_FEATURES_SPARK_PUBLISHER_ASSETS)" \
		RAW_FEATURES_SPARK_PUBLISHER_TARGET_COMPANY="$(company)" \
		START_YEAR="$(start_year)" \
		END_YEAR="$(end_year)" \
		"$(PYTHON)" raw_features_spark_publisher.py; \
	else \
		echo "INFO: process mode full assets scan"; \
		cd "$(RAW_FEATURES_SPARK_PUBLISHER_ROOT)" && \
		RAW_FEATURES_SPARK_PUBLISHER_ROOT="$(RAW_FEATURES_SPARK_PUBLISHER_ROOT)" \
		RAW_FEATURES_SPARK_PUBLISHER_KAFKA_HOST="$(RAW_FEATURES_SPARK_PUBLISHER_KAFKA_HOST)" \
		RAW_FEATURES_SPARK_PUBLISHER_KAFKA_PORT="$(RAW_FEATURES_SPARK_PUBLISHER_KAFKA_PORT)" \
		RAW_FEATURES_SPARK_PUBLISHER_TARGET_KAFKA_CHANNEL="$(RAW_FEATURES_SPARK_PUBLISHER_TARGET_KAFKA_CHANNEL)" \
		RAW_FEATURES_SPARK_PUBLISHER_ASSETS="$(RAW_FEATURES_SPARK_PUBLISHER_ASSETS)" \
		START_YEAR="$(start_year)" \
		END_YEAR="$(end_year)" \
		"$(PYTHON)" raw_features_spark_publisher.py; \
	fi

do_export: raw_features
	@if [ -z "$(RAW_FEATURES_SPARK_PUBLISHER_ROOT)" ]; then \
		echo "ERROR: RAW_FEATURES_SPARK_PUBLISHER_ROOT is required"; \
		exit 1; \
	fi
	@if [ ! -d "$(assets_dir)" ]; then \
		echo "ERROR: assets dir not found: $(assets_dir)"; \
		exit 1; \
	fi
	@mkdir -p "$(dir $(RAW_FEATURES_SPARK_PUBLISHER_ASSETS_EXPORT))"
	@rm -f "$(RAW_FEATURES_SPARK_PUBLISHER_ASSETS_EXPORT)"
	@cd "$(assets_dir)" && zip -rq "$(RAW_FEATURES_SPARK_PUBLISHER_ASSETS_EXPORT)" .
	@echo "INFO: assets exported to $(RAW_FEATURES_SPARK_PUBLISHER_ASSETS_EXPORT)"

do_import: raw_features
	@if [ -z "$(RAW_FEATURES_SPARK_PUBLISHER_ROOT)" ]; then \
		echo "ERROR: RAW_FEATURES_SPARK_PUBLISHER_ROOT is required"; \
		exit 1; \
	fi
	@if [ ! -f "$(RAW_FEATURES_SPARK_PUBLISHER_ASSETS_EXPORT)" ]; then \
		echo "ERROR: export zip not found: $(RAW_FEATURES_SPARK_PUBLISHER_ASSETS_EXPORT)"; \
		exit 1; \
	fi
	@if [ -d "$(RAW_FEATURES_SPARK_PUBLISHER_ASSETS)" ] && [ "$$(ls -A "$(RAW_FEATURES_SPARK_PUBLISHER_ASSETS)" 2>/dev/null)" ]; then \
		echo "WARN: assets import is skipped because assets already exist: $(RAW_FEATURES_SPARK_PUBLISHER_ASSETS)"; \
	else \
		if [ ! -d "$(RAW_FEATURES_SPARK_PUBLISHER_ASSETS)" ]; then \
			mkdir -p "$(RAW_FEATURES_SPARK_PUBLISHER_ASSETS)"; \
		fi; \
		unzip -qo "$(RAW_FEATURES_SPARK_PUBLISHER_ASSETS_EXPORT)" -d "$(RAW_FEATURES_SPARK_PUBLISHER_ASSETS)"; \
		echo "INFO: assets imported to $(RAW_FEATURES_SPARK_PUBLISHER_ASSETS)"; \
	fi

run-altman-dual-etl:
	@echo "Starting Dual-Scoring Spark Consumer (Original Z-Score & Z'-Score)..."
	RAW_FEATURES_SPARK_PUBLISHER_ROOT="$(RAW_FEATURES_SPARK_PUBLISHER_ROOT)" \
	spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 spark_altman_dual_etl.py

run-producer_api:
	START_YEAR="$(start_year)" \
	END_YEAR="$(end_year)" \
	TARGET_TICKER="$(ticker)" \
	RAW_FEATURES_SPARK_PUBLISHER_ROOT="$(RAW_FEATURES_SPARK_PUBLISHER_ROOT)" \
	spark-submit produser_api.py

demo-phase1:
	@echo "=== INITIATING PHASE 1: Historical Data Loading (2015-2020) ==="
	@echo "Publishing initial dataset to Kafka topic: $(RAW_FEATURES_SPARK_PUBLISHER_TARGET_KAFKA_CHANNEL)"
	START_YEAR=2015 END_YEAR=2020 $(PYTHON) $(publisher_script)

demo-phase2:
	@echo "=== INITIATING PHASE 2: Live Stream Simulation (2021-2024) ==="
	@echo "Publishing fresh annual reports to Kafka topic: $(RAW_FEATURES_SPARK_PUBLISHER_TARGET_KAFKA_CHANNEL)"
	START_YEAR=2021 END_YEAR=2024 $(PYTHON) $(publisher_script)

%:
	@echo "ERROR: unsupported component '$@'. Supported component: $(supported_component)"
	@exit 1
