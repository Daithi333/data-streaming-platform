SHELL := /bin/bash

# --- Versions / packages for spark-submit ---
DELTA_VERSION ?= 3.2.0
SPARK_VERSION ?= 3.5.1

SPARK_PACKAGES := io.delta:delta-spark_2.12:$(DELTA_VERSION),org.apache.spark:spark-sql-kafka-0-10_2.12:$(SPARK_VERSION)

# --- Kafka / topics ---
KAFKA_BROKERS ?= kafka:9092
KAFKA_TOPIC ?= taxi_trips

# --- Local paths (mounted into spark container via volumes) ---
DSP_DATA_DIR ?= /opt/dsp/data
DSP_CHECKPOINT_DIR ?= /opt/dsp/checkpoints

.PHONY: help up down build ps logs topic-create produce bronze

help:
	@echo "Infrastructure:"
	@echo "  make up               - start stack"
	@echo "  make down             - stop stack"
	@echo "  make build            - rebuild images"
	@echo "  make ps               - show containers"
	@echo "  make logs             - tail logs"
	@echo ""
	@echo "Kafka:"
	@echo "  make topic-create     - create Kafka topic (default: $(KAFKA_TOPIC))"
	@echo "  make produce          - produce sample taxi events to Kafka"
	@echo ""
	@echo "Pipelines:"
	@echo "  make bronze           - run Spark bronze streaming job (Kafka -> Delta)"
	@echo "  make silver           - run Spark silver streaming job (Bronze -> Silver Delta)"
	@echo ""
	@echo "Testing:"
	@echo "  make unit             - run all unit tests"
	@echo "  make unit-fast        - run unit tests (exclude Spark-dependent)"
	@echo "  make integration      - run integration tests (requires Docker)"
	@echo "  make integration-delta - run Delta-specific integration tests"
	@echo "  make test             - run unit-fast + integration"
	@echo ""
	@echo "Code Quality:"
	@echo "  make lint             - run ruff linter"
	@echo "  make format           - format code with ruff"
	@echo "  make format-check     - check code formatting"
	@echo "  make typecheck        - run type checker"
	@echo "  make pre-commit-install - install pre-commit hooks"
	@echo "  make pre-commit-run   - run pre-commit on all files"
	@echo ""
	@echo "Development:"
	@echo "  make spark-shell      - open bash shell in Spark container"

up:
	docker compose up -d --build

down:
	docker compose down -v

build:
	docker compose build --no-cache

ps:
	docker compose ps

logs:
	docker compose logs -f --tail=200

topic-create:
	docker compose exec kafka rpk topic create $(KAFKA_TOPIC) -p 6 || true
	docker compose exec kafka rpk topic describe $(KAFKA_TOPIC)

produce:
	docker compose --profile tools run --rm producer bash -lc '\
		pip install --no-cache-dir confluent-kafka typer >/dev/null && \
		python scripts/produce_taxi_events.py --brokers kafka:9092 --topic taxi_trips --rate 10 --minutes 1 \
	'

bronze:
	docker compose exec spark spark-submit \
		--master local[*] \
		--packages "$(SPARK_PACKAGES)" \
		/opt/dsp/src/dsp/pipelines/taxi/bronze_stream.py

silver:
	docker compose exec spark spark-submit \
		--master local[*] \
		--packages "$(SPARK_PACKAGES)" \
		/opt/dsp/src/dsp/pipelines/taxi/silver_stream.py

unit:
	uv run pytest tests/unit -v

unit-fast:
	uv run pytest tests/unit -m "not spark"  -v

integration:
	docker compose run --rm spark-tests

integration-delta:
	docker compose run --rm spark-tests bash -lc 'pytest -q tests/integration -m "spark and delta"'

test: unit-fast integration

lint:
	uv run ruff check .

format:
	uv run ruff format .

format-check:
	uv run ruff format --check .

typecheck:
	uv run ty check

pre-commit-install:
	uv run pre-commit install

pre-commit-run:
	uv run pre-commit run --all-files

spark-shell:
	docker compose exec spark bash
