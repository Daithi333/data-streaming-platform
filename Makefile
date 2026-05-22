SHELL := /bin/bash

# --- Kafka / topics ---
KAFKA_BROKERS ?= kafka:9092
KAFKA_TOPIC ?= taxi_trips

# --- Local paths (mounted into spark container via volumes) ---
DSP_DATA_DIR ?= /opt/dsp/data
DSP_CHECKPOINT_DIR ?= /opt/dsp/checkpoints

# --- Generated requirements for Docker ---
SPARK_REQS := docker/spark/requirements.txt
SPARK_REQS_DEV := docker/spark/requirements-dev.txt

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
	@echo "  make spark-sql        - open Spark SQL REPL"

up:
	docker compose up -d --build

down:
	docker compose down

build:
	docker compose build --no-cache

requirements:
	uv export --frozen --no-dev --no-hashes -o $(SPARK_REQS)
	uv export --frozen --no-hashes -o $(SPARK_REQS_DEV)

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
		/opt/dsp/src/dsp/pipelines/taxi/bronze_stream.py

silver:
	docker compose exec spark spark-submit \
		--master local[*] \
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
	uv run mypy src

pre-commit-install:
	uv run pre-commit install

pre-commit-run:
	uv run pre-commit run --all-files

spark-shell:
	docker compose exec spark bash

spark-sql:
	docker compose exec spark spark-sql
