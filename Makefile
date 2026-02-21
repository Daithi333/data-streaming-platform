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
	@echo "Targets:"
	@echo "  make up             - start stack"
	@echo "  make down           - stop stack"
	@echo "  make build          - rebuild images"
	@echo "  make ps             - show containers"
	@echo "  make logs           - tail logs"
	@echo "  make topic-create   - create Kafka topic (default: $(KAFKA_TOPIC))"
	@echo "  make produce        - produce sample taxi events to Kafka"
	@echo "  make bronze         - run Spark bronze streaming job (Kafka -> Delta)"

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
