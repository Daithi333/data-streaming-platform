# DSP — Data Streaming Platform

A local, production-inspired streaming data platform demonstrating:

- Event-driven ingestion with Kafka
- PySpark Structured Streaming
- Delta Lake (ACID data lake tables)
- Medallion architecture (Bronze → Silver → Gold)
- Checkpointed fault-tolerant pipelines
- Deterministic identifiers for idempotent processing
- Reproducible infrastructure via Docker

The project focuses on distributed systems concepts and production data engineering patterns rather than cloud-vendor specifics.


---

## Architecture

Event Generator  
→ Kafka (Redpanda)  
→ PySpark Structured Streaming  
→ Delta Lake (Bronze layer)  
→ Downstream Silver and Gold transformations  

Conceptually equivalent to:

- Azure Event Hubs
- Azure Databricks Structured Streaming
- Delta Lake on cloud object storage


---

## Technology Stack

- Python 3.11+
- PySpark 4.0
- Delta Lake
- Kafka-compatible broker (Redpanda)
- Docker Compose
- UV (dependency management)
- Pytest (testing)


---

## Quick Start

### Install dependencies

```bash
uv sync
```

### Set up pre-commit hooks (optional)

```bash
make pre-commit-install
```

### Start infrastructure

```bash
make up
```

### Create Kafka topic

```bash
make topic-create
```

### Produce synthetic taxi events

```bash
make produce
```

### Run Bronze streaming pipeline

```bash
make bronze
```

Spark UI is available at:

http://localhost:4040


---

## Data Layout

Bronze Delta table:

```
data/bronze/taxi_trips/
```

Checkpoint state:

```
checkpoints/bronze/taxi_trips/
```


---

## Make Targets

Run `make help` for full list. Common commands:

| Command | Description |
|----------|------------|
| `make up` | Start Kafka and Spark |
| `make down` | Stop stack and remove volumes |
| `make topic-create` | Create Kafka topic |
| `make produce` | Produce synthetic events |
| `make bronze` | Run Bronze streaming job |
| `make test` | Run unit and integration tests |
| `make integration` | Run integration tests in Docker |


---

## Design Principles

**Medallion layering**  
Bronze captures raw events with ingestion metadata.  
Silver enforces schema, validation, and deduplication.  
Gold provides curated, query-optimized aggregates.

**Idempotency**  
Deterministic hashing generates stable `trip_id` values to support replay and upsert patterns.

**Fault tolerance**  
Structured Streaming uses checkpointing to track offsets and maintain state.

**ACID guarantees**  
Delta Lake provides transactional consistency, time travel, and safe concurrent writes.

**Separation of concerns**  
Core Spark utilities, IO helpers, and pipeline definitions are modular to support extension to additional datasets.


---

## Roadmap

- Silver streaming layer with validation and deduplication
- Gold aggregates (e.g., hourly demand by zone)
- Data quality checks integrated into pipeline
- CI pipeline (linting, testing, packaging)
- Terraform scaffold for cloud deployment


---

## Purpose

This repository demonstrates production-oriented data engineering patterns:

- Distributed stream processing
- Transactional data lakes
- Reproducible infrastructure
- Clean, extensible project structure

It is intended as a hands-on exploration of modern lakehouse architecture concepts.
