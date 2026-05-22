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

## Verifying Delta Lake Data

Once the Bronze (and optionally Silver) pipelines have processed events, you can query the Delta tables using Spark SQL.

### Open a Spark SQL shell

```bash
docker compose exec spark spark-sql
```

This picks up the Delta and Kafka config from `spark-defaults.conf` automatically.

### Query Bronze table

```sql
SELECT COUNT(*) FROM delta.`/opt/dsp/data/bronze/taxi_trips`;

SELECT * FROM delta.`/opt/dsp/data/bronze/taxi_trips`
ORDER BY ingest_ts DESC LIMIT 10;

-- Partition distribution
SELECT kafka_partition, COUNT(*) AS cnt
FROM delta.`/opt/dsp/data/bronze/taxi_trips`
GROUP BY kafka_partition ORDER BY kafka_partition;

-- Parse success rate
SELECT parse_ok, COUNT(*) AS cnt
FROM delta.`/opt/dsp/data/bronze/taxi_trips`
GROUP BY parse_ok;
```

### Query Silver table

```sql
SELECT COUNT(*) FROM delta.`/opt/dsp/data/silver/taxi_trips`;

SELECT * FROM delta.`/opt/dsp/data/silver/taxi_trips`
ORDER BY silver_ingest_ts DESC LIMIT 10;

-- Check deduplication (trip_id should be unique)
SELECT COUNT(*) FROM (
  SELECT trip_id FROM delta.`/opt/dsp/data/silver/taxi_trips`
  GROUP BY trip_id HAVING COUNT(*) > 1
);
```

### Delta Lake metadata

```sql
DESCRIBE HISTORY delta.`/opt/dsp/data/bronze/taxi_trips`;

DESCRIBE DETAIL delta.`/opt/dsp/data/bronze/taxi_trips`;
```

### Quick validation checklist

| Check | Expected |
|-------|----------|
| Bronze row count > 0 | Events are landing |
| `parse_ok` mostly `true` | JSON schema matches producer output |
| Silver count <= Bronze count | Validation and dedup are filtering |
| History shows multiple versions | Micro-batches are committing |
| No duplicate `trip_id` in Silver | Deduplication is working |


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
