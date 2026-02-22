import importlib
import types
import pytest

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
    LongType,
)


# ---------- Helpers ----------

KAFKA_BATCH_SCHEMA = StructType(
    [
        StructField("topic", StringType(), False),
        StructField("partition", IntegerType(), False),
        StructField("offset", LongType(), False),
        StructField("timestamp", TimestampType(), True),
        StructField("key", StringType(), True),
        StructField("value", StringType(), True),
    ]
)


def kafka_batch_df(spark, rows):
    return spark.createDataFrame(rows, schema=KAFKA_BATCH_SCHEMA)


class FakeSettings:
    """
    Avoid relying on real env/settings at import time.
    We only need a few attributes so the module imports cleanly.
    """

    kafka_topic = "taxi_trips"
    kafka_brokers = "kafka:9092"
    kafka_starting_offsets = "earliest"
    trigger_interval = "30 seconds"
    app_log_level = "INFO"
    spark_log_level = "WARN"
    log_query_progress_seconds = None

    def bronze_path(self, name):
        return f"/tmp/bronze/{name}"

    def bronze_checkpoint(self, name):
        return f"/tmp/chk/bronze/{name}"


def import_bronze_stream_module(monkeypatch):
    """
    Import dsp.pipelines.taxi.bronze_stream while patching settings/logging modules
    so import-time globals don't require real env.
    """
    fake_settings_mod = types.SimpleNamespace(get_settings=lambda: FakeSettings())
    monkeypatch.setitem(importlib.sys.modules, "dsp.config.settings", fake_settings_mod)

    fake_logs_mod = types.SimpleNamespace(configure_logging=lambda level: None)
    monkeypatch.setitem(importlib.sys.modules, "dsp.config.logs", fake_logs_mod)

    module_name = "dsp.pipelines.taxi.bronze_stream"
    if module_name in importlib.sys.modules:
        del importlib.sys.modules[module_name]
    return importlib.import_module(module_name)


def apply_parse(raw_df, event_schema):
    """
    Mirrors bronze_stream parsed = (...) using real Spark functions.
    """
    return (
        raw_df.select(
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("raw_json"),
        )
        .withColumn("payload", F.from_json(F.col("raw_json"), event_schema))
        .withColumn(
            "parse_ok",
            F.col("payload").isNotNull()
            & F.col("payload.event_id").isNotNull()
            & F.col("payload.event_ts").isNotNull(),
        )
        .select("*", F.col("payload.*"))
        .drop("payload")
    )


def apply_enrich(parsed_df):
    """
    Mirrors bronze_stream enriched = (...) using real Spark functions.
    """
    return (
        parsed_df.withColumn("ingest_ts", F.current_timestamp())
        .withColumn(
            "trip_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.coalesce(F.col("vendor_id").cast("string"), F.lit("")),
                    F.coalesce(F.col("pickup_ts"), F.lit("")),
                    F.coalesce(F.col("dropoff_ts"), F.lit("")),
                    F.coalesce(F.col("pu_location_id").cast("string"), F.lit("")),
                    F.coalesce(F.col("do_location_id").cast("string"), F.lit("")),
                    F.coalesce(F.col("total_amount").cast("string"), F.lit("")),
                ),
                256,
            ),
        )
        .withColumn("event_ts_parsed", F.try_to_timestamp("event_ts"))
        .withColumn("pickup_ts_parsed", F.try_to_timestamp("pickup_ts"))
        .withColumn("dropoff_ts_parsed", F.try_to_timestamp("dropoff_ts"))
    )


# ---------- Tests ----------


@pytest.mark.spark
def test_parse_ok_true_false_integration(spark, monkeypatch):
    bronze_stream = import_bronze_stream_module(monkeypatch)
    schema = bronze_stream.EVENT_SCHEMA

    rows = [
        (
            "taxi_trips",
            0,
            1,
            None,
            "k1",
            '{"event_id":"e1","event_ts":"2024-01-01 00:00:00","vendor_id":1}',
        ),
        ("taxi_trips", 0, 2, None, "k2", "not json"),
        (
            "taxi_trips",
            0,
            3,
            None,
            "k3",
            '{"vendor_id":1}',  # missing required event_id/event_ts => payload null => parse_ok false
        ),
    ]
    raw = kafka_batch_df(spark, rows)
    out = (
        apply_parse(raw, schema)
        .select("kafka_offset", "parse_ok")
        .orderBy("kafka_offset")
        .collect()
    )
    assert [r["parse_ok"] for r in out] == [True, False, False]


@pytest.mark.spark
def test_expected_columns_exist_after_parse_integration(spark, monkeypatch):
    bronze_stream = import_bronze_stream_module(monkeypatch)
    schema = bronze_stream.EVENT_SCHEMA

    raw = kafka_batch_df(
        spark,
        [
            (
                "taxi_trips",
                0,
                1,
                None,
                "k1",
                '{"event_id":"e1","event_ts":"2024-01-01 00:00:00"}',
            )
        ],
    )

    parsed = apply_parse(raw, schema)
    cols = set(parsed.columns)

    for c in [
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "kafka_key",
        "raw_json",
        "parse_ok",
    ]:
        assert c in cols

    # flattened payload
    assert "event_id" in cols
    assert "event_ts" in cols


@pytest.mark.spark
def test_trip_id_deterministic_integration(spark, monkeypatch):
    bronze_stream = import_bronze_stream_module(monkeypatch)
    schema = bronze_stream.EVENT_SCHEMA

    payload = (
        '{"event_id":"e1","event_ts":"2024-01-01 00:00:00",'
        '"vendor_id":1,"pickup_ts":"2024-01-01 00:10:00","dropoff_ts":"2024-01-01 00:20:00",'
        '"pu_location_id":100,"do_location_id":200,"total_amount":12.5}'
    )

    raw = kafka_batch_df(spark, [("taxi_trips", 0, 1, None, "k1", payload)])

    id1 = apply_enrich(apply_parse(raw, schema)).select("trip_id").first()["trip_id"]
    id2 = apply_enrich(apply_parse(raw, schema)).select("trip_id").first()["trip_id"]
    assert id1 == id2


@pytest.mark.spark
def test_trip_id_changes_when_key_fields_change_integration(spark, monkeypatch):
    bronze_stream = import_bronze_stream_module(monkeypatch)
    schema = bronze_stream.EVENT_SCHEMA

    base = (
        '{"event_id":"e1","event_ts":"2024-01-01 00:00:00",'
        '"vendor_id":1,"pickup_ts":"2024-01-01 00:10:00","dropoff_ts":"2024-01-01 00:20:00",'
        '"pu_location_id":100,"do_location_id":200,"total_amount":12.5}'
    )
    changed = (
        '{"event_id":"e1","event_ts":"2024-01-01 00:00:00",'
        '"vendor_id":1,"pickup_ts":"2024-01-01 00:10:00","dropoff_ts":"2024-01-01 00:20:00",'
        '"pu_location_id":100,"do_location_id":200,"total_amount":99.9}'
    )

    raw = kafka_batch_df(
        spark,
        [
            ("taxi_trips", 0, 1, None, "k1", base),
            ("taxi_trips", 0, 2, None, "k2", changed),
        ],
    )

    out = (
        apply_enrich(apply_parse(raw, schema))
        .select("kafka_offset", "trip_id")
        .orderBy("kafka_offset")
        .collect()
    )
    assert out[0]["trip_id"] != out[1]["trip_id"]


@pytest.mark.spark
def test_timestamp_parsing_nullable_integration(spark, monkeypatch):
    bronze_stream = import_bronze_stream_module(monkeypatch)
    schema = bronze_stream.EVENT_SCHEMA

    rows = [
        (
            "taxi_trips",
            0,
            1,
            None,
            "k1",
            '{"event_id":"e1","event_ts":"2024-01-01 00:00:00",'
            '"pickup_ts":"2024-01-01 00:10:00","dropoff_ts":"2024-01-01 00:20:00"}',
        ),
        ("taxi_trips", 0, 2, None, "k2", '{"event_id":"e2","event_ts":"not-a-date"}'),
    ]
    raw = kafka_batch_df(spark, rows)
    out = (
        apply_enrich(apply_parse(raw, schema))
        .select(
            "kafka_offset", "event_ts_parsed", "pickup_ts_parsed", "dropoff_ts_parsed"
        )
        .orderBy("kafka_offset")
        .collect()
    )

    assert out[0]["event_ts_parsed"] is not None
    assert out[0]["pickup_ts_parsed"] is not None
    assert out[0]["dropoff_ts_parsed"] is not None

    assert out[1]["event_ts_parsed"] is None


@pytest.mark.spark
def test_missing_nullable_fields_do_not_crash_integration(spark, monkeypatch):
    bronze_stream = import_bronze_stream_module(monkeypatch)
    schema = bronze_stream.EVENT_SCHEMA

    raw = kafka_batch_df(
        spark,
        [
            (
                "taxi_trips",
                0,
                1,
                None,
                "k1",
                '{"event_id":"e1","event_ts":"2024-01-01 00:00:00"}',
            )
        ],
    )

    row = (
        apply_enrich(apply_parse(raw, schema))
        .select("parse_ok", "trip_id", "vendor_id", "pickup_ts", "total_amount")
        .first()
    )

    assert row["parse_ok"] is True
    assert row["trip_id"] is not None
    assert row["vendor_id"] is None
    assert row["pickup_ts"] is None
    assert row["total_amount"] is None


@pytest.mark.spark
@pytest.mark.delta
def test_delta_batch_write_read_smoke(spark, tmp_path):
    """
    Optional: validates Delta is available in the *test environment*.
    Skips if Delta isn't on the classpath.
    """
    path = str(tmp_path / "delta_smoke")

    try:
        spark.range(0, 3).write.format("delta").mode("overwrite").save(path)
    except Exception:
        pytest.skip("Delta not available on Spark classpath in this test environment.")

    back = spark.read.format("delta").load(path).orderBy("id").collect()
    assert [r["id"] for r in back] == [0, 1, 2]
