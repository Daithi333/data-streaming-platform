import importlib
import types


# ----------------------------
# Fakes
# ----------------------------


class FakeExpr:
    def __init__(self, repr_):
        self.repr_ = repr_

    def alias(self, name):
        return FakeExpr(f"{self.repr_}.alias({name})")

    def cast(self, t):
        return FakeExpr(f"{self.repr_}.cast({t})")

    def isNotNull(self):
        return FakeExpr(f"{self.repr_}.isNotNull()")

    def __and__(self, other):
        return FakeExpr(f"({self.repr_} & {getattr(other, 'repr_', other)})")


class FakeF:
    @staticmethod
    def col(name):
        return FakeExpr(f"col({name})")

    @staticmethod
    def lit(v):
        return FakeExpr(f"lit({v})")

    @staticmethod
    def from_json(expr, schema):
        return FakeExpr(f"from_json({getattr(expr, 'repr_', expr)}, schema)")

    @staticmethod
    def current_timestamp():
        return FakeExpr("current_timestamp()")

    @staticmethod
    def sha2(expr, bits):
        return FakeExpr(f"sha2({getattr(expr, 'repr_', expr)},{bits})")

    @staticmethod
    def concat_ws(sep, *exprs):
        parts = ",".join(getattr(e, "repr_", str(e)) for e in exprs)
        return FakeExpr(f"concat_ws({sep},{parts})")

    @staticmethod
    def coalesce(expr, default):
        return FakeExpr(
            f"coalesce({getattr(expr, 'repr_', expr)},{getattr(default, 'repr_', default)})"
        )

    @staticmethod
    def to_timestamp(colname):
        return FakeExpr(f"to_timestamp({colname})")

    @staticmethod
    def try_to_timestamp(colname):
        return FakeExpr(f"try_to_timestamp({colname})")


class FakeWriter:
    def __init__(self):
        self.calls = []

    def format(self, fmt):
        self.calls.append(("format", fmt))
        return self

    def option(self, key, value):
        self.calls.append(("option", key, value))
        return self

    def load(self):
        self.calls.append(("load",))
        return FakeDataFrame(name="raw")


class FakeReadStream:
    def __init__(self):
        self.writer = FakeWriter()

    def format(self, fmt):
        return self.writer.format(fmt)


class FakeSparkContext:
    def __init__(self):
        self.level = None

    def setLogLevel(self, level):
        self.level = level


class FakeSparkSession:
    def __init__(self):
        self.sparkContext = FakeSparkContext()
        self.readStream = FakeReadStream()


class FakeSparkBuilder:
    def __init__(self, spark):
        self.spark = spark
        self.app_name = None

    def appName(self, name):
        self.app_name = name
        return self

    def getOrCreate(self):
        return self.spark


class FakeDataFrame:
    """
    Mimics DataFrame chaining. We record calls to validate the DAG-building logic.
    """

    def __init__(self, name="df"):
        self.name = name
        self.calls = []

    def select(self, *cols):
        self.calls.append(("select", len(cols)))
        return self

    def withColumn(self, name, expr):
        self.calls.append(("withColumn", name))
        return self

    def drop(self, *cols):
        self.calls.append(("drop", cols))
        return self


class FakeLogger:
    def __init__(self):
        self.bound = {}
        self.events = []

    def bind(self, **kwargs):
        self.bound.update(kwargs)
        return self

    def info(self, event, **fields):
        self.events.append(("info", event, fields))

    def critical(self, event, **fields):
        self.events.append(("critical", event, fields))


class FakeSettings:
    kafka_topic = "taxi_trips"
    kafka_brokers = "kafka:9092"
    kafka_starting_offsets = "earliest"
    trigger_interval = "30 seconds"
    app_log_level = "INFO"
    spark_log_level = "WARN"
    log_query_progress_seconds = None

    def bronze_path(self, name):
        return f"/opt/dsp/data/bronze/{name}"

    def bronze_checkpoint(self, name):
        return f"/opt/dsp/checkpoints/bronze/{name}"


# ----------------------------
# Helper: import module with patched get_settings
# ----------------------------


def import_bronze_stream_with_patches(monkeypatch):
    fake_settings_mod = types.SimpleNamespace(get_settings=lambda: FakeSettings())
    monkeypatch.setitem(importlib.sys.modules, "dsp.config.settings", fake_settings_mod)

    fake_logs_mod = types.SimpleNamespace(configure_logging=lambda level: None)
    monkeypatch.setitem(importlib.sys.modules, "dsp.config.logs", fake_logs_mod)

    module_name = "dsp.pipelines.taxi.bronze_stream"
    if module_name in importlib.sys.modules:
        del importlib.sys.modules[module_name]

    mod = importlib.import_module(module_name)

    # Patch pyspark functions used in the module
    monkeypatch.setattr(mod, "F", FakeF)

    return mod


# ----------------------------
# Tests
# ----------------------------


def test_main_builds_kafka_readstream_and_calls_runner(monkeypatch):
    bronze_stream = import_bronze_stream_with_patches(monkeypatch)

    # Patch structlog get_logger
    fake_logger = FakeLogger()
    monkeypatch.setattr(bronze_stream, "get_logger", lambda: fake_logger)

    # Patch SparkSession.builder...
    fake_spark = FakeSparkSession()
    fake_builder = FakeSparkBuilder(fake_spark)
    monkeypatch.setattr(bronze_stream.SparkSession, "builder", fake_builder)

    # Patch assert_delta_available (don’t require delta jars)
    called = {"delta_probe": 0, "start": 0, "monitor": 0}

    def fake_assert_delta_available(spark):
        assert spark is fake_spark
        called["delta_probe"] += 1

    monkeypatch.setattr(
        bronze_stream, "assert_delta_available", fake_assert_delta_available
    )

    # Patch start_delta_stream/run_and_monitor to observe inputs
    def fake_start_delta_stream(df, sink, cfg):
        called["start"] += 1
        # sink paths should match FakeSettings
        assert sink.path.endswith("/opt/dsp/data/bronze/taxi_trips")
        assert sink.checkpoint.endswith("/opt/dsp/checkpoints/bronze/taxi_trips")
        assert cfg.query_name == "bronze_taxi_trips_to_delta"
        return object()  # fake query

    def fake_run_and_monitor(query, cfg, extra_log_fields=None):
        called["monitor"] += 1
        assert extra_log_fields == {"topic": FakeSettings.kafka_topic}

    monkeypatch.setattr(bronze_stream, "start_delta_stream", fake_start_delta_stream)
    monkeypatch.setattr(bronze_stream, "run_and_monitor", fake_run_and_monitor)

    # Patch the DataFrame produced by kafka load to be our FakeDataFrame
    # We do this by patching FakeWriter.load() return above. It returns FakeDataFrame("raw").
    # But bronze_stream then chains transformations. We want those methods available (they are).
    bronze_stream.main()

    # Assert we did the delta probe and started monitoring
    assert called["delta_probe"] == 1
    assert called["start"] == 1
    assert called["monitor"] == 1

    # Validate Spark app name and log level were set
    assert fake_builder.app_name == "dsp_taxi_bronze_stream"
    assert fake_spark.sparkContext.level == FakeSettings.spark_log_level

    # Validate the kafka reader options were set correctly
    kafka_calls = fake_spark.readStream.writer.calls
    assert ("format", "kafka") in kafka_calls
    assert (
        "option",
        "kafka.bootstrap.servers",
        FakeSettings.kafka_brokers,
    ) in kafka_calls
    assert ("option", "subscribe", FakeSettings.kafka_topic) in kafka_calls
    assert (
        "option",
        "startingOffsets",
        FakeSettings.kafka_starting_offsets,
    ) in kafka_calls
    assert ("load",) in kafka_calls


def test_main_logs_bronze_starting(monkeypatch):
    bronze_stream = import_bronze_stream_with_patches(monkeypatch)

    fake_logger = FakeLogger()
    monkeypatch.setattr(bronze_stream, "get_logger", lambda: fake_logger)

    fake_spark = FakeSparkSession()
    fake_builder = FakeSparkBuilder(fake_spark)
    monkeypatch.setattr(bronze_stream.SparkSession, "builder", fake_builder)

    monkeypatch.setattr(bronze_stream, "assert_delta_available", lambda spark: None)
    monkeypatch.setattr(
        bronze_stream, "start_delta_stream", lambda df, sink, cfg: object()
    )
    monkeypatch.setattr(
        bronze_stream, "run_and_monitor", lambda query, cfg, extra_log_fields=None: None
    )

    bronze_stream.main()

    events = [(lvl, ev) for (lvl, ev, fields) in fake_logger.events]
    assert ("info", "bronze_starting") in events
