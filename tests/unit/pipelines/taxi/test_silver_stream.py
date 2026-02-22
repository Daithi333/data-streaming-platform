import importlib
import types


class FakeExpr:
    def __init__(self, repr_):
        self.repr_ = repr_

    def alias(self, name):
        return FakeExpr(f"{self.repr_}.alias({name})")

    def isNotNull(self):
        return FakeExpr(f"{self.repr_}.isNotNull()")

    def isNull(self):
        return FakeExpr(f"{self.repr_}.isNull()")

    def __eq__(self, other):
        return FakeExpr(f"({self.repr_} == {other})")

    def __le__(self, other):
        return FakeExpr(f"({self.repr_} <= {getattr(other, 'repr_', other)})")

    def __ge__(self, other):
        return FakeExpr(f"({self.repr_} >= {other})")

    def __gt__(self, other):
        return FakeExpr(f"({self.repr_} > {other})")

    def __or__(self, other):
        return FakeExpr(f"({self.repr_} | {getattr(other, 'repr_', other)})")


class FakeF:
    @staticmethod
    def col(name):
        return FakeExpr(f"col({name})")

    @staticmethod
    def current_timestamp():
        return FakeExpr("current_timestamp()")


class FakeReader:
    def __init__(self):
        self.calls = []

    def format(self, fmt):
        self.calls.append(("format", fmt))
        return self

    def load(self, path):
        self.calls.append(("load", path))
        return FakeDataFrame(name="bronze")


class FakeReadStream:
    def __init__(self):
        self.reader = FakeReader()

    def format(self, fmt):
        return self.reader.format(fmt)


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
    def __init__(self, name="df"):
        self.name = name
        self.calls = []

    def filter(self, expr):
        self.calls.append(("filter", getattr(expr, "repr_", str(expr))))
        return self

    def withWatermark(self, col, threshold):
        self.calls.append(("withWatermark", col, threshold))
        return self

    def dropDuplicates(self, cols):
        self.calls.append(("dropDuplicates", cols))
        return self

    def withColumn(self, name, expr):
        self.calls.append(("withColumn", name))
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


class FakeSettings:
    kafka_topic = "taxi_trips"
    kafka_brokers = "kafka:9092"
    trigger_interval = "30 seconds"
    app_log_level = "INFO"
    spark_log_level = "WARN"
    log_query_progress_seconds = None

    def bronze_path(self, name):
        return f"/opt/dsp/data/bronze/{name}"

    def silver_path(self, name):
        return f"/opt/dsp/data/silver/{name}"

    def silver_checkpoint(self, name):
        return f"/opt/dsp/checkpoints/silver/{name}"


def import_silver_stream_with_patches(monkeypatch):
    fake_settings_mod = types.SimpleNamespace(get_settings=lambda: FakeSettings())
    monkeypatch.setitem(importlib.sys.modules, "dsp.config.settings", fake_settings_mod)

    fake_logs_mod = types.SimpleNamespace(configure_logging=lambda level: None)
    monkeypatch.setitem(importlib.sys.modules, "dsp.config.logs", fake_logs_mod)

    module_name = "dsp.pipelines.taxi.silver_stream"
    if module_name in importlib.sys.modules:
        del importlib.sys.modules[module_name]

    mod = importlib.import_module(module_name)
    monkeypatch.setattr(mod, "F", FakeF)

    return mod


def test_main_reads_bronze_delta_stream(monkeypatch):
    silver_stream = import_silver_stream_with_patches(monkeypatch)

    fake_logger = FakeLogger()
    monkeypatch.setattr(silver_stream, "get_logger", lambda: fake_logger)

    fake_spark = FakeSparkSession()
    fake_builder = FakeSparkBuilder(fake_spark)
    monkeypatch.setattr(silver_stream.SparkSession, "builder", fake_builder)

    called = {"delta_probe": 0, "start": 0, "monitor": 0}

    def fake_assert_delta_available(spark):
        assert spark is fake_spark
        called["delta_probe"] += 1

    monkeypatch.setattr(
        silver_stream, "assert_delta_available", fake_assert_delta_available
    )

    def fake_start_delta_stream(df, sink, cfg):
        called["start"] += 1
        assert sink.path.endswith("/opt/dsp/data/silver/taxi_trips")
        assert sink.checkpoint.endswith("/opt/dsp/checkpoints/silver/taxi_trips")
        assert cfg.query_name == "silver_taxi_trips_to_delta"
        return object()

    def fake_run_and_monitor(query, cfg, extra_log_fields=None):
        called["monitor"] += 1
        assert extra_log_fields == {"source": "bronze_taxi_trips"}

    monkeypatch.setattr(silver_stream, "start_delta_stream", fake_start_delta_stream)
    monkeypatch.setattr(silver_stream, "run_and_monitor", fake_run_and_monitor)

    silver_stream.main()

    assert called["delta_probe"] == 1
    assert called["start"] == 1
    assert called["monitor"] == 1

    assert fake_builder.app_name == "dsp_taxi_silver_stream"
    assert fake_spark.sparkContext.level == FakeSettings.spark_log_level

    reader_calls = fake_spark.readStream.reader.calls
    assert ("format", "delta") in reader_calls
    assert ("load", FakeSettings().bronze_path("taxi_trips")) in reader_calls


def test_main_applies_validation_filters(monkeypatch):
    silver_stream = import_silver_stream_with_patches(monkeypatch)

    fake_logger = FakeLogger()
    monkeypatch.setattr(silver_stream, "get_logger", lambda: fake_logger)

    fake_spark = FakeSparkSession()
    fake_builder = FakeSparkBuilder(fake_spark)
    monkeypatch.setattr(silver_stream.SparkSession, "builder", fake_builder)

    monkeypatch.setattr(silver_stream, "assert_delta_available", lambda spark: None)

    captured_df = None

    def fake_start_delta_stream(df, sink, cfg):
        nonlocal captured_df
        captured_df = df
        return object()

    monkeypatch.setattr(silver_stream, "start_delta_stream", fake_start_delta_stream)
    monkeypatch.setattr(
        silver_stream, "run_and_monitor", lambda query, cfg, extra_log_fields=None: None
    )

    silver_stream.main()

    assert captured_df is not None
    filter_calls = [c for c in captured_df.calls if c[0] == "filter"]
    assert len(filter_calls) >= 7

    watermark_calls = [c for c in captured_df.calls if c[0] == "withWatermark"]
    assert len(watermark_calls) == 1
    assert watermark_calls[0][1] == "event_ts_parsed"

    dedup_calls = [c for c in captured_df.calls if c[0] == "dropDuplicates"]
    assert len(dedup_calls) == 1
    assert dedup_calls[0][1] == ["trip_id"]


def test_main_logs_silver_starting(monkeypatch):
    silver_stream = import_silver_stream_with_patches(monkeypatch)

    fake_logger = FakeLogger()
    monkeypatch.setattr(silver_stream, "get_logger", lambda: fake_logger)

    fake_spark = FakeSparkSession()
    fake_builder = FakeSparkBuilder(fake_spark)
    monkeypatch.setattr(silver_stream.SparkSession, "builder", fake_builder)

    monkeypatch.setattr(silver_stream, "assert_delta_available", lambda spark: None)
    monkeypatch.setattr(
        silver_stream, "start_delta_stream", lambda df, sink, cfg: object()
    )
    monkeypatch.setattr(
        silver_stream, "run_and_monitor", lambda query, cfg, extra_log_fields=None: None
    )

    silver_stream.main()

    events = [(lvl, ev) for (lvl, ev, fields) in fake_logger.events]
    assert ("info", "silver_starting") in events
