import pytest

from dsp.streaming import runner
from dsp.streaming.runner import StreamStoppedUnexpectedly, StreamFailed


# -------------------------
# Helpers / fakes
# -------------------------


class FakeLogger:
    def __init__(self):
        self.events = []  # list of tuples: (level, event, fields)

    def bind(self, **kwargs):
        # mimic structlog logger .bind returning a logger
        self.bound = kwargs
        return self

    def info(self, event, **fields):
        self.events.append(("info", event, fields))

    def critical(self, event, **fields):
        self.events.append(("critical", event, fields))


class FakeStructlog:
    def __init__(self, logger):
        self._logger = logger

    def get_logger(self):
        return self._logger


class FakeStreamingQuery:
    """
    A controllable fake for StreamingQuery.
    """

    def __init__(
        self,
        *,
        active_sequence=None,
        exception_sequence=None,
        progress_sequence=None,
        await_raises=None,
    ):
        # Sequences let us control changes across loop iterations.
        self._active_sequence = list(active_sequence or [False])
        self._exception_sequence = list(exception_sequence or [None])
        self._progress_sequence = list(progress_sequence or [None])
        self._await_raises = await_raises

        self.await_called = False

    @property
    def isActive(self):
        if len(self._active_sequence) > 1:
            return self._active_sequence.pop(0)
        return self._active_sequence[0]

    def exception(self):
        if len(self._exception_sequence) > 1:
            return self._exception_sequence.pop(0)
        return self._exception_sequence[0]

    @property
    def lastProgress(self):
        if len(self._progress_sequence) > 1:
            return self._progress_sequence.pop(0)
        return self._progress_sequence[0]

    def awaitTermination(self):
        self.await_called = True
        if self._await_raises:
            raise self._await_raises


# Writer chain fake for start_delta_stream
class FakeWriter:
    def __init__(self):
        self.calls = []
        self._trigger = None
        self._started = object()

    def format(self, fmt):
        self.calls.append(("format", fmt))
        return self

    def queryName(self, name):
        self.calls.append(("queryName", name))
        return self

    def outputMode(self, mode):
        self.calls.append(("outputMode", mode))
        return self

    def option(self, key, value):
        self.calls.append(("option", key, value))
        return self

    def trigger(self, **kwargs):
        self.calls.append(("trigger", kwargs))
        return self

    def start(self):
        self.calls.append(("start",))
        return self._started


class FakeDataFrame:
    def __init__(self, writer):
        self.writeStream = writer


# Spark/Delta probe fake
class _FakeDeltaWrite:
    def __init__(self, should_fail):
        self._should_fail = should_fail

    def format(self, fmt):
        return self

    def mode(self, mode):
        return self

    def save(self, path):
        if self._should_fail:
            raise Exception("delta missing")


class _FakeRangeDF:
    def __init__(self, should_fail):
        self.write = _FakeDeltaWrite(should_fail)


class FakeSpark:
    def __init__(self, should_fail=False):
        self._should_fail = should_fail

    def range(self, n):
        return _FakeRangeDF(self._should_fail)


# -------------------------
# assert_delta_available
# -------------------------


def test_assert_delta_available_ok():
    spark = FakeSpark(should_fail=False)
    runner.assert_delta_available(spark, probe_path="/tmp/probe")  # should not raise


def test_assert_delta_available_raises_runtime_error():
    spark = FakeSpark(should_fail=True)
    with pytest.raises(RuntimeError) as e:
        runner.assert_delta_available(spark, probe_path="/tmp/probe")
    assert "Delta is not available at runtime" in str(e.value)


# -------------------------
# start_delta_stream
# -------------------------


def test_start_delta_stream_sets_writer_options_without_trigger():
    writer = FakeWriter()
    df = FakeDataFrame(writer)

    sink = runner.DeltaSink(
        path="/data/bronze", checkpoint="/chk/bronze", output_mode="append"
    )
    cfg = runner.StreamRunConfig(
        app_name="app", query_name="q1", trigger_processing_time=None
    )

    q = runner.start_delta_stream(df, sink, cfg)

    assert q is writer._started
    # Validate key calls occurred
    assert ("format", "delta") in writer.calls
    assert ("queryName", "q1") in writer.calls
    assert ("outputMode", "append") in writer.calls
    assert ("option", "checkpointLocation", "/chk/bronze") in writer.calls
    assert ("option", "path", "/data/bronze") in writer.calls
    # Ensure trigger not called
    assert not any(c[0] == "trigger" for c in writer.calls)


def test_start_delta_stream_applies_trigger_when_configured():
    writer = FakeWriter()
    df = FakeDataFrame(writer)

    sink = runner.DeltaSink(
        path="/data/bronze", checkpoint="/chk/bronze", output_mode="append"
    )
    cfg = runner.StreamRunConfig(
        app_name="app", query_name="q1", trigger_processing_time="30 seconds"
    )

    runner.start_delta_stream(df, sink, cfg)

    assert ("trigger", {"processingTime": "30 seconds"}) in writer.calls


# -------------------------
# run_and_monitor (no progress logging)
# -------------------------


def test_run_and_monitor_no_progress_logging_clean_exit(monkeypatch):
    fake_logger = FakeLogger()
    monkeypatch.setattr(runner.structlog, "get_logger", lambda: fake_logger)

    q = FakeStreamingQuery(
        active_sequence=[False],
        exception_sequence=[None],
    )

    cfg = runner.StreamRunConfig(
        app_name="app", query_name="q1", log_progress_seconds=None
    )

    runner.run_and_monitor(q, cfg, extra_log_fields={"x": "y"})

    assert q.await_called is True
    # Verify expected logs
    events = [e[1] for e in fake_logger.events]
    assert "stream_started" in events
    assert "stream_terminated_cleanly" in events


def test_run_and_monitor_no_progress_logging_exits_on_exception(monkeypatch):
    fake_logger = FakeLogger()
    monkeypatch.setattr(runner.structlog, "get_logger", lambda: fake_logger)

    q = FakeStreamingQuery(
        active_sequence=[False],
        exception_sequence=[Exception("boom")],
    )

    cfg = runner.StreamRunConfig(
        app_name="app", query_name="q1", log_progress_seconds=None
    )

    with pytest.raises(StreamFailed) as exc_info:
        runner.run_and_monitor(q, cfg)

    assert exc_info.value.args[0] == "boom"
    assert q.await_called is True
    assert any(
        ev[0] == "critical" and ev[1] == "stream_failed" for ev in fake_logger.events
    )


# -------------------------
# run_and_monitor (progress logging enabled)
# -------------------------


def test_run_and_monitor_progress_logs_each_new_batch(monkeypatch):
    fake_logger = FakeLogger()
    monkeypatch.setattr(runner.structlog, "get_logger", lambda: fake_logger)

    # avoid real sleep
    monkeypatch.setattr(runner.time, "sleep", lambda _: None)

    q = FakeStreamingQuery(
        active_sequence=[True, True, True, False],
        exception_sequence=[None, None, None, None],
        progress_sequence=[
            {
                "batchId": 1,
                "numInputRows": 10,
                "inputRowsPerSecond": 5.0,
                "processedRowsPerSecond": 4.0,
                "durationMs": {"addBatch": 100},
            },
            {
                "batchId": 1,
                "numInputRows": 10,
                "inputRowsPerSecond": 5.0,
                "processedRowsPerSecond": 4.0,
                "durationMs": {"addBatch": 100},
            },  # duplicate
            {
                "batchId": 2,
                "numInputRows": 0,
                "inputRowsPerSecond": 0.0,
                "processedRowsPerSecond": 0.0,
                "durationMs": {"addBatch": 50},
            },
            None,
        ],
    )

    cfg = runner.StreamRunConfig(
        app_name="app", query_name="q1", log_progress_seconds=1
    )

    # When query stops without exception
    with pytest.raises(StreamStoppedUnexpectedly) as e:
        runner.run_and_monitor(q, cfg)

    assert e.value.args[0] == "Query stopped without exception"

    batch_logs = [ev for ev in fake_logger.events if ev[1] == "stream_batch_completed"]
    # Should log batch 1 once, batch 2 once => 2 logs
    assert len(batch_logs) == 2
    assert batch_logs[0][2]["batch_id"] == 1
    assert batch_logs[1][2]["batch_id"] == 2


def test_run_and_monitor_progress_exits_immediately_on_exception(monkeypatch):
    fake_logger = FakeLogger()
    monkeypatch.setattr(runner.structlog, "get_logger", lambda: fake_logger)
    monkeypatch.setattr(runner.time, "sleep", lambda _: None)

    q = FakeStreamingQuery(
        active_sequence=[True],
        exception_sequence=[Exception("boom")],
        progress_sequence=[None],
    )

    cfg = runner.StreamRunConfig(
        app_name="app", query_name="q1", log_progress_seconds=1
    )

    with pytest.raises(StreamFailed) as e:
        runner.run_and_monitor(q, cfg)

    assert e.value.args[0] == "boom"
    assert any(
        ev[0] == "critical" and ev[1] == "stream_failed" for ev in fake_logger.events
    )
