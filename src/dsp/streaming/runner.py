import time
from dataclasses import dataclass
from typing import Optional

import structlog
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery


class StreamFailed(RuntimeError):
    pass


class StreamStoppedUnexpectedly(RuntimeError):
    pass


@dataclass(frozen=True)
class DeltaSink:
    path: str
    checkpoint: str
    output_mode: str = "append"


@dataclass(frozen=True)
class StreamRunConfig:
    app_name: str
    query_name: str
    trigger_processing_time: Optional[str] = None
    log_progress_seconds: Optional[int] = None  # None disables query logging entirely
    spark_log_level: str = "WARN"
    fail_fast_delta_probe: bool = True


def assert_delta_available(
    spark: SparkSession, probe_path: str = "/tmp/_delta_probe"
) -> None:
    """Fail fast if Delta isn't on the classpath."""
    try:
        spark.range(1).write.format("delta").mode("overwrite").save(probe_path)
    except Exception as e:
        raise RuntimeError(
            "Delta is not available at runtime. Ensure spark-submit includes "
            "io.delta:delta-spark_2.12:<version> and spark.sql.extensions / catalog configs."
        ) from e


def start_delta_stream(
    df: DataFrame,
    sink: DeltaSink,
    cfg: StreamRunConfig,
) -> StreamingQuery:
    """Start a single Structured Streaming query writing df -> Delta."""
    writer = (
        df.writeStream.format("delta")
        .queryName(cfg.query_name)
        .outputMode(sink.output_mode)
        .option("checkpointLocation", sink.checkpoint)
        .option("path", sink.path)
    )

    if cfg.trigger_processing_time:
        writer = writer.trigger(processingTime=cfg.trigger_processing_time)

    return writer.start()


def run_and_monitor(
    query: StreamingQuery, cfg: StreamRunConfig, extra_log_fields: Optional[dict] = None
) -> None:
    """Run query until termination; optionally log progress; fail loudly on exception."""
    log = structlog.get_logger().bind(
        app=cfg.app_name,
        query=cfg.query_name,
        **(extra_log_fields or {}),
    )

    log.info("stream_started")

    if not cfg.log_progress_seconds:
        # block until termination, then raise if failed.
        query.awaitTermination()
        exc = query.exception()
        if exc is not None:
            log.critical("stream_failed", error=str(exc))
            raise StreamFailed(str(exc))
        log.info("stream_terminated_cleanly")
        return

    # If progress logging enabled: emit one log per completed batch.
    last_batch_id = None
    while query.isActive:
        exc = query.exception()
        if exc is not None:
            log.critical("stream_failed", error=str(exc))
            raise StreamFailed(str(exc))

        lp = query.lastProgress
        if lp:
            batch_id = lp.get("batchId")
            if batch_id != last_batch_id:
                last_batch_id = batch_id
                log.info(
                    "stream_batch_completed",
                    batch_id=batch_id,
                    num_input_rows=lp.get("numInputRows"),
                    input_rps=lp.get("inputRowsPerSecond"),
                    processed_rps=lp.get("processedRowsPerSecond"),
                    duration_ms=lp.get("durationMs"),
                )

        time.sleep(cfg.log_progress_seconds)

    # Query stopped; check exception one last time.
    exc = query.exception()
    if exc is not None:
        log.critical("stream_failed", error=str(exc))
        raise StreamFailed(str(exc))

    log.critical("stream_stopped_unexpectedly")
    raise StreamStoppedUnexpectedly("Query stopped without exception")
