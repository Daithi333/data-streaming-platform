from pyspark.sql import SparkSession, functions as F
from structlog import get_logger

from dsp.config.logs import configure_logging
from dsp.config.settings import get_settings
from dsp.streaming.runner import (
    assert_delta_available,
    DeltaSink,
    StreamRunConfig,
    start_delta_stream,
    run_and_monitor,
)


s = get_settings()

BRONZE_PATH = s.bronze_path("taxi_trips")
SILVER_PATH = s.silver_path("taxi_trips")
SILVER_CHECKPOINT = s.silver_checkpoint("taxi_trips")


def main() -> None:
    configure_logging(s.app_log_level)
    log = get_logger().bind(app="silver")

    spark = SparkSession.builder.appName("dsp_taxi_silver_stream").getOrCreate()
    spark.sparkContext.setLogLevel(s.spark_log_level)

    assert_delta_available(spark)

    log.info(
        "silver_starting",
        bronze_path=BRONZE_PATH,
        silver_path=SILVER_PATH,
        checkpoint_path=SILVER_CHECKPOINT,
        log_progress_seconds=s.log_query_progress_seconds,
    )

    bronze = spark.readStream.format("delta").load(BRONZE_PATH)

    validated = (
        bronze.filter(F.col("parse_ok") == F.lit(True))
        .filter(F.col("event_ts_parsed").isNotNull())
        .filter(
            (F.col("pickup_ts_parsed").isNull())
            | (F.col("dropoff_ts_parsed").isNull())
            | (F.col("pickup_ts_parsed") <= F.col("dropoff_ts_parsed"))
        )
        .filter((F.col("fare_amount").isNull()) | (F.col("fare_amount") >= 0))
        .filter((F.col("tip_amount").isNull()) | (F.col("tip_amount") >= 0))
        .filter((F.col("total_amount").isNull()) | (F.col("total_amount") >= 0))
        .filter((F.col("trip_distance").isNull()) | (F.col("trip_distance") >= 0))
        .filter((F.col("passenger_count").isNull()) | (F.col("passenger_count") > 0))
    )

    deduplicated = validated.withWatermark("event_ts_parsed", "1 hour").dropDuplicates(
        ["trip_id"]
    )

    cleaned = deduplicated.withColumn("silver_ingest_ts", F.current_timestamp())

    sink = DeltaSink(path=SILVER_PATH, checkpoint=SILVER_CHECKPOINT)

    cfg = StreamRunConfig(
        app_name="dsp_taxi_silver_stream",
        query_name="silver_taxi_trips_to_delta",
        trigger_processing_time=s.trigger_interval,
        log_progress_seconds=s.log_query_progress_seconds,
        spark_log_level=s.spark_log_level,
        fail_fast_delta_probe=True,
    )

    query = start_delta_stream(cleaned, sink, cfg)
    run_and_monitor(query, cfg, extra_log_fields={"source": "bronze_taxi_trips"})


if __name__ == "__main__":
    main()
