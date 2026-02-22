from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)
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

TOPIC = s.kafka_topic
BROKERS = s.kafka_brokers

BRONZE_PATH = s.bronze_path("taxi_trips")
BRONZE_CHECKPOINT = s.bronze_checkpoint("taxi_trips")
TRIGGER = s.trigger_interval

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("event_ts", StringType(), False),
        StructField("vendor_id", IntegerType(), True),
        StructField("pickup_ts", StringType(), True),
        StructField("dropoff_ts", StringType(), True),
        StructField("pu_location_id", IntegerType(), True),
        StructField("do_location_id", IntegerType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("ratecode_id", IntegerType(), True),
    ]
)


def main() -> None:
    configure_logging(s.app_log_level)
    log = get_logger().bind(app="bronze")

    spark = SparkSession.builder.appName("dsp_taxi_bronze_stream").getOrCreate()
    spark.sparkContext.setLogLevel(s.spark_log_level)

    assert_delta_available(spark)

    log.info(
        "bronze_starting",
        topic=TOPIC,
        brokers=BROKERS,
        bronze_path=BRONZE_PATH,
        checkpoint_path=BRONZE_CHECKPOINT,
        log_progress_seconds=s.log_query_progress_seconds,
    )

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BROKERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", s.kafka_starting_offsets)
        .load()
    )

    parsed = (
        raw.select(
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("raw_json"),
        )
        .withColumn("payload", F.from_json(F.col("raw_json"), EVENT_SCHEMA))
        .withColumn(
            "parse_ok",
            F.col("payload").isNotNull()
            & F.col("payload.event_id").isNotNull()
            & F.col("payload.event_ts").isNotNull(),
        )
        .select("*", F.col("payload.*"))
        .drop("payload")
    )

    enriched = (
        parsed.withColumn("ingest_ts", F.current_timestamp())
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

    sink = DeltaSink(path=BRONZE_PATH, checkpoint=BRONZE_CHECKPOINT)

    cfg = StreamRunConfig(
        app_name="dsp_taxi_bronze_stream",
        query_name="bronze_taxi_trips_to_delta",
        trigger_processing_time=s.trigger_interval,
        log_progress_seconds=s.log_query_progress_seconds,
        spark_log_level=s.spark_log_level,
        fail_fast_delta_probe=True,
    )

    query = start_delta_stream(enriched, sink, cfg)
    run_and_monitor(query, cfg, extra_log_fields={"topic": TOPIC})


if __name__ == "__main__":
    main()
