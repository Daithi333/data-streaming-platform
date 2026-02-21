from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

from dsp.config.settings import get_settings

s = get_settings()

TOPIC = s.kafka_topic
BROKERS = s.kafka_brokers

BRONZE_PATH = s.bronze_path("taxi_trips")
BRONZE_CHECKPOINT = s.bronze_checkpoint("taxi_trips")
TRIGGER = s.trigger_interval


# Schema for the JSON we produced
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
    spark = SparkSession.builder.appName("dsp_taxi_bronze_stream").getOrCreate()

    spark.sparkContext.setLogLevel(s.spark_log_level)

    # Read Kafka
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BROKERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", s.kafka_starting_offsets)
        .load()
    )

    (
        raw.selectExpr(
            "CAST(key AS STRING)",
            "CAST(value AS STRING)",
            "topic",
            "partition",
            "offset",
        )
        .writeStream.format("console")
        .outputMode("append")
        .start()
        .awaitTermination()
    )

    # Parse payload
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
        .select(
            "*",
            F.col("payload.*"),
        )
        .drop("payload")
    )

    # Add ingest metadata + deterministic trip_id for downstream dedupe/upserts
    # trip_id here is stable for identical business fields (useful for idempotency)
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
        .withColumn("event_ts_parsed", F.to_timestamp("event_ts"))
        .withColumn("pickup_ts_parsed", F.to_timestamp("pickup_ts"))
        .withColumn("dropoff_ts_parsed", F.to_timestamp("dropoff_ts"))
    )

    # Write to Delta (Bronze is typically append-only)
    query = (
        enriched.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", BRONZE_CHECKPOINT)
        .option("path", BRONZE_PATH)
        .trigger(processingTime=s.trigger_interval)
        .start()
    )

    print(f"[dsp] Streaming bronze -> {BRONZE_PATH}")
    print(f"[dsp] Checkpoint -> {BRONZE_CHECKPOINT}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
