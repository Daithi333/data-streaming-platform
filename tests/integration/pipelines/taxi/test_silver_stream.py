import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
    BooleanType,
)
from datetime import datetime


@pytest.mark.spark
def test_silver_filters_parse_ok_false(spark, tmp_path):
    bronze_path = str(tmp_path / "bronze")

    schema = StructType(
        [
            StructField("trip_id", StringType(), False),
            StructField("parse_ok", BooleanType(), False),
            StructField("event_ts_parsed", TimestampType(), True),
            StructField("total_amount", DoubleType(), True),
        ]
    )

    data = [
        ("trip1", True, datetime(2024, 1, 1, 10, 0, 0), 10.0),
        ("trip2", False, datetime(2024, 1, 1, 11, 0, 0), 20.0),
        ("trip3", True, datetime(2024, 1, 1, 12, 0, 0), 30.0),
    ]

    spark.createDataFrame(data, schema).write.format("delta").mode("overwrite").save(
        bronze_path
    )

    bronze = spark.read.format("delta").load(bronze_path)
    filtered = bronze.filter(F.col("parse_ok") == F.lit(True))

    result = filtered.select("trip_id").collect()
    trip_ids = [r["trip_id"] for r in result]

    assert "trip1" in trip_ids
    assert "trip3" in trip_ids
    assert "trip2" not in trip_ids


@pytest.mark.spark
def test_silver_filters_null_event_ts(spark, tmp_path):
    bronze_path = str(tmp_path / "bronze")

    schema = StructType(
        [
            StructField("trip_id", StringType(), False),
            StructField("parse_ok", BooleanType(), False),
            StructField("event_ts_parsed", TimestampType(), True),
        ]
    )

    data = [
        ("trip1", True, datetime(2024, 1, 1, 10, 0, 0)),
        ("trip2", True, None),
        ("trip3", True, datetime(2024, 1, 1, 12, 0, 0)),
    ]

    spark.createDataFrame(data, schema).write.format("delta").mode("overwrite").save(
        bronze_path
    )

    bronze = spark.read.format("delta").load(bronze_path)
    filtered = bronze.filter(F.col("parse_ok") == F.lit(True)).filter(
        F.col("event_ts_parsed").isNotNull()
    )

    result = filtered.select("trip_id").collect()
    trip_ids = [r["trip_id"] for r in result]

    assert "trip1" in trip_ids
    assert "trip3" in trip_ids
    assert "trip2" not in trip_ids


@pytest.mark.spark
def test_silver_validates_timestamp_order(spark, tmp_path):
    bronze_path = str(tmp_path / "bronze")

    schema = StructType(
        [
            StructField("trip_id", StringType(), False),
            StructField("parse_ok", BooleanType(), False),
            StructField("event_ts_parsed", TimestampType(), False),
            StructField("pickup_ts_parsed", TimestampType(), True),
            StructField("dropoff_ts_parsed", TimestampType(), True),
        ]
    )

    data = [
        (
            "trip1",
            True,
            datetime(2024, 1, 1, 10, 0, 0),
            datetime(2024, 1, 1, 10, 10, 0),
            datetime(2024, 1, 1, 10, 20, 0),
        ),
        (
            "trip2",
            True,
            datetime(2024, 1, 1, 11, 0, 0),
            datetime(2024, 1, 1, 11, 20, 0),
            datetime(2024, 1, 1, 11, 10, 0),
        ),
        ("trip3", True, datetime(2024, 1, 1, 12, 0, 0), None, None),
    ]

    spark.createDataFrame(data, schema).write.format("delta").mode("overwrite").save(
        bronze_path
    )

    bronze = spark.read.format("delta").load(bronze_path)
    filtered = (
        bronze.filter(F.col("parse_ok") == F.lit(True))
        .filter(F.col("event_ts_parsed").isNotNull())
        .filter(
            (F.col("pickup_ts_parsed").isNull())
            | (F.col("dropoff_ts_parsed").isNull())
            | (F.col("pickup_ts_parsed") <= F.col("dropoff_ts_parsed"))
        )
    )

    result = filtered.select("trip_id").orderBy("trip_id").collect()
    trip_ids = [r["trip_id"] for r in result]

    assert trip_ids == ["trip1", "trip3"]


@pytest.mark.spark
def test_silver_validates_amounts_non_negative(spark, tmp_path):
    bronze_path = str(tmp_path / "bronze")

    schema = StructType(
        [
            StructField("trip_id", StringType(), False),
            StructField("parse_ok", BooleanType(), False),
            StructField("event_ts_parsed", TimestampType(), False),
            StructField("fare_amount", DoubleType(), True),
            StructField("tip_amount", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
        ]
    )

    data = [
        ("trip1", True, datetime(2024, 1, 1, 10, 0, 0), 10.0, 2.0, 12.0),
        ("trip2", True, datetime(2024, 1, 1, 11, 0, 0), -5.0, 2.0, 12.0),
        ("trip3", True, datetime(2024, 1, 1, 12, 0, 0), 10.0, -1.0, 12.0),
        ("trip4", True, datetime(2024, 1, 1, 13, 0, 0), 10.0, 2.0, -12.0),
        ("trip5", True, datetime(2024, 1, 1, 14, 0, 0), None, None, None),
    ]

    spark.createDataFrame(data, schema).write.format("delta").mode("overwrite").save(
        bronze_path
    )

    bronze = spark.read.format("delta").load(bronze_path)
    filtered = (
        bronze.filter(F.col("parse_ok") == F.lit(True))
        .filter(F.col("event_ts_parsed").isNotNull())
        .filter((F.col("fare_amount").isNull()) | (F.col("fare_amount") >= 0))
        .filter((F.col("tip_amount").isNull()) | (F.col("tip_amount") >= 0))
        .filter((F.col("total_amount").isNull()) | (F.col("total_amount") >= 0))
    )

    result = filtered.select("trip_id").orderBy("trip_id").collect()
    trip_ids = [r["trip_id"] for r in result]

    assert trip_ids == ["trip1", "trip5"]


@pytest.mark.spark
def test_silver_validates_trip_distance_and_passenger_count(spark, tmp_path):
    bronze_path = str(tmp_path / "bronze")

    schema = StructType(
        [
            StructField("trip_id", StringType(), False),
            StructField("parse_ok", BooleanType(), False),
            StructField("event_ts_parsed", TimestampType(), False),
            StructField("trip_distance", DoubleType(), True),
            StructField("passenger_count", IntegerType(), True),
        ]
    )

    data = [
        ("trip1", True, datetime(2024, 1, 1, 10, 0, 0), 5.0, 2),
        ("trip2", True, datetime(2024, 1, 1, 11, 0, 0), -1.0, 2),
        ("trip3", True, datetime(2024, 1, 1, 12, 0, 0), 5.0, 0),
        ("trip4", True, datetime(2024, 1, 1, 13, 0, 0), None, None),
    ]

    spark.createDataFrame(data, schema).write.format("delta").mode("overwrite").save(
        bronze_path
    )

    bronze = spark.read.format("delta").load(bronze_path)
    filtered = (
        bronze.filter(F.col("parse_ok") == F.lit(True))
        .filter(F.col("event_ts_parsed").isNotNull())
        .filter((F.col("trip_distance").isNull()) | (F.col("trip_distance") >= 0))
        .filter((F.col("passenger_count").isNull()) | (F.col("passenger_count") > 0))
    )

    result = filtered.select("trip_id").orderBy("trip_id").collect()
    trip_ids = [r["trip_id"] for r in result]

    assert trip_ids == ["trip1", "trip4"]


@pytest.mark.spark
def test_silver_deduplication_by_trip_id(spark, tmp_path):
    bronze_path = str(tmp_path / "bronze")

    schema = StructType(
        [
            StructField("trip_id", StringType(), False),
            StructField("parse_ok", BooleanType(), False),
            StructField("event_ts_parsed", TimestampType(), False),
            StructField("total_amount", DoubleType(), True),
        ]
    )

    data = [
        ("trip1", True, datetime(2024, 1, 1, 10, 0, 0), 10.0),
        ("trip1", True, datetime(2024, 1, 1, 10, 1, 0), 10.0),
        ("trip2", True, datetime(2024, 1, 1, 11, 0, 0), 20.0),
    ]

    spark.createDataFrame(data, schema).write.format("delta").mode("overwrite").save(
        bronze_path
    )

    bronze = spark.read.format("delta").load(bronze_path)
    deduplicated = (
        bronze.filter(F.col("parse_ok") == F.lit(True))
        .filter(F.col("event_ts_parsed").isNotNull())
        .dropDuplicates(["trip_id"])
    )

    result = deduplicated.select("trip_id").orderBy("trip_id").collect()
    trip_ids = [r["trip_id"] for r in result]

    assert trip_ids == ["trip1", "trip2"]
    assert len(trip_ids) == 2
