import pytest


@pytest.fixture(scope="session")
def spark():
    """
    Real Spark JVM session for integration tests.
    Run these tests in the Spark container so Java+Spark are available.
    """
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("dsp-integration-tests")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield spark
    spark.stop()
