import pytest


@pytest.mark.spark
@pytest.mark.delta
def test_delta_write_read_smoke(spark, tmp_path):
    """
    Verifies Delta is available on the Spark classpath.
    Fails fast if Delta jars/config are missing.
    """
    path = str(tmp_path / "delta_smoke")

    try:
        spark.range(0, 3).write.format("delta").mode("overwrite").save(path)
    except Exception as e:
        pytest.fail(f"Delta write failed — likely missing delta-spark jars/config: {e}")

    rows = spark.read.format("delta").load(path).orderBy("id").collect()
    assert [r["id"] for r in rows] == [0, 1, 2]
