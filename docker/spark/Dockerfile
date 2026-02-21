FROM apache/spark:3.5.1-python3

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    bash curl ca-certificates tini \
 && rm -rf /var/lib/apt/lists/*

# Ensure Spark is always on PATH
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# Install Python deps needed by jobs (driver + executors in local mode)
COPY docker/spark/requirements.spark.txt /tmp/requirements.spark.txt
RUN pip install --no-cache-dir -r /tmp/requirements.spark.txt

COPY docker/spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

WORKDIR /opt/dsp
