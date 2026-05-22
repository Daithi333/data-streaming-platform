FROM apache/spark:4.0.2-python3 AS spark

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    bash curl ca-certificates tini \
 && rm -rf /var/lib/apt/lists/*

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

WORKDIR /opt/dsp

COPY docker/spark/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
 && rm /tmp/requirements.txt

COPY docker/spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# ---- test image variant ----
FROM spark AS spark-test

COPY docker/spark/requirements-dev.txt /tmp/requirements-dev.txt
RUN pip install --no-cache-dir -r /tmp/requirements-dev.txt \
 && rm /tmp/requirements-dev.txt
