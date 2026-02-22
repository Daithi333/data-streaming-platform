FROM apache/spark:4.0.2-python3 AS spark

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    bash curl ca-certificates tini \
 && rm -rf /var/lib/apt/lists/*

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

RUN pip install --no-cache-dir uv

WORKDIR /opt/dsp

COPY pyproject.toml uv.lock /opt/dsp/

RUN uv sync --frozen --no-dev

COPY docker/spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# ---- test image variant ----
FROM spark AS spark-test

RUN uv sync --frozen
