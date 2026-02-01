# Based on the Python image with uv pre-installed.
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS base

ENV UV_LOCKED=1
ENV UV_NO_DEV=1
ENV UV_NO_CACHE=1

COPY pyproject.toml uv.lock /app/
WORKDIR /app


FROM base AS producer

RUN uv sync --only-group producer
COPY ./src/producer/ ./src/producer

ENTRYPOINT ["uv", "run", "python", "-m", "src.producer"]


FROM base AS consumer

# Install Java (required by PySpark) and curl (for downloading JARs).
# The symlink provides an architecture-agnostic JAVA_HOME path.
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s /usr/lib/jvm/java-17-openjdk-* /usr/lib/jvm/java-17-openjdk

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk

RUN uv sync --only-group consumer

# Download Hadoop AWS JARs for S3 support. These must match Hadoop version
# bundled with PySpark (3.4.x for PySpark 4.x). Installing at build time avoids
# download overhead at startup.
# - hadoop-aws: S3A filesystem implementation
# - aws-java-sdk-bundle: AWS SDK v1 (legacy, still required by hadoop-aws)
# - bundle (awssdk): AWS SDK v2 (required by Hadoop 3.4+)
RUN SPARK_JARS=$(uv run python -c "import pyspark; print(pyspark.__path__[0])")/jars && \
    curl -o $SPARK_JARS/hadoop-aws-3.4.1.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar && \
    curl -o $SPARK_JARS/aws-java-sdk-bundle-1.12.367.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar && \
    curl -o $SPARK_JARS/bundle-2.29.51.jar https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.29.51/bundle-2.29.51.jar

COPY ./src/consumer.py ./src/consumer.py

ENTRYPOINT ["uv", "run", "python", "-m", "src.consumer"]


FROM duckdb/duckdb:1.4.0 AS duckdb

# .duckdbrc is executed on shell startup to configure S3 access and create
# convenience views for raw and bronze data.
COPY src/duckdb/.duckdbrc ./

ENTRYPOINT ["duckdb", "-init", ".duckdbrc"]
