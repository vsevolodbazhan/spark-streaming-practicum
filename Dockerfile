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

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s /usr/lib/jvm/java-17-openjdk-* /usr/lib/jvm/java-17-openjdk

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk

RUN uv sync --only-group consumer

RUN SPARK_JARS=$(uv run python -c "import pyspark; print(pyspark.__path__[0])")/jars && \
    curl -o $SPARK_JARS/hadoop-aws-3.4.1.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar && \
    curl -o $SPARK_JARS/aws-java-sdk-bundle-1.12.367.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar && \
    curl -o $SPARK_JARS/bundle-2.29.51.jar https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.29.51/bundle-2.29.51.jar

COPY ./src/consumer.py ./src/consumer.py

ENTRYPOINT ["uv", "run", "python", "-m", "src.consumer"]


FROM duckdb/duckdb:1.4.0 AS duckdb

COPY src/duckdb/.duckdbrc ./

ENTRYPOINT ["duckdb", "-init", ".duckdbrc"]
