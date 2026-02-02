# Spark Streaming Practicum

Practice project to explore and implement streaming data ingestion patterns using Apache Spark Structured Streaming. The project is structured into three components: **producer** is a generator of artificial data implemented in Python, **consumer** is a Spark Streaming–based data ingestion pipeline, **duckdb** is a DuckDB-powered reader of ingested data. Storage is handled by S3-like MinIO.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="diagrams/architecture-dark.png">
  <source media="(prefers-color-scheme: light)" srcset="diagrams/architecture-light.png">
  <img src="diagrams/architecture-light.png">
</picture>

![](diagrams/architecture.png)

## Components

![](demos/setup.gif)

### Producer

Python script that generates events attributed to users.

- Events are being streamed to S3 in JSON format.
- User IDs are picked from a pre-generated list to simulate events from the same user.
- Events are of a single type with a static schema.
- Events are never late.
- **Events may have invalid schema.**
- **Events may be duplicated.**
- **Event schema may evolve in backward-compatible manner.**
- **Batches of events may be corrupted.**

### Consumer

Python script that uses PySpark to process the producer stream.

- Consumes raw batches of data.
- **Handles corrupted batches by routing them to dead-letter queue.**
- **Enforces schema. Routes records with invalid schema or extra fields to dead-letter queue.**
- Enriches with metadata.
- **Writes to Iceberg table.**
- **Handles implicit partitioning (powered by Iceberg).**
- **Handles schema evolution: supports addition of new columns and type-widening of existing columns (powered by Iceberg).**
- Uses checkpointing to handle job restarts and enforce exactly-once semantics (on batch level).

### DuckDB

![](demos/duckdb.gif)

DuckDB is used to explore the data. 

- Processed bronze data can be queried through `bronze.events` table.
- Dead-lettered records can be found in `dead_letters.events` table.

### MinIO

MinIO is a local S3-compatible storage. It's used to store raw data, bronze data and Spark checkpoints.

## Considerations

### Duplicates

This pipeline does not handle deduplication, as it could result in the loss of potentially valid records and should be handled downstream in the Silver layer.

If, however, it's crucial to remove duplicates, there are two approaches that could be implemented together or separately.

#### Consumer Deduplication

Spark supports removing duplicates from a data frame using the `dropDuplicates` method. However, that requires Spark to accumulate a state of seen records, which could lead to memory overflows and runtime errors if the state is not maintained properly.

State could be maintained using watermarking. The watermarking mechanism would store state for a limited window and deduplicate events within that window. That won't guarantee unique events, however, as there might be duplicates in different windows.

#### Iceberg Merge

Iceberg supports the MERGE operation, which updates (or disables updates of) previously recorded data using a primary key-like specification. This works well but might result in degraded performance on high-throughput streams.

### Iceberg Maintenance

Iceberg creates table snapshots on every DML operation on a table. This results in many snapshots that should be regularly cleaned up.

Also, while not specific to Iceberg, small batches of data create small files, which results in suboptimal performance in downstream queries. Iceberg supports compaction operations that merge multiple smaller files into bigger ones, resulting in fewer files and better read performance.

Both operations should be run periodically. While Iceberg supports running these operations concurrently with writes using optimistic concurrency, they are not implemented in the current stage of the project.

## Project Stages

This project has evolved in three stages that build on each other:

1. [Always-valid messages at the source; raw Parquet files](https://github.com/vsevolodbazhan/spark-streaming-practicum/tree/stage-1).
2. [Handling invalid schemas and corrupted batches using a dead-letter sink](https://github.com/vsevolodbazhan/spark-streaming-practicum/tree/stage-2).
3. [Sink to Iceberg; partitioning, schema evolution, handling events with extra fields, logging](https://github.com/vsevolodbazhan/spark-streaming-practicum/tree/stage-3) (current).

## Usage

> [!NOTE]
> Requires Docker, Docker Compose and Make.

Start with:
```
make start
```
This command will build local Docker images, pull third party ones and start the components in the background.

Open MinIO console in the default web browser.
```
make minio
```
The console requires a login and password. Check the values in the `.env` file under `MINIO_ROOT*`, but they should be `admin` and `password`.

Stop producer and consumer with:
```
make stop
```
Note that it will not stop containers running MinIO, so you can still navigate to the console and see the files.

Stop and cleanup everything:
```
make clean
```

Check logs using:
```
make producer-logs
make consumer-logs
```

Open Spark UI in the default browser:
```
make spark
```

Create a container with DuckDB shell:
```
make duckdb
```
```sql
select * from bronze.events;
select * from dead_letters.events;
```

## Local Development

> [!NOTE]
> Requires uv.

Install using uv:
```
uv sync --all-groups
```

Run tests:
```
uv run pytest
```

Run the producer pipeline to the local `target/producer` directory:
```
uv run -m src.producer --data-sink local
```

Run the consumer from the local producer directory to the local `target/consumer` directory:
```
uv run -m src.consumer --data-source local --data-sink local
```
