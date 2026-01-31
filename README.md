# Streaming Practicum

Practice project to explore and implement streaming data ingestion patterns using Apache Spark Structured Streaming. The project is structured into three components: **producer** is a generator of artificial data implemented in Python, **consumer** is a Spark Streaming–based data ingestion pipeline, **duckdb** is a DuckDB-powered reader of ingested data. Storage is handled by S3-like MinIO.

## Components

### Producer

Python script that generates events attributed to users.

Constraints:
- User IDs are picked from a pre-generated list to simulate events from the same user.
- Events are of a single type with a static schema.
- Events are always valid.
- Events are never duplicated.
- Events are never late.

## Usage

> [!NOTE]
> Requires Docker, Docker Compose and Make.

Start with:
```
make start
```
This command will build local Docker images, pull third party ones and start the components in a background.

Open the default web browser and navigate to MinIO console:
```
make open
```
The console requires a login and password. Check the latest ones in the `.env` file under `MINIO_ROOT*`, but they should be `admin` and `password`.

Stop producer and ingestor with:
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
make logs
```

Create a container with DuckDB shell:
```
make duckdb
```
Raw events generate by the producer are available as `raw.events` view.
```sql
select * from raw.events limit 10;
```
