# Streaming Practicum

Practice project to explore and implement streaming data ingestion patterns using Apache Spark Structured Streaming. The project is structured into three components: **producer** is a generator of artificial data implemented in Python, **ingestor** is a Spark Streaming–based data ingestion pipeline, **consumer** is a DuckDB-powered reader of ingested data. Storage is handled by S3-like MinIO.

## Producer

- Python script that generates events attributed to users.
- User IDs are picked from a pre-generated list to simulate events from the same user.
- The schema of messages is static.
- Messages are always valid.
