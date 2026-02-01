import argparse
import os
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from s3path import S3Path

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

SPARK_APP_NAME = "consumer"


load_dotenv()


class DataSourceSinkType(StrEnum):
    LOCAL_FILE = "local_file"
    S3 = "s3"


@dataclass
class ConsumerEnvironment:
    spark_session: SparkSession
    source_path: Path
    checkpoints_path: Path
    sink_path: Path
    dead_letters_sink_path: Path


def _get_base_spark_session_builder() -> SparkSession.Builder:
    return (
        SparkSession.builder.appName(SPARK_APP_NAME)
        .config("spark.ui.port", str(os.environ["CONSUMER_SPARK_UI_PORT"]))
        # Bind to all interfaces to make UI accessible from outside the container.
        .config("spark.ui.host", "0.0.0.0")
        .config("spark.ui.enabled", "true")
    )


def create_local_file_environment(
    source_path: Path,
    checkpoints_path: Path,
    sink_path: Path,
    dead_letters_sink_path: Path,
) -> ConsumerEnvironment:
    return ConsumerEnvironment(
        spark_session=_get_base_spark_session_builder().getOrCreate(),
        source_path=source_path,
        checkpoints_path=checkpoints_path,
        sink_path=sink_path,
        dead_letters_sink_path=dead_letters_sink_path,
    )


def create_s3_environment(
    aws_access_key_id: str,
    aws_secret_access_key: str,
    aws_endpoint_url: str,
    aws_region: str,
    source_path: S3Path,
    checkpoints_path: S3Path,
    sink_path: S3Path,
    dead_letters_sink_path: S3Path,
) -> ConsumerEnvironment:
    builder = (
        _get_base_spark_session_builder()
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id)
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key)
        .config("spark.hadoop.fs.s3a.endpoint", aws_endpoint_url)
        .config("spark.hadoop.fs.s3a.region", aws_region)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
    )
    return ConsumerEnvironment(
        spark_session=builder.getOrCreate(),
        source_path=source_path,
        checkpoints_path=checkpoints_path,
        sink_path=sink_path,
        dead_letters_sink_path=dead_letters_sink_path,
    )


def start_stream(environment: ConsumerEnvironment) -> None:
    from pyspark.sql.functions import coalesce, col, explode_outer, from_json, lit, when
    from pyspark.sql.types import (
        ArrayType,
        MapType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    def convert_path_to_string(path: Path) -> str:
        if isinstance(path, S3Path):
            # Enforce the use of s3a:// protocol (not s3://) for S3A filesystem connector.
            return path.as_uri().replace("s3://", "s3a://")
        return path.as_posix()

    schema = StructType(
        [
            StructField("user_id", StringType(), nullable=False),
            StructField("event_id", StringType(), nullable=False),
            StructField("event_timestamp", TimestampType(), nullable=False),
            StructField("event_type", StringType(), nullable=False),
            StructField(
                "properties", MapType(StringType(), StringType()), nullable=True
            ),
        ]
    )
    required_columns = [field.name for field in schema.fields if not field.nullable]
    raw_record_column = "_raw_record"
    raw_batch_column = "_raw_batch"
    is_corrupted_batch_column = "_is_corrupted_batch"
    dead_letter_reason_column = "_dead_letter_reason"

    def route_to_sinks(data_frame: "DataFrame", batch_id: int) -> None:
        # Build filter condition.
        has_all_required_columns = lit(True)
        for required_column in required_columns:
            has_all_required_columns = (
                has_all_required_columns & col(required_column).isNotNull()
            )

        # Determine reason for dead lettering.
        df_with_reason = data_frame.withColumn(
            dead_letter_reason_column,
            when(col(is_corrupted_batch_column), lit("corrupted_batch"))
            .when(~has_all_required_columns, lit("invalid_schema"))
            .otherwise(lit(None)),
        ).drop(is_corrupted_batch_column)

        # Records are valid if there is no dead letter reason.
        valid_df = df_with_reason.filter(col(dead_letter_reason_column).isNull()).drop(
            raw_record_column,
            dead_letter_reason_column,
        )
        # Records are invalid if they have a dead letter reason.
        invalid_df = df_with_reason.filter(col(dead_letter_reason_column).isNotNull())

        # Route batches between valid and dead letters sink.
        for batch_df, sink_path in [
            (valid_df, environment.sink_path),
            (invalid_df, environment.dead_letters_sink_path),
        ]:
            if batch_df.count() > 0:
                batch_df.write.mode("append").parquet(convert_path_to_string(sink_path))

    (
        environment.spark_session.readStream.load(
            path=convert_path_to_string(environment.source_path),
            # Read batch as is.
            format="text",
        )
        # First, explode array into raw JSON strings.
        # Use explode_outer to keep rows where parsing failed.
        # That allows to keep corrupted batches.
        .withColumn(
            # Parse value into an array to raw string records.
            "raw_records_array",
            from_json(col("value"), ArrayType(StringType())),
        )
        .select(
            col("value").alias(raw_batch_column),  # Keep original batch value.
            explode_outer("raw_records_array").alias(
                raw_record_column
            ),  # Explode array for raw recrods into individual records.
        )
        # Track if batch was corrupted (exploded array is null).
        .withColumn(is_corrupted_batch_column, col(raw_record_column).isNull())
        # For corrupted batches, use the raw batch as the raw record.
        .withColumn(
            raw_record_column,
            coalesce(raw_record_column, raw_batch_column),
        )
        .drop(raw_batch_column)
        # Parse each raw record as a JSON.
        .withColumn("parsed", from_json(raw_record_column, schema))
        .selectExpr(
            # Include metadata.
            "_metadata.file_path as _source",
            "_metadata.file_modification_time as _source_updated_at",
            "current_timestamp() as _batch_processed_at",
            # Keep the raw record and the indicator of batch corruption.
            raw_record_column,
            is_corrupted_batch_column,
            # Add all parsed fields.
            "parsed.*",
        )
        # Set up checkpointing.
        .writeStream.option(
            "checkpointLocation",
            convert_path_to_string(environment.checkpoints_path),
        )
        .foreachBatch(route_to_sinks)
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data-source-sink",
        choices=list(DataSourceSinkType),
        default=DataSourceSinkType.LOCAL_FILE,
        help="Data source and sink type.",
    )
    parser.add_argument(
        "--source-path",
        type=str,
        required=False,
        help="Local directory or S3 path to read data from.",
    )
    parser.add_argument(
        "--sink-path",
        type=str,
        required=False,
        help="Local directory or S3 path to output data to.",
    )
    parser.add_argument(
        "--dead-letters-sink-path",
        type=str,
        required=False,
        help="Local directory or S3 path to output corrupted data to.",
    )
    parser.add_argument(
        "--checkpoints-path",
        type=str,
        required=False,
        help="Local directory or S3 path to save checkpoints to.",
    )
    args = parser.parse_args()

    match data_source_sink := args.data_source_sink:
        case DataSourceSinkType.LOCAL_FILE:
            environment = create_local_file_environment(
                source_path=Path(
                    args.source_path or os.environ["CONSUMER_LOCAL_FILE_SOURCE_PATH"]
                ),
                checkpoints_path=Path(
                    args.checkpoints_path
                    or os.environ["CONSUMER_LOCAL_FILE_CHECKPOINTS_PATH"]
                ),
                sink_path=Path(
                    args.sink_path or os.environ["CONSUMER_LOCAL_FILE_SINK_PATH"]
                ),
                dead_letters_sink_path=Path(
                    args.dead_letters_sink_path
                    or os.environ["CONSUMER_LOCAL_FILE_DEAD_LETTERS_SINK_PATH"]
                ),
            )
        case DataSourceSinkType.S3:
            environment = create_s3_environment(
                aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
                aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
                aws_endpoint_url=os.environ["AWS_ENDPOINT_URL"],
                aws_region=os.environ["AWS_REGION"],
                source_path=S3Path.from_uri(
                    args.source_path or os.environ["CONSUMER_S3_SOURCE_PATH"]
                ),
                checkpoints_path=S3Path.from_uri(
                    args.checkpoints_path or os.environ["CONSUMER_S3_CHECKPOINTS_PATH"]
                ),
                sink_path=S3Path.from_uri(
                    args.sink_path or os.environ["CONSUMER_S3_SINK_PATH"]
                ),
                dead_letters_sink_path=S3Path(
                    args.dead_letters_sink_path
                    or os.environ["CONSUMER_S3_DEAD_LETTERS_SINK_PATH"]
                ),
            )
        case _:
            raise NotImplementedError(
                f"Unsupported data source/sink type: {data_source_sink}"
            )

    start_stream(environment)
