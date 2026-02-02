import argparse
import os
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, explode_outer, from_json, lit, when
from pyspark.sql.types import (
    ArrayType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from s3path import S3Path

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

SPARK_APP_NAME = "consumer"


load_dotenv()


class DataSourceType(StrEnum):
    LOCAL_FILE = "local_file"
    S3 = "s3"


class DataSinkType(StrEnum):
    LOCAL_FILE = "local_file"
    S3 = "s3"


class DataSinkFormat(StrEnum):
    PARQUET = "parquet"
    ICEBERG = "iceberg"


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


class JsonArrayStreamProcessor:
    """
    Process stream of JSON arrays.

    Handles corrupted batches and records with invalid schema by
    routing them to a dead letter queue. Adds metadata about source data.
    Converts parsed records to Parquet files.
    """

    RAW_ARRAY_COL_NAME = "_raw_array"
    RAW_BATCH_COL_NAME = "_raw_batch"
    RAW_RECORD_COL_NAME = "_raw_record"
    IS_CORRUPTED_BATCH_COL_NAME = "_is_corrupted_batch"
    PARSED_RECORD_COL_NAME = "_parsed"
    DEAD_LETTER_REASON_COL_NAME = "_dead_letter_reason"

    def __init__(
        self,
        environment: ConsumerEnvironment,
        schema: "StructType",
        trigger_interval: str = "20 seconds",
    ) -> None:
        self._environment = environment
        self._schema = schema
        self._trigger_interval = trigger_interval
        self._required_columns = [f.name for f in schema.fields if not f.nullable]

    def _convert_path_to_string(self, path: Path) -> str:
        """Convert Path to string, using s3a:// protocol for S3 paths."""
        if isinstance(path, S3Path):
            return path.as_uri().replace("s3://", "s3a://")
        return path.as_posix()

    def _build_pipeline(self, raw_stream: "DataFrame") -> "DataFrame":
        return (
            raw_stream.select(
                # Keep raw batch value.
                col("value").alias(self.RAW_BATCH_COL_NAME),
                # Parse batch value into an array to strings (raw records).
                explode_outer(
                    from_json(
                        col("value"),
                        ArrayType(StringType()),
                    ),
                ).alias(self.RAW_RECORD_COL_NAME),
            )
            # Parse raw records against the schema.
            .withColumn(
                self.PARSED_RECORD_COL_NAME,
                from_json(
                    col(self.RAW_RECORD_COL_NAME),
                    self._schema,
                ),
            )
            # Mark batches where JSON parsing failed
            # (exploded array of raw records in empty).
            .withColumn(
                self.IS_CORRUPTED_BATCH_COL_NAME,
                col(self.RAW_RECORD_COL_NAME).isNull(),
            )
            # For corrupted batches, use the original batch as the raw record.
            .withColumn(
                self.RAW_RECORD_COL_NAME,
                coalesce(
                    col(self.RAW_RECORD_COL_NAME),
                    col(self.RAW_BATCH_COL_NAME),
                ),
            )
            .drop(self.RAW_BATCH_COL_NAME)
            # Add metadata and flatten parsed records.
            .selectExpr(
                "_metadata.file_path as _source",
                "_metadata.file_modification_time as _source_updated_at",
                "current_timestamp() as _batch_processed_at",
                self.RAW_RECORD_COL_NAME,
                self.IS_CORRUPTED_BATCH_COL_NAME,
                f"{self.PARSED_RECORD_COL_NAME}.*",
            )
        )

    def _route_to_sinks(self, df: "DataFrame", batch_id: int) -> None:
        """Route valid records to sink and invalid records to dead letters."""

        # Build validation condition: all required columns must be present.
        has_all_required = lit(True)
        for column in self._required_columns:
            has_all_required = has_all_required & col(column).isNotNull()

        # Determine dead letter reason.
        df_with_reason = df.withColumn(
            self.DEAD_LETTER_REASON_COL_NAME,
            when(col(self.IS_CORRUPTED_BATCH_COL_NAME), lit("corrupted_batch"))
            .when(~has_all_required, lit("invalid_schema"))
            .otherwise(lit(None)),
        ).drop(self.IS_CORRUPTED_BATCH_COL_NAME)

        # If there is no reason to dead letter, drop service columns
        # and route to valid records.
        valid_df = df_with_reason.filter(
            col(self.DEAD_LETTER_REASON_COL_NAME).isNull()
        ).drop(
            self.RAW_RECORD_COL_NAME,
            self.DEAD_LETTER_REASON_COL_NAME,
        )
        # If there is a reason to dead letter, route to invalid records.
        invalid_df = df_with_reason.filter(
            col(self.DEAD_LETTER_REASON_COL_NAME).isNotNull()
        ).select(
            # Reorder columns to put dead letter reason first.
            self.DEAD_LETTER_REASON_COL_NAME,
            *[
                column
                for column in df_with_reason.columns
                if column != self.DEAD_LETTER_REASON_COL_NAME
            ],
        )

        # Write to sinks.
        for batch_df, sink_path in [
            (valid_df, self._environment.sink_path),
            (invalid_df, self._environment.dead_letters_sink_path),
        ]:
            if not batch_df.isEmpty():
                batch_df.write.mode("append").parquet(
                    self._convert_path_to_string(sink_path)
                )

    def start(self) -> None:
        """Start the streaming pipeline."""
        (
            self._build_pipeline(
                self._environment.spark_session.readStream.load(
                    path=self._convert_path_to_string(self._environment.source_path),
                    # Read batches as-is.
                    format="text",
                )
            )
            .writeStream.option(
                "checkpointLocation",
                self._convert_path_to_string(self._environment.checkpoints_path),
            )
            .trigger(processingTime=self._trigger_interval)
            .foreachBatch(self._route_to_sinks)
            .start()
            .awaitTermination()
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data-source",
        choices=list(DataSourceType),
        default=DataSourceType.LOCAL_FILE,
        help="Data source type.",
    )
    parser.add_argument(
        "--data-sink",
        choices=list(DataSinkType),
        default=DataSinkType.LOCAL_FILE,
        help="Data sink type.",
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

    if args.data_source != args.data_sink:
        raise NotImplementedError(
            f"Mismatched data source ({args.data_source}) and sink ({args.data_sink}) "
            "types are not supported"
        )

    match data_source_type := args.data_source:
        case DataSourceType.LOCAL_FILE:
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

        case DataSourceType.S3:
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
                dead_letters_sink_path=S3Path.from_uri(
                    args.dead_letters_sink_path
                    or os.environ["CONSUMER_S3_DEAD_LETTERS_SINK_PATH"]
                ),
            )

        case _:
            raise NotImplementedError(
                f"Unsupported data source/sink type: {data_source_type}"
            )

    processor = JsonArrayStreamProcessor(
        environment=environment,
        schema=StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("event_id", StringType(), nullable=False),
                StructField("event_timestamp", TimestampType(), nullable=False),
                StructField("event_type", StringType(), nullable=False),
                StructField(
                    "properties", MapType(StringType(), StringType()), nullable=True
                ),
            ]
        ),
    )
    processor.start()
