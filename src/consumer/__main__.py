import argparse
import os
from enum import StrEnum
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql.functions import col, days
from pyspark.sql.types import (
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from s3path import S3Path

from .batch_parsers import JsonArrayBatchParser
from .data_sinks import IcebergDataSink, LocalDataSink, S3DataSink
from .data_sources import LocalFileDataSource, S3DataSource
from .session_builder import SessionBuilder
from .stream_processor import StreamProcessor


class DataSourceType(StrEnum):
    LOCAL = "local"
    S3 = "s3"


class DataSinkType(StrEnum):
    LOCAL = "local"
    S3 = "s3"
    ICEBERG = "iceberg"


if __name__ == "__main__":
    load_dotenv()

    parser = argparse.ArgumentParser("consumer")
    parser.add_argument("--data-source", choices=list(DataSourceType))
    parser.add_argument("--data-sink", choices=list(DataSinkType))
    args = parser.parse_args()

    print(args.data_source, args.data_sink)

    match data_source_type := args.data_source:
        case DataSourceType.LOCAL:
            data_source = LocalFileDataSource(
                path=Path(os.environ["CONSUMER_LOCAL_SOURCE_PATH"])
            )
        case DataSourceType.S3:
            data_source = S3DataSource(
                path=S3Path.from_uri(os.environ["CONSUMER_S3_SOURCE_PATH"])
            )
        case _:
            raise NotImplementedError(
                f"Unsupported data source type: {data_source_type}"
            )

    match data_sink_type := args.data_sink:
        case DataSinkType.LOCAL:
            data_sink = LocalDataSink(path=Path(os.environ["CONSUMER_LOCAL_SINK_PATH"]))
            dead_letters_sink = LocalDataSink(
                path=Path(os.environ["CONSUMER_LOCAL_DEAD_LETTERS_SINK_PATH"])
            )
        case DataSinkType.S3:
            data_sink = S3DataSink(
                path=S3Path.from_uri(os.environ["CONSUMER_S3_SINK_PATH"])
            )
            dead_letters_sink = S3DataSink(
                path=S3Path.from_uri(os.environ["CONSUMER_S3_DEAD_LETTERS_SINK_PATH"])
            )
        case DataSinkType.ICEBERG:
            data_sink = IcebergDataSink(
                table_name=os.environ["CONSUMER_ICEBERG_TABLE_NAME"]
            )
            dead_letters_sink = S3DataSink(
                path=S3Path.from_uri(os.environ["CONSUMER_S3_DEAD_LETTERS_SINK_PATH"])
            )
        case _:
            raise NotImplementedError(f"Unsupported data sink type: {data_sink_type}")

    checkpoint_location = (
        Path(os.environ["CONSUMER_LOCAL_CHECKPOINTS_PATH"])
        if args.data_sink == DataSinkType.LOCAL
        else S3Path.from_uri(os.environ["CONSUMER_S3_CHECKPOINTS_PATH"])
    )

    session_builder = SessionBuilder()
    session_builder.with_spark_ui(
        port=int(os.environ.get("CONSUMER_SPARK_UI_PORT", "4040"))
    )
    if (
        isinstance(data_source, S3DataSource)
        or isinstance(data_sink, (S3DataSink, IcebergDataSink))
        or isinstance(dead_letters_sink, S3DataSink)
        or isinstance(checkpoint_location, S3Path)
    ):
        session_builder.with_s3(
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            aws_endpoint_url=os.environ["AWS_ENDPOINT_URL"],
            aws_region=os.environ["AWS_REGION"],
        )
    if isinstance(data_sink, IcebergDataSink):
        session_builder.with_iceberg(
            catalog_name=os.environ.get("CONSUMER_ICEBERG_CATALOG_NAME", "iceberg"),
            warehouse_path=S3Path.from_uri(
                os.environ["CONSUMER_ICEBERG_WAREHOUSE_PATH"]
            ),
        )

    stream_processor = StreamProcessor(
        session_builder.build(),
        source=data_source,
        dead_letters_sink=dead_letters_sink,
        sink=(
            data_sink.with_partitioned_by(days(col("event_timestamp")))
            if isinstance(data_sink, IcebergDataSink)
            else data_sink
        ),
        parser=JsonArrayBatchParser(
            parsed_record_schema=StructType(
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
        ),
        checkpoint_location=checkpoint_location,
        trigger_interval="30 seconds",
    )
    stream_processor.start()
