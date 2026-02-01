import argparse
import os
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from textwrap import dedent

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from s3path import S3Path

SPARK_APP_NAME = "consumer"


class DataSourceSinkType(StrEnum):
    LOCAL_FILE = "local_file"
    S3 = "s3"


@dataclass
class ConsumerEnvironment:
    spark_session: SparkSession
    source_path: Path
    checkpoints_path: Path
    sink_path: Path


def create_local_file_environment(
    source_path: Path,
    checkpoints_path: Path,
    sink_path: Path,
) -> ConsumerEnvironment:
    return ConsumerEnvironment(
        spark_session=SparkSession.builder.appName(SPARK_APP_NAME).getOrCreate(),
        source_path=source_path,
        checkpoints_path=checkpoints_path,
        sink_path=sink_path,
    )


def create_s3_environment(
    aws_access_key_id: str,
    aws_secret_access_key: str,
    aws_endpoint_url: str,
    aws_region: str,
    source_path: S3Path,
    checkpoints_path: S3Path,
    sink_path: S3Path,
) -> ConsumerEnvironment:
    builder = SparkSession.builder.appName(SPARK_APP_NAME)
    builder = (
        # TODO: Add packages during build time to avoid startup overhead.
        builder.config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id)
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key)
        .config("spark.hadoop.fs.s3a.endpoint", aws_endpoint_url)
        .config("spark.hadoop.fs.s3a.region", aws_region)
    )
    return ConsumerEnvironment(
        spark_session=builder.getOrCreate(),
        source_path=source_path,
        checkpoints_path=checkpoints_path,
        sink_path=sink_path,
    )


def start_stream(environment: ConsumerEnvironment) -> None:
    (
        environment.spark_session.readStream.load(
            path=str(environment.source_path),
            format="json",
            schema=dedent(
                """
                user_id string,
                event_id string,
                event_timestamp timestamp,
                event_type string,
                properties map<string, string>
                """
            ),
        )
        .selectExpr(
            "_metadata.file_path as _source",
            "_metadata.file_modification_time as _source_updated_at",
            "current_timestamp() as _batch_processed_at",
            "*",
        )
        .writeStream.option(
            "checkpointLocation",
            str(environment.checkpoints_path),
        )
        .start(
            path=str(environment.sink_path),
            format="parquet",
            outputMode="append",
        )
        .awaitTermination()
    )


if __name__ == "__main__":
    load_dotenv()

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data-source-sink-type",
        choices=list(DataSourceSinkType),
        default=DataSourceSinkType.LOCAL_FILE,
        help="Data source and sink type.",
    )
    parser.add_argument(
        "--source-path",
        type=str,
        required=True,
        help="Local directory or S3 path to read data from.",
    )
    parser.add_argument(
        "--sink-path",
        type=str,
        required=True,
        help="Local directory or S3 path to output data to.",
    )
    parser.add_argument(
        "--checkpoints-path",
        type=str,
        required=True,
        help="Local directory or S3 path to save checkpoints to.",
    )
    args = parser.parse_args()

    match data_source_sink_type := args.data_source_sink_type:
        case DataSourceSinkType.LOCAL_FILE:
            environment = create_local_file_environment(
                source_path=Path(args.source_path),
                checkpoints_path=Path(args.checkpoints_path),
                sink_path=Path(args.sink_path),
            )
        case DataSourceSinkType.S3:
            environment = create_s3_environment(
                aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
                aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
                aws_endpoint_url=os.environ["AWS_ENDPOINT_URL"],
                aws_region=os.environ["AWS_REGION"],
                source_path=S3Path(args.source_path),
                checkpoints_path=S3Path(args.checkpoints_path),
                sink_path=S3Path(args.sink_path),
            )
        case _:
            raise NotImplementedError(
                f"Unsupported data source/sink type: {data_source_sink_type}"
            )

    start_stream(environment)
