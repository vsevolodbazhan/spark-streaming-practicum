import argparse
import json
import os
import random
import sys
from datetime import datetime, timezone
from enum import StrEnum
from functools import cache
from io import BytesIO
from pathlib import Path
from time import sleep
from typing import Any, Iterable, Iterator
from uuid import uuid4

import boto3
import structlog
from dotenv import load_dotenv
from faker import Faker
from rich import print

fake = Faker()
logger = structlog.getLogger()


class EventType(StrEnum):
    PAGE_VIEW = "page_view"


class EventFactory:
    """
    Factory that generates batches of random events.
    """

    def create_random_events(self, n: int = 1) -> Iterator[dict]:
        """
        Create a batch of random events.
        """
        for _ in range(n):
            yield self.create_event()

    def create_event(self, event_type: EventType | None = None) -> dict:
        """
        Create event. If the event type is not specified, it will be chosen
        randomly from the possible event types.
        """
        if event_type is None:
            event_type = random.choice(list(EventType))

        match event_type:
            case EventType.PAGE_VIEW:
                return self._create_page_view_event()
            case _:
                raise NotImplementedError()

    @cache
    def _get_pregenerated_user_ids(self) -> list[str]:
        return (Path(__file__).parent / "user_ids.txt").read_text().splitlines()

    def _get_random_user_id(self) -> str:
        return random.choice(self._get_pregenerated_user_ids())

    def _create_event_id(self) -> str:
        return str(uuid4())

    def _create_event_timestamp(self) -> str:
        return datetime.now(tz=timezone.utc).isoformat()

    def _create_event_scaffold(self) -> dict:
        return {
            "user_id": self._get_random_user_id(),
            "event_id": self._create_event_id(),
            "event_timestamp": datetime.now(tz=timezone.utc).isoformat(),
        }

    def _create_page_view_event(self) -> dict:
        return self._create_event_scaffold() | {
            "event_type": str(EventType.PAGE_VIEW),
            "properties": {
                "url": fake.url(),
                "user_agent": fake.user_agent(),
            },
        }


class DataSinkType(StrEnum):
    STDOUT = "stdout"
    LOCAL_FILE = "local_file"
    S3 = "s3"


class DataSinkFormat(StrEnum):
    JSON = "json"


class DataSink:
    """
    Base class for data sinks.
    """

    def __init__(self, *, format: DataSinkFormat = DataSinkFormat.JSON) -> None:
        self._format = format

    def _serialize(self, batch: Iterable[dict]) -> bytes:
        match self._format:
            case DataSinkFormat.JSON:
                batch = list(batch)
                serialized_batch = json.dumps(batch).encode("utf-8")
            case _:
                raise NotImplementedError("Unsupported data format.")

        logger.info(
            "Serialized batch",
            batch_size=sys.getsizeof(serialized_batch),
            units="bytes",
        )
        return serialized_batch

    def _write(self, serialized_batch: bytes) -> Any:
        """
        This method is designed to be implemented in subclasses.
        """
        raise NotImplementedError()

    def sink(self, batch: Iterable[dict]) -> Any:
        serialized_batch = self._serialize(batch)
        self._write(serialized_batch)


class StdoutDataSink(DataSink):
    """
    Prints data to standard output.
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        if self._format != DataSinkFormat.JSON:
            raise ValueError("Formats other than JSON are not supported by this sink.")

    def _write(self, serialized_batch: bytes) -> Any:
        for datum in json.loads(serialized_batch):
            print(datum)


class _FileDataSink(DataSink):
    def _generate_file_name(self) -> str:
        return f"{uuid4()}.{self._format}"


class LocalFileDataSink(_FileDataSink):
    """
    Write data as local files.
    """

    def __init__(self, *, output: Path, **kwargs) -> None:
        super().__init__(**kwargs)

        if output.exists() and not output.is_dir():
            raise ValueError("Output must be a directory.")

        output.mkdir(parents=True, exist_ok=True)
        self._output = output

    def _write(self, serialized_batch: bytes) -> Any:
        file = self._output / self._generate_file_name()
        file.write_bytes(serialized_batch)
        logger.info("Written batch to file", file=file)


class S3DataSink(_FileDataSink):
    """
    Write data to S3 bucket.
    """

    def __init__(
        self,
        *,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        region: str,
        bucket: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._bucket = bucket
        self._client = boto3.client(
            service_name="s3",
            region_name=region,
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
        logger.info(
            "Set up sink",
            endpoint_url=endpoint_url,
            region=region,
            bucket=bucket,
        )

    def _write(self, serialized_batch: bytes) -> Any:
        key = self._generate_file_name()
        self._client.upload_fileobj(
            Fileobj=BytesIO(serialized_batch),
            Bucket=self._bucket,
            Key=key,
        )
        logger.info("Uploaded batch", bucket=self._bucket, key=key)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog=Path(__file__).stem)
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="The number of events in a single batch.",
    )
    parser.add_argument(
        "--sleep-between-batches-seconds",
        type=int,
        default=3,
        help="Backoff time between batch generations.",
    )
    parser.add_argument(
        "--data-sink",
        choices=list(DataSinkType),
        default=DataSinkType.STDOUT,
        help="Data sink type.",
    )
    parser.add_argument(
        "--local-file-output",
        type=str,
        default=".output",
        help="Output directory for local file sink.",
    )
    args = parser.parse_args()

    load_dotenv()

    match data_sink_type := args.data_sink:
        case DataSinkType.STDOUT:
            data_sink = StdoutDataSink()
        case DataSinkType.LOCAL_FILE:
            data_sink = LocalFileDataSink(output=Path(args.local_file_output))
        case DataSinkType.S3:
            data_sink = S3DataSink(
                endpoint_url=f"http://localhost:{os.environ['MINIO_SERVER_PORT']}",
                region=os.environ["AWS_REGION"],
                access_key=os.environ["AWS_ACCESS_KEY_ID"],
                secret_key=os.environ["AWS_SECRET_ACCESS_KEY"],
                bucket=os.environ["AWS_RAW_BUCKET"],
            )
        case _:
            raise NotImplementedError(f"Unsupported data sink type: {data_sink_type}")

    event_factory = EventFactory()
    while True:
        events = event_factory.create_random_events(n=args.batch_size)
        data_sink.sink(events)
        sleep_duration = args.sleep_between_batches_seconds
        logger.info("Sleeping", duration=sleep_duration, units="seconds")
        sleep(sleep_duration)
