import argparse
import json
import random
from datetime import datetime, timezone
from enum import StrEnum
from functools import cache
from pathlib import Path
from time import sleep
from typing import Any, Iterable, Iterator
from uuid import uuid4

from faker import Faker
from rich import print

fake = Faker()


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
                return json.dumps(batch).encode("utf-8")
            case _:
                raise NotImplementedError("Unsupported data format.")

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
    A data sink that prints batches to standard output.
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        if self._format != DataSinkFormat.JSON:
            raise ValueError("Formats other than JSON are not supported by this sink.")

    def _write(self, serialized_batch: bytes) -> Any:
        for datum in json.loads(serialized_batch):
            print(datum)


class LocalFileDataSink(DataSink):
    """
    A data sink that writes data as local files.
    """

    def __init__(self, *, output: Path, **kwargs) -> None:
        super().__init__(**kwargs)

        if output.exists() and not output.is_dir():
            raise ValueError("Output must be a directory.")

        output.mkdir(parents=True, exist_ok=True)
        self._output = output

    def _write(self, serialized_batch: bytes) -> Any:
        (self._output / f"{uuid4()}.{self._format}").write_bytes(serialized_batch)


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

    match data_sink_type := args.data_sink:
        case DataSinkType.STDOUT:
            data_sink = StdoutDataSink()
        case DataSinkType.LOCAL_FILE:
            data_sink = LocalFileDataSink(output=Path(args.local_file_output))
        case _:
            raise NotImplementedError(f"Unsupported data sink type: {data_sink_type}")

    event_factory = EventFactory()
    while True:
        events = event_factory.create_random_events(n=args.batch_size)
        data_sink.sink(events)
        sleep(args.sleep_between_batches_seconds)
