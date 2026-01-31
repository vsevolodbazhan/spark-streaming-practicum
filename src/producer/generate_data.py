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
    def create_random_events(self, n: int = 1) -> Iterator[str]:
        """
        Create a batch of random events.
        """
        for _ in range(n):
            yield self.create_event()

    def create_event(self, event_type: EventType | None = None) -> str:
        """
        Create event. If the event type is not specified, it will be chosen
        randomly from the possible event types.
        """
        if event_type is None:
            event_type = random.choice(list(EventType))

        match event_type:
            case EventType.PAGE_VIEW:
                event = self._create_page_view_event()
            case _:
                raise NotImplementedError()

        return json.dumps(event)

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


class DataSink:
    def write(self, batch: Iterable[Any]) -> Any:
        raise NotImplementedError()


class StdoutDataSink(DataSink):
    def write(self, batch: Iterable[Any]) -> Any:
        for datum in batch:
            print(datum)


if __name__ == "__main__":
    event_factory = EventFactory()
    data_sink = StdoutDataSink()
    while True:
        events = event_factory.create_random_events(10)
        data_sink.write(events)
        sleep(1)
