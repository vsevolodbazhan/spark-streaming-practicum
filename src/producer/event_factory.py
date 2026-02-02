import random
from datetime import datetime, timezone
from enum import StrEnum
from functools import cache
from pathlib import Path
from typing import Iterator
from uuid import uuid4

import structlog
from faker import Faker

fake = Faker()
logger = structlog.getLogger()


class EventType(StrEnum):
    PAGE_VIEW = "page_view"
    PURCHASE = "purchase"


def _validate_chance_value(value: float) -> None:
    if value < 0 or value > 1:
        raise ValueError(f"Chance value must be between 0 and 1: {value}")


class EventFactory:
    """
    Factory that generates batches of random events.
    """

    def __init__(
        self,
        invalid_schema_chance: float = 0,
        duplicate_chance: float = 0,
    ) -> None:
        _validate_chance_value(invalid_schema_chance)
        _validate_chance_value(duplicate_chance)
        self._invalid_schema_chance = invalid_schema_chance
        self._duplicate_chance = duplicate_chance
        self._saved_event: dict | None = None

    def _should_create_invalid_event(self) -> bool:
        return random.random() < self._invalid_schema_chance

    def _should_create_duplicated_event(self) -> bool:
        return random.random() < self._duplicate_chance

    def create_random_events(self, n: int = 1) -> Iterator[dict]:
        """
        Create a batch of events.
        """
        for _ in range(n):
            yield self.create_event(
                invalid=self._should_create_invalid_event(),
                duplicate=self._should_create_duplicated_event(),
            )

    def create_event(
        self,
        event_type: EventType | None = None,
        invalid: bool = False,
        duplicate: bool = False,
    ) -> dict:
        """
        Create event. If the event type is not specified, it will be chosen
        randomly from the possible event types.
        """
        if invalid is True:
            return self._create_invalid_event()

        if duplicate is True and self._saved_event is not None:
            return self._saved_event

        if event_type is None:
            event_type = random.choice(list(EventType))

        match event_type:
            case EventType.PAGE_VIEW:
                event = self._create_page_view_event()
            case EventType.PURCHASE:
                event = self._create_purchase_event()
            case _:
                raise NotImplementedError()

        self._saved_event = event
        return event

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
        """
        Valid page-view like event.
        """
        return self._create_event_scaffold() | {
            "event_type": str(EventType.PAGE_VIEW),
            "properties": {
                "url": fake.url(),
                "user_agent": fake.user_agent(),
            },
        }

    def _create_purchase_event(self) -> dict:
        """
        Purchase event with new top-level field.
        """
        return self._create_event_scaffold() | {
            "event_type": str(EventType.PURCHASE),
            # New top-level field for schema evolution.
            "product_id": fake.uuid4(),
            "properties": {
                "amount": round(random.uniform(1.0, 500.0), 2),
                "currency": random.choice(["USD", "EUR", "GBP"]),
            },
        }

    def _create_invalid_event(self) -> dict:
        """
        Invalid event with random column with random value.
        """
        return {fake.pystr(): fake.pystr()}
