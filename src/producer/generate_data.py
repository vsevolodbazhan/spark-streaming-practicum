import random
from enum import StrEnum
from functools import cache
from pathlib import Path


class EventType(StrEnum):
    PAGE_VIEW = "page_view"
    CLICK = "click"
    PURCHASE = "purchase"
    SEARCH = "search"


class EventFactory:
    def create_event(self, event_type: EventType) -> dict:
        raise NotImplementedError()

    @cache
    def _get_user_ids(self) -> list[str]:
        return (Path(__file__).parent / "user_ids.txt").read_text().splitlines()

    def _get_user_id(self) -> str:
        return random.choice(self._get_user_ids())

    def _create_page_view_event(self) -> dict:
        raise NotImplementedError()

    def _create_click_event(self) -> dict:
        raise NotImplementedError()

    def _create_purchase_event(self) -> dict:
        raise NotImplementedError()

    def _create_search_event(self) -> dict:
        raise NotImplementedError()


if __name__ == "__main__":
    event_factory = EventFactory()
    print(event_factory._get_user_ids())
