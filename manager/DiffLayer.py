#  Copyright (c) 2026. Alexander Schmid
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum


class ChangeEventType(Enum):
    ADDED = "ADDED"
    REMOVED = "REMOVED"
    UPDATED = "UPDATED"


@dataclass(frozen=True)
class ChangeEvent[T, K]:
    type: ChangeEventType
    before: T | None  # None for ADDED
    after: T | None  # None for REMOVED
    key: K  # identifier for the object (e.g. id, identity)


class DiffLayer[T, K]:
    def __init__(self, key_func: Callable[[T], K]):
        """
        :param key_func: function to extract unique key from an object
        """
        self.key_func = key_func

    def diff(self, old_items: list[T], new_items: list[T]) -> list[ChangeEvent[T, K]]:
        old_map = {self.key_func(item): item for item in old_items}
        new_map = {self.key_func(item): item for item in new_items}

        events: list[ChangeEvent[T, K]] = []

        # detect removed
        for k, old_item in old_map.items():
            if k not in new_map:
                events.append(ChangeEvent(type=ChangeEventType.REMOVED, before=old_item, after=None, key=k))

        # detect added
        for k, new_item in new_map.items():
            if k not in old_map:
                events.append(ChangeEvent(type=ChangeEventType.ADDED, before=None, after=new_item, key=k))

        # detect updated
        for k in old_map.keys() & new_map.keys():  # intersection
            old_item = old_map[k]
            new_item = new_map[k]
            if old_item != new_item:
                events.append(ChangeEvent(type=ChangeEventType.UPDATED, before=old_item, after=new_item, key=k))

        return events

# Usage:
# diff_layer = DiffLayer[Course](key_func=lambda c: c.id)
# events = diff_layer.diff(old_courses, new_courses)
#
# diff_layer = DiffLayer[Referee](key_func=lambda r: r.identity)
# events = diff_layer.diff(old_referees, new_referees)
