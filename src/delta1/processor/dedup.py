"""Bounded in-memory deduplication of event ids."""

from __future__ import annotations

from collections import OrderedDict


class Deduplicator:
    """Remembers the last ``capacity`` event ids and reports repeats.

    Kafka gives at-least-once delivery and producers retry, so the same event can arrive
    twice. Duplicates almost always arrive close together, which is why a bounded window is
    enough here. The storage layer has its own safety net (ReplacingMergeTree for bets).
    """

    def __init__(self, capacity: int) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self.capacity = capacity
        self._seen: OrderedDict[str, None] = OrderedDict()

    def seen(self, event_id: str) -> bool:
        """Return True if the id was already seen; otherwise remember it and return False."""
        if event_id in self._seen:
            self._seen.move_to_end(event_id)
            return True
        self._seen[event_id] = None
        if len(self._seen) > self.capacity:
            self._seen.popitem(last=False)
        return False

    def __len__(self) -> int:
        return len(self._seen)
