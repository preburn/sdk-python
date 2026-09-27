"""Cache of the fallback outcomes the server sent, for checks that cannot reach Preburn."""

import threading
from collections import OrderedDict

from preburn._models import FallbackOutcome

FALLBACK_CACHE_MAXIMUM_ENTRIES = 10_000
DEFAULT_FALLBACK_OUTCOME: FallbackOutcome = "allow"

FallbackKey = tuple[str | None, str]


class FallbackCache:
    """Thread-safe least recently used cache of the last `fallback_outcome` per key.

    Each recorded outcome is stored under its customer and feature and under its feature
    alone. The cache holds at most `FALLBACK_CACHE_MAXIMUM_ENTRIES` entries of both kinds
    together.
    """

    def __init__(self) -> None:
        """Creates an empty cache."""
        self._outcomes: OrderedDict[FallbackKey, FallbackOutcome] = OrderedDict()
        self._lock = threading.Lock()

    def __len__(self) -> int:
        """Returns the number of cached entries of both kinds."""
        with self._lock:
            return len(self._outcomes)

    def record(self, customer_id: str, feature: str, outcome: FallbackOutcome) -> None:
        """Stores the `fallback_outcome` of a server response for the customer and feature."""
        with self._lock:
            for key in ((customer_id, feature), (None, feature)):
                self._outcomes[key] = outcome
                self._outcomes.move_to_end(key)
            while len(self._outcomes) > FALLBACK_CACHE_MAXIMUM_ENTRIES:
                self._outcomes.popitem(last=False)

    def outcome(self, customer_id: str, feature: str) -> FallbackOutcome:
        """Returns the outcome to use when a check for the customer and feature cannot be made.

        Returns:
            The last outcome recorded for the customer and feature, else the last one recorded
            for the feature, else `allow`.
        """
        with self._lock:
            for key in ((customer_id, feature), (None, feature)):
                if key in self._outcomes:
                    self._outcomes.move_to_end(key)
                    return self._outcomes[key]
            return DEFAULT_FALLBACK_OUTCOME
