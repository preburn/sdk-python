"""Thread-safe queue of encoded reports waiting to be sent."""

import threading
from collections import deque

from preburn._errors import ConfigurationError

PendingReport = bytes
"""Report body encoded as JSON."""


class ReportBuffer:
    """Queue of pending reports that drops the oldest report when full.

    Every dropped report is counted until a flush acknowledges it, so the count can be sent in
    the `Preburn-Dropped-Reports` header.
    """

    def __init__(self, max_pending: int) -> None:
        """Creates an empty buffer holding at most `max_pending` reports.

        Raises:
            ConfigurationError: `max_pending` is below 1.
        """
        if max_pending < 1:
            raise ConfigurationError(f"max_pending must be at least 1 max_pending={max_pending}")
        self._max_pending = max_pending
        self._pending: deque[PendingReport] = deque()
        self._dropped = 0
        self._lock = threading.Lock()

    def __len__(self) -> int:
        """Returns the number of pending reports."""
        with self._lock:
            return len(self._pending)

    @property
    def dropped(self) -> int:
        """Reports dropped and not yet acknowledged."""
        with self._lock:
            return self._dropped

    def add(self, report: PendingReport) -> None:
        """Appends a report, dropping and counting the oldest pending report when full."""
        with self._lock:
            if len(self._pending) == self._max_pending:
                self._pending.popleft()
                self._dropped += 1
            self._pending.append(report)

    def take_batch(self, maximum_count: int, maximum_bytes: int) -> list[PendingReport]:
        """Removes and returns the oldest pending reports, oldest first.

        The batch holds at most `maximum_count` reports of at most `maximum_bytes` in total. It
        always holds the oldest report when one is pending, even a larger one, so every report
        leaves the buffer.

        Raises:
            ValueError: `maximum_count` is below 1.
        """
        if maximum_count < 1:
            raise ValueError(f"batch maximum must be at least 1 maximum_count={maximum_count}")
        batch: list[PendingReport] = []
        batch_bytes = 0
        with self._lock:
            while self._pending and len(batch) < maximum_count:
                report_bytes = len(self._pending[0])
                if batch and batch_bytes + report_bytes > maximum_bytes:
                    break
                batch.append(self._pending.popleft())
                batch_bytes += report_bytes
        return batch

    def requeue_front(self, batch: list[PendingReport]) -> None:
        """Puts a batch that could not be sent back in front of the pending reports.

        When the buffer cannot hold the whole batch, the oldest reports of the batch are dropped
        and counted.
        """
        with self._lock:
            overflow = max(0, len(self._pending) + len(batch) - self._max_pending)
            self._dropped += overflow
            self._pending.extendleft(reversed(batch[overflow:]))

    def count_dropped(self, count: int) -> None:
        """Adds `count` reports dropped outside the buffer, such as a batch the server rejected."""
        with self._lock:
            self._dropped += count

    def reset_after_fork(self) -> None:
        """Empties the buffer and its dropped count under a new lock in a forked child.

        The reports and the count belong to the parent process, and the parent's threads may
        have held the old lock when the process forked.
        """
        self._lock = threading.Lock()
        self._pending = deque()
        self._dropped = 0

    def acknowledge_dropped(self, count: int) -> None:
        """Subtracts `count` dropped reports after a request carrying that count succeeded.

        Raises:
            ValueError: `count` is negative or exceeds the dropped reports.
        """
        with self._lock:
            if count < 0 or count > self._dropped:
                raise ValueError(f"acknowledged count outside dropped count={count}")
            self._dropped -= count
