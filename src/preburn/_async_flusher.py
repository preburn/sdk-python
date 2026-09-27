"""Background task that sends the async client's buffered reports in batches."""

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable

import httpx

from preburn._buffer import PendingReport, ReportBuffer
from preburn._errors import PreburnError
from preburn._flusher import (
    BATCH_REPORTS_MAXIMUM_BYTES,
    REPORTS_FLUSH_FAILED_EVENT,
    REPORTS_UNSENT_EVENT,
    RETRY_DELAYS_SECONDS,
    build_batch_content,
    build_batch_headers,
    drop_oversized_report,
    read_batch_answer,
    record_batch_results,
    record_rejected_batch,
    requeue_batch,
)
from preburn._transport import FALLBACK_EXCEPTIONS, REPORT_BATCH_MAXIMUM, REPORTS_PATH

logger = logging.getLogger("preburn")


class AsyncReportFlusher:
    """Sends the reports of a `ReportBuffer` from a task on the running event loop.

    The task starts with the first `add` and flushes every `flush_interval` seconds, and
    sooner once `batch_size` reports are pending. It belongs to the loop it started on: an
    `add`, `flush` or `aclose` on another loop, such as the next `asyncio.run`, gives the
    flusher a new task on that loop. Batches, retries, requeues and dropped counts follow
    `ReportFlusher`.
    """

    def __init__(
        self,
        http_client: Callable[[], httpx.AsyncClient],
        buffer: ReportBuffer,
        *,
        flush_interval: float,
        batch_size: int,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        """Creates the flusher without starting its task.

        Args:
            http_client: Returns the client of the running event loop, shared with the rest
                of the SDK.
            buffer: Pending reports.
            flush_interval: Seconds between flushes.
            batch_size: Pending reports that start a flush before the interval ends.
            sleep: Waits between retries.
        """
        self._http_client = http_client
        self._buffer = buffer
        self._flush_interval = flush_interval
        self._batch_size = batch_size
        self._sleep = sleep
        self._loop: asyncio.AbstractEventLoop | None = None
        self._flush_lock = asyncio.Lock()
        self._wake = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._closed = False

    def add(self, report: PendingReport) -> None:
        """Queues a report and wakes the task once `batch_size` reports are pending.

        Starts the task on the running event loop when the flusher has none there. A report
        larger than `BATCH_REPORTS_MAXIMUM_BYTES` is dropped, counted and logged instead.

        Raises:
            RuntimeError: The flusher was closed, or no event loop is running.
        """
        if self._closed:
            raise RuntimeError("client closed")
        if len(report) > BATCH_REPORTS_MAXIMUM_BYTES:
            drop_oversized_report(self._buffer, report)
            return
        loop = self._bind_to_running_loop()
        if self._task is None or self._task.done():
            self._task = loop.create_task(self._run())
        self._buffer.add(report)
        if len(self._buffer) >= self._batch_size:
            self._wake.set()

    async def flush(self) -> bool:
        """Sends every pending report now, one batch after the other.

        A batch lost to an unexpected exception is added to the dropped count before the
        exception propagates.

        Returns:
            False when a batch went back to the buffer, else True.
        """
        self._bind_to_running_loop()
        async with self._flush_lock:
            while batch := self._buffer.take_batch(
                REPORT_BATCH_MAXIMUM, BATCH_REPORTS_MAXIMUM_BYTES
            ):
                try:
                    sent = await self._send(batch)
                except asyncio.CancelledError:
                    self._buffer.requeue_front(batch)
                    raise
                except Exception:
                    self._buffer.count_dropped(len(batch))
                    raise
                if not sent:
                    return False
            return True

    async def aclose(self) -> None:
        """Cancels the task, then sends every pending report once more on the running loop.

        A batch the task was sending when cancelled goes back to the buffer and is sent
        again. Reports still pending afterwards are logged as unsent.
        """
        self._closed = True
        self._bind_to_running_loop()
        if self._task is not None:
            self._task.cancel()
            await asyncio.wait({self._task})
        await self.flush()
        pending = len(self._buffer)
        if pending:
            logger.warning(f"{REPORTS_UNSENT_EVENT} pending={pending}")

    def _bind_to_running_loop(self) -> asyncio.AbstractEventLoop:
        loop = asyncio.get_running_loop()
        if loop is not self._loop:
            self._loop = loop
            self._flush_lock = asyncio.Lock()
            self._wake = asyncio.Event()
            self._task = None
        return loop

    async def _run(self) -> None:
        while True:
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._wake.wait(), self._flush_interval)
            self._wake.clear()
            if not await self._flush_logging_failures():
                await asyncio.sleep(self._flush_interval)

    async def _flush_logging_failures(self) -> bool:
        try:
            return await self.flush()
        except Exception:
            logger.exception(f"{REPORTS_FLUSH_FAILED_EVENT} pending={len(self._buffer)}")
            return False

    async def _send(self, batch: list[PendingReport]) -> bool:
        dropped = self._buffer.dropped
        headers = build_batch_headers(dropped)
        content = build_batch_content(batch)
        for retry_delay in (*RETRY_DELAYS_SECONDS, None):
            try:
                response = await self._http_client().post(
                    REPORTS_PATH, content=content, headers=headers
                )
                results = read_batch_answer(response, len(batch))
            except FALLBACK_EXCEPTIONS as error:
                cause = f"error={type(error).__name__}"
            except PreburnError as error:
                return record_rejected_batch(self._buffer, batch, error)
            else:
                if results is not None:
                    self._buffer.acknowledge_dropped(dropped)
                    return record_batch_results(self._buffer, batch, results)
                cause = f"status={response.status_code}"
            if retry_delay is None:
                break
            await self._sleep(retry_delay)
        requeue_batch(self._buffer, batch, cause)
        return False
