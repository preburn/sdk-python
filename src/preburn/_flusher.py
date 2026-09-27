"""Background thread that sends buffered reports to the Preburn API in batches."""

import atexit
import logging
import threading
import time
from collections.abc import Callable

import httpx

from preburn._buffer import PendingReport, ReportBuffer
from preburn._errors import UNEXPECTED_RESPONSE_CODE, APIError, PreburnError
from preburn._models import ReportResult, parse_report_batch
from preburn._transport import (
    DROPPED_REPORTS_HEADER,
    FALLBACK_EXCEPTIONS,
    REPORT_BATCH_MAXIMUM,
    REPORT_BATCH_MAXIMUM_BYTES,
    REPORTS_PATH,
    Classification,
    classify,
    read_json_body,
)

RETRY_DELAYS_SECONDS = (0.5, 1.0, 2.0)
EXIT_FLUSH_TIMEOUT_SECONDS = 5.0
RATE_LIMITED_STATUS = 429
SERVER_ERROR_STATUS = 500
THREAD_NAME = "preburn-report-flusher"
BATCH_CONTENT_PREFIX = b'{"reports":['
BATCH_CONTENT_SUFFIX = b"]}"
BATCH_REPORTS_MAXIMUM_BYTES = (
    REPORT_BATCH_MAXIMUM_BYTES
    - len(BATCH_CONTENT_PREFIX)
    - len(BATCH_CONTENT_SUFFIX)
    - (REPORT_BATCH_MAXIMUM - 1)
)
"""Bytes the encoded reports of one batch may take, leaving room for the batch framing."""
BATCH_HEADERS = {"Content-Type": "application/json"}
REPORTS_REQUEUED_EVENT = "reports.requeued"
REPORTS_REJECTED_EVENT = "reports.rejected"
REPORTS_OVERSIZED_EVENT = "reports.oversized"
REPORTS_FLUSH_FAILED_EVENT = "reports.flush_failed"
REPORTS_FLUSH_TIMED_OUT_EVENT = "reports.flush_timed_out"
REPORTS_UNSENT_EVENT = "reports.unsent"

logger = logging.getLogger("preburn")


class ReportFlusher:
    """Sends the reports of a `ReportBuffer` from a daemon thread.

    The thread flushes every `flush_interval` seconds, and sooner once `batch_size` reports are
    pending. Each request carries at most `REPORT_BATCH_MAXIMUM` reports of at most
    `BATCH_REPORTS_MAXIMUM_BYTES` together, and the count of dropped reports not yet
    acknowledged by the server. A single report larger than that is dropped and counted
    instead of queued. A batch that cannot reach the server is retried after each of
    `RETRY_DELAYS_SECONDS`, then put back in the buffer. A batch the server answers with a 5xx
    or 429 goes back to the buffer without retries. A batch the server rejects with any other
    status, or answers with a body that is not JSON, is dropped with a warning and added to the
    dropped count. Single reports rejected inside a 2xx answer are dropped with a warning.
    """

    def __init__(
        self,
        http_client: httpx.Client,
        buffer: ReportBuffer,
        *,
        flush_interval: float,
        batch_size: int,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        """Creates the flusher without starting its thread.

        Args:
            http_client: Client sending the batches, shared with the rest of the SDK.
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
        self._started = False
        self._thread: threading.Thread | None = None
        self._thread_lock = threading.Lock()
        self._flush_lock = threading.Lock()
        self._wake = threading.Event()
        self._stop_requested = threading.Event()

    def start(self) -> None:
        """Starts the thread and registers a flush at interpreter exit.

        The exit flush waits at most `EXIT_FLUSH_TIMEOUT_SECONDS`.
        """
        self._started = True
        atexit.register(self._stop_at_exit)
        self._start_thread()

    def add(self, report: PendingReport) -> None:
        """Queues a report and wakes the thread once `batch_size` reports are pending.

        A report larger than `BATCH_REPORTS_MAXIMUM_BYTES` is dropped, counted and logged
        instead. In a forked child the first report starts the child's thread.

        Raises:
            RuntimeError: The flusher was stopped.
        """
        if self._stop_requested.is_set():
            raise RuntimeError("client closed")
        if len(report) > BATCH_REPORTS_MAXIMUM_BYTES:
            drop_oversized_report(self._buffer, report)
            return
        if self._started and self._thread is None:
            self._start_thread()
        self._buffer.add(report)
        if len(self._buffer) >= self._batch_size:
            self._wake.set()

    def flush(self) -> bool:
        """Sends every pending report now, one batch after the other.

        A batch lost to an unexpected exception is added to the dropped count before the
        exception propagates.

        Returns:
            False when a batch went back to the buffer, else True.
        """
        with self._flush_lock:
            while batch := self._buffer.take_batch(
                REPORT_BATCH_MAXIMUM, BATCH_REPORTS_MAXIMUM_BYTES
            ):
                try:
                    sent = self._send(batch)
                except Exception:
                    self._buffer.count_dropped(len(batch))
                    raise
                if not sent:
                    return False
            return True

    def stop(self, timeout: float | None = None) -> bool:
        """Flushes once more on the thread, ends it and removes the exit flush.

        Args:
            timeout: Seconds to wait for the thread, None to wait until it ends.

        Returns:
            True when the thread ended within the timeout.
        """
        atexit.unregister(self._stop_at_exit)
        return self._stop_thread(timeout)

    def reset_after_fork(self, http_client: httpx.Client) -> None:
        """Gives a forked child its own client, locks, events and empty buffer.

        The parent's pending reports stay with the parent, and the parent's threads may have
        held the locks when the process forked. A started flusher starts the child's thread
        with the child's first report.

        Args:
            http_client: Client of the child, with connections the parent does not share.
        """
        self._http_client = http_client
        self._buffer.reset_after_fork()
        self._thread = None
        self._thread_lock = threading.Lock()
        self._flush_lock = threading.Lock()
        self._wake = threading.Event()
        self._stop_requested = threading.Event()

    def _start_thread(self) -> None:
        with self._thread_lock:
            if self._thread is None:
                self._thread = threading.Thread(target=self._run, name=THREAD_NAME, daemon=True)
                self._thread.start()

    def _stop_at_exit(self) -> None:
        self._stop_thread(EXIT_FLUSH_TIMEOUT_SECONDS)

    def _stop_thread(self, timeout: float | None) -> bool:
        self._stop_requested.set()
        self._wake.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout)
            if thread.is_alive():
                logger.warning(
                    f"{REPORTS_FLUSH_TIMED_OUT_EVENT} pending={len(self._buffer)} "
                    f"timeout_seconds={timeout}"
                )
                return False
        pending = len(self._buffer)
        if pending:
            logger.warning(f"{REPORTS_UNSENT_EVENT} pending={pending}")
        return True

    def _run(self) -> None:
        while True:
            self._wake.wait(self._flush_interval)
            self._wake.clear()
            stopping = self._stop_requested.is_set()
            emptied = self._flush_logging_failures()
            if stopping:
                return
            if not emptied:
                self._stop_requested.wait(self._flush_interval)

    def _flush_logging_failures(self) -> bool:
        try:
            return self.flush()
        except Exception:
            logger.exception(f"{REPORTS_FLUSH_FAILED_EVENT} pending={len(self._buffer)}")
            return False

    def _send(self, batch: list[PendingReport]) -> bool:
        dropped = self._buffer.dropped
        headers = build_batch_headers(dropped)
        content = build_batch_content(batch)
        for retry_delay in (*RETRY_DELAYS_SECONDS, None):
            try:
                response = self._http_client.post(REPORTS_PATH, content=content, headers=headers)
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
            self._sleep(retry_delay)
        requeue_batch(self._buffer, batch, cause)
        return False


def build_batch_content(batch: list[PendingReport]) -> bytes:
    """Builds the JSON body of `POST /api/v1/reports` from encoded reports."""
    return BATCH_CONTENT_PREFIX + b",".join(batch) + BATCH_CONTENT_SUFFIX


def build_batch_headers(dropped: int) -> dict[str, str]:
    """Builds the headers of a batch, with the dropped count when there is one."""
    if not dropped:
        return BATCH_HEADERS
    return {**BATCH_HEADERS, DROPPED_REPORTS_HEADER: str(dropped)}


def read_batch_answer(
    response: httpx.Response, report_count: int
) -> list[ReportResult | PreburnError] | None:
    """Returns the per-report results of a batch answer, or None for a fallback-eligible status.

    Raises:
        PreburnError: The server rejected the batch, or a 2xx body is not JSON or holds a
            result count other than `report_count` (code `unexpected_response`).
    """
    if classify(response) is Classification.FALLBACK_ELIGIBLE:
        return None
    results = parse_report_batch(read_json_body(response))
    if len(results) != report_count:
        raise APIError(
            UNEXPECTED_RESPONSE_CODE,
            f"batch answer result count differs results={len(results)} reports={report_count}",
            status=response.status_code,
        )
    return results


def record_batch_results(
    buffer: ReportBuffer,
    batch: list[PendingReport],
    results: list[ReportResult | PreburnError],
) -> bool:
    """Applies the per-report results of a batch the server answered with a 2xx.

    Reports that failed with a 5xx or 429 go back to the buffer. Reports the server rejected
    with any other status are dropped with a warning.

    Returns:
        False when a report went back to the buffer, else True.
    """
    failed_reports: list[PendingReport] = []
    for index, (report, result) in enumerate(zip(batch, results, strict=True)):
        if not isinstance(result, PreburnError):
            continue
        if _is_retryable(result):
            failed_reports.append(report)
            event = REPORTS_REQUEUED_EVENT
        else:
            event = REPORTS_REJECTED_EVENT
        logger.warning(f"{event} index={index} status={result.status} code={result.code}")
    buffer.requeue_front(failed_reports)
    return not failed_reports


def record_rejected_batch(
    buffer: ReportBuffer, batch: list[PendingReport], error: PreburnError
) -> bool:
    """Handles a batch the server answered with an error status or an unexpected body.

    A 5xx or 429 puts the batch back in the buffer. Any other status drops the batch with a
    warning and adds its reports to the dropped count.

    Returns:
        False when the batch went back to the buffer, else True.
    """
    if _is_retryable(error):
        requeue_batch(buffer, batch, f"status={error.status} code={error.code}")
        return False
    buffer.count_dropped(len(batch))
    logger.warning(
        f"{REPORTS_REJECTED_EVENT} reports={len(batch)} status={error.status} code={error.code}"
    )
    return True


def requeue_batch(buffer: ReportBuffer, batch: list[PendingReport], cause: str) -> None:
    """Puts an unsent batch back in front of the buffer and logs the `key=value` cause."""
    buffer.requeue_front(batch)
    logger.warning(f"{REPORTS_REQUEUED_EVENT} reports={len(batch)} {cause}")


def drop_oversized_report(buffer: ReportBuffer, report: PendingReport) -> None:
    """Counts a report too large for any batch as dropped and logs its size."""
    buffer.count_dropped(1)
    logger.warning(
        f"{REPORTS_OVERSIZED_EVENT} bytes={len(report)} maximum_bytes={BATCH_REPORTS_MAXIMUM_BYTES}"
    )


def _is_retryable(error: PreburnError) -> bool:
    status = error.status
    return status is not None and (status == RATE_LIMITED_STATUS or status >= SERVER_ERROR_STATUS)
