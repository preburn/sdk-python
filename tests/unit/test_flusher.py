import json
import logging
import secrets
import threading
from collections.abc import Callable, Iterator

import httpx
import pytest

from preburn._buffer import PendingReport, ReportBuffer
from preburn._flusher import BATCH_REPORTS_MAXIMUM_BYTES, RETRY_DELAYS_SECONDS, ReportFlusher
from preburn._transport import (
    DROPPED_REPORTS_HEADER,
    REPORT_BATCH_MAXIMUM_BYTES,
    REPORTS_PATH,
    build_client,
)

API_KEY = f"pb_test_runtime_{secrets.token_hex(16)}"
BASE_URL = "http://preburn.test"
WAIT_SECONDS = 5.0
IDLE_INTERVAL_SECONDS = 3600.0
LARGE_PADDING = "x" * 300_000

Answer = Callable[[httpx.Request], httpx.Response]


class ScriptedServer:
    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.received = threading.Event()
        self._answers: list[Answer] = [build_accepted_response]
        self._lock = threading.Lock()

    def plan(self, *answers: Answer) -> None:
        with self._lock:
            self._answers = list(answers)

    def handle(self, request: httpx.Request) -> httpx.Response:
        with self._lock:
            self.requests.append(request)
            answer = self._answers.pop(0) if len(self._answers) > 1 else self._answers[0]
        self.received.set()
        return answer(request)


def make_report_body(index: int, padding: str = "") -> dict[str, object]:
    body: dict[str, object] = {
        "decision_source": "server",
        "decision_id": f"dec_{index}",
        "usage": {"input_tokens": "10"},
    }
    if padding:
        body["attributes"] = {"padding": padding}
    return body


def make_report(index: int, padding: str = "") -> PendingReport:
    return json.dumps(make_report_body(index, padding)).encode()


def make_result(index: int) -> dict[str, object]:
    return {
        "status": 202,
        "result": {
            "ledger_entry_id": f"led_{index}",
            "cost": "0.100000000",
            "cost_status": "costed",
            "duplicate": False,
        },
    }


def build_accepted_response(request: httpx.Request) -> httpx.Response:
    reports = json.loads(request.content)["reports"]
    return httpx.Response(
        202, json={"results": [make_result(index) for index in range(len(reports))]}
    )


def build_redirect_response(request: httpx.Request) -> httpx.Response:
    return httpx.Response(301, headers={"Location": f"https://preburn.test{REPORTS_PATH}"})


def make_problem_answer(status: int, code: str, headers: dict[str, str] | None = None) -> Answer:
    def build_response(request: httpx.Request) -> httpx.Response:
        body = {"title": "Problem", "status": status, "detail": f"detail of {code}", "code": code}
        return httpx.Response(status, json=body, headers=headers)

    return build_response


def make_results_answer(results: list[dict[str, object]]) -> Answer:
    def build_response(request: httpx.Request) -> httpx.Response:
        return httpx.Response(202, json={"results": results})

    return build_response


def make_raising_answer(error: Exception) -> Answer:
    def build_response(request: httpx.Request) -> httpx.Response:
        raise error

    return build_response


def collect_sent_decision_ids(request: httpx.Request) -> list[object]:
    return [report["decision_id"] for report in json.loads(request.content)["reports"]]


def collect_pending_decision_ids(buffer: ReportBuffer) -> list[object]:
    return [
        json.loads(report)["decision_id"]
        for report in buffer.take_batch(10_000, REPORT_BATCH_MAXIMUM_BYTES * 100)
    ]


@pytest.fixture(name="server")
def make_server() -> ScriptedServer:
    return ScriptedServer()


@pytest.fixture(name="http_client")
def make_http_client(server: ScriptedServer) -> Iterator[httpx.Client]:
    transport = httpx.MockTransport(server.handle)
    with build_client(API_KEY, BASE_URL, httpx.Timeout(5.0), transport) as client:
        yield client


def make_flusher(
    http_client: httpx.Client,
    buffer: ReportBuffer,
    *,
    batch_size: int = 100,
    flush_interval: float = IDLE_INTERVAL_SECONDS,
    sleeps: list[float] | None = None,
) -> ReportFlusher:
    recorded_sleeps: list[float] = [] if sleeps is None else sleeps
    return ReportFlusher(
        http_client,
        buffer,
        flush_interval=flush_interval,
        batch_size=batch_size,
        sleep=recorded_sleeps.append,
    )


def test_flush_sends_pending_reports_in_one_batch(
    server: ScriptedServer, http_client: httpx.Client
) -> None:
    buffer = ReportBuffer(max_pending=100)
    flusher = make_flusher(http_client, buffer)
    for index in range(3):
        flusher.add(make_report(index))
    if not flusher.flush():
        pytest.fail("flush reported requeued reports")
    if len(server.requests) != 1:
        pytest.fail(f"requests={len(server.requests)}")
    request = server.requests[0]
    if (request.method, request.url.path) != ("POST", REPORTS_PATH):
        pytest.fail(f"method={request.method} path={request.url.path}")
    if json.loads(request.content) != {"reports": [make_report_body(index) for index in range(3)]}:
        pytest.fail(f"content={request.content!r}")
    if request.headers["Content-Type"] != "application/json":
        pytest.fail(f"content_type={request.headers['Content-Type']}")
    if DROPPED_REPORTS_HEADER in request.headers:
        pytest.fail("dropped header sent without drops")
    if len(buffer) != 0:
        pytest.fail(f"pending={len(buffer)}")


def test_flush_splits_batches_at_the_server_maximum(
    server: ScriptedServer, http_client: httpx.Client
) -> None:
    buffer = ReportBuffer(max_pending=1000)
    flusher = make_flusher(http_client, buffer, batch_size=1000)
    for index in range(501):
        flusher.add(make_report(index))
    flusher.flush()
    sizes = [len(collect_sent_decision_ids(request)) for request in server.requests]
    if sizes != [500, 1]:
        pytest.fail(f"batch_sizes={sizes}")
    if collect_sent_decision_ids(server.requests[1]) != ["dec_500"]:
        pytest.fail(f"second_batch={collect_sent_decision_ids(server.requests[1])}")


def test_flush_splits_batches_below_the_byte_maximum(
    server: ScriptedServer, http_client: httpx.Client
) -> None:
    buffer = ReportBuffer(max_pending=100)
    flusher = make_flusher(http_client, buffer)
    for index in range(8):
        flusher.add(make_report(index, LARGE_PADDING))
    if not flusher.flush():
        pytest.fail("flush reported requeued reports")
    sizes = [len(request.content) for request in server.requests]
    if len(sizes) < 3 or max(sizes) > REPORT_BATCH_MAXIMUM_BYTES:
        pytest.fail(f"request_sizes={sizes}")
    delivered = [
        decision_id
        for request in server.requests
        for decision_id in collect_sent_decision_ids(request)
    ]
    if delivered != [f"dec_{index}" for index in range(8)]:
        pytest.fail(f"delivered={delivered}")


def test_oversized_report_is_dropped_counted_and_logged(
    server: ScriptedServer, http_client: httpx.Client, caplog: pytest.LogCaptureFixture
) -> None:
    buffer = ReportBuffer(max_pending=100)
    flusher = make_flusher(http_client, buffer)
    oversized = make_report(0, "x" * REPORT_BATCH_MAXIMUM_BYTES)
    with caplog.at_level(logging.WARNING, logger="preburn"):
        flusher.add(oversized)
    if len(buffer) != 0 or buffer.dropped != 1:
        pytest.fail(f"pending={len(buffer)} dropped={buffer.dropped}")
    expected = (
        f"reports.oversized bytes={len(oversized)} maximum_bytes={BATCH_REPORTS_MAXIMUM_BYTES}"
    )
    if caplog.messages != [expected]:
        pytest.fail(f"messages={caplog.messages}")
    flusher.add(make_report(1))
    flusher.flush()
    if server.requests[0].headers.get(DROPPED_REPORTS_HEADER) != "1":
        pytest.fail(f"headers={server.requests[0].headers}")


@pytest.mark.parametrize(
    "failure",
    [
        make_problem_answer(503, "counters_unavailable"),
        make_problem_answer(502, "bad_gateway"),
        make_raising_answer(httpx.ConnectError("connection refused")),
        make_raising_answer(httpx.ReadTimeout("read timed out")),
        make_raising_answer(httpx.DecodingError("broken gzip")),
    ],
)
def test_unreachable_server_retries_with_backoff_then_requeues(
    server: ScriptedServer, http_client: httpx.Client, failure: Answer
) -> None:
    server.plan(failure)
    buffer = ReportBuffer(max_pending=100)
    sleeps: list[float] = []
    flusher = make_flusher(http_client, buffer, sleeps=sleeps)
    for index in range(3):
        flusher.add(make_report(index))
    if flusher.flush():
        pytest.fail("flush reported an empty buffer after failing")
    if len(server.requests) != 1 + len(RETRY_DELAYS_SECONDS):
        pytest.fail(f"attempts={len(server.requests)}")
    if sleeps != [0.5, 1.0, 2.0]:
        pytest.fail(f"sleeps={sleeps}")
    if collect_pending_decision_ids(buffer) != ["dec_0", "dec_1", "dec_2"]:
        pytest.fail("reports not kept in order")


def test_retry_that_succeeds_delivers_the_batch(
    server: ScriptedServer, http_client: httpx.Client
) -> None:
    server.plan(
        make_problem_answer(503, "counters_unavailable"),
        make_raising_answer(httpx.ConnectError("connection refused")),
        build_accepted_response,
    )
    buffer = ReportBuffer(max_pending=100)
    sleeps: list[float] = []
    flusher = make_flusher(http_client, buffer, sleeps=sleeps)
    flusher.add(make_report(0))
    if not flusher.flush():
        pytest.fail("flush reported requeued reports")
    if sleeps != [0.5, 1.0] or len(server.requests) != 3:
        pytest.fail(f"sleeps={sleeps} attempts={len(server.requests)}")
    if len(buffer) != 0:
        pytest.fail(f"pending={len(buffer)}")


@pytest.mark.parametrize(
    "failure",
    [
        make_problem_answer(500, "internal_error"),
        make_problem_answer(429, "rate_limited", headers={"Retry-After": "3"}),
    ],
)
def test_server_error_requeues_without_retrying(
    server: ScriptedServer,
    http_client: httpx.Client,
    failure: Answer,
    caplog: pytest.LogCaptureFixture,
) -> None:
    server.plan(failure, build_accepted_response)
    buffer = ReportBuffer(max_pending=100)
    sleeps: list[float] = []
    flusher = make_flusher(http_client, buffer, sleeps=sleeps)
    flusher.add(make_report(0))
    flusher.add(make_report(1))
    with caplog.at_level(logging.WARNING, logger="preburn"):
        if flusher.flush():
            pytest.fail("flush reported an empty buffer after a server error")
    if len(server.requests) != 1 or sleeps != []:
        pytest.fail(f"attempts={len(server.requests)} sleeps={sleeps}")
    if len(buffer) != 2:
        pytest.fail(f"pending={len(buffer)}")
    if not any(record.getMessage().startswith("reports.requeued") for record in caplog.records):
        pytest.fail(f"messages={caplog.messages}")
    if not flusher.flush():
        pytest.fail("second flush failed")
    if collect_sent_decision_ids(server.requests[1]) != ["dec_0", "dec_1"]:
        pytest.fail(f"second_batch={collect_sent_decision_ids(server.requests[1])}")


def test_dropped_count_is_sent_until_a_flush_succeeds(
    server: ScriptedServer, http_client: httpx.Client
) -> None:
    server.plan(make_problem_answer(500, "internal_error"), build_accepted_response)
    buffer = ReportBuffer(max_pending=2)
    flusher = make_flusher(http_client, buffer, batch_size=2)
    for index in range(3):
        flusher.add(make_report(index))
    flusher.flush()
    flusher.flush()
    flusher.add(make_report(3))
    flusher.flush()
    headers = [request.headers.get(DROPPED_REPORTS_HEADER) for request in server.requests]
    if headers != ["1", "1", None]:
        pytest.fail(f"dropped_headers={headers}")
    if buffer.dropped != 0:
        pytest.fail(f"dropped={buffer.dropped}")
    if collect_sent_decision_ids(server.requests[1]) != ["dec_1", "dec_2"]:
        pytest.fail(f"delivered={collect_sent_decision_ids(server.requests[1])}")


def test_rejected_reports_are_dropped_with_a_warning(
    server: ScriptedServer, http_client: httpx.Client, caplog: pytest.LogCaptureFixture
) -> None:
    rejected = {
        "status": 422,
        "error": {
            "code": "validation_failed",
            "detail": "report is invalid",
            "errors": [{"location": "body.reports[1].usage", "message": "unknown meter"}],
        },
    }
    server.plan(make_results_answer([make_result(0), rejected]))
    buffer = ReportBuffer(max_pending=100)
    flusher = make_flusher(http_client, buffer)
    flusher.add(make_report(0))
    flusher.add(make_report(1))
    with caplog.at_level(logging.WARNING, logger="preburn"):
        if not flusher.flush():
            pytest.fail("flush reported requeued reports")
    if len(buffer) != 0:
        pytest.fail(f"pending={len(buffer)}")
    if caplog.messages != ["reports.rejected index=1 status=422 code=validation_failed"]:
        pytest.fail(f"messages={caplog.messages}")


def test_reports_failing_with_a_server_error_are_requeued(
    server: ScriptedServer, http_client: httpx.Client
) -> None:
    failed = {"status": 500, "error": {"code": "internal_error", "detail": "internal error"}}
    server.plan(make_results_answer([make_result(0), failed, make_result(2)]))
    buffer = ReportBuffer(max_pending=100)
    flusher = make_flusher(http_client, buffer)
    for index in range(3):
        flusher.add(make_report(index))
    if flusher.flush():
        pytest.fail("flush reported an empty buffer after a failed report")
    if collect_pending_decision_ids(buffer) != ["dec_1"]:
        pytest.fail("failed report not requeued alone")


@pytest.mark.parametrize(
    ("rejection", "expected_message"),
    [
        (
            make_problem_answer(401, "authentication_required"),
            "reports.rejected reports=2 status=401 code=authentication_required",
        ),
        (
            build_redirect_response,
            "reports.rejected reports=2 status=301 code=unexpected_response",
        ),
    ],
)
def test_rejected_batch_is_dropped_and_counted_with_a_warning(
    server: ScriptedServer,
    http_client: httpx.Client,
    rejection: Answer,
    expected_message: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    server.plan(rejection, build_accepted_response)
    buffer = ReportBuffer(max_pending=100)
    sleeps: list[float] = []
    flusher = make_flusher(http_client, buffer, sleeps=sleeps)
    flusher.add(make_report(0))
    flusher.add(make_report(1))
    with caplog.at_level(logging.WARNING, logger="preburn"):
        flusher.flush()
    if len(buffer) != 0 or len(server.requests) != 1 or sleeps != []:
        pytest.fail(f"pending={len(buffer)} attempts={len(server.requests)} sleeps={sleeps}")
    if buffer.dropped != 2:
        pytest.fail(f"dropped={buffer.dropped}")
    if caplog.messages != [expected_message]:
        pytest.fail(f"messages={caplog.messages}")
    flusher.add(make_report(2))
    flusher.flush()
    dropped_header = server.requests[1].headers.get(DROPPED_REPORTS_HEADER)
    if dropped_header != "2" or buffer.dropped != 0:
        pytest.fail(f"dropped_header={dropped_header} dropped={buffer.dropped}")


def test_answer_that_is_not_json_drops_and_counts_the_batch(
    server: ScriptedServer, http_client: httpx.Client, caplog: pytest.LogCaptureFixture
) -> None:
    server.plan(lambda request: httpx.Response(202, text="<html>sign in</html>"))
    buffer = ReportBuffer(max_pending=100)
    sleeps: list[float] = []
    flusher = make_flusher(http_client, buffer, sleeps=sleeps)
    flusher.add(make_report(0))
    flusher.add(make_report(1))
    with caplog.at_level(logging.WARNING, logger="preburn"):
        flusher.flush()
    if len(buffer) != 0 or buffer.dropped != 2 or sleeps != []:
        pytest.fail(f"pending={len(buffer)} dropped={buffer.dropped} sleeps={sleeps}")
    if caplog.messages != ["reports.rejected reports=2 status=202 code=unexpected_response"]:
        pytest.fail(f"messages={caplog.messages}")


def test_batch_lost_to_an_unexpected_answer_counts_as_dropped(
    server: ScriptedServer, http_client: httpx.Client
) -> None:
    server.plan(
        lambda request: httpx.Response(202, json={"unexpected": True}), build_accepted_response
    )
    buffer = ReportBuffer(max_pending=2)
    flusher = make_flusher(http_client, buffer)
    for index in range(3):
        flusher.add(make_report(index))
    with pytest.raises(KeyError):
        flusher.flush()
    if len(buffer) != 0 or buffer.dropped != 3:
        pytest.fail(f"pending={len(buffer)} dropped={buffer.dropped}")
    flusher.add(make_report(3))
    flusher.flush()
    dropped_headers = [request.headers.get(DROPPED_REPORTS_HEADER) for request in server.requests]
    if dropped_headers != ["1", "3"] or buffer.dropped != 0:
        pytest.fail(f"dropped_headers={dropped_headers} dropped={buffer.dropped}")


def test_thread_flushes_once_batch_size_is_reached(
    server: ScriptedServer, http_client: httpx.Client
) -> None:
    flusher = make_flusher(http_client, ReportBuffer(max_pending=100), batch_size=2)
    flusher.start()
    try:
        flusher.add(make_report(0))
        flusher.add(make_report(1))
        if not server.received.wait(WAIT_SECONDS):
            pytest.fail("batch size did not start a flush")
    finally:
        flusher.stop()
    if collect_sent_decision_ids(server.requests[0]) != ["dec_0", "dec_1"]:
        pytest.fail(f"batch={collect_sent_decision_ids(server.requests[0])}")


def test_thread_flushes_every_interval(server: ScriptedServer, http_client: httpx.Client) -> None:
    flusher = make_flusher(http_client, ReportBuffer(max_pending=100), flush_interval=0.01)
    flusher.start()
    try:
        flusher.add(make_report(0))
        if not server.received.wait(WAIT_SECONDS):
            pytest.fail("interval did not start a flush")
    finally:
        flusher.stop()


def test_stop_flushes_pending_reports_and_ends_the_thread(
    server: ScriptedServer, http_client: httpx.Client
) -> None:
    buffer = ReportBuffer(max_pending=100)
    flusher = make_flusher(http_client, buffer)
    flusher.start()
    flusher.add(make_report(0))
    flusher.add(make_report(1))
    if not flusher.stop():
        pytest.fail("thread still running after stop")
    if len(server.requests) != 1 or collect_sent_decision_ids(server.requests[0]) != [
        "dec_0",
        "dec_1",
    ]:
        pytest.fail(f"requests={len(server.requests)}")
    if len(buffer) != 0:
        pytest.fail(f"pending={len(buffer)}")


def test_stop_gives_up_after_its_timeout(
    server: ScriptedServer, http_client: httpx.Client, caplog: pytest.LogCaptureFixture
) -> None:
    release = threading.Event()

    def build_response_after_release(request: httpx.Request) -> httpx.Response:
        release.wait(WAIT_SECONDS)
        return build_accepted_response(request)

    server.plan(build_response_after_release)
    flusher = make_flusher(http_client, ReportBuffer(max_pending=100))
    flusher.start()
    flusher.add(make_report(0))
    with caplog.at_level(logging.WARNING, logger="preburn"):
        finished = flusher.stop(timeout=0.05)
    release.set()
    flusher.stop()
    if finished:
        pytest.fail("stop finished while the flush was blocked")
    if caplog.messages != ["reports.flush_timed_out pending=0 timeout_seconds=0.05"]:
        pytest.fail(f"messages={caplog.messages}")


def test_stop_warns_about_reports_left_unsent(
    server: ScriptedServer, http_client: httpx.Client, caplog: pytest.LogCaptureFixture
) -> None:
    server.plan(make_problem_answer(500, "internal_error"))
    flusher = make_flusher(http_client, ReportBuffer(max_pending=100))
    flusher.start()
    flusher.add(make_report(0))
    with caplog.at_level(logging.WARNING, logger="preburn"):
        flusher.stop()
    if "reports.unsent pending=1" not in caplog.messages:
        pytest.fail(f"messages={caplog.messages}")


def test_add_after_stop_raises(http_client: httpx.Client) -> None:
    flusher = make_flusher(http_client, ReportBuffer(max_pending=100))
    flusher.start()
    flusher.stop()
    with pytest.raises(RuntimeError, match="client closed"):
        flusher.add(make_report(0))


def test_thread_logs_unexpected_errors_and_keeps_flushing(
    server: ScriptedServer, http_client: httpx.Client, caplog: pytest.LogCaptureFixture
) -> None:
    second_request = threading.Event()

    def build_second_response(request: httpx.Request) -> httpx.Response:
        second_request.set()
        return build_accepted_response(request)

    server.plan(
        lambda request: httpx.Response(202, json={"unexpected": True}), build_second_response
    )
    flusher = make_flusher(http_client, ReportBuffer(max_pending=100), flush_interval=0.01)
    with caplog.at_level(logging.WARNING, logger="preburn"):
        flusher.start()
        try:
            flusher.add(make_report(0))
            if not server.received.wait(WAIT_SECONDS):
                pytest.fail("first flush did not run")
            flusher.add(make_report(1))
            if not second_request.wait(WAIT_SECONDS):
                pytest.fail("thread stopped flushing after an unexpected error")
        finally:
            flusher.stop()
    failures = [
        record
        for record in caplog.records
        if record.getMessage().startswith("reports.flush_failed pending=")
    ]
    if len(failures) != 1 or failures[0].exc_info is None:
        pytest.fail(f"messages={caplog.messages}")
