import asyncio
import json
import logging
import secrets
import time
from dataclasses import dataclass, field

import httpx
import pytest

from preburn._async_flusher import AsyncReportFlusher
from preburn._buffer import PendingReport, ReportBuffer
from preburn._flusher import BATCH_REPORTS_MAXIMUM_BYTES, RETRY_DELAYS_SECONDS
from preburn._transport import (
    DROPPED_REPORTS_HEADER,
    REPORT_BATCH_MAXIMUM_BYTES,
    REPORTS_PATH,
    build_async_client,
)

API_KEY = f"pb_test_runtime_{secrets.token_hex(16)}"
BASE_URL = "http://preburn.test"
WAIT_SECONDS = 5.0
IDLE_INTERVAL_SECONDS = 3600.0
POLL_INTERVAL_SECONDS = 0.01
ACCEPTED_STATUS = 202
LARGE_PADDING = "x" * 300_000


@dataclass
class ScriptedServer:
    answers: list[int | list[int] | Exception | httpx.Response] = field(default_factory=list)
    requests: list[httpx.Request] = field(default_factory=list)
    received: asyncio.Event = field(default_factory=asyncio.Event)

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        self.received.set()
        reports = json.loads(request.content)["reports"]
        answer = self.answers.pop(0) if self.answers else [ACCEPTED_STATUS] * len(reports)
        if isinstance(answer, Exception):
            raise answer
        if isinstance(answer, httpx.Response):
            return answer
        if isinstance(answer, int):
            return make_problem(answer, f"code_{answer}")
        results = [make_item_result(index, status) for index, status in enumerate(answer)]
        return httpx.Response(ACCEPTED_STATUS, json={"results": results})

    def collect_sent_decision_ids(self, request_index: int) -> list[object]:
        reports = json.loads(self.requests[request_index].content)["reports"]
        return [report["decision_id"] for report in reports]


@dataclass
class RecordingSleep:
    delays: list[float] = field(default_factory=list)

    async def __call__(self, delay: float) -> None:
        self.delays.append(delay)


def make_report_body(index: int, padding: str = "") -> dict[str, object]:
    body: dict[str, object] = {"decision_source": "server", "decision_id": f"dec_{index}"}
    if padding:
        body["attributes"] = {"padding": padding}
    return body


def make_report(index: int, padding: str = "") -> PendingReport:
    return json.dumps(make_report_body(index, padding)).encode()


def make_item_result(index: int, status: int) -> dict[str, object]:
    if status == ACCEPTED_STATUS:
        return {
            "status": status,
            "result": {
                "ledger_entry_id": f"led_{index}",
                "cost": "0.100000000",
                "cost_status": "costed",
                "duplicate": False,
            },
        }
    return {"status": status, "error": {"code": f"code_{status}", "detail": "report failed"}}


def make_problem(status: int, code: str) -> httpx.Response:
    headers = {"Content-Type": "application/problem+json", "Retry-After": "3"}
    return httpx.Response(
        status,
        json={"title": "Problem", "status": status, "detail": "batch failed", "code": code},
        headers=headers,
    )


def make_flusher(
    server: ScriptedServer,
    buffer: ReportBuffer,
    *,
    sleep: RecordingSleep | None = None,
    batch_size: int = 100,
    flush_interval: float = IDLE_INTERVAL_SECONDS,
) -> AsyncReportFlusher:
    http_client = build_async_client(
        API_KEY, BASE_URL, httpx.Timeout(WAIT_SECONDS), httpx.MockTransport(server.handle)
    )
    return AsyncReportFlusher(
        lambda: http_client,
        buffer,
        flush_interval=flush_interval,
        batch_size=batch_size,
        sleep=RecordingSleep() if sleep is None else sleep,
    )


def add_reports(flusher: AsyncReportFlusher, count: int, start: int = 0) -> None:
    for index in range(start, start + count):
        flusher.add(make_report(index))


def collect_pending_decision_ids(buffer: ReportBuffer) -> list[object]:
    return [
        json.loads(report)["decision_id"]
        for report in buffer.take_batch(10_000, REPORT_BATCH_MAXIMUM_BYTES * 100)
    ]


async def wait_for_requests(server: ScriptedServer, count: int) -> None:
    deadline = time.monotonic() + WAIT_SECONDS
    while len(server.requests) < count:
        if time.monotonic() > deadline:
            pytest.fail(f"requests={len(server.requests)} expected={count}")
        await asyncio.sleep(POLL_INTERVAL_SECONDS)


def collect_other_tasks() -> set[asyncio.Task[object]]:
    return {task for task in asyncio.all_tasks() if task is not asyncio.current_task()}


@pytest.mark.asyncio
async def test_flush_sends_pending_reports_in_one_batch() -> None:
    server = ScriptedServer()
    buffer = ReportBuffer(max_pending=10)
    flusher = make_flusher(server, buffer)
    add_reports(flusher, 3)
    if not await flusher.flush():
        pytest.fail("flush reported requeued reports")
    if len(server.requests) != 1:
        pytest.fail(f"requests={len(server.requests)}")
    request = server.requests[0]
    if (request.method, request.url.path) != ("POST", REPORTS_PATH):
        pytest.fail(f"method={request.method} path={request.url.path}")
    if json.loads(request.content) != {"reports": [make_report_body(index) for index in range(3)]}:
        pytest.fail(f"content={request.content!r}")
    if DROPPED_REPORTS_HEADER in request.headers:
        pytest.fail(f"dropped_header={request.headers[DROPPED_REPORTS_HEADER]}")
    if len(buffer) != 0:
        pytest.fail(f"pending={len(buffer)}")
    await flusher.aclose()


@pytest.mark.asyncio
async def test_flush_splits_requests_at_report_batch_maximum() -> None:
    server = ScriptedServer()
    flusher = make_flusher(server, ReportBuffer(max_pending=1000), batch_size=1000)
    add_reports(flusher, 501)
    await flusher.flush()
    sizes = [len(json.loads(request.content)["reports"]) for request in server.requests]
    if sizes != [500, 1]:
        pytest.fail(f"sizes={sizes}")
    if server.collect_sent_decision_ids(1) != ["dec_500"]:
        pytest.fail(f"second_batch={server.collect_sent_decision_ids(1)}")
    await flusher.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        503,
        502,
        504,
        httpx.ConnectError("connection refused"),
        httpx.ReadTimeout("slow"),
        httpx.DecodingError("broken gzip"),
    ],
)
async def test_unreachable_server_retries_with_backoff_then_requeues(
    failure: int | Exception,
) -> None:
    server = ScriptedServer(answers=[failure] * (len(RETRY_DELAYS_SECONDS) + 1))
    buffer = ReportBuffer(max_pending=10)
    sleep = RecordingSleep()
    flusher = make_flusher(server, buffer, sleep=sleep)
    add_reports(flusher, 3)
    if await flusher.flush():
        pytest.fail("flush reported an empty buffer after failing")
    if len(server.requests) != len(RETRY_DELAYS_SECONDS) + 1:
        pytest.fail(f"attempts={len(server.requests)}")
    if sleep.delays != [0.5, 1.0, 2.0]:
        pytest.fail(f"delays={sleep.delays}")
    if collect_pending_decision_ids(buffer) != ["dec_0", "dec_1", "dec_2"]:
        pytest.fail("reports not kept in order")


@pytest.mark.asyncio
async def test_retry_that_succeeds_delivers_the_batch() -> None:
    server = ScriptedServer(answers=[503, httpx.ConnectError("connection refused")])
    buffer = ReportBuffer(max_pending=10)
    sleep = RecordingSleep()
    flusher = make_flusher(server, buffer, sleep=sleep)
    add_reports(flusher, 1)
    if not await flusher.flush():
        pytest.fail("flush reported requeued reports")
    if sleep.delays != [0.5, 1.0] or len(server.requests) != 3:
        pytest.fail(f"delays={sleep.delays} attempts={len(server.requests)}")
    if len(buffer) != 0:
        pytest.fail(f"pending={len(buffer)}")
    await flusher.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [500, 429])
async def test_server_error_requeues_without_retrying(
    status: int, caplog: pytest.LogCaptureFixture
) -> None:
    server = ScriptedServer(answers=[status])
    buffer = ReportBuffer(max_pending=10)
    sleep = RecordingSleep()
    flusher = make_flusher(server, buffer, sleep=sleep)
    add_reports(flusher, 2)
    with caplog.at_level(logging.WARNING, logger="preburn"):
        if await flusher.flush():
            pytest.fail("flush reported an empty buffer after a server error")
    if len(server.requests) != 1 or sleep.delays != []:
        pytest.fail(f"attempts={len(server.requests)} delays={sleep.delays}")
    if len(buffer) != 2:
        pytest.fail(f"pending={len(buffer)}")
    if not any(message.startswith("reports.requeued") for message in caplog.messages):
        pytest.fail(f"messages={caplog.messages}")
    if not await flusher.flush():
        pytest.fail("second flush failed")
    if server.collect_sent_decision_ids(1) != ["dec_0", "dec_1"]:
        pytest.fail(f"second_batch={server.collect_sent_decision_ids(1)}")
    await flusher.aclose()


@pytest.mark.asyncio
async def test_dropped_count_is_sent_until_a_flush_succeeds() -> None:
    server = ScriptedServer(answers=[500])
    buffer = ReportBuffer(max_pending=2)
    flusher = make_flusher(server, buffer)
    add_reports(flusher, 3)
    await flusher.flush()
    await flusher.flush()
    add_reports(flusher, 1, start=3)
    await flusher.flush()
    headers = [request.headers.get(DROPPED_REPORTS_HEADER) for request in server.requests]
    if headers != ["1", "1", None]:
        pytest.fail(f"dropped_headers={headers}")
    if buffer.dropped != 0:
        pytest.fail(f"dropped={buffer.dropped}")
    if server.collect_sent_decision_ids(1) != ["dec_1", "dec_2"]:
        pytest.fail(f"delivered={server.collect_sent_decision_ids(1)}")
    await flusher.aclose()


@pytest.mark.asyncio
async def test_rejected_reports_are_dropped_with_a_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    server = ScriptedServer(answers=[[ACCEPTED_STATUS, 422, 409]])
    buffer = ReportBuffer(max_pending=10)
    flusher = make_flusher(server, buffer)
    add_reports(flusher, 3)
    with caplog.at_level(logging.WARNING, logger="preburn"):
        if not await flusher.flush():
            pytest.fail("flush reported requeued reports")
    if len(buffer) != 0:
        pytest.fail(f"pending={len(buffer)}")
    expected = [
        "reports.rejected index=1 status=422 code=code_422",
        "reports.rejected index=2 status=409 code=code_409",
    ]
    if caplog.messages != expected:
        pytest.fail(f"messages={caplog.messages}")
    await flusher.aclose()


@pytest.mark.asyncio
async def test_reports_failing_with_a_server_error_are_requeued() -> None:
    server = ScriptedServer(answers=[[ACCEPTED_STATUS, 500, ACCEPTED_STATUS]])
    buffer = ReportBuffer(max_pending=10)
    flusher = make_flusher(server, buffer)
    add_reports(flusher, 3)
    if await flusher.flush():
        pytest.fail("flush reported an empty buffer after a failed report")
    if collect_pending_decision_ids(buffer) != ["dec_1"]:
        pytest.fail("failed report not requeued alone")


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [401, 301])
async def test_rejected_batch_is_dropped_and_counted_with_a_warning(
    status: int, caplog: pytest.LogCaptureFixture
) -> None:
    server = ScriptedServer(answers=[status])
    buffer = ReportBuffer(max_pending=10)
    sleep = RecordingSleep()
    flusher = make_flusher(server, buffer, sleep=sleep)
    add_reports(flusher, 2)
    with caplog.at_level(logging.WARNING, logger="preburn"):
        await flusher.flush()
    if len(buffer) != 0 or len(server.requests) != 1 or sleep.delays != []:
        pytest.fail(f"pending={len(buffer)} attempts={len(server.requests)} delays={sleep.delays}")
    if buffer.dropped != 2:
        pytest.fail(f"dropped={buffer.dropped}")
    if caplog.messages != [f"reports.rejected reports=2 status={status} code=code_{status}"]:
        pytest.fail(f"messages={caplog.messages}")
    add_reports(flusher, 1, start=2)
    await flusher.flush()
    dropped_header = server.requests[1].headers.get(DROPPED_REPORTS_HEADER)
    if dropped_header != "2" or buffer.dropped != 0:
        pytest.fail(f"dropped_header={dropped_header} dropped={buffer.dropped}")
    await flusher.aclose()


@pytest.mark.asyncio
async def test_flush_splits_batches_below_the_byte_maximum() -> None:
    server = ScriptedServer()
    flusher = make_flusher(server, ReportBuffer(max_pending=10))
    for index in range(8):
        flusher.add(make_report(index, LARGE_PADDING))
    if not await flusher.flush():
        pytest.fail("flush reported requeued reports")
    sizes = [len(request.content) for request in server.requests]
    if len(sizes) < 3 or max(sizes) > REPORT_BATCH_MAXIMUM_BYTES:
        pytest.fail(f"request_sizes={sizes}")
    delivered = [
        decision_id
        for request_index in range(len(server.requests))
        for decision_id in server.collect_sent_decision_ids(request_index)
    ]
    if delivered != [f"dec_{index}" for index in range(8)]:
        pytest.fail(f"delivered={delivered}")
    await flusher.aclose()


@pytest.mark.asyncio
async def test_oversized_report_is_dropped_counted_and_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    buffer = ReportBuffer(max_pending=10)
    flusher = make_flusher(ScriptedServer(), buffer)
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
    await flusher.aclose()


@pytest.mark.asyncio
async def test_answer_that_is_not_json_drops_and_counts_the_batch(
    caplog: pytest.LogCaptureFixture,
) -> None:
    server = ScriptedServer(answers=[httpx.Response(ACCEPTED_STATUS, text="<html>sign in</html>")])
    buffer = ReportBuffer(max_pending=10)
    flusher = make_flusher(server, buffer)
    add_reports(flusher, 2)
    with caplog.at_level(logging.WARNING, logger="preburn"):
        await flusher.flush()
    if len(buffer) != 0 or buffer.dropped != 2:
        pytest.fail(f"pending={len(buffer)} dropped={buffer.dropped}")
    if caplog.messages != ["reports.rejected reports=2 status=202 code=unexpected_response"]:
        pytest.fail(f"messages={caplog.messages}")
    await flusher.aclose()


@pytest.mark.asyncio
async def test_batch_lost_to_an_unexpected_answer_counts_as_dropped() -> None:
    server = ScriptedServer(answers=[httpx.Response(ACCEPTED_STATUS, json={"unexpected": True})])
    buffer = ReportBuffer(max_pending=2)
    flusher = make_flusher(server, buffer)
    add_reports(flusher, 3)
    with pytest.raises(KeyError):
        await flusher.flush()
    if len(buffer) != 0 or buffer.dropped != 3:
        pytest.fail(f"pending={len(buffer)} dropped={buffer.dropped}")
    add_reports(flusher, 1, start=3)
    await flusher.flush()
    dropped_headers = [request.headers.get(DROPPED_REPORTS_HEADER) for request in server.requests]
    if dropped_headers != ["1", "3"] or buffer.dropped != 0:
        pytest.fail(f"dropped_headers={dropped_headers} dropped={buffer.dropped}")
    await flusher.aclose()


def test_flusher_restarts_its_task_in_each_new_event_loop() -> None:
    server = ScriptedServer()
    flusher = make_flusher(server, ReportBuffer(max_pending=10), flush_interval=0.01)

    async def add_and_wait(index: int) -> None:
        add_reports(flusher, 1, start=index)
        await wait_for_requests(server, index + 1)

    async def add_only() -> None:
        add_reports(flusher, 1, start=2)

    asyncio.run(add_and_wait(0))
    asyncio.run(add_and_wait(1))
    asyncio.run(add_only())
    asyncio.run(flusher.aclose())
    delivered = [
        decision_id
        for request_index in range(len(server.requests))
        for decision_id in server.collect_sent_decision_ids(request_index)
    ]
    if delivered != ["dec_0", "dec_1", "dec_2"]:
        pytest.fail(f"delivered={delivered}")


@pytest.mark.asyncio
async def test_task_flushes_once_batch_size_is_reached() -> None:
    server = ScriptedServer()
    flusher = make_flusher(server, ReportBuffer(max_pending=10), batch_size=2)
    add_reports(flusher, 1)
    await asyncio.sleep(0)
    if server.requests:
        pytest.fail(f"requests={len(server.requests)} before batch size")
    add_reports(flusher, 1, start=1)
    await asyncio.wait_for(server.received.wait(), WAIT_SECONDS)
    if server.collect_sent_decision_ids(0) != ["dec_0", "dec_1"]:
        pytest.fail(f"batch={server.collect_sent_decision_ids(0)}")
    await flusher.aclose()


@pytest.mark.asyncio
async def test_task_flushes_every_interval() -> None:
    server = ScriptedServer()
    flusher = make_flusher(server, ReportBuffer(max_pending=10), flush_interval=0.01)
    add_reports(flusher, 1)
    await asyncio.wait_for(server.received.wait(), WAIT_SECONDS)
    if server.collect_sent_decision_ids(0) != ["dec_0"]:
        pytest.fail(f"batch={server.collect_sent_decision_ids(0)}")
    await flusher.aclose()


@pytest.mark.asyncio
async def test_aclose_flushes_pending_reports_and_leaves_no_task() -> None:
    server = ScriptedServer()
    buffer = ReportBuffer(max_pending=10)
    flusher = make_flusher(server, buffer)
    add_reports(flusher, 2)
    await asyncio.sleep(0)
    if not collect_other_tasks():
        pytest.fail("background task not started")
    await asyncio.wait_for(flusher.aclose(), WAIT_SECONDS)
    if len(server.requests) != 1 or server.collect_sent_decision_ids(0) != ["dec_0", "dec_1"]:
        pytest.fail(f"requests={len(server.requests)}")
    if len(buffer) != 0 or collect_other_tasks():
        pytest.fail(f"pending={len(buffer)} tasks={collect_other_tasks()}")


@pytest.mark.asyncio
async def test_aclose_resends_a_batch_cancelled_in_flight() -> None:
    never_answered = asyncio.Event()
    first_request = asyncio.Event()
    requests: list[httpx.Request] = []

    async def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            first_request.set()
            await never_answered.wait()
        reports = json.loads(request.content)["reports"]
        results = [make_item_result(index, ACCEPTED_STATUS) for index in range(len(reports))]
        return httpx.Response(ACCEPTED_STATUS, json={"results": results})

    buffer = ReportBuffer(max_pending=10)
    http_client = build_async_client(
        API_KEY, BASE_URL, httpx.Timeout(WAIT_SECONDS), httpx.MockTransport(handle)
    )
    flusher = AsyncReportFlusher(
        lambda: http_client, buffer, flush_interval=IDLE_INTERVAL_SECONDS, batch_size=1
    )
    flusher.add(make_report(0))
    await asyncio.wait_for(first_request.wait(), WAIT_SECONDS)
    await asyncio.wait_for(flusher.aclose(), WAIT_SECONDS)
    if len(requests) != 2 or json.loads(requests[1].content)["reports"] != [make_report_body(0)]:
        pytest.fail(f"requests={len(requests)}")
    if len(buffer) != 0 or collect_other_tasks():
        pytest.fail(f"pending={len(buffer)} tasks={collect_other_tasks()}")


@pytest.mark.asyncio
async def test_aclose_warns_about_reports_left_unsent(caplog: pytest.LogCaptureFixture) -> None:
    server = ScriptedServer(answers=[500])
    flusher = make_flusher(server, ReportBuffer(max_pending=10))
    add_reports(flusher, 1)
    with caplog.at_level(logging.WARNING, logger="preburn"):
        await flusher.aclose()
    if "reports.unsent pending=1" not in caplog.messages:
        pytest.fail(f"messages={caplog.messages}")


@pytest.mark.asyncio
async def test_add_after_aclose_raises() -> None:
    flusher = make_flusher(ScriptedServer(), ReportBuffer(max_pending=10))
    await flusher.aclose()
    with pytest.raises(RuntimeError, match="client closed"):
        flusher.add(make_report(0))


@pytest.mark.asyncio
async def test_task_logs_unexpected_errors_and_keeps_flushing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    requests: list[httpx.Request] = []
    first_request = asyncio.Event()
    second_request = asyncio.Event()

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            first_request.set()
            return httpx.Response(ACCEPTED_STATUS, json={"unexpected": True})
        second_request.set()
        results = [make_item_result(0, ACCEPTED_STATUS)]
        return httpx.Response(ACCEPTED_STATUS, json={"results": results})

    http_client = build_async_client(
        API_KEY, BASE_URL, httpx.Timeout(WAIT_SECONDS), httpx.MockTransport(handle)
    )
    flusher = AsyncReportFlusher(
        lambda: http_client, ReportBuffer(max_pending=10), flush_interval=0.01, batch_size=100
    )
    with caplog.at_level(logging.WARNING, logger="preburn"):
        flusher.add(make_report(0))
        await asyncio.wait_for(first_request.wait(), WAIT_SECONDS)
        flusher.add(make_report(1))
        await asyncio.wait_for(second_request.wait(), WAIT_SECONDS)
        await flusher.aclose()
    if json.loads(requests[1].content)["reports"] != [make_report_body(1)]:
        pytest.fail(f"second={requests[1].content!r}")
    failures = [
        record
        for record in caplog.records
        if record.getMessage().startswith("reports.flush_failed ")
    ]
    if len(failures) != 1 or failures[0].exc_info is None:
        pytest.fail(f"messages={caplog.messages}")
