import asyncio
import json
import logging
import secrets
import socket
import time
import traceback
import urllib.request
from collections.abc import AsyncIterator, Callable
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import httpx
import pytest

from preburn._async_client import AsyncPreburn
from preburn._client import ReportMode
from preburn._errors import (
    APIError,
    AuthenticationError,
    ConfigurationError,
    PreburnError,
    ValidationError,
)
from preburn._models import (
    Customer,
    Decision,
    ReportFields,
    ReportResult,
    RevenueEntry,
    parse_timestamp,
)
from preburn._transport import (
    CHECK_PATH,
    DROPPED_REPORTS_HEADER,
    RELEASE_PATH,
    REPORT_PATH,
    REPORTS_PATH,
    REVENUE_PATH,
    build_async_client,
)

API_KEY = f"pb_test_runtime_{secrets.token_hex(16)}"
BASE_URL = "http://preburn.test"
IDLE_INTERVAL_SECONDS = 3600.0
WAIT_SECONDS = 5.0
POLL_INTERVAL_SECONDS = 0.01
SLOW_PHASE_SECONDS = 0.15
CUSTOMER_PATH = "/api/v1/customers/customer_1"
REQUEST_TIMEOUT = {"connect": 5.0, "read": 5.0, "write": 5.0, "pool": 5.0}
SERVERLESS_VARIABLES = (
    "AWS_LAMBDA_FUNCTION_NAME",
    "K_SERVICE",
    "FUNCTIONS_WORKER_RUNTIME",
    "VERCEL",
)
DECISION_ID = "dec_01jbvagescfn78y0938nkrkayd"
USAGE: dict[str, Decimal | int] = {"input_tokens": 1000, "output_tokens": Decimal("250")}
SENT_USAGE = {"input_tokens": "1000", "output_tokens": "250"}
CHECK_RESPONSE: dict[str, object] = {
    "decision_id": DECISION_ID,
    "outcome": "allow",
    "reason": "no_policy_matched",
    "provider": "openai",
    "model": "gpt-6-sol",
    "overrides": {},
    "estimated_cost": "0.012000000",
    "reserved_amount": "0.012000000",
    "estimate_basis": "request_estimate",
    "cost_status": "costed",
    "matched_policy_id": None,
    "fallback_outcome": "allow",
    "expires_at": "2026-09-26T10:10:00Z",
    "signals": {
        "period_revenue_net": "30.000000000",
        "cost_allowance": "18.000000000",
        "cost_to_date": "4.250000000",
        "reserved": "0.012000000",
        "elapsed_fraction": "0.2500",
        "allowance_remaining": "13.738000000",
        "pace": "0.9444",
        "projected_margin": "0.4333",
        "request_estimated_cost": "0.012000000",
        "period_decision_count": 41,
        "features": {},
    },
}
REPORT_RESPONSE: dict[str, object] = {
    "ledger_entry_id": "led_01jbvagescfn78y0938nkrkayd",
    "cost": "0.011500000",
    "cost_status": "costed",
    "duplicate": False,
}
CUSTOMER_RESPONSE: dict[str, object] = {
    "id": "cust_01jbvagescfn78y0938nkrkayd",
    "external_id": "team:acme",
    "display_name": "Acme",
    "plan_id": "pln_01jbvagescfn78y0938nkrkayd",
    "metadata": {"tier": "gold"},
    "status": "active",
    "created_at": "2026-09-26T10:00:00Z",
    "updated_at": "2026-09-26T10:00:00Z",
}
REVENUE_RESPONSE: dict[str, object] = {
    "duplicate": False,
    "id": "rev_01jbvagescfn78y0938nkrkayd",
    "customer_id": "customer_1",
    "kind": "subscription",
    "amount": "30.000000000",
    "period_start": "2026-09-01T00:00:00Z",
    "period_end": "2026-10-01T00:00:00Z",
    "source": "api",
    "source_reference": "in_123",
    "occurred_at": "2026-09-01T00:00:00Z",
    "created_at": "2026-09-26T10:00:00Z",
}

Answer = Callable[[httpx.Request], httpx.Response]


class FakeServer:
    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self._answers: dict[str, list[Answer]] = {}

    def plan(self, path: str, *answers: Answer) -> None:
        self._answers[path] = list(answers)

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        answers = self._answers[request.url.raw_path.decode("ascii")]
        answer = answers.pop(0) if len(answers) > 1 else answers[0]
        return answer(request)

    def collect_requests(self, path: str) -> list[httpx.Request]:
        return [
            request for request in self.requests if request.url.raw_path.decode("ascii") == path
        ]


class LoopBoundTransport(httpx.AsyncBaseTransport):
    def __init__(self, handle: Answer) -> None:
        self._handle = handle
        self._loop: asyncio.AbstractEventLoop | None = None

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        loop = asyncio.get_running_loop()
        if self._loop is None:
            self._loop = loop
        if self._loop is not loop:
            raise RuntimeError("connection pool bound to a different event loop")
        await request.aread()
        return self._handle(request)


class SlowStream(httpx.AsyncByteStream):
    async def __aiter__(self) -> AsyncIterator[bytes]:
        await asyncio.sleep(SLOW_PHASE_SECONDS)
        yield json.dumps(CHECK_RESPONSE).encode()


def make_json_answer(status: int, body: dict[str, object]) -> Answer:
    def build_response(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, json=body)

    return build_response


def make_check_answer(fallback_outcome: str = "allow") -> Answer:
    return make_json_answer(200, {**CHECK_RESPONSE, "fallback_outcome": fallback_outcome})


def make_problem_answer(status: int, code: str) -> Answer:
    body: dict[str, object] = {
        "title": "Problem",
        "status": status,
        "detail": f"detail of {code}",
        "code": code,
    }
    return make_json_answer(status, body)


def make_raising_answer(error: Exception) -> Answer:
    def build_response(request: httpx.Request) -> httpx.Response:
        raise error

    return build_response


def make_text_answer(status: int, text: str) -> Answer:
    def build_response(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, text=text)

    return build_response


def make_undecodable_answer(status: int) -> Answer:
    def build_response(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status, headers={"Content-Encoding": "gzip"}, stream=httpx.ByteStream(b"not gzip")
        )

    return build_response


def build_batch_response(request: httpx.Request) -> httpx.Response:
    reports = json.loads(request.content)["reports"]
    results = [{"status": 202, "result": REPORT_RESPONSE} for report in reports]
    return httpx.Response(202, json={"results": results})


def parse_body(request: httpx.Request) -> Any:
    return json.loads(request.content)


def make_fields(customer_id: str = "customer_1") -> ReportFields:
    return ReportFields(
        customer_id=customer_id, feature="chat", provider="openai", model="gpt-6-sol"
    )


def make_client(
    server: FakeServer,
    *,
    report_mode: ReportMode = "sync",
    max_pending: int = 10_000,
) -> AsyncPreburn:
    return AsyncPreburn(
        api_key=API_KEY,
        base_url=BASE_URL,
        report_mode=report_mode,
        flush_interval=IDLE_INTERVAL_SECONDS,
        max_pending=max_pending,
        transport=httpx.MockTransport(server.handle),
    )


def collect_sent_customer_ids(server: FakeServer) -> list[str]:
    return [
        report["customer_id"]
        for request in server.collect_requests(REPORTS_PATH)
        for report in parse_body(request)["reports"]
    ]


async def wait_for_sent_reports(server: FakeServer, count: int) -> None:
    deadline = time.monotonic() + WAIT_SECONDS
    while len(collect_sent_customer_ids(server)) < count:
        if time.monotonic() > deadline:
            pytest.fail(f"sent={collect_sent_customer_ids(server)} expected={count}")
        await asyncio.sleep(POLL_INTERVAL_SECONDS)


def collect_other_tasks() -> set[asyncio.Task[object]]:
    return {task for task in asyncio.all_tasks() if task is not asyncio.current_task()}


@pytest.fixture(autouse=True)
def clear_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (*SERVERLESS_VARIABLES, "PREBURN_API_KEY", "PREBURN_BASE_URL"):
        monkeypatch.delenv(name, raising=False)


@pytest.fixture(name="server")
def make_server() -> FakeServer:
    return FakeServer()


@pytest.mark.asyncio
async def test_check_returns_the_server_decision(server: FakeServer) -> None:
    server.plan(CHECK_PATH, make_check_answer("deny"))
    async with make_client(server) as client:
        decision = await client.check(
            "customer_1",
            "chat",
            "openai",
            "gpt-6-sol",
            attributes={"service_tier": "priority"},
            usage_estimate=USAGE,
            usage_ceiling={"output_tokens": 4096},
            customer_user_id="user_7",
        )
    request = server.requests[0]
    expected_body = {
        "customer_id": "customer_1",
        "feature": "chat",
        "provider": "openai",
        "model": "gpt-6-sol",
        "attributes": {"service_tier": "priority"},
        "usage_estimate": SENT_USAGE,
        "usage_ceiling": {"output_tokens": "4096"},
        "customer_user_id": "user_7",
    }
    if (request.method, parse_body(request)) != ("POST", expected_body):
        pytest.fail(f"method={request.method} body={parse_body(request)}")
    if decision.is_fallback or decision.decision_id != DECISION_ID:
        pytest.fail(f"decision_id={decision.decision_id}")
    if (decision.outcome, decision.fallback_outcome) != ("allow", "deny"):
        pytest.fail(f"outcome={decision.outcome} fallback_outcome={decision.fallback_outcome}")
    if (decision.customer_id, decision.feature, decision.attributes) != (
        "customer_1",
        "chat",
        {"service_tier": "priority"},
    ):
        pytest.fail(f"customer_id={decision.customer_id} feature={decision.feature}")


@pytest.mark.asyncio
async def test_check_deadline_covers_connect_and_read(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def answer_slowly(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(SLOW_PHASE_SECONDS)
        return httpx.Response(200, stream=SlowStream())

    client = AsyncPreburn(
        api_key=API_KEY,
        base_url=BASE_URL,
        report_mode="sync",
        transport=httpx.MockTransport(answer_slowly),
    )
    async with client:
        with caplog.at_level(logging.WARNING, logger="preburn"):
            started = time.monotonic()
            decision = await client.check("customer_1", "chat", "openai", "gpt-6-sol")
            elapsed = time.monotonic() - started
    if not decision.is_fallback or decision.outcome != "allow":
        pytest.fail(f"decision_id={decision.decision_id} outcome={decision.outcome}")
    if elapsed >= SLOW_PHASE_SECONDS * 2:
        pytest.fail(f"elapsed={elapsed}")
    if client.check_timeout != 0.25:
        pytest.fail(f"check_timeout={client.check_timeout}")
    if caplog.messages != ["check.fallback feature=chat outcome=allow error=TimeoutError"]:
        pytest.fail(f"messages={caplog.messages}")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure", "cause"),
    [
        (make_raising_answer(httpx.ReadTimeout("read timed out")), "error=ReadTimeout"),
        (make_raising_answer(httpx.ConnectTimeout("connect timed out")), "error=ConnectTimeout"),
        (make_raising_answer(httpx.ConnectError("connection refused")), "error=ConnectError"),
        (make_problem_answer(502, "bad_gateway"), "status=502"),
        (make_problem_answer(503, "counters_unavailable"), "status=503"),
        (make_problem_answer(504, "gateway_timeout"), "status=504"),
        (make_undecodable_answer(502), "error=DecodingError"),
        (make_undecodable_answer(200), "error=DecodingError"),
        (make_text_answer(200, "<html>sign in</html>"), "status=200 error=JSONDecodeError"),
    ],
)
async def test_check_falls_back_when_preburn_cannot_answer(
    server: FakeServer, failure: Answer, cause: str, caplog: pytest.LogCaptureFixture
) -> None:
    server.plan(CHECK_PATH, failure)
    async with make_client(server) as client:
        with caplog.at_level(logging.WARNING, logger="preburn"):
            decision = await client.check(
                "customer_1",
                "chat",
                "openai",
                "gpt-6-sol",
                attributes={"service_tier": "priority"},
            )
    if not decision.is_fallback or decision.decision_id is not None:
        pytest.fail(f"decision_id={decision.decision_id}")
    if decision.outcome != "allow":
        pytest.fail(f"outcome={decision.outcome}")
    if (decision.provider, decision.model, decision.attributes) != (
        "openai",
        "gpt-6-sol",
        {"service_tier": "priority"},
    ):
        pytest.fail(f"provider={decision.provider} model={decision.model}")
    if decision.idempotency_key is None:
        pytest.fail("fallback decision without idempotency key")
    if caplog.messages != [f"check.fallback feature=chat outcome=allow {cause}"]:
        pytest.fail(f"messages={caplog.messages}")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "code", "error_class"),
    [
        (422, "validation_failed", ValidationError),
        (401, "authentication_required", AuthenticationError),
        (500, "internal_error", APIError),
    ],
)
async def test_check_raises_other_errors_without_fallback(
    server: FakeServer, status: int, code: str, error_class: type[PreburnError]
) -> None:
    server.plan(CHECK_PATH, make_problem_answer(status, code))
    async with make_client(server) as client:
        with pytest.raises(error_class) as raised:
            await client.check("customer_1", "chat", "openai", "gpt-6-sol")
    if (raised.value.status, raised.value.code) != (status, code):
        pytest.fail(f"status={raised.value.status} code={raised.value.code}")


@pytest.mark.asyncio
async def test_fallback_outcome_follows_the_last_server_fallback_outcome(
    server: FakeServer,
) -> None:
    unavailable = make_problem_answer(503, "counters_unavailable")
    server.plan(
        CHECK_PATH,
        make_check_answer("deny"),
        unavailable,
        unavailable,
        unavailable,
        make_check_answer("allow"),
        unavailable,
    )
    calls = [
        ("customer_1", "chat"),
        ("customer_1", "chat"),
        ("customer_2", "chat"),
        ("customer_1", "search"),
        ("customer_2", "chat"),
        ("customer_1", "chat"),
        ("customer_3", "chat"),
    ]
    async with make_client(server) as client:
        decisions = [
            await client.check(customer_id, feature, "openai", "gpt-6-sol")
            for customer_id, feature in calls
        ]
    fallback_outcomes = [
        decision.outcome if decision.is_fallback else "server" for decision in decisions
    ]
    expected = ["server", "deny", "deny", "allow", "server", "deny", "allow"]
    if fallback_outcomes != expected:
        pytest.fail(f"outcomes={fallback_outcomes}")


@pytest.mark.asyncio
async def test_buffered_reports_flush_in_one_batch(server: FakeServer) -> None:
    server.plan(CHECK_PATH, make_check_answer(), make_problem_answer(503, "counters_unavailable"))
    server.plan(REPORTS_PATH, build_batch_response)
    fields = make_fields("customer_3")
    async with make_client(server, report_mode="buffered") as client:
        server_decision = await client.check("customer_1", "chat", "openai", "gpt-6-sol")
        fallback_decision = await client.check("customer_2", "chat", "openai", "gpt-6-sol")
        results = [
            await client.report(server_decision, USAGE),
            await client.report(fallback_decision, USAGE, attributes={"service_tier": "flex"}),
            await client.report(fields, USAGE),
        ]
        if results != [None, None, None]:
            pytest.fail(f"results={results}")
        if server.collect_requests(REPORTS_PATH):
            pytest.fail("reports sent before the flush")
        await client.flush()
        batches = server.collect_requests(REPORTS_PATH)
        if len(batches) != 1:
            pytest.fail(f"batches={len(batches)}")
    first, second, third = parse_body(batches[0])["reports"]
    if (first["decision_source"], first["decision_id"], first["usage"]) != (
        "server",
        DECISION_ID,
        SENT_USAGE,
    ):
        pytest.fail(f"first={first}")
    if (second["decision_source"], second["idempotency_key"], second["attributes"]) != (
        "fallback",
        fallback_decision.idempotency_key,
        {"service_tier": "flex"},
    ):
        pytest.fail(f"second={second}")
    if (third["decision_source"], third["idempotency_key"], third["customer_id"]) != (
        "fallback",
        fields.idempotency_key,
        "customer_3",
    ):
        pytest.fail(f"third={third}")


@pytest.mark.asyncio
async def test_failing_flush_keeps_the_reports(server: FakeServer) -> None:
    server.plan(REPORTS_PATH, make_problem_answer(500, "internal_error"), build_batch_response)
    async with make_client(server, report_mode="buffered") as client:
        await client.report(make_fields("customer_1"), USAGE)
        await client.report(make_fields("customer_2"), USAGE)
        await client.flush()
        await client.flush()
    batches = [
        [report["customer_id"] for report in parse_body(request)["reports"]]
        for request in server.collect_requests(REPORTS_PATH)
    ]
    if batches != [["customer_1", "customer_2"], ["customer_1", "customer_2"]]:
        pytest.fail(f"batches={batches}")


@pytest.mark.asyncio
async def test_overflow_counts_dropped_reports_on_the_next_successful_flush(
    server: FakeServer,
) -> None:
    server.plan(REPORTS_PATH, build_batch_response)
    async with make_client(server, report_mode="buffered", max_pending=2) as client:
        for index in range(3):
            await client.report(make_fields(f"customer_{index}"), USAGE)
        await client.flush()
        await client.report(make_fields("customer_3"), USAGE)
    batches = server.collect_requests(REPORTS_PATH)
    dropped_headers = [request.headers.get(DROPPED_REPORTS_HEADER) for request in batches]
    if dropped_headers != ["1", None]:
        pytest.fail(f"dropped_headers={dropped_headers}")
    customer_ids = [report["customer_id"] for report in parse_body(batches[0])["reports"]]
    if customer_ids != ["customer_1", "customer_2"]:
        pytest.fail(f"customer_ids={customer_ids}")


@pytest.mark.asyncio
@pytest.mark.parametrize("report_mode", ["buffered", "sync"])
async def test_report_rejects_attributes_that_cannot_be_encoded(
    server: FakeServer, report_mode: ReportMode
) -> None:
    attributes: dict[str, Any] = {"duration": Decimal("5")}
    async with make_client(server, report_mode=report_mode) as client:
        with pytest.raises(TypeError, match="Decimal"):
            await client.report(make_fields(), USAGE, attributes=attributes)
        await client.flush()
    if server.requests:
        pytest.fail(f"requests={len(server.requests)}")


def test_reports_made_in_later_event_loops_are_sent(server: FakeServer) -> None:
    server.plan(REPORTS_PATH, build_batch_response)
    client = AsyncPreburn(
        api_key=API_KEY,
        base_url=BASE_URL,
        report_mode="buffered",
        flush_interval=0.01,
        transport=httpx.MockTransport(server.handle),
    )

    async def report(customer_ids: list[str]) -> None:
        for customer_id in customer_ids:
            await client.report(make_fields(customer_id), USAGE)

    async def report_and_wait(customer_ids: list[str], sent_count: int) -> None:
        await report(customer_ids)
        await wait_for_sent_reports(server, sent_count)

    asyncio.run(report_and_wait(["customer_1"], 1))
    asyncio.run(report_and_wait(["customer_2", "customer_3"], 3))
    asyncio.run(report(["customer_4"]))
    asyncio.run(client.aclose())
    sent = collect_sent_customer_ids(server)
    if sent != ["customer_1", "customer_2", "customer_3", "customer_4"]:
        pytest.fail(f"sent={sent}")


def test_each_event_loop_gets_its_own_connections(
    server: FakeServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    server.plan(CHECK_PATH, make_check_answer())
    server.plan(REPORTS_PATH, build_batch_response)

    def build_loop_bound_client(
        api_key: str,
        base_url: str,
        timeout: httpx.Timeout,
        transport: httpx.AsyncBaseTransport,
    ) -> httpx.AsyncClient:
        return build_async_client(api_key, base_url, timeout, LoopBoundTransport(server.handle))

    monkeypatch.setattr("preburn._async_client.build_async_client", build_loop_bound_client)
    client = AsyncPreburn(
        api_key=API_KEY,
        base_url=BASE_URL,
        report_mode="buffered",
        flush_interval=IDLE_INTERVAL_SECONDS,
    )

    async def check_and_report(customer_id: str) -> Decision:
        decision = await client.check(customer_id, "chat", "openai", "gpt-6-sol")
        await client.report(decision, USAGE)
        await client.flush()
        return decision

    decisions = [
        asyncio.run(check_and_report("customer_1")),
        asyncio.run(check_and_report("customer_2")),
    ]
    asyncio.run(client.aclose())
    if any(decision.is_fallback for decision in decisions):
        pytest.fail("check fell back in a later event loop")
    if len(server.collect_requests(REPORTS_PATH)) != 2:
        pytest.fail(f"batches={len(server.collect_requests(REPORTS_PATH))}")


def test_proxy_is_resolved_once_for_every_event_loop(monkeypatch: pytest.MonkeyPatch) -> None:
    lookups: list[str] = []

    def look_up_proxies() -> dict[str, str]:
        lookups.append("getproxies")
        return {}

    monkeypatch.setattr(urllib.request, "getproxies", look_up_proxies)
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        closed_port = int(probe.getsockname()[1])
    client = AsyncPreburn(
        api_key=API_KEY, base_url=f"http://127.0.0.1:{closed_port}", report_mode="sync"
    )

    async def check() -> Decision:
        return await client.check("customer_1", "chat", "openai", "gpt-6-sol")

    decisions = [asyncio.run(check()), asyncio.run(check())]
    asyncio.run(client.aclose())
    if not all(decision.is_fallback for decision in decisions) or lookups != ["getproxies"]:
        pytest.fail(f"lookups={lookups} fallbacks={[d.is_fallback for d in decisions]}")


@pytest.mark.asyncio
async def test_api_key_stays_out_of_logs_errors_and_reprs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    api_key = f"pb_live_runtime_{secrets.token_hex(16)}"
    server = FakeServer()
    server.plan(
        CHECK_PATH,
        make_check_answer(),
        make_problem_answer(503, "counters_unavailable"),
        make_problem_answer(401, "authentication_required"),
    )
    server.plan(RELEASE_PATH, make_problem_answer(401, "authentication_required"))
    server.plan(REPORTS_PATH, make_json_answer(202, {"unexpected": True}))
    errors: list[BaseException] = []
    with caplog.at_level(logging.DEBUG, logger="preburn"):
        client = AsyncPreburn(
            api_key=api_key,
            base_url=BASE_URL,
            report_mode="buffered",
            flush_interval=0.01,
            transport=httpx.MockTransport(server.handle),
        )
        server_decision = await client.check("customer_1", "chat", "openai", "gpt-6-sol")
        fallback_decision = await client.check("customer_1", "chat", "openai", "gpt-6-sol")
        with pytest.raises(AuthenticationError) as check_raised:
            await client.check("customer_1", "chat", "openai", "gpt-6-sol")
        errors.append(check_raised.value)
        with pytest.raises(AuthenticationError) as release_raised:
            await client.release(server_decision)
        errors.append(release_raised.value)
        await client.report(fallback_decision, USAGE)
        await wait_until_logged(caplog, "reports.flush_failed")
        await client.aclose()
    if server.requests[0].headers["Authorization"] != f"Bearer {api_key}":
        pytest.fail("api key not sent")
    formatter = logging.Formatter()
    texts = [formatter.format(record) for record in caplog.records]
    for error in errors:
        texts.extend([str(error), repr(error), "".join(traceback.format_exception(error))])
    texts.extend([repr(client), repr(vars(client)), repr(server_decision)])
    for request in server.requests:
        texts.extend([repr(request), repr(request.headers)])
    secret = api_key.removeprefix("pb_live_runtime_")
    exposed = [text for text in texts if secret in text]
    if exposed:
        pytest.fail(f"exposed_in={len(exposed)} texts")


async def wait_until_logged(caplog: pytest.LogCaptureFixture, event: str) -> None:
    deadline = time.monotonic() + WAIT_SECONDS
    while not any(message.startswith(event) for message in caplog.messages):
        if time.monotonic() > deadline:
            pytest.fail(f"messages={caplog.messages}")
        await asyncio.sleep(POLL_INTERVAL_SECONDS)


@pytest.mark.asyncio
async def test_success_answers_that_are_not_json_raise_unexpected_response(
    server: FakeServer,
) -> None:
    server.plan(CUSTOMER_PATH, make_text_answer(200, "<html>sign in</html>"))
    server.plan(REPORT_PATH, make_text_answer(202, "<html>sign in</html>"))
    async with make_client(server) as client:
        with pytest.raises(APIError) as upsert_raised:
            await client.customers.upsert("customer_1")
        with pytest.raises(APIError) as report_raised:
            await client.report(make_fields(), USAGE)
    for raised, status in ((upsert_raised, 200), (report_raised, 202)):
        if (raised.value.code, raised.value.status) != ("unexpected_response", status):
            pytest.fail(f"code={raised.value.code} status={raised.value.status}")


@pytest.mark.asyncio
async def test_undecodable_answer_raises_unexpected_response(server: FakeServer) -> None:
    server.plan(CUSTOMER_PATH, make_undecodable_answer(200))
    async with make_client(server) as client:
        with pytest.raises(APIError) as raised:
            await client.customers.upsert("customer_1")
    if (raised.value.code, raised.value.status) != ("unexpected_response", None):
        pytest.fail(f"code={raised.value.code} status={raised.value.status}")


@pytest.mark.asyncio
async def test_sync_mode_returns_the_report_result(server: FakeServer) -> None:
    server.plan(CHECK_PATH, make_check_answer())
    server.plan(REPORT_PATH, make_json_answer(202, REPORT_RESPONSE))
    occurred_at = datetime(2026, 9, 26, 10, 0, tzinfo=timezone.utc)
    async with make_client(server, report_mode="sync") as client:
        decision = await client.check("customer_1", "chat", "openai", "gpt-6-sol")
        result = await client.report(
            decision, USAGE, attributes={"service_tier": "flex"}, occurred_at=occurred_at
        )
    expected = ReportResult(
        ledger_entry_id="led_01jbvagescfn78y0938nkrkayd",
        cost=Decimal("0.0115"),
        cost_status="costed",
        duplicate=False,
    )
    if result != expected:
        pytest.fail(f"result={result}")
    request = server.collect_requests(REPORT_PATH)[0]
    expected_body = {
        "decision_source": "server",
        "decision_id": DECISION_ID,
        "usage": SENT_USAGE,
        "attributes": {"service_tier": "flex"},
        "occurred_at": "2026-09-26T10:00:00Z",
    }
    if parse_body(request) != expected_body:
        pytest.fail(f"body={parse_body(request)}")
    if request.extensions["timeout"] != REQUEST_TIMEOUT:
        pytest.fail(f"timeout={request.extensions['timeout']}")


@pytest.mark.asyncio
async def test_report_stamps_the_current_time_when_occurred_at_is_omitted(
    server: FakeServer,
) -> None:
    server.plan(REPORTS_PATH, build_batch_response)
    async with make_client(server, report_mode="buffered") as client:
        before = datetime.now(timezone.utc)
        await client.report(make_fields(), USAGE)
        after = datetime.now(timezone.utc)
    stamped = parse_timestamp(parse_body(server.requests[0])["reports"][0]["occurred_at"])
    if not before <= stamped <= after:
        pytest.fail(f"occurred_at={stamped} before={before} after={after}")


@pytest.mark.asyncio
async def test_sync_report_raises_when_rejected(server: FakeServer) -> None:
    server.plan(REPORT_PATH, make_problem_answer(409, "decision_not_reportable"))
    async with make_client(server, report_mode="sync") as client:
        with pytest.raises(APIError) as raised:
            await client.report(make_fields(), USAGE)
    if (raised.value.status, raised.value.code) != (409, "decision_not_reportable"):
        pytest.fail(f"status={raised.value.status} code={raised.value.code}")


@pytest.mark.asyncio
async def test_fallback_decision_reports_with_its_idempotency_key(server: FakeServer) -> None:
    server.plan(CHECK_PATH, make_problem_answer(503, "database_unavailable"))
    server.plan(REPORT_PATH, make_json_answer(202, REPORT_RESPONSE))
    occurred_at = datetime(2026, 9, 26, 10, 0, tzinfo=timezone.utc)
    async with make_client(server, report_mode="sync") as client:
        decision = await client.check(
            "customer_1", "chat", "openai", "gpt-6-sol", attributes={"service_tier": "priority"}
        )
        await client.report(decision, USAGE, occurred_at=occurred_at)
    expected_body = {
        "decision_source": "fallback",
        "idempotency_key": decision.idempotency_key,
        "customer_id": "customer_1",
        "feature": "chat",
        "provider": "openai",
        "model": "gpt-6-sol",
        "usage": SENT_USAGE,
        "attributes": {"service_tier": "priority"},
        "occurred_at": "2026-09-26T10:00:00Z",
    }
    body = parse_body(server.collect_requests(REPORT_PATH)[0])
    if body != expected_body:
        pytest.fail(f"body={body}")


@pytest.mark.asyncio
async def test_report_rejects_invalid_usage_before_queueing(server: FakeServer) -> None:
    async with make_client(server, report_mode="buffered") as client:
        with pytest.raises(ValueError, match="quantity"):
            await client.report(make_fields(), {"input_tokens": Decimal("0.0000001")})
        await client.flush()
    if server.requests:
        pytest.fail(f"requests={len(server.requests)}")


@pytest.mark.asyncio
@pytest.mark.parametrize("variable", SERVERLESS_VARIABLES)
async def test_serverless_variables_select_sync_mode(
    server: FakeServer, monkeypatch: pytest.MonkeyPatch, variable: str
) -> None:
    monkeypatch.setenv(variable, "1")
    server.plan(REPORT_PATH, make_json_answer(202, REPORT_RESPONSE))
    async with make_client(server, report_mode="auto") as client:
        result = await client.report(make_fields(), USAGE)
    if not isinstance(result, ReportResult):
        pytest.fail(f"result={result}")


@pytest.mark.asyncio
async def test_auto_mode_buffers_outside_serverless_environments(server: FakeServer) -> None:
    server.plan(REPORTS_PATH, build_batch_response)
    async with make_client(server, report_mode="auto") as client:
        if await client.report(make_fields(), USAGE) is not None:
            pytest.fail("auto mode sent the report synchronously")
        if server.requests:
            pytest.fail("report sent before the flush")
        await client.flush()
    if len(server.collect_requests(REPORTS_PATH)) != 1:
        pytest.fail(f"requests={len(server.requests)}")


@pytest.mark.asyncio
async def test_aclose_flushes_pending_reports_and_leaves_no_task(server: FakeServer) -> None:
    server.plan(REPORTS_PATH, build_batch_response)
    client = make_client(server, report_mode="buffered")
    await client.report(make_fields("customer_1"), USAGE)
    await client.report(make_fields("customer_2"), USAGE)
    await asyncio.sleep(0)
    if server.requests or not collect_other_tasks():
        pytest.fail(f"requests={len(server.requests)} tasks={collect_other_tasks()}")
    await asyncio.wait_for(client.aclose(), WAIT_SECONDS)
    batches = server.collect_requests(REPORTS_PATH)
    customer_ids = [report["customer_id"] for report in parse_body(batches[0])["reports"]]
    if len(batches) != 1 or customer_ids != ["customer_1", "customer_2"]:
        pytest.fail(f"batches={len(batches)} customer_ids={customer_ids}")
    if collect_other_tasks():
        pytest.fail(f"tasks={collect_other_tasks()}")


@pytest.mark.asyncio
async def test_leaving_the_context_flushes_pending_reports(server: FakeServer) -> None:
    server.plan(REPORTS_PATH, build_batch_response)
    async with make_client(server, report_mode="buffered") as client:
        await client.report(make_fields(), USAGE)
    if len(server.collect_requests(REPORTS_PATH)) != 1 or collect_other_tasks():
        pytest.fail(f"requests={len(server.requests)} tasks={collect_other_tasks()}")


@pytest.mark.asyncio
async def test_report_after_aclose_raises(server: FakeServer) -> None:
    client = make_client(server, report_mode="buffered")
    await client.aclose()
    with pytest.raises(RuntimeError, match="client closed"):
        await client.report(make_fields(), USAGE)


def test_client_created_outside_a_loop_starts_its_task_on_first_report(
    server: FakeServer,
) -> None:
    server.plan(REPORTS_PATH, build_batch_response)
    client = make_client(server, report_mode="buffered")

    async def report_and_close() -> None:
        await client.report(make_fields(), USAGE)
        await client.aclose()

    asyncio.run(report_and_close())
    if len(server.collect_requests(REPORTS_PATH)) != 1:
        pytest.fail(f"requests={len(server.requests)}")


@pytest.mark.asyncio
async def test_release_posts_the_decision_id(server: FakeServer) -> None:
    server.plan(CHECK_PATH, make_check_answer())
    server.plan(RELEASE_PATH, make_json_answer(200, {}))
    async with make_client(server) as client:
        await client.release(await client.check("customer_1", "chat", "openai", "gpt-6-sol"))
    request = server.collect_requests(RELEASE_PATH)[0]
    if (request.method, parse_body(request)) != ("POST", {"decision_id": DECISION_ID}):
        pytest.fail(f"method={request.method} body={parse_body(request)}")


@pytest.mark.asyncio
async def test_release_of_a_fallback_decision_sends_nothing(server: FakeServer) -> None:
    server.plan(CHECK_PATH, make_problem_answer(503, "counters_unavailable"))
    async with make_client(server) as client:
        await client.release(await client.check("customer_1", "chat", "openai", "gpt-6-sol"))
    if server.collect_requests(RELEASE_PATH):
        pytest.fail("fallback decision released")


@pytest.mark.asyncio
async def test_release_raises_when_rejected(server: FakeServer) -> None:
    server.plan(CHECK_PATH, make_check_answer())
    server.plan(RELEASE_PATH, make_problem_answer(401, "authentication_required"))
    async with make_client(server) as client:
        decision = await client.check("customer_1", "chat", "openai", "gpt-6-sol")
        with pytest.raises(AuthenticationError):
            await client.release(decision)


@pytest.mark.asyncio
async def test_customer_upsert_replaces_the_customer(server: FakeServer) -> None:
    path = "/api/v1/customers/team%3Aacme"
    server.plan(path, make_json_answer(200, CUSTOMER_RESPONSE))
    async with make_client(server) as client:
        customer = await client.customers.upsert(
            "team:acme",
            display_name="Acme",
            plan_id="pln_01jbvagescfn78y0938nkrkayd",
            metadata={"tier": "gold"},
        )
    request = server.requests[0]
    expected_body = {
        "display_name": "Acme",
        "plan_id": "pln_01jbvagescfn78y0938nkrkayd",
        "metadata": {"tier": "gold"},
    }
    if (request.method, parse_body(request)) != ("PUT", expected_body):
        pytest.fail(f"method={request.method} body={parse_body(request)}")
    if customer != Customer.from_response(CUSTOMER_RESPONSE):
        pytest.fail(f"customer={customer}")


@pytest.mark.asyncio
async def test_customer_upsert_raises_when_rejected(server: FakeServer) -> None:
    server.plan("/api/v1/customers/team%3Aacme", make_problem_answer(422, "plan_not_found"))
    async with make_client(server) as client:
        with pytest.raises(ValidationError) as raised:
            await client.customers.upsert("team:acme", plan_id="pln_01jbvagescfn78y0938nkrkayd")
    if raised.value.code != "plan_not_found":
        pytest.fail(f"code={raised.value.code}")


@pytest.mark.asyncio
async def test_revenue_record_posts_the_entry(server: FakeServer) -> None:
    server.plan(REVENUE_PATH, make_json_answer(201, REVENUE_RESPONSE))
    async with make_client(server) as client:
        entry = await client.revenue.record(
            "customer_1",
            "subscription",
            Decimal("30"),
            datetime(2026, 9, 1, tzinfo=timezone.utc),
            datetime(2026, 10, 1, tzinfo=timezone.utc),
            "in_123",
        )
    expected_body = {
        "customer_id": "customer_1",
        "kind": "subscription",
        "amount": "30.000000000",
        "period_start": "2026-09-01T00:00:00Z",
        "period_end": "2026-10-01T00:00:00Z",
        "source_reference": "in_123",
    }
    request = server.requests[0]
    if (request.method, parse_body(request)) != ("POST", expected_body):
        pytest.fail(f"method={request.method} body={parse_body(request)}")
    if entry != RevenueEntry.from_response(REVENUE_RESPONSE):
        pytest.fail(f"entry={entry}")


@pytest.mark.asyncio
async def test_options_fall_back_to_environment_variables(
    server: FakeServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PREBURN_API_KEY", API_KEY)
    monkeypatch.setenv("PREBURN_BASE_URL", "https://preburn.test/prefix")
    server.plan(f"/prefix{CHECK_PATH}", make_check_answer())
    client = AsyncPreburn(report_mode="sync", transport=httpx.MockTransport(server.handle))
    async with client:
        await client.check("customer_1", "chat", "openai", "gpt-6-sol")
    request = server.requests[0]
    if request.headers["Authorization"] != f"Bearer {API_KEY}":
        pytest.fail("environment api key not sent")
    if str(request.url) != f"https://preburn.test/prefix{CHECK_PATH}":
        pytest.fail(f"url={request.url}")


@pytest.mark.parametrize(
    ("options", "named_option"),
    [
        ({"api_key": None, "base_url": BASE_URL}, "PREBURN_API_KEY"),
        ({"api_key": API_KEY, "base_url": None}, "PREBURN_BASE_URL"),
        ({"api_key": "pb_test_runtime_short", "base_url": BASE_URL}, "api_key"),
        ({"api_key": API_KEY, "base_url": "ftp://preburn.test"}, "base_url"),
        ({"api_key": API_KEY, "base_url": BASE_URL, "check_timeout": 0}, "check_timeout"),
        ({"api_key": API_KEY, "base_url": BASE_URL, "batch_size": 0}, "batch_size"),
    ],
)
def test_invalid_options_raise_configuration_error(
    options: dict[str, Any], named_option: str
) -> None:
    with pytest.raises(ConfigurationError, match=named_option):
        AsyncPreburn(**options)
