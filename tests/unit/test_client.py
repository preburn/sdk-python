import atexit
import json
import logging
import os
import secrets
import signal
import socket
import subprocess
import sys
import threading
import time
import traceback
import urllib.request
import uuid
from collections.abc import Callable
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, NoReturn

import httpx
import httpx._utils
import pytest

from preburn import (
    APIError,
    AuthenticationError,
    ConfigurationError,
    Customer,
    Preburn,
    PreburnError,
    ReportFields,
    ReportMode,
    ReportResult,
    RevenueEntry,
    ValidationError,
)
from preburn._models import parse_timestamp
from preburn._transport import (
    CHECK_PATH,
    RELEASE_PATH,
    REPORT_BATCH_MAXIMUM_BYTES,
    REPORT_PATH,
    REPORTS_PATH,
    REVENUE_PATH,
)

API_KEY_BODY = secrets.token_hex(16)
API_KEY = f"pb_test_runtime_{API_KEY_BODY}"
MALFORMED_API_KEYS = (
    f"sk_live_{API_KEY_BODY}",
    f"pb_live_runtime_{API_KEY_BODY[:10]}",
    f"pb_prod_runtime_{API_KEY_BODY}",
    f"pb_live_billing_{API_KEY_BODY}",
    f"pb_live_runtime_{API_KEY_BODY}w",
    f"pb_live_runtime_{API_KEY_BODY[:31]}-",
    "key_01jbvagescfn78y0938nkrkayd",
)
WELL_FORMED_API_KEYS = (
    f"pb_live_runtime_{API_KEY_BODY[:16]}{API_KEY_BODY[16:].upper()}",
    f"pb_test_admin_{API_KEY_BODY}",
)
BASE_URL = "http://preburn.test"
IDLE_INTERVAL_SECONDS = 3600.0
WAIT_SECONDS = 5.0
FORK_WAIT_SECONDS = 10.0
POLL_INTERVAL_SECONDS = 0.01
STALLED_RESOLUTION_SECONDS = 2.0
FORK_WARNING_FILTER = "ignore:This process .* is multi-threaded:DeprecationWarning"
CUSTOMER_PATH = "/api/v1/customers/customer_1"
CHECK_TIMEOUT = {"connect": 0.25, "read": 0.25, "write": 0.25, "pool": 0.25}
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
EXIT_SCRIPT = """
import json
import sys

import httpx

from preburn import Preburn, ReportFields

RESULT = {
    "status": 202,
    "result": {
        "ledger_entry_id": "led_1",
        "cost": None,
        "cost_status": "uncosted",
        "duplicate": False,
    },
}


def record_batch(request):
    reports = json.loads(request.content)["reports"]
    with open(sys.argv[1], "a") as output:
        output.write(f"{request.url.path} {len(reports)}\\n")
    return httpx.Response(202, json={"results": [RESULT] * len(reports)})


client = Preburn(
    api_key=sys.argv[2],
    base_url="http://preburn.test",
    report_mode="buffered",
    flush_interval=3600,
    transport=httpx.MockTransport(record_batch),
)
for index in range(2):
    fields = ReportFields(
        customer_id=f"customer_{index}", feature="chat", provider="openai", model="gpt-6-sol"
    )
    client.report(fields, {"input_tokens": 10})
"""

Answer = Callable[[httpx.Request], httpx.Response]


class FakeServer:
    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self._answers: dict[str, list[Answer]] = {}
        self._lock = threading.Lock()

    def plan(self, path: str, *answers: Answer) -> None:
        with self._lock:
            self._answers[path] = list(answers)

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.raw_path.decode("ascii")
        with self._lock:
            self.requests.append(request)
            answers = self._answers[path]
            answer = answers.pop(0) if len(answers) > 1 else answers[0]
        return answer(request)

    def collect_requests(self, path: str) -> list[httpx.Request]:
        with self._lock:
            return [
                request for request in self.requests if request.url.raw_path.decode("ascii") == path
            ]


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


def make_client(server: FakeServer, *, report_mode: ReportMode = "sync") -> Preburn:
    return Preburn(
        api_key=API_KEY,
        base_url=BASE_URL,
        report_mode=report_mode,
        flush_interval=IDLE_INTERVAL_SECONDS,
        transport=httpx.MockTransport(server.handle),
    )


def make_recording_handler(record_path: Path) -> Answer:
    def handle(request: httpx.Request) -> httpx.Response:
        body = parse_body(request)
        if request.url.path == REPORTS_PATH:
            customer_ids = [report["customer_id"] for report in body["reports"]]
            response = build_batch_response(request)
        else:
            customer_ids = [body["customer_id"]]
            response = httpx.Response(200, json=CHECK_RESPONSE)
        entry = {"process_id": os.getpid(), "path": request.url.path, "customer_ids": customer_ids}
        with record_path.open("a") as output:
            output.write(json.dumps(entry) + "\n")
        return response

    return handle


def read_recorded_requests(record_path: Path) -> list[tuple[bool, str, list[str]]]:
    if not record_path.exists():
        return []
    entries = [json.loads(line) for line in record_path.read_text().splitlines()]
    return [
        (entry["process_id"] == os.getpid(), entry["path"], entry["customer_ids"])
        for entry in entries
    ]


def wait_until(condition: Callable[[], bool], description: str) -> None:
    deadline = time.monotonic() + WAIT_SECONDS
    while not condition():
        if time.monotonic() > deadline:
            pytest.fail(f"timed out waiting for {description}")
        time.sleep(POLL_INTERVAL_SECONDS)


def find_closed_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def run_in_forked_child(work: Callable[[], None], failure_path: Path) -> None:
    process_id = os.fork()
    if process_id == 0:
        exit_code = 1
        try:
            work()
            exit_code = 0
        except BaseException as error:
            failure_path.write_text("".join(traceback.format_exception(error)))
        finally:
            os._exit(exit_code)
    deadline = time.monotonic() + FORK_WAIT_SECONDS
    finished_process_id, status = os.waitpid(process_id, os.WNOHANG)
    while finished_process_id == 0:
        if time.monotonic() > deadline:
            os.kill(process_id, signal.SIGKILL)
            os.waitpid(process_id, 0)
            pytest.fail("forked child blocked")
        time.sleep(POLL_INTERVAL_SECONDS)
        finished_process_id, status = os.waitpid(process_id, os.WNOHANG)
    if os.waitstatus_to_exitcode(status) != 0:
        exit_code = os.waitstatus_to_exitcode(status)
        pytest.fail(failure_path.read_text() if failure_path.exists() else f"exit_code={exit_code}")


@pytest.fixture(autouse=True)
def clear_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (*SERVERLESS_VARIABLES, "PREBURN_API_KEY", "PREBURN_BASE_URL"):
        monkeypatch.delenv(name, raising=False)


@pytest.fixture(name="server")
def make_server() -> FakeServer:
    return FakeServer()


def test_check_returns_the_server_decision(server: FakeServer) -> None:
    server.plan(CHECK_PATH, make_check_answer("deny"))
    with make_client(server) as client:
        decision = client.check(
            "customer_1",
            "chat",
            "openai",
            "gpt-6-sol",
            attributes={"service_tier": "priority"},
            usage_estimate=USAGE,
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
        "customer_user_id": "user_7",
    }
    if (request.method, parse_body(request)) != ("POST", expected_body):
        pytest.fail(f"method={request.method} body={parse_body(request)}")
    if request.extensions["timeout"] != CHECK_TIMEOUT:
        pytest.fail(f"timeout={request.extensions['timeout']}")
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


def test_check_timeout_applies_to_each_phase(server: FakeServer) -> None:
    server.plan(CHECK_PATH, make_check_answer())
    transport = httpx.MockTransport(server.handle)
    with Preburn(
        api_key=API_KEY,
        base_url=BASE_URL,
        check_timeout=0.5,
        report_mode="sync",
        transport=transport,
    ) as client:
        client.check("customer_1", "chat", "openai", "gpt-6-sol")
    expected = {"connect": 0.5, "read": 0.5, "write": 0.5, "pool": 0.5}
    if server.requests[0].extensions["timeout"] != expected:
        pytest.fail(f"timeout={server.requests[0].extensions['timeout']}")
    if client.check_timeout != 0.5:
        pytest.fail(f"check_timeout={client.check_timeout}")


def test_check_deadline_covers_name_resolution(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    resolution_released = threading.Event()

    def resolve_after_release(*arguments: object) -> NoReturn:
        resolution_released.wait(STALLED_RESOLUTION_SECONDS)
        raise socket.gaierror(socket.EAI_NONAME, "name resolution stalled")

    monkeypatch.setattr(socket, "getaddrinfo", resolve_after_release)
    with (
        Preburn(api_key=API_KEY, base_url=BASE_URL, report_mode="sync") as client,
        caplog.at_level(logging.WARNING, logger="preburn"),
    ):
        started = time.monotonic()
        try:
            decision = client.check("customer_1", "chat", "openai", "gpt-6-sol")
        finally:
            elapsed = time.monotonic() - started
            resolution_released.set()
    if not decision.is_fallback or elapsed >= STALLED_RESOLUTION_SECONDS / 2:
        pytest.fail(f"decision_id={decision.decision_id} elapsed={elapsed}")
    if caplog.messages != ["check.fallback feature=chat outcome=allow error=TimeoutError"]:
        pytest.fail(f"messages={caplog.messages}")


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
def test_check_falls_back_when_preburn_cannot_answer(
    server: FakeServer, failure: Answer, cause: str, caplog: pytest.LogCaptureFixture
) -> None:
    server.plan(CHECK_PATH, failure)
    with make_client(server) as client, caplog.at_level(logging.WARNING, logger="preburn"):
        decision = client.check(
            "customer_1", "chat", "openai", "gpt-6-sol", attributes={"service_tier": "priority"}
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


@pytest.mark.parametrize(
    ("status", "code", "error_class"),
    [
        (422, "validation_failed", ValidationError),
        (401, "authentication_required", AuthenticationError),
        (500, "internal_error", APIError),
    ],
)
def test_check_raises_other_errors_without_fallback(
    server: FakeServer, status: int, code: str, error_class: type[PreburnError]
) -> None:
    server.plan(CHECK_PATH, make_problem_answer(status, code))
    with make_client(server) as client, pytest.raises(error_class) as raised:
        client.check("customer_1", "chat", "openai", "gpt-6-sol")
    if (raised.value.status, raised.value.code) != (status, code):
        pytest.fail(f"status={raised.value.status} code={raised.value.code}")


def test_fallback_outcome_follows_the_last_server_fallback_outcome(server: FakeServer) -> None:
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
    with make_client(server) as client:
        decisions = [
            client.check(customer_id, feature, "openai", "gpt-6-sol")
            for customer_id, feature in calls
        ]
    fallback_outcomes = [
        decision.outcome if decision.is_fallback else "server" for decision in decisions
    ]
    expected = ["server", "deny", "deny", "allow", "server", "deny", "allow"]
    if fallback_outcomes != expected:
        pytest.fail(f"outcomes={fallback_outcomes}")


def test_buffered_reports_flush_in_one_batch(server: FakeServer) -> None:
    server.plan(CHECK_PATH, make_check_answer(), make_problem_answer(503, "counters_unavailable"))
    server.plan(REPORTS_PATH, build_batch_response)
    fields = make_fields("customer_3")
    with make_client(server, report_mode="buffered") as client:
        server_decision = client.check("customer_1", "chat", "openai", "gpt-6-sol")
        fallback_decision = client.check("customer_2", "chat", "openai", "gpt-6-sol")
        results = [
            client.report(server_decision, USAGE),
            client.report(fallback_decision, USAGE, attributes={"service_tier": "flex"}),
            client.report(fields, USAGE),
        ]
        if results != [None, None, None]:
            pytest.fail(f"results={results}")
        if server.collect_requests(REPORTS_PATH):
            pytest.fail("reports sent before the flush")
        client.flush()
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


def test_sync_mode_returns_the_report_result(server: FakeServer) -> None:
    server.plan(CHECK_PATH, make_check_answer())
    server.plan(REPORT_PATH, make_json_answer(202, REPORT_RESPONSE))
    occurred_at = datetime(2026, 9, 26, 10, 0, tzinfo=timezone.utc)
    with make_client(server, report_mode="sync") as client:
        decision = client.check("customer_1", "chat", "openai", "gpt-6-sol")
        result = client.report(
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


def test_report_stamps_the_current_time_when_occurred_at_is_omitted(server: FakeServer) -> None:
    server.plan(REPORT_PATH, make_json_answer(202, REPORT_RESPONSE))
    with make_client(server, report_mode="sync") as client:
        before = datetime.now(timezone.utc)
        client.report(make_fields(), USAGE)
        after = datetime.now(timezone.utc)
    stamped = parse_timestamp(parse_body(server.requests[0])["occurred_at"])
    if not before <= stamped <= after:
        pytest.fail(f"occurred_at={stamped} before={before} after={after}")


def test_sync_report_raises_when_rejected(server: FakeServer) -> None:
    server.plan(REPORT_PATH, make_problem_answer(409, "decision_not_reportable"))
    with make_client(server, report_mode="sync") as client, pytest.raises(APIError) as raised:
        client.report(make_fields(), USAGE)
    if (raised.value.status, raised.value.code) != (409, "decision_not_reportable"):
        pytest.fail(f"status={raised.value.status} code={raised.value.code}")


def test_fallback_decision_reports_with_its_idempotency_key(server: FakeServer) -> None:
    server.plan(CHECK_PATH, make_problem_answer(503, "database_unavailable"))
    server.plan(REPORT_PATH, make_json_answer(202, REPORT_RESPONSE))
    occurred_at = datetime(2026, 9, 26, 10, 0, tzinfo=timezone.utc)
    with make_client(server, report_mode="sync") as client:
        decision = client.check(
            "customer_1", "chat", "openai", "gpt-6-sol", attributes={"service_tier": "priority"}
        )
        client.report(decision, USAGE, occurred_at=occurred_at)
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


def test_report_fields_keep_one_idempotency_key(server: FakeServer) -> None:
    server.plan(REPORT_PATH, make_json_answer(202, REPORT_RESPONSE))
    fields = make_fields()
    with make_client(server, report_mode="sync") as client:
        client.report(fields, USAGE)
        client.report(fields, USAGE)
    keys = [parse_body(request)["idempotency_key"] for request in server.requests]
    if keys != [fields.idempotency_key, fields.idempotency_key]:
        pytest.fail(f"keys={keys} fields_key={fields.idempotency_key}")
    if uuid.UUID(fields.idempotency_key).version != 4:
        pytest.fail(f"idempotency_key={fields.idempotency_key}")
    if make_fields().idempotency_key == fields.idempotency_key:
        pytest.fail("two report fields share an idempotency key")


@pytest.mark.parametrize("report_mode", ["buffered", "sync"])
def test_report_rejects_attributes_that_cannot_be_encoded(
    server: FakeServer, report_mode: ReportMode
) -> None:
    attributes: dict[str, Any] = {"duration": Decimal("5")}
    with make_client(server, report_mode=report_mode) as client:
        with pytest.raises(TypeError, match="Decimal"):
            client.report(make_fields(), USAGE, attributes=attributes)
        client.flush()
    if server.requests:
        pytest.fail(f"requests={len(server.requests)}")


def test_oversized_report_is_dropped_and_counted(
    server: FakeServer, caplog: pytest.LogCaptureFixture
) -> None:
    server.plan(REPORTS_PATH, build_batch_response)
    oversized: dict[str, Any] = {"prompt": "x" * REPORT_BATCH_MAXIMUM_BYTES}
    with make_client(server, report_mode="buffered") as client:
        with caplog.at_level(logging.WARNING, logger="preburn"):
            client.report(make_fields("customer_1"), USAGE, attributes=oversized)
        client.report(make_fields("customer_2"), USAGE)
    batches = server.collect_requests(REPORTS_PATH)
    customer_ids = [report["customer_id"] for report in parse_body(batches[0])["reports"]]
    if len(batches) != 1 or customer_ids != ["customer_2"]:
        pytest.fail(f"batches={len(batches)} customer_ids={customer_ids}")
    if batches[0].headers.get("Preburn-Dropped-Reports") != "1":
        pytest.fail(f"headers={batches[0].headers}")
    if not caplog.messages or not caplog.messages[0].startswith("reports.oversized bytes="):
        pytest.fail(f"messages={caplog.messages}")


def test_report_rejects_invalid_usage_before_queueing(server: FakeServer) -> None:
    with make_client(server, report_mode="buffered") as client:
        with pytest.raises(ValueError, match="quantity"):
            client.report(make_fields(), {"input_tokens": Decimal("0.0000001")})
        client.flush()
    if server.requests:
        pytest.fail(f"requests={len(server.requests)}")


@pytest.mark.parametrize("variable", SERVERLESS_VARIABLES)
def test_serverless_variables_select_sync_mode(
    server: FakeServer, monkeypatch: pytest.MonkeyPatch, variable: str
) -> None:
    monkeypatch.setenv(variable, "1")
    server.plan(REPORT_PATH, make_json_answer(202, REPORT_RESPONSE))
    with make_client(server, report_mode="auto") as client:
        result = client.report(make_fields(), USAGE)
    if not isinstance(result, ReportResult):
        pytest.fail(f"result={result}")


def test_auto_mode_buffers_outside_serverless_environments(server: FakeServer) -> None:
    server.plan(REPORTS_PATH, build_batch_response)
    with make_client(server, report_mode="auto") as client:
        if client.report(make_fields(), USAGE) is not None:
            pytest.fail("auto mode sent the report synchronously")
        if server.requests:
            pytest.fail("report sent before the flush")
        client.flush()
    if len(server.collect_requests(REPORTS_PATH)) != 1:
        pytest.fail(f"requests={len(server.requests)}")


def test_close_flushes_pending_reports(server: FakeServer) -> None:
    server.plan(REPORTS_PATH, build_batch_response)
    client = make_client(server, report_mode="buffered")
    client.report(make_fields("customer_1"), USAGE)
    client.report(make_fields("customer_2"), USAGE)
    if server.requests:
        pytest.fail("reports sent before close")
    client.close()
    batches = server.collect_requests(REPORTS_PATH)
    customer_ids = [report["customer_id"] for report in parse_body(batches[0])["reports"]]
    if len(batches) != 1 or customer_ids != ["customer_1", "customer_2"]:
        pytest.fail(f"batches={len(batches)} customer_ids={customer_ids}")


def test_leaving_the_context_flushes_pending_reports(server: FakeServer) -> None:
    server.plan(REPORTS_PATH, build_batch_response)
    with make_client(server, report_mode="buffered") as client:
        client.report(make_fields(), USAGE)
    if len(server.collect_requests(REPORTS_PATH)) != 1:
        pytest.fail(f"requests={len(server.requests)}")


def test_report_after_close_raises(server: FakeServer) -> None:
    client = make_client(server, report_mode="buffered")
    client.close()
    with pytest.raises(RuntimeError, match="client closed"):
        client.report(make_fields(), USAGE)


def test_buffered_client_registers_an_exit_flush_until_closed(
    server: FakeServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    registered: list[Callable[[], None]] = []
    unregistered: list[Callable[[], None]] = []
    monkeypatch.setattr(atexit, "register", registered.append)
    monkeypatch.setattr(atexit, "unregister", unregistered.append)
    make_client(server, report_mode="sync").close()
    if registered or unregistered:
        pytest.fail(f"sync client registered={registered} unregistered={unregistered}")
    buffered_client = make_client(server, report_mode="buffered")
    if len(registered) != 1 or unregistered:
        pytest.fail(f"registered={registered} unregistered={unregistered}")
    buffered_client.close()
    if unregistered != registered:
        pytest.fail(f"registered={registered} unregistered={unregistered}")


def test_exit_flushes_reports_of_an_unclosed_client(tmp_path: Path) -> None:
    output_path = tmp_path / "batches.txt"
    subprocess.run(
        [sys.executable, "-c", EXIT_SCRIPT, str(output_path), API_KEY],
        check=True,
        timeout=30,
        capture_output=True,
    )
    if output_path.read_text() != f"{REPORTS_PATH} 2\n":
        pytest.fail(f"batches={output_path.read_text()!r}")


@pytest.mark.skipif(not hasattr(os, "fork"), reason="os.fork is POSIX only")
@pytest.mark.filterwarnings(FORK_WARNING_FILTER)
def test_forked_child_checks_and_sends_its_own_reports(tmp_path: Path) -> None:
    record_path = tmp_path / "requests.jsonl"
    client = Preburn(
        api_key=API_KEY,
        base_url=BASE_URL,
        report_mode="buffered",
        flush_interval=IDLE_INTERVAL_SECONDS,
        batch_size=2,
        transport=httpx.MockTransport(make_recording_handler(record_path)),
    )
    client.report(make_fields("parent_1"), USAGE)

    def check_and_report() -> None:
        if client.check("child_check", "chat", "openai", "gpt-6-sol").is_fallback:
            pytest.fail("check in the child fell back")
        client.report(make_fields("child_1"), USAGE)
        client.report(make_fields("child_2"), USAGE)
        wait_until(
            lambda: (
                (True, REPORTS_PATH, ["child_1", "child_2"]) in read_recorded_requests(record_path)
            ),
            "the flush thread of the child",
        )
        client.report(make_fields("child_3"), USAGE)
        client.close()

    run_in_forked_child(check_and_report, tmp_path / "failure.txt")
    client.close()
    expected = [
        (False, CHECK_PATH, ["child_check"]),
        (False, REPORTS_PATH, ["child_1", "child_2"]),
        (False, REPORTS_PATH, ["child_3"]),
        (True, REPORTS_PATH, ["parent_1"]),
    ]
    if read_recorded_requests(record_path) != expected:
        pytest.fail(f"requests={read_recorded_requests(record_path)}")


@pytest.mark.skipif(not hasattr(os, "fork"), reason="os.fork is POSIX only")
@pytest.mark.filterwarnings(FORK_WARNING_FILTER)
def test_forked_child_is_not_blocked_by_a_flush_in_flight(tmp_path: Path) -> None:
    record_path = tmp_path / "requests.jsonl"
    parent_process_id = os.getpid()
    flush_entered = threading.Event()
    flush_released = threading.Event()
    record = make_recording_handler(record_path)

    def hold_the_parent_flush(request: httpx.Request) -> httpx.Response:
        if os.getpid() == parent_process_id and not flush_released.is_set():
            flush_entered.set()
            flush_released.wait(WAIT_SECONDS)
        return record(request)

    client = Preburn(
        api_key=API_KEY,
        base_url=BASE_URL,
        report_mode="buffered",
        flush_interval=IDLE_INTERVAL_SECONDS,
        batch_size=1,
        transport=httpx.MockTransport(hold_the_parent_flush),
    )
    client.report(make_fields("parent_1"), USAGE)
    if not flush_entered.wait(WAIT_SECONDS):
        pytest.fail("flush of the parent did not start")

    def report_and_close() -> None:
        client.report(make_fields("child_1"), USAGE)
        client.close()

    try:
        run_in_forked_child(report_and_close, tmp_path / "failure.txt")
    finally:
        flush_released.set()
    client.close()
    expected = [(False, REPORTS_PATH, ["child_1"]), (True, REPORTS_PATH, ["parent_1"])]
    if read_recorded_requests(record_path) != expected:
        pytest.fail(f"requests={read_recorded_requests(record_path)}")


@pytest.mark.skipif(not hasattr(os, "fork"), reason="os.fork is POSIX only")
@pytest.mark.filterwarnings(FORK_WARNING_FILTER)
def test_forked_child_never_looks_up_proxies(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    parent_process_id = os.getpid()
    failure_path = tmp_path / "failure.txt"
    parent_lookups: list[int] = []

    def look_up_proxies_in_the_parent_only() -> dict[str, str]:
        if os.getpid() != parent_process_id:
            failure_path.write_text("proxy lookup in the forked child")
            os._exit(1)
        parent_lookups.append(parent_process_id)
        return {}

    monkeypatch.setattr(urllib.request, "getproxies", look_up_proxies_in_the_parent_only)
    monkeypatch.setattr(httpx._utils, "getproxies", look_up_proxies_in_the_parent_only)
    client = Preburn(
        api_key=API_KEY,
        base_url=f"http://127.0.0.1:{find_closed_port()}",
        report_mode="buffered",
        flush_interval=IDLE_INTERVAL_SECONDS,
    )

    def report_in_child() -> None:
        client.report(make_fields("child_1"), USAGE)

    try:
        run_in_forked_child(report_in_child, failure_path)
    finally:
        client.close()
    if parent_lookups != [parent_process_id]:
        pytest.fail(f"parent_lookups={len(parent_lookups)}")


@pytest.mark.skipif(not hasattr(os, "fork"), reason="os.fork is POSIX only")
@pytest.mark.filterwarnings(FORK_WARNING_FILTER)
def test_forked_child_builds_its_own_connections(tmp_path: Path) -> None:
    client = Preburn(
        api_key=API_KEY,
        base_url=f"http://127.0.0.1:{find_closed_port()}",
        report_mode="buffered",
        flush_interval=IDLE_INTERVAL_SECONDS,
    )
    parent_decision = client.check("parent_1", "chat", "openai", "gpt-6-sol")

    def check_and_report_in_child() -> None:
        decision = client.check("child_1", "chat", "openai", "gpt-6-sol")
        if not decision.is_fallback:
            pytest.fail(f"decision_id={decision.decision_id}")
        client.report(decision, USAGE)

    try:
        run_in_forked_child(check_and_report_in_child, tmp_path / "failure.txt")
    finally:
        client.close()
    if not parent_decision.is_fallback:
        pytest.fail(f"decision_id={parent_decision.decision_id}")


def test_api_key_stays_out_of_logs_errors_and_reprs(caplog: pytest.LogCaptureFixture) -> None:
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
        client = Preburn(
            api_key=api_key,
            base_url=BASE_URL,
            report_mode="buffered",
            flush_interval=IDLE_INTERVAL_SECONDS,
            transport=httpx.MockTransport(server.handle),
        )
        server_decision = client.check("customer_1", "chat", "openai", "gpt-6-sol")
        fallback_decision = client.check("customer_1", "chat", "openai", "gpt-6-sol")
        with pytest.raises(AuthenticationError) as check_raised:
            client.check("customer_1", "chat", "openai", "gpt-6-sol")
        errors.append(check_raised.value)
        with pytest.raises(AuthenticationError) as release_raised:
            client.release(server_decision)
        errors.append(release_raised.value)
        client.report(fallback_decision, USAGE)
        client.close()
    if server.requests[0].headers["Authorization"] != f"Bearer {api_key}":
        pytest.fail("api key not sent")
    if not any(record.exc_info for record in caplog.records):
        pytest.fail(f"no traceback logged messages={caplog.messages}")
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


def test_success_answers_that_are_not_json_raise_unexpected_response(server: FakeServer) -> None:
    server.plan(CUSTOMER_PATH, make_text_answer(200, "<html>sign in</html>"))
    server.plan(REPORT_PATH, make_text_answer(202, "<html>sign in</html>"))
    with make_client(server) as client:
        with pytest.raises(APIError) as upsert_raised:
            client.customers.upsert("customer_1")
        with pytest.raises(APIError) as report_raised:
            client.report(make_fields(), USAGE)
    for raised, status in ((upsert_raised, 200), (report_raised, 202)):
        if (raised.value.code, raised.value.status) != ("unexpected_response", status):
            pytest.fail(f"code={raised.value.code} status={raised.value.status}")


def test_undecodable_answer_raises_unexpected_response(server: FakeServer) -> None:
    server.plan(CUSTOMER_PATH, make_undecodable_answer(200))
    with make_client(server) as client, pytest.raises(APIError) as raised:
        client.customers.upsert("customer_1")
    if (raised.value.code, raised.value.status) != ("unexpected_response", None):
        pytest.fail(f"code={raised.value.code} status={raised.value.status}")


def test_release_posts_the_decision_id(server: FakeServer) -> None:
    server.plan(CHECK_PATH, make_check_answer())
    server.plan(RELEASE_PATH, make_json_answer(200, {}))
    with make_client(server) as client:
        client.release(client.check("customer_1", "chat", "openai", "gpt-6-sol"))
    request = server.collect_requests(RELEASE_PATH)[0]
    if (request.method, parse_body(request)) != ("POST", {"decision_id": DECISION_ID}):
        pytest.fail(f"method={request.method} body={parse_body(request)}")


def test_release_of_a_fallback_decision_sends_nothing(server: FakeServer) -> None:
    server.plan(CHECK_PATH, make_problem_answer(503, "counters_unavailable"))
    with make_client(server) as client:
        client.release(client.check("customer_1", "chat", "openai", "gpt-6-sol"))
    if server.collect_requests(RELEASE_PATH):
        pytest.fail("fallback decision released")


def test_customer_upsert_replaces_the_customer(server: FakeServer) -> None:
    path = "/api/v1/customers/team%3Aacme"
    server.plan(path, make_json_answer(200, CUSTOMER_RESPONSE))
    with make_client(server) as client:
        customer = client.customers.upsert(
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


def test_revenue_record_posts_the_entry(server: FakeServer) -> None:
    server.plan(REVENUE_PATH, make_json_answer(201, REVENUE_RESPONSE))
    with make_client(server) as client:
        entry = client.revenue.record(
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


def test_options_fall_back_to_environment_variables(
    server: FakeServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PREBURN_API_KEY", API_KEY)
    monkeypatch.setenv("PREBURN_BASE_URL", "https://preburn.test/prefix")
    server.plan(f"/prefix{CHECK_PATH}", make_check_answer())
    with Preburn(report_mode="sync", transport=httpx.MockTransport(server.handle)) as client:
        client.check("customer_1", "chat", "openai", "gpt-6-sol")
    request = server.requests[0]
    if request.headers["Authorization"] != f"Bearer {API_KEY}":
        pytest.fail("environment api key not sent")
    if str(request.url) != f"https://preburn.test/prefix{CHECK_PATH}":
        pytest.fail(f"url={request.url}")


@pytest.mark.parametrize(
    ("api_key", "base_url", "named_variable"),
    [
        (None, BASE_URL, "PREBURN_API_KEY"),
        (API_KEY, None, "PREBURN_BASE_URL"),
    ],
)
def test_missing_options_raise_configuration_error(
    monkeypatch: pytest.MonkeyPatch, api_key: str | None, base_url: str | None, named_variable: str
) -> None:
    with pytest.raises(ConfigurationError, match=named_variable):
        Preburn(api_key=api_key, base_url=base_url)
    monkeypatch.setenv(named_variable, "")
    with pytest.raises(ConfigurationError, match=named_variable):
        Preburn(api_key=api_key, base_url=base_url)


@pytest.mark.parametrize("api_key", MALFORMED_API_KEYS)
def test_malformed_api_key_raises_without_echoing_it(api_key: str) -> None:
    with pytest.raises(ConfigurationError) as raised:
        Preburn(api_key=api_key, base_url=BASE_URL)
    if raised.value.code != "configuration_invalid" or api_key in str(raised.value):
        pytest.fail(f"code={raised.value.code}")


@pytest.mark.parametrize("api_key", WELL_FORMED_API_KEYS)
def test_well_formed_api_keys_are_accepted(api_key: str) -> None:
    Preburn(api_key=api_key, base_url=BASE_URL, report_mode="sync").close()


@pytest.mark.parametrize(
    "base_url",
    [
        "preburn.test",
        "ftp://preburn.test",
        "http://",
        "https:///api",
        "file:///tmp/preburn",
        "http://[::1",
    ],
)
def test_base_url_needs_an_http_scheme_and_a_host(base_url: str) -> None:
    with pytest.raises(ConfigurationError, match="base_url"):
        Preburn(api_key=API_KEY, base_url=base_url)


@pytest.mark.parametrize(
    ("options", "named_option"),
    [
        ({"check_timeout": 0}, "check_timeout"),
        ({"check_timeout": -1.0}, "check_timeout"),
        ({"check_timeout": float("nan")}, "check_timeout"),
        ({"flush_interval": 0}, "flush_interval"),
        ({"flush_interval": float("inf")}, "flush_interval"),
        ({"max_pending": 0}, "max_pending"),
        ({"batch_size": 0}, "batch_size"),
        ({"report_mode": "Buffered"}, "report_mode"),
    ],
)
def test_invalid_options_raise_configuration_error(
    options: dict[str, Any], named_option: str
) -> None:
    with pytest.raises(ConfigurationError, match=named_option):
        Preburn(api_key=API_KEY, base_url=BASE_URL, **options)
