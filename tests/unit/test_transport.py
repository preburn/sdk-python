import asyncio
import concurrent.futures
import json
import secrets
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import httpx
import pytest

from preburn._errors import (
    APIError,
    AuthenticationError,
    InvalidField,
    PreburnError,
    RateLimitError,
    ScopeError,
    ValidationError,
)
from preburn._models import Decision
from preburn._transport import (
    USER_AGENT,
    Classification,
    build_async_client,
    build_check_body,
    build_client,
    build_customer_body,
    build_customer_path,
    build_fallback_report_body,
    build_release_body,
    build_report_body,
    build_revenue_body,
    classify,
    encode_report,
    parse_error,
    resolve_proxy,
)
from preburn._version import __version__

API_KEY = f"pb_test_runtime_{secrets.token_hex(16)}"
BASE_URL = "http://preburn.test"


def make_problem_response(
    status: int,
    code: str,
    *,
    errors: list[dict[str, str]] | None = None,
    headers: dict[str, str] | None = None,
) -> httpx.Response:
    body: dict[str, object] = {
        "type": f"https://github.com/preburn/preburn/blob/main/docs/errors.md#{code}",
        "title": "Problem",
        "status": status,
        "detail": f"detail of {code}",
        "code": code,
    }
    if errors is not None:
        body["errors"] = errors
    return httpx.Response(
        status,
        content=json.dumps(body).encode(),
        headers={"Content-Type": "application/problem+json", **(headers or {})},
    )


def make_server_decision() -> Decision:
    return Decision(
        decision_id="dec_01jbvagescfn78y0938nkrkayd",
        outcome="allow",
        reason="no_policy_matched",
        provider="fal_ai",
        model="veo-3",
        overrides={},
        estimated_cost=Decimal("3.2"),
        reserved_amount=Decimal("3.2"),
        estimate_basis="request_estimate",
        cost_status="costed",
        matched_policy_id=None,
        fallback_outcome="allow",
        expires_at=datetime(2026, 9, 26, 10, 10, tzinfo=timezone.utc),
        signals=None,
        customer_id="customer_1",
        feature="text_to_video",
        attributes={"resolution": "1080p"},
        idempotency_key=None,
    )


def test_client_sends_authorization_user_agent_and_json() -> None:
    captured: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json={})

    with build_client(API_KEY, BASE_URL, httpx.Timeout(1.0), httpx.MockTransport(handle)) as client:
        client.post("/api/v1/check", json={"customer_id": "customer_1"})
    request = captured[0]
    if request.headers["Authorization"] != f"Bearer {API_KEY}":
        pytest.fail("authorization header not set")
    if request.headers["User-Agent"] != f"preburn-sdk-python/{__version__}":
        pytest.fail(f"user_agent={request.headers['User-Agent']}")
    if request.headers["Content-Type"] != "application/json":
        pytest.fail(f"content_type={request.headers['Content-Type']}")
    if str(request.url) != "http://preburn.test/api/v1/check":
        pytest.fail(f"url={request.url}")
    if json.loads(request.content) != {"customer_id": "customer_1"}:
        pytest.fail(f"content={request.content!r}")


def test_async_client_sends_authorization_and_user_agent() -> None:
    captured: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json={})

    async def send() -> None:
        async with build_async_client(
            API_KEY, f"{BASE_URL}/prefix", httpx.Timeout(1.0), httpx.MockTransport(handle)
        ) as client:
            await client.post("/api/v1/check", json={})

    asyncio.run(send())
    request = captured[0]
    if request.headers["Authorization"] != f"Bearer {API_KEY}":
        pytest.fail("authorization header not set")
    if request.headers["User-Agent"] != USER_AGENT:
        pytest.fail(f"user_agent={request.headers['User-Agent']}")
    if str(request.url) != "http://preburn.test/prefix/api/v1/check":
        pytest.fail(f"url={request.url}")


@pytest.mark.parametrize(
    ("status", "code", "error_class"),
    [
        (401, "authentication_required", AuthenticationError),
        (403, "scope_forbidden", ScopeError),
        (403, "csrf_invalid", ScopeError),
        (422, "validation_failed", ValidationError),
        (400, "validation_failed", APIError),
        (404, "not_found", APIError),
        (409, "decision_not_reportable", APIError),
        (413, "payload_too_large", APIError),
        (500, "internal_error", APIError),
    ],
)
def test_status_maps_to_error_class(
    status: int, code: str, error_class: type[PreburnError]
) -> None:
    response = make_problem_response(
        status,
        code,
        errors=[{"location": "body.usage.output_seconds", "message": "must be a decimal"}],
    )
    with pytest.raises(error_class) as raised:
        classify(response)
    error = raised.value
    if type(error) is not error_class:
        pytest.fail(f"error_class={type(error).__name__}")
    if (error.status, error.code, error.detail) != (status, code, f"detail of {code}"):
        pytest.fail(f"status={error.status} code={error.code} detail={error.detail}")
    expected_errors = (
        InvalidField(location="body.usage.output_seconds", message="must be a decimal"),
    )
    if error.errors != expected_errors:
        pytest.fail(f"errors={error.errors}")


def test_rate_limited_carries_retry_after() -> None:
    response = make_problem_response(429, "rate_limited", headers={"Retry-After": "12"})
    with pytest.raises(RateLimitError) as raised:
        classify(response)
    error = raised.value
    if (error.status, error.code, error.retry_after, error.errors) != (429, "rate_limited", 12, ()):
        pytest.fail(f"status={error.status} code={error.code} retry_after={error.retry_after}")


@pytest.mark.parametrize("retry_after", [None, "soon", "-1", "Wed, 21 Oct 2026 07:28:00 GMT"])
def test_rate_limited_without_whole_seconds_is_unexpected(retry_after: str | None) -> None:
    headers = {} if retry_after is None else {"Retry-After": retry_after}
    response = make_problem_response(429, "rate_limited", headers=headers)
    error = parse_error(response)
    if type(error) is not APIError or (error.status, error.code) != (429, "unexpected_response"):
        pytest.fail(f"error_class={type(error).__name__} code={error.code}")


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(
            400, content=b"<html>Bad Request</html>", headers={"Content-Type": "text/html"}
        ),
        httpx.Response(401, json={"error": "unauthorized"}),
        httpx.Response(422, json=["validation_failed"]),
        httpx.Response(404, content=b""),
        httpx.Response(500, content=b"\xff\xfe not utf-8"),
        httpx.Response(
            422, json={"code": "validation_failed", "detail": "bad", "errors": [{"location": 1}]}
        ),
        httpx.Response(422, json={"code": "validation_failed", "detail": "bad", "errors": "body"}),
        httpx.Response(301, headers={"Location": "https://preburn.test/api/v1/check"}),
    ],
)
def test_body_that_is_not_a_problem_is_unexpected_response(response: httpx.Response) -> None:
    with pytest.raises(APIError) as raised:
        classify(response)
    error = raised.value
    if (error.status, error.code) != (response.status_code, "unexpected_response"):
        pytest.fail(f"status={error.status} code={error.code}")


@pytest.mark.parametrize("status", [502, 503, 504])
def test_gateway_and_unavailable_statuses_are_fallback_eligible(status: int) -> None:
    html = httpx.Response(status, content=b"<html>Bad Gateway</html>")
    if classify(html) is not Classification.FALLBACK_ELIGIBLE:
        pytest.fail(f"status={status} html body not fallback eligible")
    problem = make_problem_response(status, "counters_unavailable")
    if classify(problem) is not Classification.FALLBACK_ELIGIBLE:
        pytest.fail(f"status={status} problem body not fallback eligible")


def test_internal_error_is_not_fallback_eligible() -> None:
    response = make_problem_response(500, "internal_error")
    with pytest.raises(APIError) as raised:
        classify(response)
    if raised.value.status != 500:
        pytest.fail(f"status={raised.value.status}")


@pytest.mark.parametrize("code", ["counters_unavailable", "database_unavailable"])
def test_unavailable_problem_maps_to_api_error(code: str) -> None:
    error = parse_error(make_problem_response(503, code))
    if type(error) is not APIError or (error.status, error.code) != (503, code):
        pytest.fail(f"error_class={type(error).__name__} status={error.status} code={error.code}")


@pytest.mark.parametrize("status", [200, 201, 202])
def test_success_statuses_classify_as_success(status: int) -> None:
    if classify(httpx.Response(status, json={})) is not Classification.SUCCESS:
        pytest.fail(f"status={status}")


@pytest.mark.parametrize(
    "exception",
    [
        httpx.ConnectError("connection refused"),
        httpx.ConnectTimeout("connect timeout"),
        httpx.ReadTimeout("read timeout"),
        httpx.PoolTimeout("pool timeout"),
        httpx.RemoteProtocolError("server disconnected"),
        httpx.DecodingError("broken gzip"),
        TimeoutError(),
        asyncio.TimeoutError(),
        concurrent.futures.TimeoutError(),
    ],
)
def test_transport_errors_and_timeouts_are_fallback_eligible(exception: Exception) -> None:
    if classify(exception) is not Classification.FALLBACK_ELIGIBLE:
        pytest.fail(f"exception={type(exception).__name__}")


def test_other_exceptions_are_raised_again() -> None:
    with pytest.raises(KeyError):
        classify(KeyError("missing"))


@pytest.fixture(name="proxy_environment")
def clear_proxy_environment(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    for name in ("http_proxy", "https_proxy", "all_proxy", "no_proxy"):
        monkeypatch.delenv(name, raising=False)
        monkeypatch.delenv(name.upper(), raising=False)
    return monkeypatch


@pytest.mark.parametrize(
    ("variables", "base_url", "expected"),
    [
        (
            {"HTTPS_PROXY": "http://proxy.test:3128"},
            "https://preburn.test",
            "http://proxy.test:3128",
        ),
        ({"HTTPS_PROXY": "proxy.test:3128"}, "https://preburn.test", "http://proxy.test:3128"),
        ({"HTTPS_PROXY": "http://proxy.test:3128"}, "http://preburn.test", None),
        ({"ALL_PROXY": "http://proxy.test:3128"}, "http://preburn.test", "http://proxy.test:3128"),
        (
            {"HTTP_PROXY": "http://proxy.test:3128", "NO_PROXY": "preburn.test"},
            "http://preburn.test:8080",
            None,
        ),
        ({"HTTP_PROXY": "http://proxy.test:3128", "NO_PROXY": "*"}, "http://preburn.test", None),
    ],
)
def test_proxy_comes_from_the_environment(
    proxy_environment: pytest.MonkeyPatch,
    variables: dict[str, str],
    base_url: str,
    expected: str | None,
) -> None:
    for name, value in variables.items():
        proxy_environment.setenv(name, value)
    if resolve_proxy(base_url) != expected:
        pytest.fail(f"proxy={resolve_proxy(base_url)} expected={expected}")


def test_encoded_report_rejects_values_that_are_not_json() -> None:
    with pytest.raises(TypeError, match="Decimal"):
        encode_report({"attributes": {"duration": Decimal("5")}})
    with pytest.raises(ValueError, match="JSON compliant"):
        encode_report({"attributes": {"duration": float("nan")}})
    if encode_report({"feature": "chat", "attributes": {"tier": "é"}}) != (
        '{"feature":"chat","attributes":{"tier":"é"}}'.encode()
    ):
        pytest.fail("report not encoded as compact UTF-8 JSON")


def test_check_body_formats_quantities_and_omits_absent_fields() -> None:
    body = build_check_body(
        customer_id="customer_1",
        feature="text_to_video",
        provider="fal_ai",
        model="veo-3",
        attributes={"resolution": "1080p", "audio": True, "steps": 30},
        usage_estimate={"output_seconds": Decimal("8.500"), "input_tokens": 1200},
        usage_ceiling=None,
        customer_user_id=None,
    )
    expected = {
        "customer_id": "customer_1",
        "feature": "text_to_video",
        "provider": "fal_ai",
        "model": "veo-3",
        "attributes": {"resolution": "1080p", "audio": True, "steps": 30},
        "usage_estimate": {"output_seconds": "8.5", "input_tokens": "1200"},
    }
    if body != expected:
        pytest.fail(f"body={body}")


def test_check_body_includes_ceiling_and_customer_user() -> None:
    body = build_check_body(
        customer_id="customer_1",
        feature="text_to_video",
        provider="fal_ai",
        model="veo-3",
        attributes=None,
        usage_estimate=None,
        usage_ceiling={"output_seconds": Decimal("10")},
        customer_user_id="user_7",
    )
    expected = {
        "customer_id": "customer_1",
        "feature": "text_to_video",
        "provider": "fal_ai",
        "model": "veo-3",
        "usage_ceiling": {"output_seconds": "10"},
        "customer_user_id": "user_7",
    }
    if body != expected:
        pytest.fail(f"body={body}")


def test_report_body_for_server_decision_names_only_the_decision() -> None:
    decision = make_server_decision()
    occurred_at = datetime(2026, 9, 26, 12, 0, 0, 250000, tzinfo=timezone(timedelta(hours=2)))
    body = build_report_body(
        decision, {"output_seconds": Decimal("6")}, {"audio": False}, occurred_at
    )
    expected = {
        "decision_source": "server",
        "decision_id": "dec_01jbvagescfn78y0938nkrkayd",
        "usage": {"output_seconds": "6"},
        "attributes": {"audio": False},
        "occurred_at": "2026-09-26T10:00:00.250000Z",
    }
    if body != expected:
        pytest.fail(f"body={body}")


def test_report_body_for_server_decision_omits_absent_fields() -> None:
    body = build_report_body(make_server_decision(), {"output_seconds": 6}, None, None)
    expected = {
        "decision_source": "server",
        "decision_id": "dec_01jbvagescfn78y0938nkrkayd",
        "usage": {"output_seconds": "6"},
    }
    if body != expected:
        pytest.fail(f"body={body}")


def test_report_body_for_fallback_decision_carries_request_and_key() -> None:
    decision = Decision.fallback(
        customer_id="customer_1",
        feature="text_to_video",
        provider="fal_ai",
        model="veo-3",
        attributes={"resolution": "720p", "audio": True},
        outcome="allow",
    )
    body = build_report_body(
        decision,
        {"output_seconds": Decimal("4")},
        {"audio": False},
        datetime(2026, 9, 26, 10, 0, tzinfo=timezone.utc),
    )
    expected = {
        "decision_source": "fallback",
        "idempotency_key": decision.idempotency_key,
        "customer_id": "customer_1",
        "feature": "text_to_video",
        "provider": "fal_ai",
        "model": "veo-3",
        "usage": {"output_seconds": "4"},
        "attributes": {"resolution": "720p", "audio": False},
        "occurred_at": "2026-09-26T10:00:00Z",
    }
    if body != expected:
        pytest.fail(f"body={body}")


def test_fallback_report_body_from_explicit_fields() -> None:
    idempotency_key = str(uuid.uuid4())
    body = build_fallback_report_body(
        idempotency_key=idempotency_key,
        customer_id="customer_1",
        feature="chat",
        provider="openai",
        model="gpt-6-sol",
        usage={"input_tokens": 1200, "output_tokens": 300},
        attributes=None,
        occurred_at=None,
    )
    expected = {
        "decision_source": "fallback",
        "idempotency_key": idempotency_key,
        "customer_id": "customer_1",
        "feature": "chat",
        "provider": "openai",
        "model": "gpt-6-sol",
        "usage": {"input_tokens": "1200", "output_tokens": "300"},
    }
    if body != expected:
        pytest.fail(f"body={body}")


def test_report_body_rejects_invalid_usage_before_queueing() -> None:
    decision = make_server_decision()
    usage = {"output_seconds": Decimal("0.0000001")}
    with pytest.raises(ValueError, match="quantity"):
        build_report_body(decision, usage, None, None)


def test_report_body_rejects_naive_timestamps() -> None:
    decision = make_server_decision()
    naive_time = datetime(2026, 9, 26, 10, 0)
    with pytest.raises(ValueError, match="timezone"):
        build_report_body(decision, {"output_seconds": 1}, None, naive_time)


def test_release_body_names_the_decision() -> None:
    body = build_release_body(make_server_decision())
    if body != {"decision_id": "dec_01jbvagescfn78y0938nkrkayd"}:
        pytest.fail(f"body={body}")


def test_release_body_rejects_fallback_decision() -> None:
    decision = Decision.fallback(
        customer_id="customer_1",
        feature="chat",
        provider="openai",
        model="gpt-6-sol",
        attributes={},
        outcome="allow",
    )
    with pytest.raises(ValueError, match="fallback"):
        build_release_body(decision)


def test_customer_body_replaces_every_field() -> None:
    body = build_customer_body(display_name=None, plan_id=None, metadata=None)
    if body != {"display_name": None, "plan_id": None, "metadata": {}}:
        pytest.fail(f"body={body}")
    body = build_customer_body(display_name="Acme", plan_id="pln_1", metadata={"tier": "gold"})
    if body != {"display_name": "Acme", "plan_id": "pln_1", "metadata": {"tier": "gold"}}:
        pytest.fail(f"body={body}")


def test_customer_path_escapes_the_external_id() -> None:
    path = build_customer_path("team:acme@example.com/1")
    if path != "/api/v1/customers/team%3Aacme%40example.com%2F1":
        pytest.fail(f"path={path}")


def test_revenue_body_formats_amount_and_times() -> None:
    body = build_revenue_body(
        customer_id="customer_1",
        kind="subscription",
        amount=Decimal("30.00"),
        period_start=datetime(2026, 9, 1, tzinfo=timezone.utc),
        period_end=datetime(2026, 10, 1, tzinfo=timezone.utc),
        source_reference="in_123",
        occurred_at=None,
    )
    expected = {
        "customer_id": "customer_1",
        "kind": "subscription",
        "amount": "30.000000000",
        "period_start": "2026-09-01T00:00:00Z",
        "period_end": "2026-10-01T00:00:00Z",
        "source_reference": "in_123",
    }
    if body != expected:
        pytest.fail(f"body={body}")
    with_occurred_at = build_revenue_body(
        customer_id="customer_1",
        kind="refund",
        amount=5,
        period_start=datetime(2026, 9, 1, tzinfo=timezone.utc),
        period_end=datetime(2026, 9, 1, tzinfo=timezone.utc),
        source_reference="re_1",
        occurred_at=datetime(2026, 9, 3, 8, 30, tzinfo=timezone.utc),
    )
    if (
        with_occurred_at["occurred_at"] != "2026-09-03T08:30:00Z"
        or with_occurred_at["amount"] != "5.000000000"
    ):
        pytest.fail(f"body={with_occurred_at}")
