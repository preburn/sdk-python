"""HTTP clients, JSON request bodies and response classification for the Preburn API."""

import asyncio
import concurrent.futures
import enum
import json
import re
import urllib.request
from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal
from typing import Any
from urllib.parse import quote, urlsplit

import httpx

from preburn._errors import (
    UNEXPECTED_RESPONSE_CODE,
    APIError,
    InvalidField,
    PreburnError,
    RateLimitError,
    make_status_error,
)
from preburn._models import AttributeValue, Decision, RevenueKind, format_timestamp
from preburn._money import format_amount, format_quantity
from preburn._version import __version__

USER_AGENT = f"preburn-sdk-python/{__version__}"
CHECK_PATH = "/api/v1/check"
REPORT_PATH = "/api/v1/report"
REPORTS_PATH = "/api/v1/reports"
RELEASE_PATH = "/api/v1/release"
REVENUE_PATH = "/api/v1/revenue"
CUSTOMERS_PATH = "/api/v1/customers"
DROPPED_REPORTS_HEADER = "Preburn-Dropped-Reports"
REPORT_BATCH_MAXIMUM = 500
REPORT_BATCH_MAXIMUM_BYTES = 1_000_000
FALLBACK_STATUSES = frozenset({502, 503, 504})
FALLBACK_EXCEPTIONS: tuple[type[Exception], ...] = (
    httpx.TransportError,
    httpx.DecodingError,
    TimeoutError,
    asyncio.TimeoutError,
    concurrent.futures.TimeoutError,
)
RATE_LIMITED_STATUS = 429
UNEXPECTED_RESPONSE_DETAIL = "response body is not a problem document"
NOT_JSON_RESPONSE_DETAIL = "response body is not JSON"
UNDECODABLE_RESPONSE_DETAIL = "response body cannot be decoded"

_RETRY_AFTER_PATTERN = re.compile(r"[0-9]+")

JSONObject = dict[str, object]
Usage = Mapping[str, Decimal | int]


class Classification(enum.Enum):
    """How the client should treat the result of a request."""

    SUCCESS = "success"
    FALLBACK_ELIGIBLE = "fallback_eligible"


def resolve_proxy(base_url: str) -> str | None:
    """Returns the proxy the environment sets for requests to `base_url`, or None.

    Reads `HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY` and `NO_PROXY`, and on macOS and Windows the
    system proxy settings. The macOS lookup crashes a forked child, so a client resolves its
    proxy once when it is created and builds every later transport from the result.
    """
    parts = urlsplit(base_url)
    proxies = urllib.request.getproxies()
    proxy = proxies.get(parts.scheme) or proxies.get("all")
    if proxy is None or urllib.request.proxy_bypass(parts.netloc):
        return None
    return proxy if "://" in proxy else f"http://{proxy}"


def build_client(
    api_key: str, base_url: str, timeout: httpx.Timeout, transport: httpx.BaseTransport
) -> httpx.Client:
    """Builds a client that sends the API key and the SDK user agent on every request.

    The client reads nothing from the environment, so building one in a forked child is safe.
    """
    return httpx.Client(
        base_url=base_url,
        headers=_build_headers(api_key),
        timeout=timeout,
        transport=transport,
        trust_env=False,
    )


def build_async_client(
    api_key: str, base_url: str, timeout: httpx.Timeout, transport: httpx.AsyncBaseTransport
) -> httpx.AsyncClient:
    """Builds an async client that sends the API key and the SDK user agent on every request.

    The client reads nothing from the environment.
    """
    return httpx.AsyncClient(
        base_url=base_url,
        headers=_build_headers(api_key),
        timeout=timeout,
        transport=transport,
        trust_env=False,
    )


def classify(response_or_exception: httpx.Response | Exception) -> Classification:
    """Classifies a response, or the exception a request raised instead of answering.

    Returns:
        `SUCCESS` for a 2xx response. `FALLBACK_ELIGIBLE` for transport errors, answers that
        cannot be decoded, timeouts and 502, 503 or 504 responses.

    Raises:
        PreburnError: The response has any other status, mapped by `parse_error`.
        Exception: The given exception, when it is not fallback-eligible.
    """
    if isinstance(response_or_exception, Exception):
        if isinstance(response_or_exception, FALLBACK_EXCEPTIONS):
            return Classification.FALLBACK_ELIGIBLE
        raise response_or_exception
    if response_or_exception.is_success:
        return Classification.SUCCESS
    if response_or_exception.status_code in FALLBACK_STATUSES:
        return Classification.FALLBACK_ELIGIBLE
    raise parse_error(response_or_exception)


def parse_error(response: httpx.Response) -> PreburnError:
    """Builds the error a response that is not a success describes in its problem body.

    401 maps to `AuthenticationError`, 403 to `ScopeError`, 422 to `ValidationError`, 429 to
    `RateLimitError` and every other status to `APIError`. A body that is not a problem
    document, or a 429 without a whole-seconds `Retry-After` header, becomes an `APIError`
    with the code `unexpected_response`.
    """
    status = response.status_code
    problem = _parse_problem(response)
    if problem is None:
        return APIError(UNEXPECTED_RESPONSE_CODE, UNEXPECTED_RESPONSE_DETAIL, status=status)
    code, detail, invalid_fields = problem
    if status != RATE_LIMITED_STATUS:
        return make_status_error(status, code, detail, invalid_fields)
    retry_after = response.headers.get("Retry-After")
    if retry_after is None or _RETRY_AFTER_PATTERN.fullmatch(retry_after) is None:
        return APIError(UNEXPECTED_RESPONSE_CODE, UNEXPECTED_RESPONSE_DETAIL, status=status)
    return RateLimitError(
        code, detail, status=status, errors=invalid_fields, retry_after=int(retry_after)
    )


def read_json_body(response: httpx.Response) -> Any:
    """Returns the JSON body of a response.

    Raises:
        APIError: The body is not JSON, with the code `unexpected_response`.
    """
    try:
        return response.json()
    except ValueError as error:
        raise APIError(
            UNEXPECTED_RESPONSE_CODE, NOT_JSON_RESPONSE_DETAIL, status=response.status_code
        ) from error


def encode_report(body: JSONObject) -> bytes:
    """Encodes a report body as compact JSON, the form a buffered report waits in.

    Raises:
        TypeError: A value, such as an attribute, is not a JSON type.
        ValueError: A float value is not finite.
    """
    return json.dumps(body, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()


def build_check_body(
    *,
    customer_id: str,
    feature: str,
    provider: str,
    model: str,
    attributes: Mapping[str, AttributeValue] | None,
    usage_estimate: Usage | None,
    usage_ceiling: Usage | None,
    customer_user_id: str | None,
) -> JSONObject:
    """Builds the body of `POST /api/v1/check`, leaving out the optional fields that are None.

    Raises:
        ValueError: A quantity is not a valid API quantity.
    """
    body: JSONObject = {
        "customer_id": customer_id,
        "feature": feature,
        "provider": provider,
        "model": model,
    }
    if attributes is not None:
        body["attributes"] = dict(attributes)
    if usage_estimate is not None:
        body["usage_estimate"] = _format_usage(usage_estimate)
    if usage_ceiling is not None:
        body["usage_ceiling"] = _format_usage(usage_ceiling)
    if customer_user_id is not None:
        body["customer_user_id"] = customer_user_id
    return body


def build_report_body(
    decision: Decision,
    usage: Usage,
    attributes: Mapping[str, AttributeValue] | None,
    occurred_at: datetime | None,
) -> JSONObject:
    """Builds the report of a decision's usage for `/api/v1/report` or a batch.

    A server decision is reported by its id, and the server applies `attributes` over the
    decision's attributes. A fallback decision is reported with its idempotency key and the
    checked request, its attributes merged with `attributes`.

    Raises:
        ValueError: A quantity is not a valid API quantity or `occurred_at` has no timezone.
    """
    if decision.idempotency_key is not None:
        return build_fallback_report_body(
            idempotency_key=decision.idempotency_key,
            customer_id=decision.customer_id,
            feature=decision.feature,
            provider=decision.provider,
            model=decision.model,
            usage=usage,
            attributes={**decision.attributes, **(attributes or {})},
            occurred_at=occurred_at,
        )
    body: JSONObject = {
        "decision_source": "server",
        "decision_id": decision.decision_id,
        "usage": _format_usage(usage),
    }
    _add_optional_report_fields(body, attributes, occurred_at)
    return body


def build_fallback_report_body(
    *,
    idempotency_key: str,
    customer_id: str,
    feature: str,
    provider: str,
    model: str,
    usage: Usage,
    attributes: Mapping[str, AttributeValue] | None,
    occurred_at: datetime | None,
) -> JSONObject:
    """Builds the report of usage that ran without a server decision.

    Raises:
        ValueError: A quantity is not a valid API quantity or `occurred_at` has no timezone.
    """
    body: JSONObject = {
        "decision_source": "fallback",
        "idempotency_key": idempotency_key,
        "customer_id": customer_id,
        "feature": feature,
        "provider": provider,
        "model": model,
        "usage": _format_usage(usage),
    }
    _add_optional_report_fields(body, attributes, occurred_at)
    return body


def build_release_body(decision: Decision) -> JSONObject:
    """Builds the body of `POST /api/v1/release` for a server decision.

    Raises:
        ValueError: The decision is a fallback decision, which holds no reservation.
    """
    if decision.decision_id is None:
        raise ValueError("fallback decision has no reservation to release")
    return {"decision_id": decision.decision_id}


def build_customer_path(external_id: str) -> str:
    """Builds the path of `PUT /api/v1/customers/{external_id}` with the id escaped."""
    return f"{CUSTOMERS_PATH}/{quote(external_id, safe='')}"


def build_customer_body(
    *,
    display_name: str | None,
    plan_id: str | None,
    metadata: Mapping[str, object] | None,
) -> JSONObject:
    """Builds the body of a customer upsert, which replaces every field of the customer.

    A None display name or plan clears it, and None metadata stores an empty object.
    """
    return {
        "display_name": display_name,
        "plan_id": plan_id,
        "metadata": {} if metadata is None else dict(metadata),
    }


def build_revenue_body(
    *,
    customer_id: str,
    kind: RevenueKind,
    amount: Decimal | int,
    period_start: datetime,
    period_end: datetime,
    source_reference: str,
    occurred_at: datetime | None,
) -> JSONObject:
    """Builds the body of `POST /api/v1/revenue`.

    Raises:
        ValueError: The amount is not a valid API amount or a datetime has no timezone.
    """
    body: JSONObject = {
        "customer_id": customer_id,
        "kind": kind,
        "amount": format_amount(amount),
        "period_start": format_timestamp(period_start),
        "period_end": format_timestamp(period_end),
        "source_reference": source_reference,
    }
    if occurred_at is not None:
        body["occurred_at"] = format_timestamp(occurred_at)
    return body


def _build_headers(api_key: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {api_key}", "User-Agent": USER_AGENT}


def _parse_problem(
    response: httpx.Response,
) -> tuple[str, str, tuple[InvalidField, ...]] | None:
    try:
        body = response.json()
    except ValueError:
        return None
    if not isinstance(body, dict):
        return None
    code = body.get("code")
    detail = body.get("detail")
    errors = body.get("errors", [])
    if not isinstance(code, str) or not isinstance(detail, str) or not isinstance(errors, list):
        return None
    invalid_fields: list[InvalidField] = []
    for error in errors:
        if not isinstance(error, dict):
            return None
        location = error.get("location")
        message = error.get("message")
        if not isinstance(location, str) or not isinstance(message, str):
            return None
        invalid_fields.append(InvalidField(location=location, message=message))
    return code, detail, tuple(invalid_fields)


def _format_usage(usage: Usage) -> dict[str, str]:
    return {meter: format_quantity(quantity) for meter, quantity in usage.items()}


def _add_optional_report_fields(
    body: JSONObject,
    attributes: Mapping[str, AttributeValue] | None,
    occurred_at: datetime | None,
) -> None:
    if attributes is not None:
        body["attributes"] = dict(attributes)
    if occurred_at is not None:
        body["occurred_at"] = format_timestamp(occurred_at)
