"""Synchronous Preburn client, with the option and report rules the async client shares."""

import contextvars
import logging
import math
import os
import re
import weakref
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from functools import partial
from types import TracebackType
from typing import Literal
from urllib.parse import urlsplit

import httpx

from preburn._buffer import ReportBuffer
from preburn._errors import UNEXPECTED_RESPONSE_CODE, APIError, ConfigurationError
from preburn._fallback import FallbackCache
from preburn._flusher import ReportFlusher
from preburn._models import (
    AttributeValue,
    Customer,
    Decision,
    ReportFields,
    ReportResult,
    RevenueEntry,
    RevenueKind,
)
from preburn._transport import (
    CHECK_PATH,
    FALLBACK_EXCEPTIONS,
    RELEASE_PATH,
    REPORT_PATH,
    REVENUE_PATH,
    UNDECODABLE_RESPONSE_DETAIL,
    Classification,
    JSONObject,
    Usage,
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
    read_json_body,
    resolve_proxy,
)

ReportMode = Literal["auto", "sync", "buffered"]
"""How `report` sends reports.

`sync` posts each report and returns its result. `buffered` queues reports for a background
flush. `auto` picks `sync` when a serverless environment variable is set, else `buffered`.
"""
Send = Callable[[str, str, JSONObject], httpx.Response]
"""Sends a JSON request and returns the success response, raising the error of any other."""

API_KEY_VARIABLE = "PREBURN_API_KEY"
BASE_URL_VARIABLE = "PREBURN_BASE_URL"
SERVERLESS_VARIABLES = (
    "AWS_LAMBDA_FUNCTION_NAME",
    "K_SERVICE",
    "FUNCTIONS_WORKER_RUNTIME",
    "VERCEL",
)
REPORT_MODES = frozenset({"auto", "sync", "buffered"})
BASE_URL_SCHEMES = frozenset({"http", "https"})
REQUEST_TIMEOUT_SECONDS = 5.0
CHECK_THREAD_MAXIMUM = 100
CHECK_THREAD_NAME_PREFIX = "preburn-check"
CHECK_FALLBACK_EVENT = "check.fallback"

_API_KEY_PATTERN = re.compile(r"pb_(test|live)_(runtime|admin)_[0-9A-Za-z]{32}")

logger = logging.getLogger("preburn")


@dataclass(frozen=True, kw_only=True)
class ClientOptions:
    """Validated options of a client, with the environment variable fallbacks applied.

    Attributes:
        api_key: API key secret.
        base_url: Base URL of the Preburn server.
        check_timeout: Seconds a check may take in total, from resolving the host to
            reading the answer, before it falls back.
        buffer_reports: True when reports are queued for the background flush.
        flush_interval: Seconds between background flushes.
        batch_size: Pending reports that start a flush before the interval ends.
        max_pending: Pending reports kept before the oldest is dropped.
    """

    api_key: str = field(repr=False)
    base_url: str
    check_timeout: float
    buffer_reports: bool
    flush_interval: float
    batch_size: int
    max_pending: int


class Customers:
    """Customer routes, available as `Preburn.customers`."""

    def __init__(self, send: Send) -> None:
        """Creates the routes on the client's request sender."""
        self._send = send

    def upsert(
        self,
        external_id: str,
        display_name: str | None = None,
        plan_id: str | None = None,
        metadata: Mapping[str, object] | None = None,
    ) -> Customer:
        """Creates the customer or replaces every field of it.

        Args:
            external_id: Id of the customer in your system.
            display_name: Name the dashboard shows. None clears it.
            plan_id: Plan id, such as `pln_01jbvagescfn78y0938nkrkayd`. None applies the
                default plan of the environment.
            metadata: JSON object to attach. None stores an empty object.

        Returns:
            The stored customer.

        Raises:
            PreburnError: The server rejected the request, or its answer cannot be read.
            httpx.HTTPError: The request did not reach the server.
        """
        body = build_customer_body(display_name=display_name, plan_id=plan_id, metadata=metadata)
        response = self._send("PUT", build_customer_path(external_id), body)
        return Customer.from_response(read_json_body(response))


class Revenue:
    """Revenue routes, available as `Preburn.revenue`."""

    def __init__(self, send: Send) -> None:
        """Creates the routes on the client's request sender."""
        self._send = send

    def record(
        self,
        customer_id: str,
        kind: RevenueKind,
        amount: Decimal | int,
        period_start: datetime,
        period_end: datetime,
        source_reference: str,
        occurred_at: datetime | None = None,
    ) -> RevenueEntry:
        """Records a revenue entry for the customer.

        Recording the same `source_reference` again returns the stored entry with
        `duplicate` set.

        Args:
            customer_id: Id of the customer in your system.
            kind: `subscription` and `adjustment` add the amount to net revenue, `stripe_fee`,
                `refund` and `credit_note` subtract it.
            amount: Non-negative USD amount with at most 9 decimals.
            period_start: Start of the period the entry belongs to, timezone-aware.
            period_end: End of the period, timezone-aware. Equal to `period_start` for a
                one-time line.
            source_reference: Id of the entry in your billing system.
            occurred_at: When the revenue was recognized. The server uses the current time
                when None.

        Returns:
            The stored entry.

        Raises:
            ValueError: The amount or a datetime cannot be sent.
            PreburnError: The server rejected the request, or its answer cannot be read.
            httpx.HTTPError: The request did not reach the server.
        """
        body = build_revenue_body(
            customer_id=customer_id,
            kind=kind,
            amount=amount,
            period_start=period_start,
            period_end=period_end,
            source_reference=source_reference,
            occurred_at=occurred_at,
        )
        return RevenueEntry.from_response(read_json_body(self._send("POST", REVENUE_PATH, body)))


class Preburn:
    """Synchronous client of the Preburn API.

    Check each AI request before it runs, then report its usage or release its decision. A
    check that does not answer within `check_timeout`, cannot connect, gets a 502, 503 or 504
    answer, or gets an answer that cannot be read returns a fallback decision instead of
    raising. Its outcome is the last `fallback_outcome` the server sent for the customer and
    feature, else for the feature, else `allow`.

    In buffered report mode a daemon thread sends reports in batches. Close the client, or
    use it as a context manager, to send the reports still pending. An unclosed client sends
    them at interpreter exit, waiting at most 5 seconds.

    A client survives `os.fork()`. The child gets its own connections, an empty fallback cache
    and an empty report queue with its own flush thread, started by the child's first report.
    The parent keeps its pending reports.

    Attributes:
        customers: Customer routes.
        revenue: Revenue routes.
    """

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        check_timeout: float = 0.25,
        report_mode: ReportMode = "auto",
        flush_interval: float = 1.0,
        batch_size: int = 100,
        max_pending: int = 10_000,
        *,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        """Creates the client and, in buffered report mode, starts its flush thread.

        Args:
            api_key: API key secret, such as `pb_live_runtime_...`. Defaults to the
                `PREBURN_API_KEY` environment variable.
            base_url: Base URL of the Preburn server, such as `https://preburn.example.com`.
                Defaults to the `PREBURN_BASE_URL` environment variable.
            check_timeout: Seconds a check may take in total, from resolving the host to
                reading the answer, before it falls back.
            report_mode: `sync`, `buffered`, or `auto` for `sync` in serverless environments
                (`AWS_LAMBDA_FUNCTION_NAME`, `K_SERVICE`, `FUNCTIONS_WORKER_RUNTIME` or
                `VERCEL` set) and `buffered` elsewhere.
            flush_interval: Seconds between background flushes.
            batch_size: Pending reports that start a flush before the interval ends.
            max_pending: Pending reports kept while the server cannot be reached. Beyond it
                the oldest report is dropped, and the drop count is sent with the next batch.
            transport: HTTP transport to send requests through, such as a proxy transport.

        Raises:
            ConfigurationError: An option is missing or invalid.
        """
        options = resolve_options(
            api_key=api_key,
            base_url=base_url,
            check_timeout=check_timeout,
            report_mode=report_mode,
            flush_interval=flush_interval,
            batch_size=batch_size,
            max_pending=max_pending,
        )
        self._options = options
        self._transport = transport
        self._proxy = None if transport is not None else resolve_proxy(options.base_url)
        self._http_client = self._build_http_client()
        self._check_executor = ThreadPoolExecutor(CHECK_THREAD_MAXIMUM, CHECK_THREAD_NAME_PREFIX)
        self._fallback_outcomes = FallbackCache()
        self._flusher: ReportFlusher | None = None
        if options.buffer_reports:
            self._flusher = ReportFlusher(
                self._http_client,
                ReportBuffer(options.max_pending),
                flush_interval=options.flush_interval,
                batch_size=options.batch_size,
            )
            self._flusher.start()
        self.customers = Customers(self._send)
        self.revenue = Revenue(self._send)
        if hasattr(os, "register_at_fork"):
            os.register_at_fork(after_in_child=partial(_reset_in_forked_child, weakref.ref(self)))

    @property
    def check_timeout(self) -> float:
        """Seconds a check may take in total before it falls back."""
        return self._options.check_timeout

    def __enter__(self) -> "Preburn":
        """Returns the client."""
        return self

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Closes the client."""
        self.close()

    def check(
        self,
        customer_id: str,
        feature: str,
        provider: str,
        model: str,
        attributes: Mapping[str, AttributeValue] | None = None,
        usage_estimate: Usage | None = None,
        usage_ceiling: Usage | None = None,
        customer_user_id: str | None = None,
    ) -> Decision:
        """Decides whether a request may run, and on which model and parameters.

        Args:
            customer_id: Id of the customer in your system.
            feature: Feature of your product the request serves, such as `chat`.
            provider: Provider the request would run on, such as `openai`.
            model: Model the request would run on.
            attributes: Request attributes that select the price, such as a resolution.
            usage_estimate: Expected usage by meter, such as `{"input_tokens": 1200}`.
            usage_ceiling: Most usage the request can reach by meter.
            customer_user_id: Id of the customer's user in your system.

        Returns:
            The server's decision, or a fallback decision when Preburn cannot answer.

        Raises:
            ValueError: A quantity cannot be sent.
            PreburnError: The server rejected the check with a status other than 502, 503
                or 504.
        """
        body = build_check_body(
            customer_id=customer_id,
            feature=feature,
            provider=provider,
            model=model,
            attributes=attributes,
            usage_estimate=usage_estimate,
            usage_ceiling=usage_ceiling,
            customer_user_id=customer_user_id,
        )
        request = self._check_executor.submit(
            contextvars.copy_context().run,
            self._http_client.post,
            CHECK_PATH,
            json=body,
            timeout=httpx.Timeout(self._options.check_timeout),
        )
        answer: httpx.Response | Exception
        try:
            answer = request.result(timeout=self._options.check_timeout)
        except FALLBACK_EXCEPTIONS as error:
            request.cancel()
            answer = error
        return decide_check(
            self._fallback_outcomes,
            answer,
            customer_id=customer_id,
            feature=feature,
            provider=provider,
            model=model,
            attributes={} if attributes is None else attributes,
        )

    def report(
        self,
        decision_or_fields: Decision | ReportFields,
        usage: Usage,
        attributes: Mapping[str, AttributeValue] | None = None,
        occurred_at: datetime | None = None,
    ) -> ReportResult | None:
        """Reports the usage of a request.

        A server decision is reported by its id, and `attributes` override the checked
        attributes. A fallback decision or `ReportFields` is reported with its idempotency key.

        Args:
            decision_or_fields: The request's decision, or `ReportFields` for a request that
                had no check.
            usage: Measured usage by meter, such as `{"input_tokens": 1180}`.
            attributes: Attributes to rate the usage with.
            occurred_at: When the request ran, timezone-aware. Defaults to now, taken when
                `report` is called so a buffered report keeps its time.

        Returns:
            The stored ledger entry in sync report mode. None in buffered report mode, where
            the report is queued.

        Raises:
            ValueError: A quantity or `occurred_at` cannot be sent.
            TypeError: An attribute value is not a JSON type.
            RuntimeError: The client is closed.
            PreburnError: In sync report mode, the server rejected the report, or its answer
                cannot be read.
            httpx.HTTPError: In sync report mode, the request did not reach the server.
        """
        body = build_report_request(decision_or_fields, usage, attributes, occurred_at)
        if self._flusher is not None:
            self._flusher.add(encode_report(body))
            return None
        return ReportResult.from_response(read_json_body(self._send("POST", REPORT_PATH, body)))

    def release(self, decision: Decision) -> None:
        """Releases the reservation of a decision whose request did not run.

        Releasing a decision twice changes nothing. A fallback decision reserves nothing, so
        releasing it sends no request.

        Raises:
            PreburnError: The server rejected the release.
            httpx.HTTPError: The request did not reach the server.
        """
        if decision.is_fallback:
            return
        self._send("POST", RELEASE_PATH, build_release_body(decision))

    def flush(self) -> None:
        """Sends the pending reports now in buffered report mode.

        Reports that cannot be sent stay pending for the next flush.
        """
        if self._flusher is not None:
            self._flusher.flush()

    def close(self) -> None:
        """Sends the pending reports, stops the flush thread and closes the connections."""
        if self._flusher is not None:
            self._flusher.stop()
        self._check_executor.shutdown(wait=False, cancel_futures=True)
        self._http_client.close()

    def _send(self, method: str, path: str, body: JSONObject) -> httpx.Response:
        try:
            response = self._http_client.request(method, path, json=body)
        except httpx.DecodingError as error:
            raise APIError(UNEXPECTED_RESPONSE_CODE, UNDECODABLE_RESPONSE_DETAIL) from error
        if not response.is_success:
            raise parse_error(response)
        return response

    def _build_http_client(self) -> httpx.Client:
        transport = self._transport
        if transport is None:
            transport = httpx.HTTPTransport(proxy=self._proxy)
        return build_client(
            self._options.api_key,
            self._options.base_url,
            httpx.Timeout(REQUEST_TIMEOUT_SECONDS),
            transport,
        )

    def _reset_after_fork(self) -> None:
        if self._http_client.is_closed:
            return
        self._http_client = self._build_http_client()
        self._check_executor = ThreadPoolExecutor(CHECK_THREAD_MAXIMUM, CHECK_THREAD_NAME_PREFIX)
        self._fallback_outcomes = FallbackCache()
        if self._flusher is not None:
            self._flusher.reset_after_fork(self._http_client)


def resolve_options(
    *,
    api_key: str | None,
    base_url: str | None,
    check_timeout: float,
    report_mode: ReportMode,
    flush_interval: float,
    batch_size: int,
    max_pending: int,
) -> ClientOptions:
    """Applies the environment variable fallbacks to the client options and validates them.

    An empty environment variable counts as unset.

    Raises:
        ConfigurationError: An option is missing or invalid.
    """
    resolved_api_key = os.environ.get(API_KEY_VARIABLE, "") if api_key is None else api_key
    if not resolved_api_key:
        raise ConfigurationError(f"api_key missing, pass it or set {API_KEY_VARIABLE}")
    if _API_KEY_PATTERN.fullmatch(resolved_api_key) is None:
        raise ConfigurationError(
            "api_key does not have the form pb_<environment>_<scope>_<32 base62 characters>"
        )
    resolved_base_url = os.environ.get(BASE_URL_VARIABLE, "") if base_url is None else base_url
    if not resolved_base_url:
        raise ConfigurationError(f"base_url missing, pass it or set {BASE_URL_VARIABLE}")
    _check_base_url(resolved_base_url)
    if report_mode not in REPORT_MODES:
        raise ConfigurationError(f"report_mode must be auto, sync or buffered value={report_mode}")
    _check_positive("check_timeout", check_timeout)
    _check_positive("flush_interval", flush_interval)
    if max_pending < 1:
        raise ConfigurationError(f"max_pending must be at least 1 value={max_pending}")
    if batch_size < 1:
        raise ConfigurationError(f"batch_size must be at least 1 value={batch_size}")
    serverless = any(os.environ.get(name) for name in SERVERLESS_VARIABLES)
    return ClientOptions(
        api_key=resolved_api_key,
        base_url=resolved_base_url,
        check_timeout=check_timeout,
        buffer_reports=report_mode == "buffered" or (report_mode == "auto" and not serverless),
        flush_interval=flush_interval,
        batch_size=batch_size,
        max_pending=max_pending,
    )


def build_report_request(
    decision_or_fields: Decision | ReportFields,
    usage: Usage,
    attributes: Mapping[str, AttributeValue] | None,
    occurred_at: datetime | None,
) -> JSONObject:
    """Builds the report of a decision or of `ReportFields`, stamped now when `occurred_at` is None.

    Raises:
        ValueError: A quantity or `occurred_at` cannot be sent.
    """
    stamped_at = datetime.now(timezone.utc) if occurred_at is None else occurred_at
    if isinstance(decision_or_fields, Decision):
        return build_report_body(decision_or_fields, usage, attributes, stamped_at)
    return build_fallback_report_body(
        idempotency_key=decision_or_fields.idempotency_key,
        customer_id=decision_or_fields.customer_id,
        feature=decision_or_fields.feature,
        provider=decision_or_fields.provider,
        model=decision_or_fields.model,
        usage=usage,
        attributes=attributes,
        occurred_at=stamped_at,
    )


def decide_check(
    fallback_outcomes: FallbackCache,
    answer: httpx.Response | Exception,
    *,
    customer_id: str,
    feature: str,
    provider: str,
    model: str,
    attributes: Mapping[str, AttributeValue],
) -> Decision:
    """Builds the decision of a check from the server's answer or the exception raised instead.

    A fallback-eligible exception, a 502, 503 or 504 answer and a 2xx answer whose body is not
    JSON give a fallback decision, logged as `check.fallback`. A server decision records its
    `fallback_outcome` for later fallbacks.

    Args:
        fallback_outcomes: Outcomes the server sent in earlier check responses.
        answer: The check response, or the fallback-eligible exception the request raised.
        customer_id: Id of the customer in your system.
        feature: Feature from the check.
        provider: Provider from the check.
        model: Model from the check.
        attributes: Attributes from the check.

    Raises:
        PreburnError: The server answered an error status other than 502, 503 or 504.
    """
    if isinstance(answer, Exception):
        cause = f"error={type(answer).__name__}"
    elif classify(answer) is Classification.FALLBACK_ELIGIBLE:
        cause = f"status={answer.status_code}"
    else:
        try:
            response_body = answer.json()
        except ValueError as error:
            cause = f"status={answer.status_code} error={type(error).__name__}"
        else:
            decision = Decision.from_response(
                response_body, customer_id=customer_id, feature=feature, attributes=attributes
            )
            fallback_outcomes.record(customer_id, feature, decision.fallback_outcome)
            return decision
    outcome = fallback_outcomes.outcome(customer_id, feature)
    logger.warning(f"{CHECK_FALLBACK_EVENT} feature={feature} outcome={outcome} {cause}")
    return Decision.fallback(
        customer_id=customer_id,
        feature=feature,
        provider=provider,
        model=model,
        attributes=attributes,
        outcome=outcome,
    )


def _reset_in_forked_child(client_reference: "weakref.ref[Preburn]") -> None:
    client = client_reference()
    if client is not None:
        client._reset_after_fork()


def _check_base_url(base_url: str) -> None:
    try:
        parts = urlsplit(base_url)
    except ValueError as error:
        raise ConfigurationError("base_url is not a valid URL") from error
    if parts.scheme not in BASE_URL_SCHEMES or not parts.hostname:
        raise ConfigurationError("base_url must be an http or https URL with a host")


def _check_positive(name: str, value: float) -> None:
    if not math.isfinite(value) or value <= 0:
        raise ConfigurationError(f"{name} must be a positive number value={value}")
