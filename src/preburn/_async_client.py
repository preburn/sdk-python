"""Async client of the Preburn API."""

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from datetime import datetime
from decimal import Decimal
from types import TracebackType

import httpx

from preburn._async_flusher import AsyncReportFlusher
from preburn._buffer import ReportBuffer
from preburn._client import (
    REQUEST_TIMEOUT_SECONDS,
    ReportMode,
    build_report_request,
    decide_check,
    resolve_options,
)
from preburn._errors import UNEXPECTED_RESPONSE_CODE, APIError
from preburn._fallback import FallbackCache
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
    JSONObject,
    Usage,
    build_async_client,
    build_check_body,
    build_customer_body,
    build_customer_path,
    build_release_body,
    build_revenue_body,
    encode_report,
    parse_error,
    read_json_body,
    resolve_proxy,
)

AsyncSend = Callable[[str, str, JSONObject], Awaitable[httpx.Response]]
"""Sends a JSON request and returns the success response, raising the error of any other."""


class AsyncCustomers:
    """Customer routes, available as `AsyncPreburn.customers`."""

    def __init__(self, send: AsyncSend) -> None:
        """Creates the routes on the client's request sender."""
        self._send = send

    async def upsert(
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
        response = await self._send("PUT", build_customer_path(external_id), body)
        return Customer.from_response(read_json_body(response))


class AsyncRevenue:
    """Revenue routes, available as `AsyncPreburn.revenue`."""

    def __init__(self, send: AsyncSend) -> None:
        """Creates the routes on the client's request sender."""
        self._send = send

    async def record(
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
        response = await self._send("POST", REVENUE_PATH, body)
        return RevenueEntry.from_response(read_json_body(response))


class AsyncPreburn:
    """Async client of the Preburn API.

    Check each AI request before it runs, then report its usage or release its decision. A
    check that does not answer within `check_timeout`, cannot connect, gets a 502, 503 or 504
    answer, or gets an answer that cannot be read returns a fallback decision instead of
    raising. Its outcome is the last `fallback_outcome` the server sent for the customer and
    feature, else for the feature, else `allow`.

    In buffered report mode a task on the running event loop sends reports in batches,
    started by the first report. Close the client with `aclose`, or use it with `async with`,
    to send the reports still pending and end the task.

    The client works from one event loop after another, such as one `asyncio.run` per job.
    Each loop gets its own connections and flush task, and reports still pending when a loop
    ends are sent from the next one. A `transport` passed to the client serves every loop.

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
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        """Creates the client. It needs no running event loop until the first request.

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
        self._http_client: httpx.AsyncClient | None = None
        self._http_client_loop: asyncio.AbstractEventLoop | None = None
        self._fallback_outcomes = FallbackCache()
        self._flusher: AsyncReportFlusher | None = None
        if options.buffer_reports:
            self._flusher = AsyncReportFlusher(
                self._bind_http_client,
                ReportBuffer(options.max_pending),
                flush_interval=options.flush_interval,
                batch_size=options.batch_size,
            )
        self.customers = AsyncCustomers(self._send)
        self.revenue = AsyncRevenue(self._send)

    @property
    def check_timeout(self) -> float:
        """Seconds a check may take in total before it falls back."""
        return self._options.check_timeout

    async def __aenter__(self) -> "AsyncPreburn":
        """Returns the client."""
        return self

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Closes the client."""
        await self.aclose()

    async def check(
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
        answer: httpx.Response | Exception
        try:
            answer = await asyncio.wait_for(
                self._bind_http_client().post(CHECK_PATH, json=body), self._options.check_timeout
            )
        except FALLBACK_EXCEPTIONS as error:
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

    async def report(
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
        response = await self._send("POST", REPORT_PATH, body)
        return ReportResult.from_response(read_json_body(response))

    async def release(self, decision: Decision) -> None:
        """Releases the reservation of a decision whose request did not run.

        Releasing a decision twice, or a fallback decision, which reserves nothing, changes
        nothing.

        Raises:
            PreburnError: The server rejected the release.
            httpx.HTTPError: The request did not reach the server.
        """
        if decision.is_fallback:
            return
        await self._send("POST", RELEASE_PATH, build_release_body(decision))

    async def flush(self) -> None:
        """Sends the pending reports now in buffered report mode.

        Reports that cannot be sent stay pending for the next flush.
        """
        if self._flusher is not None:
            await self._flusher.flush()

    async def aclose(self) -> None:
        """Sends the pending reports, ends the flush task and closes the connections.

        Connections of an event loop that has ended are left to the garbage collector, because
        they cannot be closed from another loop.
        """
        if self._flusher is not None:
            await self._flusher.aclose()
        if self._http_client is not None and self._http_client_loop is asyncio.get_running_loop():
            await self._http_client.aclose()

    def _bind_http_client(self) -> httpx.AsyncClient:
        loop = asyncio.get_running_loop()
        if self._http_client is None or self._http_client_loop is not loop:
            transport = self._transport
            if transport is None:
                transport = httpx.AsyncHTTPTransport(proxy=self._proxy)
            self._http_client = build_async_client(
                self._options.api_key,
                self._options.base_url,
                httpx.Timeout(REQUEST_TIMEOUT_SECONDS),
                transport,
            )
            self._http_client_loop = loop
        return self._http_client

    async def _send(self, method: str, path: str, body: JSONObject) -> httpx.Response:
        try:
            response = await self._bind_http_client().request(method, path, json=body)
        except httpx.DecodingError as error:
            raise APIError(UNEXPECTED_RESPONSE_CODE, UNDECODABLE_RESPONSE_DETAIL) from error
        if not response.is_success:
            raise parse_error(response)
        return response
