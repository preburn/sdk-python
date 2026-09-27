import logging
import socket
import uuid
from collections.abc import Iterator
from datetime import datetime, timezone
from decimal import Decimal

import httpx
import pytest

from preburn import AsyncPreburn, Decision, Preburn, ReportFields, ReportResult
from preburn._transport import REPORTS_PATH

pytestmark = pytest.mark.integration

RUN_ID = uuid.uuid4().hex[:12]
FEATURE = "sdk_integration"
PROVIDER = "openai"
MODEL = "gpt-6-sol"
CHECK_TIMEOUT_SECONDS = 2.0
IDLE_INTERVAL_SECONDS = 3600.0
MAXIMUM_PENDING = 2
ACCEPTED_REPORT_STATUS = 202
LOCAL_HOST = "127.0.0.1"
USAGE_ESTIMATE: dict[str, Decimal | int] = {"input_tokens": 1200, "output_tokens": 300}
USAGE: dict[str, Decimal | int] = {"input_tokens": 1180, "output_tokens": 240}
SUBSCRIPTION_AMOUNT = Decimal("30.00")
DISPLAY_NAME = "SDK integration"
METADATA: dict[str, object] = {"source": "sdk", "seats": 3}


class StatusRecordingTransport(httpx.BaseTransport):
    def __init__(self) -> None:
        self._transport = httpx.HTTPTransport()
        self.statuses: list[tuple[str, int]] = []

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        response = self._transport.handle_request(request)
        self.statuses.append((request.url.path, response.status_code))
        return response

    def close(self) -> None:
        self._transport.close()


def build_customer_id(name: str) -> str:
    return f"sdk-it-{RUN_ID}-{name}"


def build_current_month() -> tuple[datetime, datetime]:
    start = datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end = start.replace(year=start.year + start.month // 12, month=start.month % 12 + 1)
    return start, end


def find_closed_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind((LOCAL_HOST, 0))
        port: int = probe.getsockname()[1]
    return port


def check_server_decision(decision: Decision) -> None:
    if decision.is_fallback or decision.decision_id is None:
        pytest.fail(f"check fell back outcome={decision.outcome}")
    if not decision.decision_id.startswith("dec_"):
        pytest.fail(f"decision id has no dec_ prefix decision_id={decision.decision_id}")


def check_stored(result: ReportResult | None, duplicate: bool) -> ReportResult:
    if result is None:
        pytest.fail("sync report returned no result")
    if not result.ledger_entry_id.startswith("led_") or result.duplicate is not duplicate:
        pytest.fail(f"report result differs result={result} duplicate={duplicate}")
    return result


@pytest.fixture(name="client")
def make_client() -> Iterator[Preburn]:
    with Preburn(check_timeout=CHECK_TIMEOUT_SECONDS, report_mode="sync") as client:
        yield client


def test_check_then_report_stores_one_ledger_entry(client: Preburn) -> None:
    decision = client.check(
        build_customer_id("report"), FEATURE, PROVIDER, MODEL, usage_estimate=USAGE_ESTIMATE
    )
    check_server_decision(decision)
    if decision.outcome != "allow" or decision.cost_status != "costed":
        pytest.fail(f"check differs outcome={decision.outcome} cost_status={decision.cost_status}")
    first = check_stored(client.report(decision, USAGE), duplicate=False)
    repeat = check_stored(client.report(decision, USAGE), duplicate=True)
    if (first.cost_status, repeat.ledger_entry_id) != ("costed", first.ledger_entry_id):
        pytest.fail(f"reports differ first={first} repeat={repeat}")


def test_release_frees_the_reservation(client: Preburn) -> None:
    customer_id = build_customer_id("release")
    released = client.check(customer_id, FEATURE, PROVIDER, MODEL, usage_estimate=USAGE_ESTIMATE)
    check_server_decision(released)
    if released.reserved_amount <= 0:
        pytest.fail(f"check reserved nothing reserved_amount={released.reserved_amount}")
    client.release(released)
    client.release(released)
    later = client.check(customer_id, FEATURE, PROVIDER, MODEL, usage_estimate=USAGE_ESTIMATE)
    check_server_decision(later)
    client.release(later)
    if later.signals is None or later.signals.reserved != 0:
        pytest.fail(f"released reservation still held signals={later.signals}")


def test_buffered_reports_flush_in_one_batch_with_the_dropped_count(
    client: Preburn, caplog: pytest.LogCaptureFixture
) -> None:
    customer_id = build_customer_id("batch")
    recorder = StatusRecordingTransport()
    with (
        caplog.at_level(logging.WARNING, logger="preburn"),
        Preburn(
            check_timeout=CHECK_TIMEOUT_SECONDS,
            report_mode="buffered",
            flush_interval=IDLE_INTERVAL_SECONDS,
            max_pending=MAXIMUM_PENDING,
            transport=recorder,
        ) as buffered,
    ):
        first = buffered.check(customer_id, FEATURE, PROVIDER, MODEL)
        second = buffered.check(customer_id, FEATURE, PROVIDER, MODEL)
        buffered.report(
            ReportFields(customer_id=customer_id, feature=FEATURE, provider=PROVIDER, model=MODEL),
            USAGE,
        )
        buffered.report(first, USAGE)
        buffered.report(second, USAGE)
        buffered.flush()
    batch_statuses = [status for path, status in recorder.statuses if path == REPORTS_PATH]
    if batch_statuses != [ACCEPTED_REPORT_STATUS]:
        pytest.fail(f"batch statuses differ statuses={batch_statuses}")
    if caplog.records:
        pytest.fail(
            f"flush logged warnings messages={[record.message for record in caplog.records]}"
        )
    for decision in (first, second):
        check_stored(client.report(decision, USAGE), duplicate=True)


def test_fallback_report_is_idempotent(client: Preburn) -> None:
    fields = ReportFields(
        customer_id=build_customer_id("fields"), feature=FEATURE, provider=PROVIDER, model=MODEL
    )
    first = check_stored(client.report(fields, USAGE), duplicate=False)
    repeat = check_stored(client.report(fields, USAGE), duplicate=True)
    if repeat.ledger_entry_id != first.ledger_entry_id:
        pytest.fail(f"repeat stored another entry first={first} repeat={repeat}")


def test_check_falls_back_when_the_server_is_unreachable(client: Preburn) -> None:
    customer_id = build_customer_id("fallback")
    with Preburn(
        base_url=f"http://{LOCAL_HOST}:{find_closed_port()}", report_mode="sync"
    ) as unreachable:
        decision = unreachable.check(customer_id, FEATURE, PROVIDER, MODEL)
    if not decision.is_fallback or decision.outcome != "allow":
        pytest.fail(f"check did not fall back to allow decision={decision}")
    first = check_stored(client.report(decision, USAGE), duplicate=False)
    repeat = check_stored(client.report(decision, USAGE), duplicate=True)
    if repeat.ledger_entry_id != first.ledger_entry_id:
        pytest.fail(f"repeat stored another entry first={first} repeat={repeat}")


def test_customer_upsert_replaces_every_field(client: Preburn) -> None:
    external_id = build_customer_id("customer")
    created = client.customers.upsert(external_id, display_name=DISPLAY_NAME, metadata=METADATA)
    replaced = client.customers.upsert(external_id)
    if (created.external_id, created.display_name, dict(created.metadata)) != (
        external_id,
        DISPLAY_NAME,
        METADATA,
    ):
        pytest.fail(f"created customer differs customer={created}")
    if (replaced.id, replaced.display_name, dict(replaced.metadata)) != (created.id, None, {}):
        pytest.fail(f"replaced customer differs customer={replaced}")


def test_revenue_record_is_idempotent(client: Preburn) -> None:
    customer_id = build_customer_id("revenue")
    period_start, period_end = build_current_month()
    source_reference = f"{customer_id}-subscription"
    first = client.revenue.record(
        customer_id, "subscription", SUBSCRIPTION_AMOUNT, period_start, period_end, source_reference
    )
    repeat = client.revenue.record(
        customer_id, "subscription", SUBSCRIPTION_AMOUNT, period_start, period_end, source_reference
    )
    if first.duplicate or first.amount != SUBSCRIPTION_AMOUNT or first.source != "api":
        pytest.fail(f"recorded entry differs entry={first}")
    if not repeat.duplicate or repeat.id != first.id:
        pytest.fail(f"repeat recorded another entry first={first} repeat={repeat}")


@pytest.mark.asyncio
async def test_async_client_checks_reports_and_flushes(client: Preburn) -> None:
    customer_id = build_customer_id("async")
    async with AsyncPreburn(check_timeout=CHECK_TIMEOUT_SECONDS, report_mode="sync") as sync_mode:
        reported = await sync_mode.check(customer_id, FEATURE, PROVIDER, MODEL)
        check_server_decision(reported)
        check_stored(await sync_mode.report(reported, USAGE), duplicate=False)
        released = await sync_mode.check(customer_id, FEATURE, PROVIDER, MODEL)
        check_server_decision(released)
        await sync_mode.release(released)
    async with AsyncPreburn(
        check_timeout=CHECK_TIMEOUT_SECONDS,
        report_mode="buffered",
        flush_interval=IDLE_INTERVAL_SECONDS,
    ) as buffered:
        flushed = await buffered.check(customer_id, FEATURE, PROVIDER, MODEL)
        check_server_decision(flushed)
        await buffered.report(flushed, USAGE)
    check_stored(client.report(flushed, USAGE), duplicate=True)
