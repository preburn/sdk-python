import json
import secrets
from datetime import datetime, timezone
from decimal import Decimal

import httpx
import pytest

from preburn import AsyncPreburn, AttributeValue, Decision, Preburn, ReportFields
from tests.contract.openapi import (
    SDK_OPERATIONS_BY_ID,
    OpenAPIContract,
    ResponseFixture,
    load_response_fixtures,
)

API_KEY = f"pb_test_runtime_{secrets.token_hex(16)}"
BASE_URL = "http://preburn.test"
IDLE_INTERVAL_SECONDS = 3600.0
MAXIMUM_PENDING = 3
BATCH_OPERATION_ID = "report-usage-batch"
REPORT_OPERATION_ID = "report-usage"
ACCEPTED_REPORT_STATUS = 202
ANSWER_FIXTURE_NAMES = {
    "check": "check_allow",
    "report-usage": "report_costed",
    "release-decision": "release_released",
    "upsert-customer": "customer_with_plan",
    "record-revenue": "revenue_created",
}
CUSTOMER_ID = "team:acme@example.com"
CUSTOMER_USER_ID = "user.42"
FEATURE = "text_to_video"
PROVIDER = "fal_ai"
MODEL = "kling-video"
DISPLAY_NAME = "Acme"
PLAN_ID = "pln_01m3f4z9y8x7w6v5t4s3r2q1p0"
METADATA: dict[str, object] = {"tier": "gold", "seats": 12, "regions": ["eu", "us"]}
ATTRIBUTES: dict[str, AttributeValue] = {"resolution": "1080p", "duration": 8, "audio": True}
USAGE_ESTIMATE: dict[str, Decimal | int] = {"output_seconds": Decimal("8.5")}
USAGE_CEILING: dict[str, Decimal | int] = {"output_seconds": 10}
USAGE: dict[str, Decimal | int] = {"output_seconds": Decimal("8.250001"), "requests": 1}
OCCURRED_AT = datetime(2026, 9, 26, 10, 0, 0, 123456, tzinfo=timezone.utc)
PERIOD_START = datetime(2026, 9, 1, tzinfo=timezone.utc)
PERIOD_END = datetime(2026, 10, 1, tzinfo=timezone.utc)
SUBSCRIPTION_AMOUNT = Decimal("30.00")
REFUND_AMOUNT = 5
REPORT_FIELDS = ReportFields(
    customer_id=CUSTOMER_ID, feature=FEATURE, provider=PROVIDER, model=MODEL
)
FALLBACK_DECISION = Decision.fallback(
    customer_id=CUSTOMER_ID,
    feature=FEATURE,
    provider=PROVIDER,
    model=MODEL,
    attributes=ATTRIBUTES,
    outcome="allow",
)


class ContractServer:
    def __init__(self, contract: OpenAPIContract) -> None:
        fixtures = {fixture.name: fixture for fixture in load_response_fixtures()}
        self._contract = contract
        self._answers: dict[str, ResponseFixture] = {
            operation_id: fixtures[name] for operation_id, name in ANSWER_FIXTURE_NAMES.items()
        }
        self.operation_ids: set[str] = set()
        self.problems: list[str] = []

    def handle(self, request: httpx.Request) -> httpx.Response:
        checked = self._contract.check_request(request)
        self.problems.extend(checked.problems)
        if checked.operation_id is None:
            return httpx.Response(404)
        self.operation_ids.add(checked.operation_id)
        if checked.operation_id != BATCH_OPERATION_ID:
            return self._answers[checked.operation_id].to_response()
        accepted = {
            "status": ACCEPTED_REPORT_STATUS,
            "result": self._answers[REPORT_OPERATION_ID].body,
        }
        report_count = len(json.loads(request.content)["reports"])
        return httpx.Response(ACCEPTED_REPORT_STATUS, json={"results": [accepted] * report_count})

    def check_contract_kept(self) -> None:
        if self.problems:
            pytest.fail("\n".join(self.problems))
        uncalled = SDK_OPERATIONS_BY_ID.keys() - self.operation_ids
        if uncalled:
            pytest.fail(f"operations never called {sorted(uncalled)}")


def test_sync_client_requests_follow_the_document(contract: OpenAPIContract) -> None:
    server = ContractServer(contract)
    transport = httpx.MockTransport(server.handle)
    with Preburn(API_KEY, BASE_URL, report_mode="sync", transport=transport) as client:
        decision = client.check(CUSTOMER_ID, FEATURE, PROVIDER, MODEL)
        client.check(
            CUSTOMER_ID,
            FEATURE,
            PROVIDER,
            MODEL,
            attributes=ATTRIBUTES,
            usage_estimate=USAGE_ESTIMATE,
            usage_ceiling=USAGE_CEILING,
            customer_user_id=CUSTOMER_USER_ID,
        )
        client.report(decision, USAGE)
        client.report(decision, USAGE, attributes=ATTRIBUTES, occurred_at=OCCURRED_AT)
        client.report(FALLBACK_DECISION, USAGE, attributes=ATTRIBUTES)
        client.report(REPORT_FIELDS, USAGE, occurred_at=OCCURRED_AT)
        client.release(decision)
        client.customers.upsert(CUSTOMER_ID)
        client.customers.upsert(
            CUSTOMER_ID, display_name=DISPLAY_NAME, plan_id=PLAN_ID, metadata=METADATA
        )
        client.revenue.record(
            CUSTOMER_ID, "subscription", SUBSCRIPTION_AMOUNT, PERIOD_START, PERIOD_END, "in_1"
        )
        client.revenue.record(
            CUSTOMER_ID,
            "refund",
            REFUND_AMOUNT,
            PERIOD_START,
            PERIOD_START,
            "in_2",
            occurred_at=OCCURRED_AT,
        )
    with Preburn(
        API_KEY,
        BASE_URL,
        report_mode="buffered",
        flush_interval=IDLE_INTERVAL_SECONDS,
        max_pending=MAXIMUM_PENDING,
        transport=transport,
    ) as client:
        decision = client.check(CUSTOMER_ID, FEATURE, PROVIDER, MODEL)
        client.report(REPORT_FIELDS, USAGE)
        client.report(FALLBACK_DECISION, USAGE)
        client.report(decision, USAGE)
        client.report(decision, USAGE, attributes=ATTRIBUTES, occurred_at=OCCURRED_AT)
        client.flush()
    server.check_contract_kept()


@pytest.mark.asyncio
async def test_async_client_requests_follow_the_document(contract: OpenAPIContract) -> None:
    server = ContractServer(contract)
    transport = httpx.MockTransport(server.handle)
    async with AsyncPreburn(API_KEY, BASE_URL, report_mode="sync", transport=transport) as client:
        decision = await client.check(CUSTOMER_ID, FEATURE, PROVIDER, MODEL)
        await client.check(
            CUSTOMER_ID,
            FEATURE,
            PROVIDER,
            MODEL,
            attributes=ATTRIBUTES,
            usage_estimate=USAGE_ESTIMATE,
            usage_ceiling=USAGE_CEILING,
            customer_user_id=CUSTOMER_USER_ID,
        )
        await client.report(decision, USAGE)
        await client.report(decision, USAGE, attributes=ATTRIBUTES, occurred_at=OCCURRED_AT)
        await client.report(FALLBACK_DECISION, USAGE, attributes=ATTRIBUTES)
        await client.report(REPORT_FIELDS, USAGE, occurred_at=OCCURRED_AT)
        await client.release(decision)
        await client.customers.upsert(CUSTOMER_ID)
        await client.customers.upsert(
            CUSTOMER_ID, display_name=DISPLAY_NAME, plan_id=PLAN_ID, metadata=METADATA
        )
        await client.revenue.record(
            CUSTOMER_ID, "subscription", SUBSCRIPTION_AMOUNT, PERIOD_START, PERIOD_END, "in_1"
        )
        await client.revenue.record(
            CUSTOMER_ID,
            "refund",
            REFUND_AMOUNT,
            PERIOD_START,
            PERIOD_START,
            "in_2",
            occurred_at=OCCURRED_AT,
        )
    async with AsyncPreburn(
        API_KEY,
        BASE_URL,
        report_mode="buffered",
        flush_interval=IDLE_INTERVAL_SECONDS,
        max_pending=MAXIMUM_PENDING,
        transport=transport,
    ) as client:
        decision = await client.check(CUSTOMER_ID, FEATURE, PROVIDER, MODEL)
        await client.report(REPORT_FIELDS, USAGE)
        await client.report(FALLBACK_DECISION, USAGE)
        await client.report(decision, USAGE)
        await client.report(decision, USAGE, attributes=ATTRIBUTES, occurred_at=OCCURRED_AT)
        await client.flush()
    server.check_contract_kept()
