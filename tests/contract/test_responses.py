import functools
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import pytest

from preburn import (
    APIError,
    AuthenticationError,
    Customer,
    Decision,
    InvalidField,
    PreburnError,
    ReportResult,
    RevenueEntry,
    ValidationError,
)
from preburn._models import parse_report_batch
from preburn._transport import parse_error
from tests.contract.openapi import (
    FIRST_ERROR_STATUS,
    SDK_OPERATIONS,
    OpenAPIContract,
    ResponseFixture,
    load_response_fixtures,
)

BATCH_OPERATION_ID = "report-usage-batch"
DISCARDED_RESPONSE_BODIES = frozenset({"release-decision"})
ACCEPTED_REPORT_STATUS = 202
CUSTOMER_ID = "customer_1"
FEATURE = "chat"
TIMESTAMP_LAYOUT = "%Y-%m-%dT%H:%M:%S"
MICROSECOND_DIGITS = 6
ERROR_CLASSES_BY_STATUS: Mapping[int, type[PreburnError]] = {
    401: AuthenticationError,
    422: ValidationError,
}
MODEL_PARSERS: Mapping[str, Callable[[Any], object]] = {
    "check": functools.partial(
        Decision.from_response, customer_id=CUSTOMER_ID, feature=FEATURE, attributes={}
    ),
    "report-usage": ReportResult.from_response,
    "upsert-customer": Customer.from_response,
    "record-revenue": RevenueEntry.from_response,
}
RESPONSE_FIXTURES = load_response_fixtures()


def parse_expected_timestamp(text: str) -> datetime:
    whole, _, fraction = text.removesuffix("Z").partition(".")
    microsecond = int(fraction[:MICROSECOND_DIGITS].ljust(MICROSECOND_DIGITS, "0"))
    return datetime.strptime(whole, TIMESTAMP_LAYOUT).replace(
        microsecond=microsecond, tzinfo=timezone.utc
    )


def read_field(parsed: object, key: str, location: str) -> object:
    if isinstance(parsed, Mapping):
        if key not in parsed:
            pytest.fail(f"parsed mapping has no key location={location} key={key}")
        return parsed[key]
    if not hasattr(parsed, key):
        pytest.fail(f"model has no field location={location} field={key}")
    return getattr(parsed, key)


def check_model_matches(parsed: object, expected: object, location: str) -> None:
    if isinstance(expected, Mapping):
        for key, value in expected.items():
            check_model_matches(read_field(parsed, key, location), value, f"{location}.{key}")
        return
    if isinstance(expected, list):
        if not isinstance(parsed, Sequence) or len(parsed) != len(expected):
            pytest.fail(f"parsed list differs location={location} parsed={parsed!r}")
        for index, (parsed_item, expected_item) in enumerate(zip(parsed, expected, strict=True)):
            check_model_matches(parsed_item, expected_item, f"{location}[{index}]")
        return
    if isinstance(parsed, Decimal):
        matches = isinstance(expected, str) and parsed == Decimal(expected)
    elif isinstance(parsed, datetime):
        matches = isinstance(expected, str) and parsed == parse_expected_timestamp(expected)
    else:
        matches = type(parsed) is type(expected) and parsed == expected
    if not matches:
        pytest.fail(
            f"parsed value differs location={location} parsed={parsed!r} expected={expected!r}"
        )


def check_error_matches(
    error: object, status: int, failure: Mapping[str, Any], location: str
) -> None:
    if not isinstance(error, PreburnError):
        pytest.fail(f"parsed value is not an error location={location} parsed={error!r}")
    expected_fields = tuple(
        InvalidField(location=field["location"], message=field["message"])
        for field in failure.get("errors", [])
    )
    actual = (type(error), error.status, error.code, error.detail, error.errors)
    expected = (
        ERROR_CLASSES_BY_STATUS.get(status, APIError),
        status,
        failure["code"],
        failure["detail"],
        expected_fields,
    )
    if actual != expected:
        pytest.fail(f"parsed error differs location={location} actual={actual} expected={expected}")


def check_batch_matches(parsed: Sequence[object], results: Sequence[Mapping[str, Any]]) -> None:
    for index, (entry, item) in enumerate(zip(parsed, results, strict=True)):
        location = f"body.results[{index}]"
        if item["status"] != ACCEPTED_REPORT_STATUS:
            check_error_matches(entry, item["status"], item["error"], location)
            continue
        if not isinstance(entry, ReportResult):
            pytest.fail(f"accepted report is not a ReportResult location={location}")
        check_model_matches(entry, item["result"], f"{location}.result")


def test_response_fixtures_cover_every_operation_and_success_status() -> None:
    success_statuses = {
        (fixture.operation_id, fixture.status)
        for fixture in RESPONSE_FIXTURES
        if fixture.status < FIRST_ERROR_STATUS
    }
    failing_operations = {
        fixture.operation_id
        for fixture in RESPONSE_FIXTURES
        if fixture.status >= FIRST_ERROR_STATUS
    }
    declared_statuses = {
        (operation.operation_id, status)
        for operation in SDK_OPERATIONS
        for status in operation.response_schemas
    }
    operation_ids = {operation.operation_id for operation in SDK_OPERATIONS}
    if success_statuses != declared_statuses:
        pytest.fail(
            f"success fixtures differ covered={success_statuses} declared={declared_statuses}"
        )
    if failing_operations != operation_ids:
        pytest.fail(f"error fixtures differ covered={failing_operations} expected={operation_ids}")


@pytest.mark.parametrize("fixture", RESPONSE_FIXTURES, ids=lambda fixture: fixture.name)
def test_response_fixture_follows_the_document_and_parses(
    contract: OpenAPIContract, fixture: ResponseFixture
) -> None:
    problems = contract.response_problems(fixture)
    if problems:
        pytest.fail("\n".join(problems))
    if fixture.status >= FIRST_ERROR_STATUS:
        check_error_matches(
            parse_error(fixture.to_response()), fixture.status, fixture.body, "body"
        )
    elif fixture.operation_id == BATCH_OPERATION_ID:
        check_batch_matches(parse_report_batch(fixture.body), fixture.body["results"])
    elif fixture.operation_id not in DISCARDED_RESPONSE_BODIES:
        check_model_matches(MODEL_PARSERS[fixture.operation_id](fixture.body), fixture.body, "body")
