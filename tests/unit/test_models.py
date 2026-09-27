import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from preburn._errors import (
    APIError,
    AuthenticationError,
    DecisionDenied,
    DecisionNotApplicableError,
    InvalidField,
    PreburnError,
    ValidationError,
)
from preburn._models import (
    CallContext,
    Customer,
    Decision,
    FeatureSignals,
    ReportResult,
    RevenueEntry,
    Signals,
    format_timestamp,
    parse_report_batch,
    parse_timestamp,
)

SIGNALS_BODY: dict[str, object] = {
    "period_revenue_net": "30.000000000",
    "cost_allowance": "18.000000000",
    "cost_to_date": "4.250000000",
    "reserved": "3.200000000",
    "elapsed_fraction": "0.2500",
    "allowance_remaining": "-0.500000000",
    "pace": "inf",
    "projected_margin": "-inf",
    "request_estimated_cost": "3.200000000",
    "period_decision_count": 41,
    "features": {
        "text_to_video": {
            "cost_to_date": "4.250000000",
            "reserved": "3.200000000",
            "period_decision_count": 40,
        }
    },
}

CHECK_BODY: dict[str, object] = {
    "decision_id": "dec_01jbvagescfn78y0938nkrkayd",
    "outcome": "cap",
    "reason": "policy_matched",
    "provider": "fal_ai",
    "model": "veo-3",
    "overrides": {"duration": 5, "audio": False, "resolution": "720p"},
    "estimated_cost": "2.000000000",
    "reserved_amount": "2.500000000",
    "estimate_basis": "ceiling",
    "cost_status": "costed",
    "matched_policy_id": "pol_01jbvagescfn78y0938nkrkayd",
    "fallback_outcome": "deny",
    "expires_at": "2026-09-26T10:10:00.123456789Z",
    "signals": SIGNALS_BODY,
}


def make_decision_from_check_body(body: dict[str, object]) -> Decision:
    return Decision.from_response(
        body, customer_id="customer_1", feature="text_to_video", attributes={"resolution": "1080p"}
    )


def test_decision_parses_every_check_response_field() -> None:
    decision = make_decision_from_check_body(CHECK_BODY)
    expected = Decision(
        decision_id="dec_01jbvagescfn78y0938nkrkayd",
        outcome="cap",
        reason="policy_matched",
        provider="fal_ai",
        model="veo-3",
        overrides={"duration": 5, "audio": False, "resolution": "720p"},
        estimated_cost=Decimal("2"),
        reserved_amount=Decimal("2.5"),
        estimate_basis="ceiling",
        cost_status="costed",
        matched_policy_id="pol_01jbvagescfn78y0938nkrkayd",
        fallback_outcome="deny",
        expires_at=datetime(2026, 9, 26, 10, 10, 0, 123456, tzinfo=timezone.utc),
        signals=Signals(
            period_revenue_net=Decimal("30"),
            cost_allowance=Decimal("18"),
            cost_to_date=Decimal("4.25"),
            reserved=Decimal("3.2"),
            elapsed_fraction=Decimal("0.25"),
            allowance_remaining=Decimal("-0.5"),
            pace=Decimal("Infinity"),
            projected_margin=Decimal("-Infinity"),
            request_estimated_cost=Decimal("3.2"),
            period_decision_count=41,
            features={
                "text_to_video": FeatureSignals(
                    cost_to_date=Decimal("4.25"), reserved=Decimal("3.2"), period_decision_count=40
                )
            },
        ),
        customer_id="customer_1",
        feature="text_to_video",
        attributes={"resolution": "1080p"},
        idempotency_key=None,
    )
    if decision != expected:
        pytest.fail(f"decision={decision}")
    if decision.is_fallback:
        pytest.fail("server decision marked fallback")
    if (
        not isinstance(decision.reserved_amount, Decimal)
        or str(decision.reserved_amount) != "2.500000000"
    ):
        pytest.fail(f"reserved_amount={decision.reserved_amount!r}")


def test_decision_parses_null_fields() -> None:
    body = {
        **CHECK_BODY,
        "estimated_cost": None,
        "matched_policy_id": None,
        "cost_status": "uncosted",
    }
    body["signals"] = {**SIGNALS_BODY, "request_estimated_cost": None, "features": {}}
    decision = make_decision_from_check_body(body)
    if decision.estimated_cost is not None:
        pytest.fail(f"estimated_cost={decision.estimated_cost}")
    if decision.matched_policy_id is not None:
        pytest.fail(f"matched_policy_id={decision.matched_policy_id}")
    if decision.signals is None or decision.signals.request_estimated_cost is not None:
        pytest.fail(f"signals={decision.signals}")


def test_decision_keeps_unknown_enum_values_from_newer_servers() -> None:
    decision = make_decision_from_check_body({**CHECK_BODY, "reason": "reason_added_later"})
    reason: str | None = decision.reason
    if reason != "reason_added_later":
        pytest.fail(f"reason={reason}")


def test_decision_with_missing_field_fails_loud() -> None:
    body = {key: value for key, value in CHECK_BODY.items() if key != "reserved_amount"}
    with pytest.raises(KeyError, match="reserved_amount"):
        make_decision_from_check_body(body)


def test_fallback_decision_shape() -> None:
    decision = Decision.fallback(
        customer_id="customer_1",
        feature="text_to_video",
        provider="fal_ai",
        model="veo-3",
        attributes={"resolution": "1080p"},
        outcome="deny",
    )
    if not decision.is_fallback or decision.decision_id is not None:
        pytest.fail(f"is_fallback={decision.is_fallback} decision_id={decision.decision_id}")
    if (decision.outcome, decision.fallback_outcome) != ("deny", "deny"):
        pytest.fail(f"outcome={decision.outcome} fallback_outcome={decision.fallback_outcome}")
    if (decision.provider, decision.model, decision.overrides) != ("fal_ai", "veo-3", {}):
        pytest.fail(
            f"provider={decision.provider} model={decision.model} overrides={decision.overrides}"
        )
    if decision.reserved_amount != Decimal(0) or decision.estimate_basis != "none":
        pytest.fail(
            f"reserved_amount={decision.reserved_amount} estimate_basis={decision.estimate_basis}"
        )
    nullable_fields = (
        decision.reason,
        decision.estimated_cost,
        decision.cost_status,
        decision.matched_policy_id,
        decision.expires_at,
        decision.signals,
    )
    if nullable_fields != (None, None, None, None, None, None):
        pytest.fail(f"nullable_fields={nullable_fields}")
    if decision.idempotency_key is None or uuid.UUID(decision.idempotency_key).version != 4:
        pytest.fail(f"idempotency_key={decision.idempotency_key}")


def test_fallback_decisions_get_distinct_idempotency_keys() -> None:
    keys = {
        Decision.fallback(
            customer_id="customer_1",
            feature="chat",
            provider="openai",
            model="gpt-6-sol",
            attributes={},
            outcome="allow",
        ).idempotency_key
        for _ in range(3)
    }
    if len(keys) != 3:
        pytest.fail(f"keys={keys}")


def test_raise_for_denial_raises_with_the_decision() -> None:
    decision = make_decision_from_check_body(
        {**CHECK_BODY, "outcome": "deny", "reason": "hard_limit_reached"}
    )
    with pytest.raises(DecisionDenied) as raised:
        decision.raise_for_denial()
    error = raised.value
    if error.decision is not decision:
        pytest.fail("error does not carry the decision")
    if (error.code, error.status) != ("decision_denied", None):
        pytest.fail(f"code={error.code} status={error.status}")
    if "hard_limit_reached" not in str(error):
        pytest.fail(f"message={error}")


def test_raise_for_denial_raises_for_fallback_deny() -> None:
    decision = Decision.fallback(
        customer_id="customer_1",
        feature="chat",
        provider="openai",
        model="gpt-6-sol",
        attributes={},
        outcome="deny",
    )
    with pytest.raises(DecisionDenied):
        decision.raise_for_denial()


@pytest.mark.parametrize("outcome", ["allow", "route", "cap"])
def test_raise_for_denial_returns_the_decision_otherwise(outcome: str) -> None:
    decision = make_decision_from_check_body({**CHECK_BODY, "outcome": outcome})
    if decision.raise_for_denial() is not decision:
        pytest.fail(f"outcome={outcome}")


def test_decision_not_applicable_carries_the_decision() -> None:
    decision = make_decision_from_check_body(
        {**CHECK_BODY, "outcome": "route", "provider": "anthropic"}
    )
    error = DecisionNotApplicableError(decision, "route target provider=anthropic")
    if error.decision is not decision or error.code != "decision_not_applicable":
        pytest.fail(f"code={error.code}")
    if not isinstance(error, PreburnError):
        pytest.fail("not a PreburnError")


def test_report_result_parses() -> None:
    result = ReportResult.from_response(
        {
            "ledger_entry_id": "led_1",
            "cost": "1.250000000",
            "cost_status": "costed",
            "duplicate": True,
        }
    )
    expected = ReportResult(
        ledger_entry_id="led_1", cost=Decimal("1.25"), cost_status="costed", duplicate=True
    )
    if result != expected:
        pytest.fail(f"result={result}")
    uncosted = ReportResult.from_response(
        {"ledger_entry_id": "led_2", "cost": None, "cost_status": "uncosted", "duplicate": False}
    )
    if uncosted.cost is not None:
        pytest.fail(f"cost={uncosted.cost}")


def test_report_batch_results_keep_order_and_map_failures() -> None:
    body = {
        "results": [
            {
                "status": 202,
                "result": {
                    "ledger_entry_id": "led_1",
                    "cost": "0.100000000",
                    "cost_status": "costed",
                    "duplicate": False,
                },
            },
            {
                "status": 422,
                "error": {
                    "code": "validation_failed",
                    "detail": "report is invalid",
                    "errors": [{"location": "body.reports[1].usage", "message": "unknown meter"}],
                },
            },
            {
                "status": 409,
                "error": {"code": "decision_not_reportable", "detail": "decision was denied"},
            },
            {"status": 401, "error": {"code": "authentication_required", "detail": "no key"}},
        ]
    }
    results = parse_report_batch(body)
    if len(results) != 4:
        pytest.fail(f"results={results}")
    first, second, third, fourth = results
    if first != ReportResult(
        ledger_entry_id="led_1", cost=Decimal("0.1"), cost_status="costed", duplicate=False
    ):
        pytest.fail(f"first={first}")
    if not isinstance(second, ValidationError) or second.errors != (
        InvalidField(location="body.reports[1].usage", message="unknown meter"),
    ):
        pytest.fail(f"second={second!r}")
    if type(third) is not APIError or (third.status, third.code, third.errors) != (
        409,
        "decision_not_reportable",
        (),
    ):
        pytest.fail(f"third={third!r}")
    if not isinstance(fourth, AuthenticationError):
        pytest.fail(f"fourth={fourth!r}")


def test_customer_parses() -> None:
    customer = Customer.from_response(
        {
            "id": "cust_01jbvagescfn78y0938nkrkayd",
            "external_id": "customer_1",
            "display_name": None,
            "plan_id": "pln_01jbvagescfn78y0938nkrkayd",
            "metadata": {"tier": "gold", "seats": 4},
            "status": "active",
            "created_at": "2026-09-26T10:00:00Z",
            "updated_at": "2026-09-26T11:00:00.5Z",
        }
    )
    expected = Customer(
        id="cust_01jbvagescfn78y0938nkrkayd",
        external_id="customer_1",
        display_name=None,
        plan_id="pln_01jbvagescfn78y0938nkrkayd",
        metadata={"tier": "gold", "seats": 4},
        status="active",
        created_at=datetime(2026, 9, 26, 10, tzinfo=timezone.utc),
        updated_at=datetime(2026, 9, 26, 11, 0, 0, 500000, tzinfo=timezone.utc),
    )
    if customer != expected:
        pytest.fail(f"customer={customer}")


def test_revenue_entry_parses() -> None:
    entry = RevenueEntry.from_response(
        {
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
    )
    expected = RevenueEntry(
        id="rev_01jbvagescfn78y0938nkrkayd",
        customer_id="customer_1",
        kind="subscription",
        amount=Decimal("30"),
        period_start=datetime(2026, 9, 1, tzinfo=timezone.utc),
        period_end=datetime(2026, 10, 1, tzinfo=timezone.utc),
        source="api",
        source_reference="in_123",
        occurred_at=datetime(2026, 9, 1, tzinfo=timezone.utc),
        created_at=datetime(2026, 9, 26, 10, tzinfo=timezone.utc),
        duplicate=False,
    )
    if entry != expected:
        pytest.fail(f"entry={entry}")


def test_call_context_takes_positional_customer_and_feature() -> None:
    context = CallContext("customer_1", "chat")
    if (context.customer_id, context.feature, context.customer_user_id) != (
        "customer_1",
        "chat",
        None,
    ):
        pytest.fail(f"context={context}")
    with_user = CallContext("customer_1", "chat", customer_user_id="user_7")
    if with_user.customer_user_id != "user_7":
        pytest.fail(f"context={with_user}")


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("2026-09-26T10:00:00Z", datetime(2026, 9, 26, 10, tzinfo=timezone.utc)),
        ("2026-09-26T10:00:00.5Z", datetime(2026, 9, 26, 10, 0, 0, 500000, tzinfo=timezone.utc)),
        (
            "2026-09-26T10:00:00.123456Z",
            datetime(2026, 9, 26, 10, 0, 0, 123456, tzinfo=timezone.utc),
        ),
        (
            "2026-09-26T10:00:00.123456789Z",
            datetime(2026, 9, 26, 10, 0, 0, 123456, tzinfo=timezone.utc),
        ),
    ],
)
def test_parse_timestamp_reads_rfc_3339_utc(text: str, expected: datetime) -> None:
    parsed = parse_timestamp(text)
    if parsed != expected or parsed.tzinfo is not timezone.utc:
        pytest.fail(f"parsed={parsed!r}")


@pytest.mark.parametrize(
    "text",
    [
        "2026-09-26T10:00:00",
        "2026-09-26T10:00:00+02:00",
        "2026-09-26 10:00:00Z",
        "2026-9-26T10:00:00Z",
        "",
    ],
)
def test_parse_timestamp_rejects_other_forms(text: str) -> None:
    with pytest.raises(ValueError, match="timestamp"):
        parse_timestamp(text)


def test_format_timestamp_writes_utc_with_z() -> None:
    offset_time = datetime(2026, 9, 26, 12, 0, tzinfo=timezone(timedelta(hours=2)))
    if format_timestamp(offset_time) != "2026-09-26T10:00:00Z":
        pytest.fail(f"formatted={format_timestamp(offset_time)}")
    precise_time = datetime(2026, 9, 26, 10, 0, 0, 1, tzinfo=timezone.utc)
    if format_timestamp(precise_time) != "2026-09-26T10:00:00.000001Z":
        pytest.fail(f"formatted={format_timestamp(precise_time)}")
    if parse_timestamp(format_timestamp(precise_time)) != precise_time:
        pytest.fail("timestamp does not round trip")


def test_format_timestamp_rejects_naive_datetimes() -> None:
    naive_time = datetime(2026, 9, 26, 10, 0)
    with pytest.raises(ValueError, match="timezone"):
        format_timestamp(naive_time)
