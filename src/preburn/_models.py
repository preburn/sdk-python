"""Values the SDK takes and returns, and the timestamp format of the API."""

import re
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Literal

from preburn._errors import (
    DecisionDenied,
    InvalidField,
    PreburnError,
    make_status_error,
)
from preburn._money import parse_amount, parse_ratio

Outcome = Literal["allow", "route", "cap", "deny"]
"""What a decision does with the request."""
FallbackOutcome = Literal["allow", "deny"]
"""Outcome to use for a customer and feature while Preburn cannot be reached."""
Reason = Literal[
    "no_policy_matched",
    "policy_matched",
    "hard_limit_reached",
    "route_chain_exhausted",
    "uncosted_allowed",
    "uncosted_denied",
    "cap_not_applicable",
]
"""Why the check decided the outcome."""
EstimateBasis = Literal["p95", "ceiling", "request_estimate", "none"]
"""Usage whose cost a decision reserved."""
CostStatus = Literal["costed", "uncosted"]
"""Whether every meter of the usage has a price."""
CustomerStatus = Literal["active", "disabled", "archived"]
"""Record status of a customer."""
RevenueKind = Literal["subscription", "adjustment", "stripe_fee", "refund", "credit_note"]
"""Kind of a revenue entry, which sets the sign of its amount."""
RevenueSource = Literal["api", "stripe", "import"]
"""Where a revenue entry came from."""
AttributeValue = str | int | bool
"""Value of a request attribute or a provider parameter override."""

REPORT_ACCEPTED_STATUS = 202
MICROSECOND_DIGITS = 6

_TIMESTAMP_PATTERN = re.compile(
    r"([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})(?:\.([0-9]{1,9}))?Z"
)


@dataclass(frozen=True)
class CallContext:
    """Who a wrapped provider call is for.

    Attributes:
        customer_id: Id of the customer in your system.
        feature: Feature of your product the call serves, such as `chat`.
        customer_user_id: Id of the customer's user in your system, if known.
    """

    customer_id: str
    feature: str
    customer_user_id: str | None = None


@dataclass(frozen=True, kw_only=True)
class ReportFields:
    """Request to report usage that has no decision, such as a call made without a check.

    The report goes out with `decision_source` `fallback`. Reporting the same `ReportFields`
    again sends the same idempotency key, so the server stores one ledger entry.

    Attributes:
        customer_id: Id of the customer in your system.
        feature: Feature of your product the call served, such as `chat`.
        provider: Provider the call ran on.
        model: Model the call ran on.
        idempotency_key: UUID the server keys the report by, a new random UUID by default.
    """

    customer_id: str
    feature: str
    provider: str
    model: str
    idempotency_key: str = field(default_factory=lambda: str(uuid.uuid4()))


@dataclass(frozen=True, kw_only=True)
class FeatureSignals:
    """Margin signals of one feature in the customer's period.

    Attributes:
        cost_to_date: Settled AI cost of the feature in USD.
        reserved: AI cost held by open reservations of the feature in USD.
        period_decision_count: Decisions for the feature in the period.
    """

    cost_to_date: Decimal
    reserved: Decimal
    period_decision_count: int

    @classmethod
    def from_response(cls, body: Mapping[str, Any]) -> "FeatureSignals":
        """Builds the signals from their JSON object in a check response."""
        return cls(
            cost_to_date=parse_amount(body["cost_to_date"]),
            reserved=parse_amount(body["reserved"]),
            period_decision_count=body["period_decision_count"],
        )


@dataclass(frozen=True, kw_only=True)
class Signals:
    """Margin signals of the customer's period at the check.

    Attributes:
        period_revenue_net: Net revenue attributed to the period in USD.
        cost_allowance: AI cost the plan allows in the period in USD.
        cost_to_date: Settled AI cost of the period in USD.
        reserved: AI cost held by open reservations in USD.
        elapsed_fraction: Elapsed share of the period, at least 0.01.
        allowance_remaining: Allowance minus settled and reserved cost in USD, negative once
            over the allowance.
        pace: Share of the allowance spent divided by the elapsed fraction, `Decimal("Infinity")`
            for cost against a zero allowance.
        projected_margin: Margin the period ends with at the current pace,
            `Decimal("-Infinity")` for cost without revenue.
        request_estimated_cost: Rated cost of the request's usage estimate in USD, None when
            the estimate is uncosted.
        period_decision_count: Decisions in the period.
        features: Signals per feature.
    """

    period_revenue_net: Decimal
    cost_allowance: Decimal
    cost_to_date: Decimal
    reserved: Decimal
    elapsed_fraction: Decimal
    allowance_remaining: Decimal
    pace: Decimal
    projected_margin: Decimal
    request_estimated_cost: Decimal | None
    period_decision_count: int
    features: Mapping[str, FeatureSignals]

    @classmethod
    def from_response(cls, body: Mapping[str, Any]) -> "Signals":
        """Builds the signals from their JSON object in a check response."""
        request_estimated_cost = body["request_estimated_cost"]
        return cls(
            period_revenue_net=parse_amount(body["period_revenue_net"]),
            cost_allowance=parse_amount(body["cost_allowance"]),
            cost_to_date=parse_amount(body["cost_to_date"]),
            reserved=parse_amount(body["reserved"]),
            elapsed_fraction=parse_ratio(body["elapsed_fraction"]),
            allowance_remaining=parse_amount(body["allowance_remaining"]),
            pace=parse_ratio(body["pace"]),
            projected_margin=parse_ratio(body["projected_margin"]),
            request_estimated_cost=(
                None if request_estimated_cost is None else parse_amount(request_estimated_cost)
            ),
            period_decision_count=body["period_decision_count"],
            features={
                feature: FeatureSignals.from_response(feature_body)
                for feature, feature_body in body["features"].items()
            },
        )


@dataclass(frozen=True, kw_only=True)
class Decision:
    """What to do with one AI request, from a check or from the fallback.

    A fallback decision is made by the SDK when Preburn cannot be reached. It has no
    `decision_id`, its outcome is the last `fallback_outcome` the server sent for the customer
    and feature, and it reserves nothing.

    Attributes:
        decision_id: Decision id, such as `dec_01jbvagescfn78y0938nkrkayd`. None for a
            fallback decision.
        outcome: `allow` runs the request as asked, `route` runs it on `provider` and `model`,
            `cap` runs it with `overrides`, `deny` rejects it.
        reason: Why the check decided the outcome. None for a fallback decision.
        provider: Provider to run the request on.
        model: Model to run the request on.
        overrides: Provider parameters to set by parameter name, empty unless the outcome is
            route or cap.
        estimated_cost: USD cost of the usage estimate, or of the ceiling without an
            estimate. None when uncosted and for a fallback decision.
        reserved_amount: USD amount held against the customer's allowance until the decision
            is reported, released or expires.
        estimate_basis: Usage whose cost the decision reserved.
        cost_status: `uncosted` when a meter of the request has no price. None for a fallback
            decision.
        matched_policy_id: Policy that decided, or None.
        fallback_outcome: Outcome to use for this customer and feature when Preburn cannot be
            reached.
        expires_at: When the reservation is released unless the request is reported first.
            None for a fallback decision.
        signals: Margin signals of the customer's period. None for a fallback decision.
        customer_id: Id of the customer in your system, from the check.
        feature: Feature from the check.
        attributes: Attributes from the check.
        idempotency_key: UUID the report of a fallback decision sends. None for a server
            decision, whose report the server keys by `decision_id`.
    """

    decision_id: str | None
    outcome: Outcome
    reason: Reason | None
    provider: str
    model: str
    overrides: Mapping[str, AttributeValue]
    estimated_cost: Decimal | None
    reserved_amount: Decimal
    estimate_basis: EstimateBasis
    cost_status: CostStatus | None
    matched_policy_id: str | None
    fallback_outcome: FallbackOutcome
    expires_at: datetime | None
    signals: Signals | None
    customer_id: str
    feature: str
    attributes: Mapping[str, AttributeValue]
    idempotency_key: str | None

    @classmethod
    def from_response(
        cls,
        body: Mapping[str, Any],
        *,
        customer_id: str,
        feature: str,
        attributes: Mapping[str, AttributeValue],
    ) -> "Decision":
        """Builds a server decision from a check response and the checked request."""
        estimated_cost = body["estimated_cost"]
        return cls(
            decision_id=body["decision_id"],
            outcome=body["outcome"],
            reason=body["reason"],
            provider=body["provider"],
            model=body["model"],
            overrides=dict(body["overrides"]),
            estimated_cost=None if estimated_cost is None else parse_amount(estimated_cost),
            reserved_amount=parse_amount(body["reserved_amount"]),
            estimate_basis=body["estimate_basis"],
            cost_status=body["cost_status"],
            matched_policy_id=body["matched_policy_id"],
            fallback_outcome=body["fallback_outcome"],
            expires_at=parse_timestamp(body["expires_at"]),
            signals=Signals.from_response(body["signals"]),
            customer_id=customer_id,
            feature=feature,
            attributes=dict(attributes),
            idempotency_key=None,
        )

    @classmethod
    def fallback(
        cls,
        *,
        customer_id: str,
        feature: str,
        provider: str,
        model: str,
        attributes: Mapping[str, AttributeValue],
        outcome: FallbackOutcome,
    ) -> "Decision":
        """Builds a fallback decision for a request Preburn could not check.

        The decision gets a new random UUID as its idempotency key, so reporting it twice
        stores one ledger entry.
        """
        return cls(
            decision_id=None,
            outcome=outcome,
            reason=None,
            provider=provider,
            model=model,
            overrides={},
            estimated_cost=None,
            reserved_amount=Decimal(0),
            estimate_basis="none",
            cost_status=None,
            matched_policy_id=None,
            fallback_outcome=outcome,
            expires_at=None,
            signals=None,
            customer_id=customer_id,
            feature=feature,
            attributes=dict(attributes),
            idempotency_key=str(uuid.uuid4()),
        )

    @property
    def is_fallback(self) -> bool:
        """True when the SDK made the decision because Preburn could not be reached."""
        return self.decision_id is None

    def raise_for_denial(self) -> "Decision":
        """Raises when the outcome is deny, else returns the decision.

        Raises:
            DecisionDenied: The outcome is deny.
        """
        if self.outcome == "deny":
            raise DecisionDenied(self)
        return self


@dataclass(frozen=True, kw_only=True)
class ReportResult:
    """Ledger entry a report stored or found.

    Attributes:
        ledger_entry_id: Ledger entry id, such as `led_01jbvagescfn78y0938nkrkayd`.
        cost: USD cost of the usage. None when uncosted.
        cost_status: `uncosted` when a meter of the usage has no price.
        duplicate: True when an earlier report of the same decision or idempotency key
            stored the entry and this one changed nothing.
    """

    ledger_entry_id: str
    cost: Decimal | None
    cost_status: CostStatus
    duplicate: bool

    @classmethod
    def from_response(cls, body: Mapping[str, Any]) -> "ReportResult":
        """Builds the result from a report response."""
        cost = body["cost"]
        return cls(
            ledger_entry_id=body["ledger_entry_id"],
            cost=None if cost is None else parse_amount(cost),
            cost_status=body["cost_status"],
            duplicate=body["duplicate"],
        )


@dataclass(frozen=True, kw_only=True)
class Customer:
    """A customer as the server stores it.

    Attributes:
        id: Customer id, such as `cust_01jbvagescfn78y0938nkrkayd`.
        external_id: Id of the customer in your system.
        display_name: Name the dashboard shows, or None.
        plan_id: Plan id, or None when the default plan of the environment applies.
        metadata: JSON object attached to the customer.
        status: Record status of the customer.
        created_at: When the customer was created.
        updated_at: When the customer was last replaced.
    """

    id: str
    external_id: str
    display_name: str | None
    plan_id: str | None
    metadata: Mapping[str, object]
    status: CustomerStatus
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_response(cls, body: Mapping[str, Any]) -> "Customer":
        """Builds the customer from a customer response."""
        return cls(
            id=body["id"],
            external_id=body["external_id"],
            display_name=body["display_name"],
            plan_id=body["plan_id"],
            metadata=dict(body["metadata"]),
            status=body["status"],
            created_at=parse_timestamp(body["created_at"]),
            updated_at=parse_timestamp(body["updated_at"]),
        )


@dataclass(frozen=True, kw_only=True)
class RevenueEntry:
    """A revenue entry as the server stores it.

    Attributes:
        id: Revenue entry id, such as `rev_01jbvagescfn78y0938nkrkayd`.
        customer_id: Id of the customer in your system.
        kind: `subscription` and `adjustment` add the amount to net revenue, `stripe_fee`,
            `refund` and `credit_note` subtract it.
        amount: Non-negative amount in USD. The kind sets its sign.
        period_start: Start of the period the entry belongs to.
        period_end: End of the period, equal to `period_start` for a one-time line.
        source: Where the entry came from.
        source_reference: Id of the entry in its source.
        occurred_at: When the revenue was recognized.
        created_at: When the entry was recorded.
        duplicate: True when an earlier request recorded the entry and this one changed
            nothing.
    """

    id: str
    customer_id: str
    kind: RevenueKind
    amount: Decimal
    period_start: datetime
    period_end: datetime
    source: RevenueSource
    source_reference: str
    occurred_at: datetime
    created_at: datetime
    duplicate: bool

    @classmethod
    def from_response(cls, body: Mapping[str, Any]) -> "RevenueEntry":
        """Builds the entry from a revenue response."""
        return cls(
            id=body["id"],
            customer_id=body["customer_id"],
            kind=body["kind"],
            amount=parse_amount(body["amount"]),
            period_start=parse_timestamp(body["period_start"]),
            period_end=parse_timestamp(body["period_end"]),
            source=body["source"],
            source_reference=body["source_reference"],
            occurred_at=parse_timestamp(body["occurred_at"]),
            created_at=parse_timestamp(body["created_at"]),
            duplicate=body["duplicate"],
        )


def parse_report_batch(body: Mapping[str, Any]) -> list[ReportResult | PreburnError]:
    """Parses a batch report response into one entry per report, in request order.

    Returns:
        The `ReportResult` of each stored report and the error of each rejected one, with the
        class its status maps to, such as `ValidationError` for 422.
    """
    results: list[ReportResult | PreburnError] = []
    for item in body["results"]:
        if item["status"] == REPORT_ACCEPTED_STATUS:
            results.append(ReportResult.from_response(item["result"]))
            continue
        failure = item["error"]
        invalid_fields = tuple(
            InvalidField(location=field["location"], message=field["message"])
            for field in failure.get("errors", [])
        )
        results.append(
            make_status_error(item["status"], failure["code"], failure["detail"], invalid_fields)
        )
    return results


def parse_timestamp(text: str) -> datetime:
    """Parses an API timestamp such as `2026-09-26T10:00:00.123456789Z` into a UTC datetime.

    Digits beyond microseconds are dropped.

    Raises:
        ValueError: The text is not an RFC 3339 timestamp ending in `Z`.
    """
    match = _TIMESTAMP_PATTERN.fullmatch(text)
    if match is None:
        raise ValueError(f"timestamp not RFC 3339 UTC value={text!r}")
    year, month, day, hour, minute, second, fraction = match.groups(default="")
    microsecond = int(fraction[:MICROSECOND_DIGITS].ljust(MICROSECOND_DIGITS, "0"))
    return datetime(
        int(year),
        int(month),
        int(day),
        int(hour),
        int(minute),
        int(second),
        microsecond,
        tzinfo=timezone.utc,
    )


def format_timestamp(value: datetime) -> str:
    """Formats a timezone-aware datetime as an API timestamp in UTC ending in `Z`.

    Raises:
        ValueError: The datetime has no timezone.
    """
    if value.utcoffset() is None:
        raise ValueError("timestamp needs a timezone")
    return value.astimezone(timezone.utc).replace(tzinfo=None).isoformat() + "Z"
