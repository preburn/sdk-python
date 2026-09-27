"""Create or replace a customer, then record its subscription revenue for the current month.

Run it against a Preburn server with a runtime API key:

    export PREBURN_BASE_URL=http://localhost:8080
    export PREBURN_API_KEY=pb_test_runtime_...
    python examples/record_revenue.py

The source reference names the month, so a second run in the same month returns the stored
entry with `duplicate=True` instead of recording it twice.
"""

from datetime import datetime, timezone
from decimal import Decimal

from preburn import Preburn

CUSTOMER_ID = "example-customer"
DISPLAY_NAME = "Example Customer"
MONTHLY_PRICE = Decimal("49.00")
DECEMBER = 12


def build_month_period(now: datetime) -> tuple[datetime, datetime]:
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if start.month == DECEMBER:
        return start, start.replace(year=start.year + 1, month=1)
    return start, start.replace(month=start.month + 1)


def main() -> None:
    period_start, period_end = build_month_period(datetime.now(timezone.utc))
    with Preburn() as preburn:
        customer = preburn.customers.upsert(
            CUSTOMER_ID, display_name=DISPLAY_NAME, metadata={"crm_id": "example-42"}
        )
        print(f"customer id={customer.id} external_id={customer.external_id}")
        entry = preburn.revenue.record(
            CUSTOMER_ID,
            "subscription",
            MONTHLY_PRICE,
            period_start,
            period_end,
            source_reference=f"example-invoice-{period_start:%Y-%m}",
        )
        print(
            f"revenue id={entry.id} kind={entry.kind} amount={entry.amount} "
            f"duplicate={entry.duplicate}"
        )


if __name__ == "__main__":
    main()
