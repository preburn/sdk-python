from collections.abc import Callable
from decimal import Decimal

import pytest

from preburn._money import (
    format_amount,
    format_quantity,
    format_ratio,
    parse_amount,
    parse_quantity,
    parse_ratio,
)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("12.500000000", Decimal("12.5")),
        ("0.000000000", Decimal("0")),
        ("-3.250000000", Decimal("-3.25")),
        ("0.000000001", Decimal("0.000000001")),
        ("999999999.999999999", Decimal("999999999.999999999")),
    ],
)
def test_amount_round_trips(text: str, expected: Decimal) -> None:
    parsed = parse_amount(text)
    if parsed != expected:
        pytest.fail(f"parsed={parsed} expected={expected}")
    formatted = format_amount(parsed)
    if formatted != text:
        pytest.fail(f"formatted={formatted} text={text}")


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (Decimal("30.00"), "30.000000000"),
        (Decimal("0.15"), "0.150000000"),
        (Decimal("999999999.999999999"), "999999999.999999999"),
        (Decimal("1E+2"), "100.000000000"),
        (12, "12.000000000"),
    ],
)
def test_amount_formats_input_values(value: Decimal | int, expected: str) -> None:
    formatted = format_amount(value)
    if formatted != expected:
        pytest.fail(f"formatted={formatted} expected={expected}")


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("8", Decimal("8")),
        ("8.5", Decimal("8.5")),
        ("0", Decimal("0")),
        ("1200", Decimal("1200")),
        ("0.000001", Decimal("0.000001")),
        ("999999999.999999", Decimal("999999999.999999")),
    ],
)
def test_quantity_round_trips(text: str, expected: Decimal) -> None:
    parsed = parse_quantity(text)
    if parsed != expected:
        pytest.fail(f"parsed={parsed} expected={expected}")
    formatted = format_quantity(parsed)
    if formatted != text:
        pytest.fail(f"formatted={formatted} text={text}")


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (Decimal("8.500"), "8.5"),
        (Decimal("8.0000000"), "8"),
        (Decimal("1E+3"), "1000"),
        (1_000_000, "1000000"),
        (0, "0"),
    ],
)
def test_quantity_formats_input_values(value: Decimal | int, expected: str) -> None:
    formatted = format_quantity(value)
    if formatted != expected:
        pytest.fail(f"formatted={formatted} expected={expected}")


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("0.4000", Decimal("0.4")),
        ("0.0100", Decimal("0.01")),
        ("-1.2500", Decimal("-1.25")),
        ("inf", Decimal("Infinity")),
        ("-inf", Decimal("-Infinity")),
    ],
)
def test_ratio_round_trips(text: str, expected: Decimal) -> None:
    parsed = parse_ratio(text)
    if parsed != expected:
        pytest.fail(f"parsed={parsed} expected={expected}")
    formatted = format_ratio(parsed)
    if formatted != text:
        pytest.fail(f"formatted={formatted} text={text}")


@pytest.mark.parametrize(
    ("parse", "text", "expected"),
    [
        (parse_amount, "9223372036.854775807", Decimal("9223372036.854775807")),
        (parse_amount, "-9223372036.854775808", Decimal("-9223372036.854775808")),
        (parse_amount, "12", Decimal("12")),
        (parse_amount, "30.00", Decimal("30")),
        (parse_ratio, "123456789012.0000", Decimal("123456789012")),
    ],
)
def test_parse_accepts_server_values_beyond_input_limits(
    parse: Callable[[str], Decimal], text: str, expected: Decimal
) -> None:
    parsed = parse(text)
    if parsed != expected:
        pytest.fail(f"parsed={parsed} expected={expected}")


def test_ratio_accepts_input_form() -> None:
    parsed = parse_ratio("0.40")
    if parsed != Decimal("0.4"):
        pytest.fail(f"parsed={parsed}")
    formatted = format_ratio(Decimal("0.40"))
    if formatted != "0.4000":
        pytest.fail(f"formatted={formatted}")


def test_infinite_ratios_are_decimal_infinity() -> None:
    positive = parse_ratio("inf")
    negative = parse_ratio("-inf")
    if not (positive.is_infinite() and not positive.is_signed()):
        pytest.fail(f"positive={positive}")
    if not (negative.is_infinite() and negative.is_signed()):
        pytest.fail(f"negative={negative}")


@pytest.mark.parametrize(
    "text",
    ["", "1.5e3", "01.000000000", "+1.000000000", "1.0000000001", " 1.5", "NaN", "inf", "1,5"],
)
def test_amount_parse_rejects_malformed(text: str) -> None:
    with pytest.raises(ValueError, match="amount"):
        parse_amount(text)


@pytest.mark.parametrize("text", ["", "-1", "1.", ".5", "1.0000001", "08", "1e3", "inf"])
def test_quantity_parse_rejects_malformed(text: str) -> None:
    with pytest.raises(ValueError, match="quantity"):
        parse_quantity(text)


@pytest.mark.parametrize("text", ["", "0.40000", "Infinity", "INF", "nan", "1.", "+inf"])
def test_ratio_parse_rejects_malformed(text: str) -> None:
    with pytest.raises(ValueError, match="ratio"):
        parse_ratio(text)


@pytest.mark.parametrize(
    ("value", "message"),
    [
        (Decimal("0.0000000001"), "decimal places"),
        (Decimal("1000000000"), "integer digits"),
        (Decimal("Infinity"), "not finite"),
        (Decimal("NaN"), "not finite"),
    ],
)
def test_amount_format_rejects_unrepresentable(value: Decimal, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        format_amount(value)


@pytest.mark.parametrize(
    ("value", "message"),
    [
        (Decimal("0.0000001"), "decimal places"),
        (Decimal("1000000000"), "integer digits"),
        (Decimal("-1"), "negative"),
        (Decimal("-0"), "negative"),
        (Decimal("NaN"), "not finite"),
    ],
)
def test_quantity_format_rejects_unrepresentable(value: Decimal, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        format_quantity(value)


def test_ratio_format_rejects_extra_decimals() -> None:
    with pytest.raises(ValueError, match="decimal places"):
        format_ratio(Decimal("0.12345"))


@pytest.mark.parametrize("value", [0.5, True, "1.5"])
def test_formatting_refuses_floats_and_other_types(value: object) -> None:
    with pytest.raises(TypeError, match="Decimal or int"):
        format_quantity(value)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="Decimal or int"):
        format_amount(value)  # type: ignore[arg-type]
