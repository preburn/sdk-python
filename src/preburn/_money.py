"""Exact conversion between API decimal strings and `decimal.Decimal`.

Amounts are USD with 9 decimals, quantities are meter units with at most 6 decimals, and
ratios have 4 decimals or are `inf` and `-inf`. Values never pass through `float`.
"""

import re
from decimal import Context, Decimal

AMOUNT_DECIMAL_PLACES = 9
QUANTITY_DECIMAL_PLACES = 6
RATIO_DECIMAL_PLACES = 4
MAXIMUM_INTEGER_DIGITS = 9
POSITIVE_INFINITY_TEXT = "inf"
NEGATIVE_INFINITY_TEXT = "-inf"

_AMOUNT_PATTERN = re.compile(r"-?(0|[1-9][0-9]*)(\.[0-9]{1,9})?")
_QUANTITY_PATTERN = re.compile(r"(0|[1-9][0-9]*)(\.[0-9]{1,6})?")
_RATIO_PATTERN = re.compile(r"-?(0|[1-9][0-9]*)(\.[0-9]{1,4})?")
_QUANTIZE_CONTEXT = Context(prec=MAXIMUM_INTEGER_DIGITS + AMOUNT_DECIMAL_PLACES)


def parse_amount(text: str) -> Decimal:
    """Parses an API amount string such as `"12.500000000"` into an exact `Decimal`.

    Raises:
        ValueError: The text is not an amount in the API format.
    """
    if _AMOUNT_PATTERN.fullmatch(text) is None:
        raise ValueError(f"amount not in API format value={text!r}")
    return Decimal(text)


def parse_quantity(text: str) -> Decimal:
    """Parses an API quantity string such as `"8.5"` into an exact `Decimal`.

    Raises:
        ValueError: The text is not a non-negative quantity with at most 6 decimals.
    """
    if _QUANTITY_PATTERN.fullmatch(text) is None:
        raise ValueError(f"quantity not in API format value={text!r}")
    return Decimal(text)


def parse_ratio(text: str) -> Decimal:
    """Parses an API ratio string such as `"0.4000"`, `"inf"` or `"-inf"` into a `Decimal`.

    Returns:
        The exact ratio, `Decimal("Infinity")` for `"inf"` and `Decimal("-Infinity")` for `"-inf"`.

    Raises:
        ValueError: The text is not a ratio in the API format.
    """
    if text == POSITIVE_INFINITY_TEXT:
        return Decimal("Infinity")
    if text == NEGATIVE_INFINITY_TEXT:
        return Decimal("-Infinity")
    if _RATIO_PATTERN.fullmatch(text) is None:
        raise ValueError(f"ratio not in API format value={text!r}")
    return Decimal(text)


def format_amount(value: Decimal | int) -> str:
    """Formats an amount as an API string with exactly 9 decimals, such as `"30.000000000"`.

    Raises:
        TypeError: The value is not a `Decimal` or an `int`.
        ValueError: The value is not finite or needs more than 9 integer digits or 9 decimals.
    """
    return format(_to_fixed_places(value, AMOUNT_DECIMAL_PLACES, "amount"), "f")


def format_quantity(value: Decimal | int) -> str:
    """Formats a quantity as an API string without trailing zeros, such as `"8.5"`.

    Raises:
        TypeError: The value is not a `Decimal` or an `int`.
        ValueError: The value is negative, not finite, or needs more than 9 integer digits or
            6 decimals.
    """
    quantized = _to_fixed_places(value, QUANTITY_DECIMAL_PLACES, "quantity")
    if quantized.is_signed():
        raise ValueError(f"quantity negative value={value}")
    text = format(quantized, "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def format_ratio(value: Decimal | int) -> str:
    """Formats a ratio as an API string with exactly 4 decimals, or as `"inf"` or `"-inf"`.

    Raises:
        TypeError: The value is not a `Decimal` or an `int`.
        ValueError: The value is NaN or needs more than 9 integer digits or 4 decimals.
    """
    decimal_value = _to_decimal(value)
    if decimal_value.is_infinite():
        return NEGATIVE_INFINITY_TEXT if decimal_value.is_signed() else POSITIVE_INFINITY_TEXT
    return format(_to_fixed_places(decimal_value, RATIO_DECIMAL_PLACES, "ratio"), "f")


def _to_decimal(value: Decimal | int) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (Decimal, int)):
        raise TypeError(f"expected Decimal or int type={type(value).__name__}")
    return Decimal(value)


def _to_fixed_places(value: Decimal | int, places: int, name: str) -> Decimal:
    decimal_value = _to_decimal(value)
    if not decimal_value.is_finite():
        raise ValueError(f"{name} not finite value={decimal_value}")
    if decimal_value.adjusted() >= MAXIMUM_INTEGER_DIGITS:
        raise ValueError(f"{name} exceeds integer digits maximum={MAXIMUM_INTEGER_DIGITS}")
    quantized = decimal_value.quantize(Decimal(f"1e-{places}"), context=_QUANTIZE_CONTEXT)
    if quantized != decimal_value:
        raise ValueError(f"{name} exceeds decimal places maximum={places}")
    return quantized
