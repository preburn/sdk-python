"""Errors the SDK raises."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from preburn._models import Decision

UNEXPECTED_RESPONSE_CODE = "unexpected_response"
CONFIGURATION_INVALID_CODE = "configuration_invalid"
DECISION_DENIED_CODE = "decision_denied"
DECISION_NOT_APPLICABLE_CODE = "decision_not_applicable"


@dataclass(frozen=True)
class InvalidField:
    """One invalid field of a rejected request.

    Attributes:
        location: Where the field is, such as `body.usage.output_seconds`.
        message: What is wrong with the field.
    """

    location: str
    message: str


class PreburnError(Exception):
    """Base class of every error the SDK raises.

    Attributes:
        status: HTTP status of the response, or None for an error raised without a response.
        code: Stable error code, such as `validation_failed`. Server codes are listed in
            the server's `docs/errors.md`.
        detail: Explanation of this occurrence of the error.
        errors: Invalid fields of the request, empty unless the server listed some.
    """

    def __init__(
        self,
        code: str,
        detail: str,
        *,
        status: int | None = None,
        errors: tuple[InvalidField, ...] = (),
    ) -> None:
        """Creates the error with its code, detail and, for server errors, status and fields."""
        if status is None:
            message = f"code={code} detail={detail}"
        else:
            message = f"code={code} status={status} detail={detail}"
        super().__init__(message)
        self.status = status
        self.code = code
        self.detail = detail
        self.errors = errors


class ConfigurationError(PreburnError):
    """The client options or environment variables are missing or invalid."""

    def __init__(self, detail: str) -> None:
        """Creates the error with a detail naming the invalid option."""
        super().__init__(CONFIGURATION_INVALID_CODE, detail)


class AuthenticationError(PreburnError):
    """The server answered 401: the API key is missing, unknown or revoked."""


class ScopeError(PreburnError):
    """The server answered 403: the API key may not call this route."""


class ValidationError(PreburnError):
    """The server answered 422: the request is invalid, as listed in `errors`."""


class RateLimitError(PreburnError):
    """The server answered 429.

    Attributes:
        retry_after: Seconds to wait before retrying, from the `Retry-After` header.
    """

    def __init__(
        self,
        code: str,
        detail: str,
        *,
        status: int,
        errors: tuple[InvalidField, ...] = (),
        retry_after: int,
    ) -> None:
        """Creates the error with the wait from the `Retry-After` header."""
        super().__init__(code, detail, status=status, errors=errors)
        self.retry_after = retry_after


class APIError(PreburnError):
    """The server answered an error status without a more specific class.

    A response whose body is not a problem document has the code `unexpected_response`.
    """


class DecisionDenied(PreburnError):
    """The decision's outcome is deny.

    Attributes:
        decision: The denied decision.
    """

    def __init__(self, decision: Decision) -> None:
        """Creates the error for a denied decision."""
        super().__init__(
            DECISION_DENIED_CODE,
            f"decision denied decision_id={decision.decision_id} reason={decision.reason}",
        )
        self.decision = decision


class DecisionNotApplicableError(PreburnError):
    """The decision asks for a change the caller cannot apply, such as a route to another provider.

    Attributes:
        decision: The decision that could not be applied.
    """

    def __init__(self, decision: Decision, detail: str) -> None:
        """Creates the error for a decision and a detail naming what could not be applied."""
        super().__init__(DECISION_NOT_APPLICABLE_CODE, detail)
        self.decision = decision


_ERROR_CLASSES_BY_STATUS: dict[int, type[PreburnError]] = {
    401: AuthenticationError,
    403: ScopeError,
    422: ValidationError,
}


def make_status_error(
    status: int, code: str, detail: str, errors: tuple[InvalidField, ...]
) -> PreburnError:
    """Builds the error for a status other than 429 from the fields of its problem."""
    error_class = _ERROR_CLASSES_BY_STATUS.get(status, APIError)
    return error_class(code, detail, status=status, errors=errors)
