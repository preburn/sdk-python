"""The server's OpenAPI document, the operations the SDK calls, and checks against both."""

import json
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import httpx
from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry
from referencing.jsonschema import DRAFT202012

OPENAPI_PATH_VARIABLE = "PREBURN_OPENAPI_PATH"
SERVER_VERSION_PATH = Path(__file__).with_name("SERVER_VERSION")
RESPONSES_DIRECTORY = Path(__file__).with_name("responses")
RELEASE_ASSET_URL = "https://github.com/preburn/preburn/releases/download/v{version}/openapi.json"
DOWNLOAD_TIMEOUT_SECONDS = 30.0
DOCUMENT_URI = "urn:preburn:openapi"
JSON_CONTENT_TYPE = "application/json"
PROBLEM_CONTENT_TYPE = "application/problem+json"
PROBLEM_SCHEMA = "Problem"
API_KEY_SECURITY_SCHEME = "api_key"
BEARER_PREFIX = "Bearer "
FIRST_ERROR_STATUS = 400
INT64_MINIMUM = -(2**63)
INT64_MAXIMUM = 2**63 - 1
DATE_TIME_LAYOUT = "%Y-%m-%dT%H:%M:%S"

_DATE_TIME_PATTERN = re.compile(
    r"([0-9]{4}-[0-9]{2}-[0-9]{2})[Tt]([0-9]{2}:[0-9]{2}:[0-9]{2})(\.[0-9]+)?"
    r"([Zz]|[+-][0-9]{2}:[0-9]{2})"
)
_INTEGER_PATTERN = re.compile(r"-?[0-9]+")
_PATH_PARAMETER_PATTERN = re.compile(r"\{([a-z_]+)\}")


@dataclass(frozen=True)
class SDKOperation:
    """An operation of the server's API that the SDK calls.

    Attributes:
        operation_id: The operation's `operationId` in the document.
        method: HTTP method in lowercase, as the document's path items key it.
        path: Path template, such as `/api/v1/customers/{external_id}`.
        request_schema: Component schema of the request body.
        response_schemas: Component schema of the body of each success status.
    """

    operation_id: str
    method: str
    path: str
    request_schema: str
    response_schemas: Mapping[int, str]


@dataclass(frozen=True)
class CheckedRequest:
    """The operation a request matched and every way it breaks the document.

    Attributes:
        operation_id: Operation the method and path matched, None when none did.
        problems: One line per mismatch, empty when the request follows the document.
    """

    operation_id: str | None
    problems: tuple[str, ...]


@dataclass(frozen=True)
class ResponseFixture:
    """A response the server can send, stored as a JSON file under `responses/`.

    Attributes:
        name: File name without the extension.
        operation_id: Operation that answers with it.
        status: HTTP status.
        body: JSON body.
    """

    name: str
    operation_id: str
    status: int
    body: Any

    def to_response(self) -> httpx.Response:
        """Builds the HTTP response, with the problem content type for an error status."""
        content_type = (
            PROBLEM_CONTENT_TYPE if self.status >= FIRST_ERROR_STATUS else JSON_CONTENT_TYPE
        )
        return httpx.Response(
            self.status,
            headers={"Content-Type": content_type},
            content=json.dumps(self.body).encode(),
        )


SDK_OPERATIONS = (
    SDKOperation("check", "post", "/api/v1/check", "CheckRequest", {200: "CheckResponse"}),
    SDKOperation("report-usage", "post", "/api/v1/report", "ReportRequest", {202: "ReportResult"}),
    SDKOperation(
        "report-usage-batch",
        "post",
        "/api/v1/reports",
        "ReportBatchRequest",
        {202: "ReportBatchResponse"},
    ),
    SDKOperation(
        "release-decision", "post", "/api/v1/release", "ReleaseRequest", {200: "ReleaseResult"}
    ),
    SDKOperation(
        "upsert-customer",
        "put",
        "/api/v1/customers/{external_id}",
        "UpsertCustomerRequest",
        {200: "CustomerResponse"},
    ),
    SDKOperation(
        "record-revenue",
        "post",
        "/api/v1/revenue",
        "RecordRevenueRequest",
        {200: "RecordedRevenueEntryResponse", 201: "RecordedRevenueEntryResponse"},
    ),
)
SDK_OPERATIONS_BY_ID = {operation.operation_id: operation for operation in SDK_OPERATIONS}


class OpenAPIContract:
    """Checks SDK requests and server responses against the server's OpenAPI document.

    Schemas are JSON Schema 2020-12, as in OpenAPI 3.1. The `date-time` and `int64` formats
    are asserted, not only annotated, because the server rejects values that break them.
    """

    def __init__(self, document: Mapping[str, Any]) -> None:
        """Creates the checks over a parsed OpenAPI 3.1 document."""
        self.document = document
        self._registry: Registry[Any] = Registry().with_resource(
            DOCUMENT_URI, DRAFT202012.create_resource(document)
        )
        self._format_checker = FormatChecker(formats=())
        self._format_checker.checks("date-time")(is_date_time)
        self._format_checker.checks("int64")(is_int64)

    def document_operation(self, operation: SDKOperation) -> Mapping[str, Any]:
        """Returns the document's operation object for an SDK operation.

        Raises:
            KeyError: The document has no such path or method.
        """
        operation_object: Mapping[str, Any] = self.document["paths"][operation.path][
            operation.method
        ]
        return operation_object

    def schema_problems(self, schema_name: str, instance: object) -> list[str]:
        """Validates a JSON value against a component schema, one line per error."""
        reference = {"$ref": f"{DOCUMENT_URI}#/components/schemas/{schema_name}"}
        return self._problems(schema_name, reference, instance)

    def check_request(self, request: httpx.Request) -> CheckedRequest:
        """Matches a request to an SDK operation and checks it against the document.

        Checks the bearer credentials, the declared path, header and query parameters, that
        no undeclared Preburn header or query parameter is sent, the content type, and the
        body against the operation's request schema.
        """
        raw_path = request.url.raw_path.decode("ascii").partition("?")[0]
        for operation in SDK_OPERATIONS:
            path_match = _compile_path_pattern(operation.path).fullmatch(raw_path)
            if request.method.lower() == operation.method and path_match is not None:
                path_values = {
                    name: unquote(value) for name, value in path_match.groupdict().items()
                }
                problems = self._request_problems(operation, request, path_values)
                return CheckedRequest(operation.operation_id, tuple(problems))
        return CheckedRequest(None, (f"no SDK operation method={request.method} path={raw_path}",))

    def response_problems(self, fixture: ResponseFixture) -> list[str]:
        """Checks a response fixture against the schema of its operation and status.

        A success status must be one the operation declares. An error status is checked
        against the problem schema of the operation's default response, and the body's
        `status` must equal it.
        """
        operation = SDK_OPERATIONS_BY_ID[fixture.operation_id]
        if fixture.status < FIRST_ERROR_STATUS:
            if fixture.status not in operation.response_schemas:
                return [f"{fixture.name} status={fixture.status} not declared"]
            return self.schema_problems(operation.response_schemas[fixture.status], fixture.body)
        problems = self.schema_problems(PROBLEM_SCHEMA, fixture.body)
        if fixture.body["status"] != fixture.status:
            problems.append(f"{fixture.name} body status differs from status={fixture.status}")
        return problems

    def _request_problems(
        self, operation: SDKOperation, request: httpx.Request, path_values: Mapping[str, str]
    ) -> list[str]:
        name = operation.operation_id
        problems: list[str] = []
        if not request.headers.get("Authorization", "").startswith(BEARER_PREFIX):
            problems.append(f"{name} sent no bearer credentials")
        parameters = self.document_operation(operation).get("parameters", [])
        values_by_location: dict[str, Mapping[str, str]] = {
            "path": path_values,
            "header": request.headers,
            "query": request.url.params,
        }
        for parameter in parameters:
            values = values_by_location[parameter["in"]]
            if parameter["name"] not in values:
                if parameter.get("required", False):
                    problems.append(f"{name} misses parameter {parameter['name']}")
                continue
            problems.extend(self._parameter_problems(name, parameter, values[parameter["name"]]))
        declared = {(parameter["in"], parameter["name"].lower()) for parameter in parameters}
        for header_name in request.headers:
            if "preburn" in header_name.lower() and ("header", header_name.lower()) not in declared:
                problems.append(f"{name} sent undeclared header {header_name}")
        for query_name in request.url.params:
            if ("query", query_name.lower()) not in declared:
                problems.append(f"{name} sent undeclared query parameter {query_name}")
        content_type = request.headers.get("Content-Type", "")
        if content_type != JSON_CONTENT_TYPE:
            problems.append(f"{name} content type {content_type!r} is not {JSON_CONTENT_TYPE}")
            return problems
        problems.extend(self.schema_problems(operation.request_schema, json.loads(request.content)))
        return problems

    def _parameter_problems(
        self, operation_name: str, parameter: Mapping[str, Any], text: str
    ) -> list[str]:
        schema = parameter["schema"]
        value: object = text
        if schema["type"] == "integer":
            if _INTEGER_PATTERN.fullmatch(text) is None:
                return [f"{operation_name} parameter {parameter['name']} is not an integer"]
            value = int(text)
        return self._problems(f"{operation_name} parameter {parameter['name']}", schema, value)

    def _problems(self, subject: str, schema: Mapping[str, Any], instance: object) -> list[str]:
        validator = Draft202012Validator(
            schema, registry=self._registry, format_checker=self._format_checker
        )
        return [
            f"{subject} at /{'/'.join(str(part) for part in error.absolute_path)}: {error.message}"
            for error in validator.iter_errors(instance)
        ]


def load_openapi_document() -> dict[str, Any]:
    """Loads the server's OpenAPI document.

    Reads the file named by `PREBURN_OPENAPI_PATH` when it is set, else downloads the
    `openapi.json` asset of the server release pinned in `SERVER_VERSION`.

    Raises:
        OSError: The file cannot be read.
        httpx.HTTPError: The download failed.
    """
    path = os.environ.get(OPENAPI_PATH_VARIABLE, "")
    if path:
        text = Path(path).read_text(encoding="utf-8")
    else:
        version = SERVER_VERSION_PATH.read_text(encoding="utf-8").strip()
        response = httpx.get(
            RELEASE_ASSET_URL.format(version=version),
            follow_redirects=True,
            timeout=DOWNLOAD_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        text = response.text
    document: dict[str, Any] = json.loads(text)
    return document


def load_response_fixtures() -> list[ResponseFixture]:
    """Loads every response fixture under `responses/`, sorted by file name."""
    fixtures: list[ResponseFixture] = []
    for path in sorted(RESPONSES_DIRECTORY.glob("*.json")):
        content = json.loads(path.read_text(encoding="utf-8"))
        fixtures.append(
            ResponseFixture(
                name=path.stem,
                operation_id=content["operation_id"],
                status=content["status"],
                body=content["body"],
            )
        )
    return fixtures


def is_date_time(value: object) -> bool:
    """Returns False for a string that is not an RFC 3339 date-time, True otherwise."""
    if not isinstance(value, str):
        return True
    match = _DATE_TIME_PATTERN.fullmatch(value)
    if match is None:
        return False
    try:
        datetime.strptime(f"{match[1]}T{match[2]}", DATE_TIME_LAYOUT)
    except ValueError:
        return False
    return True


def is_int64(value: object) -> bool:
    """Returns False for an integer outside the signed 64-bit range, True otherwise."""
    if not isinstance(value, int) or isinstance(value, bool):
        return True
    return INT64_MINIMUM <= value <= INT64_MAXIMUM


def _compile_path_pattern(template: str) -> re.Pattern[str]:
    parts = _PATH_PARAMETER_PATTERN.split(template)
    literals = parts[0::2]
    names = parts[1::2]
    pattern = re.escape(literals[0])
    for name, literal in zip(names, literals[1:], strict=True):
        pattern += f"(?P<{name}>[^/]+){re.escape(literal)}"
    return re.compile(pattern)
