from collections.abc import Mapping
from typing import Any

import pytest

from tests.contract.openapi import (
    API_KEY_SECURITY_SCHEME,
    JSON_CONTENT_TYPE,
    PROBLEM_CONTENT_TYPE,
    PROBLEM_SCHEMA,
    SDK_OPERATIONS,
    OpenAPIContract,
    SDKOperation,
)

SCHEMA_REFERENCE_PREFIX = "#/components/schemas/"
DEFAULT_RESPONSE = "default"


def read_schema_names(content: Mapping[str, Any]) -> dict[str, str]:
    return {
        content_type: media["schema"]["$ref"].removeprefix(SCHEMA_REFERENCE_PREFIX)
        for content_type, media in content.items()
    }


@pytest.mark.parametrize("operation", SDK_OPERATIONS, ids=lambda operation: operation.operation_id)
def test_document_declares_the_sdk_operation(
    contract: OpenAPIContract, operation: SDKOperation
) -> None:
    declared = contract.document_operation(operation)
    actual = {
        "operation_id": declared["operationId"],
        "security": declared["security"],
        "request": read_schema_names(declared["requestBody"]["content"]),
        "responses": {
            status: read_schema_names(response["content"])
            for status, response in declared["responses"].items()
        },
    }
    expected = {
        "operation_id": operation.operation_id,
        "security": [{API_KEY_SECURITY_SCHEME: []}],
        "request": {JSON_CONTENT_TYPE: operation.request_schema},
        "responses": {
            **{
                str(status): {JSON_CONTENT_TYPE: schema_name}
                for status, schema_name in operation.response_schemas.items()
            },
            DEFAULT_RESPONSE: {PROBLEM_CONTENT_TYPE: PROBLEM_SCHEMA},
        },
    }
    if actual != expected:
        pytest.fail(f"declaration differs actual={actual} expected={expected}")


def test_api_key_security_scheme_is_a_bearer_token(contract: OpenAPIContract) -> None:
    scheme = contract.document["components"]["securitySchemes"][API_KEY_SECURITY_SCHEME]
    if (scheme["type"], scheme["scheme"]) != ("http", "bearer"):
        pytest.fail(f"api key scheme is not http bearer scheme={scheme}")
