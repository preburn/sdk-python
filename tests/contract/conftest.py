import pytest

from tests.contract.openapi import OpenAPIContract, load_openapi_document


@pytest.fixture(name="contract", scope="session")
def make_contract() -> OpenAPIContract:
    return OpenAPIContract(load_openapi_document())
