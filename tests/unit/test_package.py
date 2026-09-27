from importlib import metadata, resources

import pytest

import preburn


def test_version_matches_installed_metadata() -> None:
    installed_version = metadata.version("preburn")
    if preburn.__version__ != installed_version:
        pytest.fail(f"version={preburn.__version__} installed_version={installed_version}")


def test_package_ships_typing_marker() -> None:
    if not resources.files("preburn").joinpath("py.typed").is_file():
        pytest.fail("typing marker missing file=py.typed")
