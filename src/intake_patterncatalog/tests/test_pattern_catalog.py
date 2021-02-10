import pytest

import pathlib
from intake_patterncatalog import PatternCatalog


@pytest.fixture(
    params=["file://./{file}.csv", "simplecache::file://./{file}.csv", "./{file}.csv"]
)
def empty_catalog(request):
    return PatternCatalog(request.param, driver="csv")


def test_pattern_generation(empty_catalog):
    """_pattern property removes filesystem prefix and caching prefix"""
    actual = pathlib.Path.cwd() / "{file}.csv"
    assert empty_catalog._pattern == str(actual)
