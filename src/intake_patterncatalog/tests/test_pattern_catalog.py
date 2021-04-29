import os
from pathlib import Path
from tempfile import TemporaryDirectory
from time import sleep
from typing import Generator

import boto3
import botocore.awsrequest
import pytest
from botocore.awsrequest import AWSResponse
from moto import mock_s3

from intake_patterncatalog import PatternCatalog


# This is necessary because of this issue
# https://github.com/aio-libs/aiobotocore/issues/755
class MonkeyPatchedAWSResponse(AWSResponse):
    raw_headers: dict = {}

    async def read(self):
        return self.text


botocore.awsrequest.AWSResponse = MonkeyPatchedAWSResponse


@pytest.fixture(
    params=["file://./{file}.csv", "simplecache::file://./{file}.csv", "./{file}.csv"]
)
def empty_catalog(request):
    return PatternCatalog(request.param, driver="csv")


@pytest.fixture
def no_ttl_config_s3():
    return {"urlpath": "s3://no_ttl/{num}.csv", "driver": "csv", "ttl": -1}


@pytest.fixture
def ttl_config_s3():
    return {"urlpath": "s3://ttl/{num}.csv", "driver": "csv", "ttl": 0.1}


@pytest.fixture
def ttl_config_s3_parquet():
    return {"urlpath": "s3://ttl/{num}.parquet", "driver": "parquet", "ttl": 0.1}


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.fixture(scope="function")
def s3(aws_credentials):
    with mock_s3():
        yield boto3.client("s3", region_name="us-east-1")


@pytest.fixture
def no_ttl_cat_s3(s3, no_ttl_config_s3):
    s3.create_bucket(Bucket="no_ttl")
    return PatternCatalog.from_dict({}, **no_ttl_config_s3)


@pytest.fixture
def ttl_cat_s3(ttl_config_s3, s3):
    s3.create_bucket(Bucket="ttl")
    return PatternCatalog.from_dict({}, **ttl_config_s3)


@pytest.fixture
def ttl_cat_s3_parquet(ttl_config_s3_parquet, s3):
    s3.create_bucket(Bucket="ttl")
    return PatternCatalog.from_dict({}, **ttl_config_s3_parquet)


def test_pattern_generation(empty_catalog):
    """_pattern property removes filesystem prefix and caching prefix"""
    actual = Path.cwd() / "{file}.csv"
    assert empty_catalog._pattern == str(actual)


def test_no_ttl_s3(s3, no_ttl_cat_s3):
    assert no_ttl_cat_s3.get_entry_kwarg_sets() == []
    s3.put_object(Body="", Bucket="no_ttl", Key="1.csv")
    s3.put_object(Body="", Bucket="no_ttl", Key="2.csv")
    assert no_ttl_cat_s3.get_entry_kwarg_sets() == [{"num": "1"}, {"num": "2"}]


def test_ttl_s3(s3, ttl_cat_s3):
    assert ttl_cat_s3.get_entry_kwarg_sets() == []
    s3.put_object(Body="", Bucket="ttl", Key="1.csv")
    s3.put_object(Body="", Bucket="ttl", Key="2.csv")
    assert ttl_cat_s3.get_entry_kwarg_sets() == []
    sleep(0.11)
    assert ttl_cat_s3.get_entry_kwarg_sets() == [{"num": "1"}, {"num": "2"}]


def test_ttl_s3_parquet(s3, ttl_cat_s3_parquet):
    assert ttl_cat_s3_parquet.get_entry_kwarg_sets() == []
    s3.put_object(Body="", Bucket="ttl", Key="1.parquet")
    s3.put_object(Body="", Bucket="ttl", Key="2.parquet")
    assert ttl_cat_s3_parquet.get_entry_kwarg_sets() == []
    sleep(0.11)
    assert ttl_cat_s3_parquet.get_entry_kwarg_sets() == [{"num": "1"}, {"num": "2"}]


@pytest.fixture
def folder_with_csvs() -> Generator[str, None, None]:
    with TemporaryDirectory() as tempdir:
        for i in range(10):
            Path(tempdir, f"{i}.csv").touch()
        yield str(tempdir)


@pytest.fixture
def ttl_config_unlistable(folder_with_csvs: str) -> dict:
    return {
        "urlpath": str(Path(folder_with_csvs, "{num}.csv")),
        "driver": "csv",
        "ttl": -1,
        "listable": False,
    }


@pytest.fixture
def unlistable_cat(ttl_config_unlistable: dict) -> PatternCatalog:
    return PatternCatalog.from_dict({}, **ttl_config_unlistable)


def test_unlistable_cat(unlistable_cat: PatternCatalog):
    # Make sure an unlistable catalog doesn't have any entries initially
    assert len(list(unlistable_cat)) == 0
    # Make sure I can access a valid entry without error
    assert unlistable_cat.get_entry(num=1)
    # After valid entry is accessed, make sure entries has been populated
    assert len(list(unlistable_cat)) == 1
    assert unlistable_cat.get_entry(num=1)
    # After valid entry is accessed again, make sure entry hasn't been duplicated in list
    assert len(list(unlistable_cat)) == 1
    # Check other valid entry
    assert unlistable_cat.get_entry(num=5)
    # After 2 valid entries are accessed, make sure entries has been populated with 2 entries
    assert len(list(unlistable_cat)) == 2
    # Make sure accessing invalid entry raises a KeyError
    with pytest.raises(KeyError):
        unlistable_cat.get_entry(num=-1)
