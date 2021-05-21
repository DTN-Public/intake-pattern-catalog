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


@pytest.fixture(scope="function")
def example_bucket(s3):
    bucket_name = "example-bucket"
    s3.create_bucket(Bucket=bucket_name)
    return bucket_name


def test_pattern_generation(empty_catalog):
    """_pattern property removes filesystem prefix and caching prefix"""
    actual = Path.cwd() / "{file}.csv"
    assert empty_catalog._pattern == str(actual)


def test_no_ttl_s3(example_bucket, s3):
    cat = PatternCatalog(
        urlpath="s3://" + example_bucket + "/{num}.csv",
        driver="csv",
        ttl=-1,
    )
    assert cat.get_entry_kwarg_sets() == []
    s3.put_object(Body="", Bucket=example_bucket, Key="1.csv")
    s3.put_object(Body="", Bucket=example_bucket, Key="2.csv")
    assert cat.get_entry_kwarg_sets() == [{"num": "1"}, {"num": "2"}]


def test_ttl_s3(example_bucket, s3):
    cat = PatternCatalog(
        urlpath="s3://" + example_bucket + "/{num}.csv",
        driver="csv",
        ttl=0.1,
    )
    assert cat.get_entry_kwarg_sets() == []
    s3.put_object(Body="", Bucket=example_bucket, Key="1.csv")
    s3.put_object(Body="", Bucket=example_bucket, Key="2.csv")
    assert cat.get_entry_kwarg_sets() == []
    sleep(0.11)
    assert cat.get_entry_kwarg_sets() == [{"num": "1"}, {"num": "2"}]


def test_ttl_s3_parquet(example_bucket, s3):
    cat = PatternCatalog(
        urlpath="s3://" + example_bucket + "/{num}.parquet",
        driver="parquet",
        ttl=0.1,
    )
    assert cat.get_entry_kwarg_sets() == []
    s3.put_object(Body="", Bucket=example_bucket, Key="1.parquet")
    s3.put_object(Body="", Bucket=example_bucket, Key="2.parquet")
    assert cat.get_entry_kwarg_sets() == []
    sleep(0.11)
    assert cat.get_entry_kwarg_sets() == [{"num": "1"}, {"num": "2"}]


@pytest.fixture
def folder_with_csvs() -> Generator[str, None, None]:
    with TemporaryDirectory() as tempdir:
        for i in range(10):
            Path(tempdir, f"{i}.csv").touch()
        yield str(tempdir)


def test_unlistable_cat(folder_with_csvs: str):
    cat = PatternCatalog(
        urlpath=str(Path(folder_with_csvs, "{num}.csv")),
        driver="csv",
        listable=False,
    )
    # Make sure an unlistable catalog doesn't have any entries initially
    assert len(list(cat)) == 0
    # Make sure I can access a valid entry without error
    assert cat.get_entry(num=1)
    # After valid entry is accessed, make sure entries has been populated
    assert len(list(cat)) == 1
    assert cat.get_entry(num=1)
    # After valid entry is accessed again, make sure entry hasn't been duplicated in list
    assert len(list(cat)) == 1
    # Check other valid entry
    assert cat.get_entry(num=5)
    # After 2 valid entries are accessed, make sure entries has been populated with 2 entries
    assert len(list(cat)) == 2
    # Make sure accessing invalid entry raises a KeyError
    with pytest.raises(KeyError):
        cat.get_entry(num=-1)
