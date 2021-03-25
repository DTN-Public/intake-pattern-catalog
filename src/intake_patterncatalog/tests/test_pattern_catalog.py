import pathlib
from time import sleep

import boto3
import botocore.awsrequest
import pytest
from botocore.awsrequest import AWSResponse
from moto import mock_s3

from intake_patterncatalog import PatternCatalog


# This is necessary because of this issue
# https://github.com/aio-libs/aiobotocore/issues/755
class MonkeyPatchedAWSResponse(AWSResponse):
    raw_headers = {}

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
    return {"path": "s3://no_ttl/{num}.csv", "driver": "csv", "ttl": -1}


@pytest.fixture
def ttl_config_s3():
    return {"path": "s3://ttl/{num}.csv", "driver": "csv", "ttl": 0.1}


@pytest.fixture
def no_ttl_cat_s3(no_ttl_config_s3):
    return PatternCatalog.from_dict({}, **no_ttl_config_s3)


@pytest.fixture
def ttl_cat_s3(ttl_config_s3):
    return PatternCatalog.from_dict({}, **ttl_config_s3)


def test_pattern_generation(empty_catalog):
    """_pattern property removes filesystem prefix and caching prefix"""
    actual = pathlib.Path.cwd() / "{file}.csv"
    assert empty_catalog._pattern == str(actual)


@mock_s3
def test_no_ttl_s3(no_ttl_cat_s3):
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="no_ttl")
    assert no_ttl_cat_s3.get_entry_kwarg_sets() == []
    s3.put_object(Body="", Bucket="no_ttl", Key="1.csv")
    s3.put_object(Body="", Bucket="no_ttl", Key="2.csv")
    assert no_ttl_cat_s3.get_entry_kwarg_sets() == [{"num": "1"}, {"num": "2"}]


@mock_s3
def test_ttl_s3(ttl_cat_s3):
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="ttl")
    assert ttl_cat_s3.get_entry_kwarg_sets() == []
    s3.put_object(Body="", Bucket="ttl", Key="1.csv")
    s3.put_object(Body="", Bucket="ttl", Key="2.csv")
    assert ttl_cat_s3.get_entry_kwarg_sets() == []
    sleep(0.11)
    assert ttl_cat_s3.get_entry_kwarg_sets() == [{"num": "1"}, {"num": "2"}]
