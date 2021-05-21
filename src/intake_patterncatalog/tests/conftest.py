import os

import boto3
import botocore.awsrequest
import pytest
from botocore.awsrequest import AWSResponse
from moto import mock_s3


# This is necessary because of this issue
# https://github.com/aio-libs/aiobotocore/issues/755
class MonkeyPatchedAWSResponse(AWSResponse):
    raw_headers: dict = {}

    async def read(self):
        return self.text


botocore.awsrequest.AWSResponse = MonkeyPatchedAWSResponse


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
