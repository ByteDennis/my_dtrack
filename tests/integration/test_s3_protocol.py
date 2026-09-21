import os
import uuid

import boto3
import pytest
from botocore.config import Config

from dtrack_distributed.adapters.storage.s3 import S3Store

pytestmark = pytest.mark.integration


def test_moto_s3_data_before_pointer_and_start_after():
    endpoint = os.getenv("DTRACK_S3_ENDPOINT_URL")
    if not endpoint:
        pytest.skip("DTRACK_S3_ENDPOINT_URL is not set")
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=Config(s3={"addressing_style": "path"}),
    )
    bucket = f"dtrack-{uuid.uuid4().hex}"
    client.create_bucket(Bucket=bucket)
    store = S3Store(f"s3://{bucket}/run", client=client)
    store.commit_unit(1, "a", {"value": 1}, {"pair": "p"})
    store.commit_unit(2, "b", {"value": 2}, {"pair": "p"})
    assert store.exists("units/a.json")
    assert store.list_keys("_INDEX/", start_after="_INDEX/00000001-a.json") == [
        "_INDEX/00000002-b.json"
    ]
