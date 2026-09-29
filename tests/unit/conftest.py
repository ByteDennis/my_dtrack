import importlib
from types import SimpleNamespace

import pytest
from support import mock_aws_env, synthetic_data

COMMON = ("dates", "hashing", "stats", "manifest")


# >>> the shared common/ package, once per side: every common test runs against both copies <<< #
@pytest.fixture(params=["dtrack_left", "dtrack_right"])
def common(request):
    modules = {name: importlib.import_module(f"{request.param}.common.{name}") for name in COMMON}
    return SimpleNamespace(side=request.param, **modules)


# >>> the e2e synthetic CSVs (left/ORDERS.csv, right/orders.csv, ...) written once per session <<< #
@pytest.fixture(scope="session")
def synthetic(tmp_path_factory):
    root = tmp_path_factory.mktemp("synthetic")
    summary = synthetic_data(root)
    return SimpleNamespace(root=root, left=root / "left", right=root / "right", summary=summary)


# >>> in-process S3 (moto.mock_aws) with one empty bucket; yields the bucket name <<< #
@pytest.fixture
def s3_bucket(monkeypatch):
    moto = pytest.importorskip("moto")
    import boto3

    mock_aws_env(monkeypatch)
    with moto.mock_aws():
        boto3.client("s3").create_bucket(Bucket="unit")
        yield "unit"
