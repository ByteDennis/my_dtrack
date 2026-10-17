import importlib
import os
from pathlib import Path
from types import SimpleNamespace

import pytest
from support import mock_aws_env, synthetic_data

COMMON = {
    "dates": "sql.dates",
    "hashing": "model.hashing",
    "manifest": "model.manifest",
}
COMMON_ROOTS = {
    "dtrack_left": "dtrack_left.common",
    "dtrack_right": "dtrack_right.engine.common",
}
REPO = Path(__file__).resolve().parents[2]
DOTENV_MODULES = (
    "dtrack_left.cli",
    "dtrack_left.cli.config",
    "dtrack_left.cli.ops.dbx",
    "dtrack_local.cli",
)


# >>> unit tests never read the developer's real .env (repo root or above) and never leak variables <<< #
@pytest.fixture(autouse=True)
def isolated_environment():
    saved = dict(os.environ)
    ancestors = {REPO, *REPO.parents}
    with pytest.MonkeyPatch.context() as patch:
        for name in DOTENV_MODULES:
            module = importlib.import_module(name)
            real = module.find_dotenv

            def guarded(*args, _real=real, **kwargs):
                found = _real(*args, **kwargs)
                return "" if found and Path(found).resolve().parent in ancestors else found

            patch.setattr(module, "find_dotenv", guarded)
        yield
    os.environ.clear()
    os.environ.update(saved)


# >>> the shared common/ package, once per side: every common test runs against both copies <<< #
@pytest.fixture(params=["dtrack_left", "dtrack_right"])
def common(request):
    root = COMMON_ROOTS[request.param]
    modules = {name: importlib.import_module(f"{root}.{path}") for name, path in COMMON.items()}
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


# >>> an isolated ~/.aws: profile `corp` runs a credential_process that mints a new 30-minute key per call <<< #
@pytest.fixture
def aws_home(tmp_path, monkeypatch):
    import sys

    home = tmp_path / "aws"
    home.mkdir()
    process = home / "mint.py"
    process.write_text(
        "import json, pathlib, datetime\n"
        f"counter = pathlib.Path({str(home / 'count')!r})\n"
        "n = int(counter.read_text()) + 1 if counter.exists() else 1\n"
        "counter.write_text(str(n))\n"
        "expires = datetime.datetime.now(datetime.UTC) + datetime.timedelta(minutes=30)\n"
        "print(json.dumps({'Version': 1, 'AccessKeyId': f'ASIAKEY{n:09d}', "
        "'SecretAccessKey': f'secret{n}', 'SessionToken': f'token{n}', "
        "'Expiration': expires.isoformat()}))\n"
    )
    (home / "config").write_text(
        f"[profile corp]\nregion = us-east-1\ncredential_process = {sys.executable} {process}\n"
    )
    for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_CONFIG_FILE", str(home / "config"))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(home / "credentials"))
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    return home
