import os

import pytest

GATES = {
    "integration": ("--run-integration", "needs the Moto S3 container; pass --run-integration"),
    "e2e": ("--run-e2e", "runs the full examples/e2e script; pass --run-e2e"),
    "spark": ("--run-spark", "starts a local pyspark session (needs Java); pass --run-spark"),
    "containers": (
        "--run-containers",
        "starts database containers with testcontainers; pass --run-containers",
    ),
    "live": ("--run-live", "needs real Oracle / Databricks credentials; pass --run-live"),
}


def pytest_addoption(parser):
    for flag, help_text in GATES.values():
        parser.addoption(flag, action="store_true", default=False, help=help_text)


# >>> integration / e2e / live tests are opt-in; everything else runs by default <<< #
def pytest_collection_modifyitems(config, items):
    for item in items:
        for marker, (flag, reason) in GATES.items():
            if marker in item.keywords and not config.getoption(flag):
                item.add_marker(pytest.mark.skip(reason=reason))


@pytest.fixture
def moto_endpoint():
    endpoint = os.getenv("DTRACK_S3_ENDPOINT_URL")
    if not endpoint:
        pytest.skip("DTRACK_S3_ENDPOINT_URL is not set")
    return endpoint
