import os
import shutil
import subprocess
from functools import cache

import pytest

GATES = {
    "moto": ("--run-moto", "needs the Moto S3 container (just s3-up); pass --run-moto"),
    "spark": ("--run-spark", "starts a local pyspark session (needs Java); pass --run-spark"),
    "oracle": ("--run-oracle", "starts an Oracle container via testcontainers; pass --run-oracle"),
    "e2e": ("--run-e2e", "runs the full examples/e2e script; pass --run-e2e"),
    "live": ("--run-live", "talks to real AWS S3 and Databricks; pass --run-live"),
}


def pytest_addoption(parser):
    for flag, help_text in GATES.values():
        parser.addoption(flag, action="store_true", default=False, help=help_text)


# >>> a Docker daemon answers `docker info`; checked once, only when a docker test is collected <<< #
@cache
def docker_available() -> bool:
    if not shutil.which("docker"):
        return False
    try:
        return (
            subprocess.run(
                ["docker", "info"], capture_output=True, timeout=15, check=False
            ).returncode
            == 0
        )
    except (OSError, subprocess.TimeoutExpired):
        return False


# >>> tests/local and tests/live are opt-in per marker; docker tests also need a reachable daemon <<< #
def pytest_collection_modifyitems(config, items):
    for item in items:
        for marker, (flag, reason) in GATES.items():
            if marker in item.keywords and not config.getoption(flag):
                item.add_marker(pytest.mark.skip(reason=reason))
        if (
            "docker" in item.keywords
            and not item.get_closest_marker("skip")
            and not docker_available()
        ):
            item.add_marker(pytest.mark.skip(reason="needs a Docker daemon (docker info failed)"))


@pytest.fixture
def moto_endpoint():
    endpoint = os.getenv("DTRACK_S3_ENDPOINT_URL")
    if not endpoint:
        pytest.skip("DTRACK_S3_ENDPOINT_URL is not set")
    return endpoint
