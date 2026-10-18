import shutil
import subprocess
from functools import cache

import pytest

GATES = {
    "spark": ("--run-spark", "pass --run-spark"),
    "oracle": ("--run-oracle", "pass --run-oracle"),
    "live": ("--run-live", "pass --run-live"),
}


def pytest_addoption(parser):
    for flag, help_text in GATES.values():
        parser.addoption(flag, action="store_true", default=False, help=help_text)


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


def pytest_collection_modifyitems(config, items):
    for item in items:
        for marker, (flag, reason) in GATES.items():
            if item.get_closest_marker(marker) is not None and not config.getoption(flag):
                item.add_marker(pytest.mark.skip(reason=reason))
        if "docker" in item.keywords and not docker_available():
            item.add_marker(pytest.mark.skip(reason="Docker is unavailable"))
