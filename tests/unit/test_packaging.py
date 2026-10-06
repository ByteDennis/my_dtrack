import subprocess
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_left_package_has_no_databricks_dependency_and_optional_left_adapters():
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())
    dependencies = project["project"]["dependencies"]
    extras = project["project"]["optional-dependencies"]
    assert any(item.startswith("oracledb") for item in dependencies)
    assert any(item.startswith("boto3") for item in dependencies)
    assert not any("databricks" in item for item in dependencies)
    assert not any("pyathena" in item or "impyla" in item for item in dependencies)
    assert extras["athena"] == ["pyathena>=2.0"]
    assert any(item.startswith("impyla") for item in extras["hadoop"])
    assert sorted(extras) == ["athena", "confluence", "dbx", "hadoop", "ldap"]
    assert extras["confluence"] == ["atlassian-python-api==5.0.5"]
    assert [item.split(">")[0] for item in extras["dbx"]] == ["databricks-sdk"]
    assert project["project"]["scripts"] == {"dtrack": "dtrack.cli:main"}


def test_right_package_never_imports_the_left_package():
    for path in (ROOT / "src/dtrack_right").rglob("*.py"):
        assert "dtrack_left" not in path.read_text(), path


# >>> the pipeline never touches Databricks; only `dtrack tools dbx` may, lazily <<< #
def test_left_pipeline_never_touches_databricks_and_dbx_imports_the_sdk_lazily():
    left = ROOT / "src/dtrack_left"
    helper = left / "backends/databricks.py"
    for path in left.rglob("*.py"):
        text = path.read_text()
        assert "DATABRICKS_TOKEN" not in text, path
        if path != helper:
            assert "databricks.sdk" not in text, path
    top_level = [
        line for line in helper.read_text().splitlines() if line.startswith(("import ", "from "))
    ]
    assert not any("databricks" in line for line in top_level)
    pipeline = [
        *(left / "checks").glob("*.py"),
        *(left / "common").glob("*.py"),
        left / "cli/checks.py",
    ]
    pipeline += [left / "backends/sources.py", left / "backends/store.py"]
    for path in pipeline:
        assert (
            "databricks import" not in path.read_text()
            and "import databricks" not in path.read_text()
        ), path


# >>> the S3 credential helper is an operator tool: no pipeline module imports it <<< #
def test_left_pipeline_never_imports_the_aws_helper():
    left = ROOT / "src/dtrack_left"
    pipeline = [*(left / "checks").glob("*.py"), *(left / "common").glob("*.py")]
    pipeline += [left / "cli/checks.py", left / "backends/sources.py", left / "backends/store.py"]
    for path in pipeline:
        text = path.read_text()
        assert "import aws" not in text and "backends.aws" not in text, path


# >>> the local backup plan builds on both sides; neither side nor the Right job may depend on it <<< #
def test_only_dtrack_local_combines_the_two_sides():
    for side in ("dtrack_left", "dtrack_right"):
        for path in (ROOT / "src" / side).rglob("*.py"):
            text = path.read_text()
            assert "import dtrack_local" not in text and "from dtrack_local" not in text, path
    local = "\n".join(path.read_text() for path in (ROOT / "src/dtrack_local").glob("*.py"))
    assert "from dtrack_left" in local and "from dtrack_right" in local


def test_right_job_installs_only_what_a_databricks_runtime_lacks():
    requirements = (ROOT / "src/dtrack_right/requirements.txt").read_text().split()
    assert sorted(name.split(">=")[0] for name in requirements) == ["boto3", "openpyxl"]


def test_python_dash_m_dtrack_is_the_same_cli():
    import dtrack
    import dtrack.cli

    assert dtrack.main is dtrack.cli.main
    completed = subprocess.run(
        [sys.executable, "-m", "dtrack", "--version"],
        capture_output=True,
        text=True,
        check=True,
        cwd=ROOT,
    )
    assert completed.stdout.startswith("dtrack, version")
