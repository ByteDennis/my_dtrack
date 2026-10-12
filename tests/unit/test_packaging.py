import ast
import json
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


def test_right_package_contains_only_databricks_runtime_code():
    right = ROOT / "src/dtrack_right"
    common = {path.name for path in (right / "common").glob("*.py")}
    assert common == {
        "__init__.py",
        "audit.py",
        "column_types.py",
        "date_platforms.py",
        "dates.py",
        "hashing.py",
        "manifest.py",
        "protocol.py",
        "sampling.py",
    }
    source = "\n".join(path.read_text() for path in right.rglob("*.py"))
    assert "CsvAnchorTarget" not in source
    assert "import pandas" not in source
    assert "sample_csv" not in source
    assert "compute_csv_statistics" not in source
    assert not (right / "__main__.py").exists()


def test_csv_sampling_is_not_reexported_from_common_hashing():
    paths = [
        path
        for root in (ROOT / "src", ROOT / "tests", ROOT / "examples")
        for path in root.rglob("*")
        if path.suffix in {".py", ".ipynb"}
    ]
    for path in paths:
        text = path.read_text()
        if path.suffix == ".ipynb":
            stale = [
                line
                for line in text.splitlines()
                if "common.hashing import " in line and "sample_csv" in line
            ]
        else:
            tree = ast.parse(text)
            stale = [
                node
                for node in ast.walk(tree)
                if (
                    isinstance(node, ast.ImportFrom)
                    and node.module == "dtrack_left.common.hashing"
                    and any(name.name == "sample_csv" for name in node.names)
                )
                or (
                    isinstance(node, ast.Attribute)
                    and node.attr == "sample_csv"
                    and ast.unparse(node.value).endswith("hashing")
                )
            ]
        assert not stale, path


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
        timeout=15,
    )
    assert completed.stdout.startswith("dtrack, version")


def test_learning_notebooks_match_the_public_cli_layout():
    notebooks = ROOT / "examples/nbs"
    expected = [
        "00_row_check.ipynb",
        "01_col_check.ipynb",
        "02_local.ipynb",
        "03_tools.ipynb",
        "04_common.ipynb",
        "05_reuse_pulled.ipynb",
        "06_pair_progress.ipynb",
    ]
    assert sorted(path.name for path in notebooks.glob("*.ipynb")) == expected
    assert not list((ROOT / "src").rglob("*.ipynb"))

    sources = {}
    for name in expected:
        document = json.loads((notebooks / name).read_text())
        markdown = sum(cell["cell_type"] == "markdown" for cell in document["cells"])
        assert markdown == (0 if name[:2] in {"05", "06"} else 1)
        assert all(not cell.get("outputs") for cell in document["cells"])
        code = "\n".join(
            "".join(line for line in cell["source"] if not line.lstrip().startswith("%"))
            for cell in document["cells"]
            if cell["cell_type"] == "code" and not "".join(cell["source"]).lstrip().startswith("%%")
        )
        ast.parse(code)
        sources[name] = code

    assert "'row-check'" in sources[expected[0]]
    assert "'col-check'" in sources[expected[1]]
    assert "'local', 'run'" in sources[expected[2]]
    assert "'tools', 'doctor'" in sources[expected[3]]
    assert "resolve_date_type('string').detect" in sources[expected[4]]
    assert "build_manifest" in sources[expected[4]]
    assert "src/dtrack_right/notebook.py" in sources[expected[0]]
    assert "SQL Warehouse" in sources[expected[1]] + sources[expected[2]]
    assert "reuse_pulled=True" in sources[expected[5]]
    assert "progress.collect" in sources[expected[6]] and "lvdash" in sources[expected[6]]
    assert all("assert " in sources[name] for name in expected)
    assert all("pytest.main" not in sources[name] for name in expected)
    assert all("subprocess.run(['pytest'" not in sources[name] for name in expected)
