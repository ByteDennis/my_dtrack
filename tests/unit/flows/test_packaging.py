import ast
import json
import os
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src"
LAYOUT_ROOTS = (SRC, ROOT / "tests")


def test_tree_layout():
    violations = []
    for root in LAYOUT_ROOTS:
        directories = [root, *(path for path in root.rglob("*") if path.is_dir())]
        for directory in sorted(directories):
            children = sorted(
                path.name
                for path in directory.iterdir()
                if path.name not in {"__pycache__", ".pytest_cache"}
                and path.suffix != ".pyc"
                and not path.name.endswith(".egg-info")
            )
            if len(children) > 5:
                relative = directory.relative_to(ROOT)
                violations.append(f"{relative}: {len(children)} children: {', '.join(children)}")
    assert not violations, "\n" + "\n".join(violations)


def test_left_dependencies():
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())
    dependencies = project["project"]["dependencies"]
    extras = project["project"]["optional-dependencies"]
    assert any(item.startswith("oracledb") for item in dependencies)
    assert any(item.startswith("boto3") for item in dependencies)
    assert not any("databricks" in item for item in dependencies)
    assert not any("pyathena" in item or "impyla" in item for item in dependencies)
    assert extras["athena"] == ["pyathena>=2.0"]
    assert extras["hadoop"] == ["pyodbc>=5.0"]
    assert sorted(extras) == ["athena", "confluence", "dbx", "hadoop", "ldap"]
    assert extras["confluence"] == ["atlassian-python-api==5.0.5"]
    assert [item.split(">")[0] for item in extras["dbx"]] == ["databricks-sdk", "pywin32"]
    assert extras["dbx"][1].endswith("sys_platform == 'win32'")
    assert project["project"]["scripts"] == {"dtrack": "dtrack.cli:main"}


def test_right_imports():
    for path in (ROOT / "src/dtrack_right").rglob("*.py"):
        assert "dtrack_left" not in path.read_text(), path


def test_right_contents():
    right = ROOT / "src/dtrack_right"
    common = right / "engine/common"
    assert {path.name for path in common.iterdir() if path.name != "__pycache__"} == {
        "__init__.py",
        "model",
        "sql",
    }
    assert {path.name for path in (common / "model").glob("*.py")} == {
        "audit.py",
        "hashing.py",
        "manifest.py",
        "protocol.py",
    }
    assert {path.name for path in (common / "sql").glob("*.py")} == {
        "column_types.py",
        "date_platforms.py",
        "dates.py",
        "sampling.py",
    }
    source = "\n".join(path.read_text() for path in right.rglob("*.py"))
    assert "CsvAnchorTarget" not in source
    assert "import pandas" not in source
    assert "sample_csv" not in source
    assert "compute_csv_statistics" not in source
    assert not (right / "__main__.py").exists()


def test_sampling_exports():
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
                    and node.module == "dtrack_left.common.model.hashing"
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
def test_databricks_boundary():
    left = ROOT / "src/dtrack_left"
    helper = left / "backends/cloud/databricks.py"
    for path in left.rglob("*.py"):
        text = path.read_text()
        assert "DATABRICKS_TOKEN" not in text, path
        if path != helper:
            assert "databricks.sdk" not in text, path
    top_level = [
        line for line in helper.read_text().splitlines() if line.startswith(("import ", "from "))
    ]
    assert not any("databricks" in line for line in top_level)
    pipeline = [*(left / "common").rglob("*.py"), left / "cli/commands/checks.py"]
    pipeline += [left / "backends/sources.py", left / "backends/store.py"]
    for path in pipeline:
        assert (
            "databricks import" not in path.read_text()
            and "import databricks" not in path.read_text()
        ), path


# >>> the S3 credential helper is an operator tool: no pipeline module imports it <<< #
def test_aws_boundary():
    left = ROOT / "src/dtrack_left"
    pipeline = [*(left / "common").rglob("*.py"), left / "cli/commands/checks.py"]
    pipeline += [left / "backends/sources.py", left / "backends/store.py"]
    for path in pipeline:
        text = path.read_text()
        assert "import aws" not in text and "backends.aws" not in text, path


# >>> the local backup plan builds on both sides; neither side nor the Right job may depend on it <<< #
def test_local_boundary():
    for side in ("dtrack_left", "dtrack_right"):
        for path in (ROOT / "src" / side).rglob("*.py"):
            text = path.read_text()
            assert "import dtrack_local" not in text and "from dtrack_local" not in text, path
    local = "\n".join(path.read_text() for path in (ROOT / "src/dtrack_local").glob("*.py"))
    assert "from dtrack_left" in local and "from dtrack_right" in local


def test_right_requirements():
    requirements = (ROOT / "src/dtrack_right/requirements.txt").read_text().split()
    assert sorted(name.split(">=")[0] for name in requirements) == ["boto3", "openpyxl"]


def test_right_standalone(tmp_path):
    package_root = tmp_path / "package"
    shutil.copytree(ROOT / "src/dtrack_right", package_root / "dtrack_right")
    env = {**os.environ, "PYTHONPATH": str(package_root)}
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            "import dtrack_right.engine.job; import dtrack_right.progress; print('right ok')",
        ],
        capture_output=True,
        text=True,
        env=env,
        cwd=tmp_path,
        timeout=15,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == "right ok"


def test_module_cli():
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


def test_notebook_layout():
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


def test_name_policy():
    paths = set((ROOT / "tests").rglob("test_*.py"))

    violations = []
    for path in sorted(paths):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        scopes = [("module", tree)]
        while scopes:
            scope_name, scope = scopes.pop()
            names = [
                node.name
                for node in scope.body
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and node.name.startswith("test_")
            ]
            duplicates = sorted({name for name in names if names.count(name) > 1})
            if duplicates:
                violations.append(f"{path.relative_to(ROOT)}:{scope_name}: duplicates {duplicates}")
            for name in names:
                words = name.removeprefix("test_").split("_")
                if len(words) > 2 or not all(
                    any(char.isalpha() for char in word) for word in words
                ):
                    violations.append(f"{path.relative_to(ROOT)}:{name}: use 1-2 semantic words")
            for node in scope.body:
                if isinstance(node, ast.ClassDef):
                    scopes.append((f"{scope_name}.{node.name}", node))

    assert not violations, "\n" + "\n".join(violations)
