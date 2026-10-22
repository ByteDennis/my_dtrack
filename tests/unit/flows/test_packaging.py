import subprocess
import sys
import tomllib
import zipfile
from pathlib import Path

from pair_conn_release.publish import create_archive

ROOT = Path(__file__).resolve().parents[3]


def test_layout():
    violations = []
    for root in (ROOT / "src", ROOT / "tests"):
        for directory in [root, *(path for path in root.rglob("*") if path.is_dir())]:
            children = [
                path
                for path in directory.iterdir()
                if path.name not in {"__pycache__", ".pytest_cache"} and path.suffix != ".pyc"
            ]
            limit = 6 if directory == ROOT / "src" else 5
            if len(children) > limit:
                violations.append(f"{directory.relative_to(ROOT)}: {len(children)}")
    assert not violations, "\n".join(violations)


def test_packages():
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    assert project["project"]["name"] == "pair-recon"
    assert project["project"]["scripts"] == {
        "pair_recon": "pair_recon.app.cli:main",
        "pair_recon_dbx": "pair_recon_dbx.cli:main",
        "pair_recon_gitlab": "pair_recon_gitlab.cli:main",
    }
    assert project["tool"]["hatch"]["build"]["targets"]["wheel"]["packages"] == [
        "src/pair_conn_release",
        "src/pair_recon",
        "src/pair_recon_dbx",
        "src/pair_recon_gitlab",
        "src/pair_recon_left",
        "src/pair_recon_right",
    ]
    assert "publish" not in project["dependency-groups"]


def test_launchers_and_source_release_use_uv_without_an_executable_build():
    recipe = (ROOT / "justfile").read_text(encoding="utf-8")
    windows_launcher = (ROOT / "run.cmd").read_text(encoding="utf-8")
    unix_launcher = (ROOT / "run.sh").read_text(encoding="utf-8")
    assert 'uv run --project "%~dp0" pair_recon %*' in windows_launcher
    assert 'uv run --project "$HERE" pair_recon "$@"' in unix_launcher
    assert "pair-recon.exe" not in windows_launcher
    assert "publish *args:" in recipe
    assert "uv run python -m pair_conn_release {{args}}" in recipe
    assert "build *args:" not in recipe
    assert "uv build" not in recipe
    assert not (ROOT / "tools" / "publish_portable.py").exists()
    assert not (ROOT / "tools" / "pair_recon_entry.py").exists()


def test_source_release_contains_only_the_main_runtime(tmp_path):
    output = tmp_path / "pair_recon.zip"

    result = create_archive(ROOT, output)

    assert result["path"] == str(output)
    assert result["bytes"] == output.stat().st_size
    with zipfile.ZipFile(output) as archive:
        names = set(archive.namelist())
        assert "pair_recon/readme.txt" in names
        assert "pair_recon/pyproject.toml" in names
        assert "pair_recon/justfile" in names
        assert "pair_recon/.pair_recon.env.example" in names
        assert "pair_recon/setup/pairs.xlsx" in names
        assert "pair_recon/src/pair_recon/__main__.py" in names
        assert "pair_recon/src/pair_recon_dbx/deploy.py" in names
        assert "pair_recon/src/pair_recon_left/__init__.py" in names
        assert "pair_recon/src/pair_recon_right/notebook.py" in names
        assert not any("pair_conn_release" in name for name in names)
        assert not any("pair_recon_gitlab" in name for name in names)
        assert "pair_recon/src/pair_recon/core/metadata/mapping.py" not in names
        assert "pair_recon/src/pair_recon/core/sas.py" not in names
        assert "pair_recon/src/pair_recon_dbx/cli.py" not in names
        assert not any("__pycache__" in name or name.endswith(".pyc") for name in names)
        assert not any(name.startswith("pair_recon/tests/") for name in names)
        assert not any(name.endswith((".md", ".ipynb", ".sql")) for name in names)

        project = tomllib.loads(archive.read("pair_recon/pyproject.toml").decode())
        assert project["project"]["scripts"] == {"pair_recon": "pair_recon.app.cli:main"}
        assert "dependency-groups" not in project
        assert project["tool"]["hatch"]["build"]["targets"]["wheel"]["packages"] == [
            "src/pair_recon",
            "src/pair_recon_dbx",
            "src/pair_recon_left",
            "src/pair_recon_right",
        ]
        assert archive.read("pair_recon/justfile").decode() == (
            "run *args:\n    uv run python -m pair_recon {{args}}\n"
        )
        readme = archive.read("pair_recon/readme.txt").decode()
        assert "just run" in readme
        assert "uv run python -m pair_recon" in readme
        assert "GitLab" not in readme


def test_setup_example_has_an_upsert_for_every_catalog_table():
    sql = (ROOT / "setup" / "setup.sql").read_text(encoding="utf-8")
    tables = (
        "pair_recon_pair_meta",
        "pair_recon_column_map",
        "pair_recon_pair_sql",
        "pair_recon_run_status",
        "pair_recon_run_error",
        "pair_recon_progress_snapshot",
    )
    for table in tables:
        assert f"-- # catalog.schema.{table}" in sql
        assert f"MERGE INTO catalog.schema.{table}" in sql
    assert sql.count("MERGE INTO catalog.schema.") == len(tables)


def test_imports():
    code = "import pair_conn_release, pair_recon, pair_recon_dbx, pair_recon_gitlab, pair_recon_left, pair_recon_right; print('ok')"
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"
