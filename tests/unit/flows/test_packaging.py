import subprocess
import sys
import tomllib
from pathlib import Path

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
            if len(children) > 5:
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
        "src/pair_recon",
        "src/pair_recon_dbx",
        "src/pair_recon_gitlab",
        "src/pair_recon_left",
        "src/pair_recon_right",
    ]
    assert "publish" not in project["dependency-groups"]


def test_windows_launcher_uses_uv_without_portable_publish_support():
    recipe = (ROOT / "justfile").read_text(encoding="utf-8")
    windows_launcher = (ROOT / "run.cmd").read_text(encoding="utf-8")
    unix_launcher = (ROOT / "run.sh").read_text(encoding="utf-8")
    assert 'uv run --project "%~dp0" pair_recon %*' in windows_launcher
    assert 'uv run --project "$HERE" pair_recon "$@"' in unix_launcher
    assert "pair-recon.exe" not in windows_launcher
    assert "publish *args:" not in recipe
    assert "build *args:" not in recipe
    assert "uv build" not in recipe
    assert not (ROOT / "tools" / "publish_portable.py").exists()
    assert not (ROOT / "tools" / "pair_recon_entry.py").exists()


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
    code = "import pair_recon, pair_recon_dbx, pair_recon_gitlab, pair_recon_left, pair_recon_right; print('ok')"
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"
