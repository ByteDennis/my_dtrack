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
    assert project["dependency-groups"]["publish"] == [
        "pyinstaller>=6.22,<7",
        "pyodbc>=5.0; sys_platform == 'win32'",
    ]


def test_portable_publish_recipe():
    recipe = (ROOT / "justfile").read_text(encoding="utf-8")
    script = (ROOT / "tools/publish_portable.py").read_text(encoding="utf-8")
    assert "publish *args:" in recipe and "tools/publish_portable.py" in recipe
    assert "--isolated --locked --no-dev" in recipe
    assert "--onedir" in script and "archive_bundle" in script
    assert "tempfile.mkdtemp" in script and "temporary.replace(target)" in script
    assert "--exclude-module=pair_recon_dbx" in script
    assert "--exclude-module=pair_recon_gitlab" in script
    assert "README.md" not in script and "setup/schema.sql" not in script


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
