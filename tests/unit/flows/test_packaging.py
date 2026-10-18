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
    assert project["project"]["scripts"] == {"pair_recon": "pair_recon.app.cli:main"}
    assert project["tool"]["hatch"]["build"]["targets"]["wheel"]["packages"] == [
        "src/pair_recon",
        "src/pair_recon_left",
        "src/pair_recon_right",
    ]


def test_imports():
    code = "import pair_recon, pair_recon_left, pair_recon_right; print('ok')"
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"
