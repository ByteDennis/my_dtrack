import subprocess
from pathlib import Path

import pytest

from pair_recon_gitlab import dispatch as gitlab
from pair_recon_gitlab import snapshot


def write(path: Path, text="value"):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def source(root: Path):
    write(root / "README.md")
    write(
        root / "pyproject.toml",
        '[project.scripts]\npair_recon = "pair_recon.app.cli:main"\npair_recon_dbx = "pair_recon_dbx.cli:main"\npair_recon_gitlab = "pair_recon_gitlab.cli:main"\n[tool.hatch.build.targets.wheel]\npackages = ["src/pair_recon", "src/pair_recon_dbx", "src/pair_recon_gitlab", "src/pair_recon_left", "src/pair_recon_right"]\n[tool.ruff.lint.per-file-ignores]\n"src/pair_recon_dbx/workspace.py" = ["BLE001"]\n',
    )
    write(root / "pair_recon.ignore", "src/pair_recon_dbx/\nsrc/pair_recon_gitlab/\n")
    write(root / ".gitignore", "ignored.py\n")
    write(root / ".pair_recon.env", "TOKEN=secret\n")
    write(root / "src/pair_recon_left/left.py")
    write(root / "src/pair_recon_left/ignored.py")
    write(root / "src/pair_recon_right/right.py")
    write(root / "src/pair_recon_dbx/dbx.py")
    write(root / "src/pair_recon_gitlab/gitlab.py")
    write(root / "tests/unit/common/test_common.py")
    write(root / "tests/unit/conftest.py")
    write(root / "tests/conftest.py")
    write(root / "tests/support.py")
    return root / "pair_recon.ignore"


@pytest.mark.parametrize(
    ("scope", "present", "absent"),
    [
        ("left", "src/pair_recon_left/left.py", "src/pair_recon_right/right.py"),
        ("right", "src/pair_recon_right/right.py", "src/pair_recon_left/left.py"),
        ("both", "src/pair_recon_left/ignored.py", "src/pair_recon_dbx/dbx.py"),
        ("all", ".gitignore", ".pair_recon.env"),
    ],
)
def test_scopes(tmp_path, scope, present, absent):
    ignore = source(tmp_path)
    result = snapshot.plan(tmp_path, scope, ignore)
    assert present in result["paths"]
    assert absent not in result["paths"]
    assert "pair_recon.ignore" in result["paths"]
    if scope in {"left", "right"}:
        assert "tests/unit/conftest.py" not in result["paths"]
        assert "tests/support.py" in result["paths"]


@pytest.mark.parametrize(
    ("repo", "expected"),
    [
        ("group/project", "git@gitlab.example:group/project.git"),
        ("git@gitlab.example:group/project.git", "git@gitlab.example:group/project.git"),
        ("https://gitlab.example/group/project.git", "https://gitlab.example/group/project.git"),
    ],
)
def test_remotes(repo, expected):
    assert gitlab.remote_url(repo, "gitlab.example") == expected


def test_private():
    result = gitlab.create_project(
        "validation", group="data", description="Pair checks", host="gitlab.example", dry_run=True
    )
    assert result["visibility"] == "private"
    assert result["command"] == [
        "glab",
        "repo",
        "create",
        "validation",
        "--private",
        "--group",
        "data",
        "--description",
        "Pair checks",
    ]


def test_push(tmp_path):
    root = tmp_path / "source"
    root.mkdir()
    ignore = source(root)
    remote = tmp_path / "remote.git"
    subprocess.run(["git", "init", "--bare", str(remote)], check=True, capture_output=True)
    url = remote.as_uri()
    result = gitlab.dispatch(root, url, scope="both", ignore=ignore, branch="validation")
    assert result["state"] == "pushed"
    checkout = tmp_path / "checkout"
    subprocess.run(
        ["git", "clone", "--branch", "validation", url, str(checkout)],
        check=True,
        capture_output=True,
    )
    assert (checkout / "src/pair_recon_left/left.py").is_file()
    assert (checkout / "src/pair_recon_right/right.py").is_file()
    assert not (checkout / "src/pair_recon_dbx").exists()
    assert (checkout / ".pair_recon-dispatch.json").is_file()
    project = (checkout / "pyproject.toml").read_text(encoding="utf-8")
    assert "pair_recon_dbx" not in project
    assert "pair_recon_gitlab" not in project
    write(checkout / "keep.txt")
    subprocess.run(["git", "-C", str(checkout), "config", "user.name", "Test"], check=True)
    subprocess.run(
        ["git", "-C", str(checkout), "config", "user.email", "test@example.com"], check=True
    )
    subprocess.run(["git", "-C", str(checkout), "add", "keep.txt"], check=True)
    subprocess.run(
        ["git", "-C", str(checkout), "commit", "-m", "keep"], check=True, capture_output=True
    )
    subprocess.run(["git", "-C", str(checkout), "push"], check=True, capture_output=True)
    assert (
        gitlab.dispatch(root, url, scope="both", ignore=ignore, branch="validation")["state"]
        == "unchanged"
    )
    assert (checkout / "keep.txt").is_file()


def test_rejects(tmp_path):
    ignore = tmp_path / "pair_recon.ignore"
    write(ignore, "!keep.py\n")
    with pytest.raises(ValueError, match="negation"):
        snapshot.patterns(ignore)
    with pytest.raises(ValueError, match="credentials"):
        gitlab.remote_url("https://token@gitlab.example/group/project.git", "gitlab.example")
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    write(
        checkout / ".pair_recon-dispatch.json",
        '{"files":["../../outside"]}',
    )
    with pytest.raises(ValueError, match="manifest path"):
        snapshot.sync(tmp_path, checkout, [], "both")
