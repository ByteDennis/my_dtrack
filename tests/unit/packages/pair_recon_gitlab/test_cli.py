import json
from pathlib import Path

import pytest

from pair_recon.app import cli as app_cli
from pair_recon_gitlab import cli


@pytest.fixture
def calls(monkeypatch):
    values = []
    for name in ("create_project", "dispatch"):
        monkeypatch.setattr(
            cli.operations,
            name,
            lambda *args, _name=name, **kwargs: (
                values.append((_name, args, kwargs)) or {"called": _name}
            ),
        )
    monkeypatch.setattr(
        cli.snapshot,
        "plan",
        lambda *args, **kwargs: values.append(("plan", args, kwargs)) or {"called": "plan"},
    )
    return values


@pytest.mark.parametrize(
    ("argv", "name"),
    [
        (["create", "project", "--group", "team", "--dry-run"], "create_project"),
        (["plan", "--scope", "left"], "plan"),
        (["dispatch", "team/project", "--scope", "right", "--dry-run"], "dispatch"),
    ],
)
def test_commands(calls, capsys, argv, name):
    assert cli.main(argv) == 0
    assert calls[0][0] == name
    assert json.loads(capsys.readouterr().out) == {"called": name}


def test_delegate(calls, capsys, tmp_path):
    assert app_cli.main(["gitlab", "plan", "--root", str(tmp_path)]) == 0
    name, args, _kwargs = calls[0]
    assert name == "plan"
    assert args == (tmp_path, "both", tmp_path / "pair_recon.ignore")
    assert cli.parser().parse_args(["--debug", "attach", "plan"]).debug == "attach"
    assert cli.parser().parse_args(["--debug", "console", "plan"]).debug == "console"
    capsys.readouterr()


def test_errors(monkeypatch, capsys):
    monkeypatch.setattr(
        cli.snapshot, "plan", lambda *_args: (_ for _ in ()).throw(ValueError("bad"))
    )
    assert cli.main(["plan", "--root", str(Path.cwd())]) == 1
    assert json.loads(capsys.readouterr().out) == {"error": "ValueError: bad"}


def test_debug_reraises_errors(monkeypatch):
    def fail(*_args):
        raise ValueError("bad")

    monkeypatch.setattr(cli.snapshot, "plan", fail)
    monkeypatch.setattr(cli, "run_debugger", lambda _mode, callback: callback())

    with pytest.raises(ValueError, match="bad"):
        cli.main(["--debug", "console", "plan", "--root", str(Path.cwd())])


def test_defaults(calls, monkeypatch, tmp_path, capsys):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("GITLAB_HOST", raising=False)
    (tmp_path / ".pair_recon.env").write_text("GITLAB_HOST=gitlab.example\n", encoding="utf-8")
    assert cli.main(["create", "project", "--dry-run"]) == 0
    assert calls[0][2]["host"] == "gitlab.example"
    capsys.readouterr()
