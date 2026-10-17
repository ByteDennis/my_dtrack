import json
from types import SimpleNamespace as NS

import boto3
import pytest

from dtrack_left.cli import main
from dtrack_left.cli.ops import s3


def run_tool(argv):
    return main(["tools", *argv])


def test_dbx_s3(capsys):
    assert run_tool(["dbx", "-h"]) == 0 and "s3" in capsys.readouterr().out
    assert run_tool(["dbx", "s3", "-h"]) == 0
    out = capsys.readouterr().out
    assert all(command in out for command in ("creds", "winscp", "whoami"))


def test_creds_writes(aws_home, capsys):
    assert run_tool(["dbx", "s3", "creds"]) == 0
    outcome = json.loads(capsys.readouterr().out)
    assert outcome["target"] == "corp-winscp" and "refreshes" not in outcome
    assert boto3.Session(profile_name="corp-winscp").get_credentials().token == "token1"


def test_creds_names(aws_home, monkeypatch, capsys):
    monkeypatch.setenv("DTRACK_AWS_TARGET", "winscp")
    assert run_tool(["dbx", "s3", "creds", "--profile", "corp"]) == 0
    assert json.loads(capsys.readouterr().out)["target"] == "winscp"
    assert "[winscp]" in (aws_home / "credentials").read_text()


def test_creds_watch(aws_home, monkeypatch, capsys):
    monkeypatch.setattr(s3.aws, "watch", lambda *a, **k: 2)
    assert run_tool(["dbx", "s3", "creds", "--watch"]) == 0
    assert json.loads(capsys.readouterr().out)["refreshes"] == 3

    def interrupted(*_a, **_k):
        raise KeyboardInterrupt

    monkeypatch.setattr(s3.aws, "watch", interrupted)
    assert run_tool(["dbx", "s3", "creds", "--watch"]) == 0
    assert json.loads(capsys.readouterr().out)["refreshes"] == 1


def test_creds_errors(aws_home, capsys):
    assert run_tool(["dbx", "s3", "creds", "--target", "corp"]) == 1
    assert "shadow" in json.loads(capsys.readouterr().out)["error"]
    assert run_tool(["dbx", "s3", "creds", "--profile", "nope"]) == 1
    assert "ProfileNotFound" in json.loads(capsys.readouterr().out)["error"]


# >>> a WinSCP stand-in that stays open for `alive` polls, with the refresh loop driven by hand <<< #
@pytest.fixture
def winscp(aws_home, monkeypatch):
    launched = []

    def launch(site):
        polls = iter([None, None, 0])
        launched.append(site)
        return NS(poll=lambda: next(polls))

    def watch(refresh, alive, *, every, first):
        count = 0
        while alive():
            refresh()
            count += 1
        return count

    monkeypatch.setattr(s3.aws, "launch_winscp", launch)
    monkeypatch.setattr(s3.aws, "watch", watch)
    return launched


def test_winscp_refreshes(winscp, monkeypatch, capsys):
    clock = iter([0.0, 3600.0])
    monkeypatch.setattr(s3.time, "monotonic", lambda: next(clock))
    assert run_tool(["dbx", "s3", "winscp", "--site", "prod"]) == 0
    outcome = json.loads(capsys.readouterr().out)
    assert winscp == ["prod"] and outcome == {
        "site": "prod",
        "target": "corp-winscp",
        "refreshes": 3,
    }


def test_winscp_warns(winscp, capsys):
    assert run_tool(["dbx", "s3", "winscp"]) == 0
    assert "creds --watch" in json.loads(capsys.readouterr().out)["note"]


def test_winscp_exe(aws_home, monkeypatch, capsys):
    monkeypatch.setenv("DTRACK_WINSCP", str(aws_home / "missing.exe"))
    monkeypatch.setattr(s3.aws, "WINSCP_PATHS", ())
    monkeypatch.setattr(s3.aws.shutil, "which", lambda name: None)
    assert run_tool(["dbx", "s3", "winscp"]) == 1
    assert "FileNotFoundError" in json.loads(capsys.readouterr().out)["error"]


def test_whoami_prints(aws_home, capsys):
    moto = pytest.importorskip("moto")
    with moto.mock_aws():
        assert run_tool(["dbx", "s3", "whoami"]) == 0
    assert json.loads(capsys.readouterr().out)["profile"] == "corp"
