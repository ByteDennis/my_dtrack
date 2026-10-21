import json
from pathlib import Path

import pytest

from pair_recon.app import cli as app_cli
from pair_recon_dbx import cli


@pytest.fixture
def calls(monkeypatch):
    values = []
    monkeypatch.setattr(cli.runtime, "client", lambda profile: f"client:{profile}")
    names = (
        "login",
        "compute_status",
        "compute_start",
        "compute_stop",
        "warehouse_status",
        "warehouse_start",
        "warehouse_stop",
        "run_sql",
        "list_runs",
        "run_detail",
    )
    for name in names:
        monkeypatch.setattr(
            cli.runtime,
            name,
            lambda *args, _name=name, **kwargs: (
                values.append((_name, args, kwargs)) or {"called": _name}
            ),
        )
    for name in ("upload", "download", "delete"):
        monkeypatch.setattr(
            cli.workspace_ops,
            name,
            lambda *args, _name=name, **kwargs: (
                values.append((_name, args, kwargs)) or {"called": _name}
            ),
        )
    return values


@pytest.mark.parametrize(
    ("argv", "expected"),
    [
        (["login", "--no-open"], ("login", ("client:corp",), {"open_browser": False})),
        (["compute", "--cluster-id", "c1"], ("compute_status", ("client:corp", "c1"), {})),
        (
            ["compute", "start", "--cluster-id", "c1", "--wait", "--timeout", "5"],
            ("compute_start", ("client:corp", "c1"), {"wait": True, "timeout_minutes": 5}),
        ),
        (
            ["warehouse", "stop", "--warehouse-id", "w1"],
            ("warehouse_stop", ("client:corp", "w1"), {"wait": False, "timeout_minutes": 20}),
        ),
        (
            ["sql", "SELECT 1", "--warehouse-id", "w1", "--limit", "2"],
            ("run_sql", ("client:corp", "SELECT 1", "w1"), {"limit": 2}),
        ),
        (
            ["runs", "--job-id", "7", "--active"],
            ("list_runs", ("client:corp",), {"limit": 10, "job_id": 7, "active": True}),
        ),
        (["runs", "--run-id", "9"], ("run_detail", ("client:corp", 9), {})),
        (
            ["workspace", "upload", "source", "/Shared/source", "--dry-run"],
            (
                "upload",
                ("client:corp", Path("source"), "/Shared/source"),
                {"overwrite": False, "dry_run": True},
            ),
        ),
        (
            ["workspace", "download", "/Shared/source", "local", "--overwrite"],
            (
                "download",
                ("client:corp", "/Shared/source", Path("local")),
                {"overwrite": True, "dry_run": False},
            ),
        ),
        (
            ["workspace", "delete", "old.py", "folder", "-rf", "--dry-run"],
            (
                "delete",
                ("client:corp", ["old.py", "folder"]),
                {"recursive": True, "force": True, "dry_run": True},
            ),
        ),
        (
            ["workspace", "rm", "old.py"],
            (
                "delete",
                ("client:corp", ["old.py"]),
                {"recursive": False, "force": False, "dry_run": False},
            ),
        ),
    ],
)
def test_commands(calls, capsys, argv, expected):
    assert cli.main(argv) == 0
    assert calls == [expected]
    assert json.loads(capsys.readouterr().out) == {"called": expected[0]}


def test_defaults(calls, monkeypatch, tmp_path, capsys):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("DBX_PROFILE", raising=False)
    monkeypatch.delenv("DBX_CLUSTER_ID", raising=False)
    monkeypatch.delenv("DBX_WAREHOUSE_ID", raising=False)
    (tmp_path / ".pair_recon.env").write_text(
        "DBX_PROFILE=team\nDBX_CLUSTER_ID=c9\nDBX_WAREHOUSE_ID=w9\n", encoding="utf-8"
    )
    assert app_cli.main(["dbx", "compute"]) == 0
    assert app_cli.main(["dbx", "warehouse"]) == 0
    assert calls == [
        ("compute_status", ("client:team", "c9"), {}),
        ("warehouse_status", ("client:team", "w9"), {}),
    ]
    assert cli.parser().parse_args(["--debug", "attach", "runs"]).debug == "attach"
    capsys.readouterr()


def test_errors(calls, monkeypatch, capsys):
    monkeypatch.delenv("DBX_CLUSTER_ID", raising=False)
    assert cli.main(["compute"]) == 1
    assert json.loads(capsys.readouterr().out) == {"error": "ValueError: --cluster-id is required"}
