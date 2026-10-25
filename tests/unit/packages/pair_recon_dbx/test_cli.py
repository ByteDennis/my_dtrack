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
        "resolve_job_id",
    )
    for name in names:
        monkeypatch.setattr(
            cli.runtime,
            name,
            lambda *args, _name=name, **kwargs: (
                values.append((_name, args, kwargs)) or {"called": _name}
            ),
        )
    for name in ("upload", "download", "ls", "copy", "move", "rm"):
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
            ["runs", "--job-id", "7", "--active"],
            ("list_runs", ("client:corp",), {"limit": 10, "job_id": 7, "active": True}),
        ),
        (
            ["runs", "named-job"],
            ("list_runs", ("client:corp",), {"limit": 10, "job_id": 42, "active": False}),
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
            ["workspace", "ls", "folder", "-R"],
            (
                "ls",
                ("client:corp", "folder"),
                {"recursive": True},
            ),
        ),
        (
            ["workspace", "cp", "old.py", "new.py", "-f", "--dry-run"],
            (
                "copy",
                ("client:corp", "old.py", "new.py"),
                {"recursive": False, "force": True, "dry_run": True},
            ),
        ),
        (
            ["workspace", "mv", "old.py", "new.py"],
            (
                "move",
                ("client:corp", "old.py", "new.py"),
                {"force": False, "dry_run": False},
            ),
        ),
        (
            ["workspace", "rm", "old.py"],
            (
                "rm",
                ("client:corp", ["old.py"]),
                {"recursive": False, "force": False, "dry_run": False},
            ),
        ),
    ],
)
def test_commands(calls, capsys, monkeypatch, argv, expected):
    if argv == ["runs", "named-job"]:
        monkeypatch.setattr(cli.runtime, "resolve_job_id", lambda _workspace, _reference: 42)
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
    assert cli.parser().parse_args(["--debug", "console", "runs"]).debug == "console"
    capsys.readouterr()


def test_runs_uses_saved_right_job_id(calls, monkeypatch, tmp_path, capsys):
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".pair_recon.yaml").write_text("right_job_id: '314'\n", encoding="utf-8")
    monkeypatch.setattr(cli.runtime, "resolve_job_id", lambda _workspace, value: int(value))

    assert cli.main(["runs"]) == 0

    assert calls == [("list_runs", ("client:corp",), {"limit": 10, "job_id": 314, "active": False})]
    capsys.readouterr()


def test_runs_explicit_job_id_wins_over_saved_default(calls, monkeypatch, tmp_path, capsys):
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".pair_recon.yaml").write_text("right_job_id: '314'\n", encoding="utf-8")

    assert cli.main(["runs", "--job-id", "7"]) == 0

    assert calls == [("list_runs", ("client:corp",), {"limit": 10, "job_id": 7, "active": False})]
    capsys.readouterr()


def test_cache_pair_shorthand_and_yaml_default(calls, monkeypatch, tmp_path, capsys):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DBX_WAREHOUSE_ID", "w1")
    monkeypatch.setenv("DBX_PAIR_TABLE", "main.recon.pair_recon_pair_meta")
    (tmp_path / ".pair_recon.yaml").write_text("pair: YAML_PAIR\n", encoding="utf-8")
    seen = []
    monkeypatch.setattr(
        cli.cache_ops,
        "explain",
        lambda *args: seen.append(args) or {"pair": args[-1]},
    )

    assert cli.main(["cache", "ORDERS"]) == 0
    assert cli.main(["cache"]) == 0

    assert [call[-1] for call in seen] == ["ORDERS", "YAML_PAIR"]
    assert all(call[1:3] == ("w1", "main.recon.pair_recon_cache_index") for call in seen)
    capsys.readouterr()


def test_cache_show_and_peek(calls, monkeypatch, capsys):
    monkeypatch.setenv("DBX_WAREHOUSE_ID", "w1")
    monkeypatch.setenv("DBX_PAIR_TABLE", "main.recon.pair_recon_pair_meta")
    seen = []
    monkeypatch.setattr(
        cli.cache_ops,
        "show",
        lambda *args: seen.append(("show", args)) or {"cache_key": args[-1]},
    )
    monkeypatch.setattr(
        cli.cache_ops,
        "peek",
        lambda *args, **kwargs: (
            seen.append(("peek", args, kwargs))
            or {
                "cache": {
                    "cache_key": args[-1],
                    "cache_type": "statistics",
                    "format": "json",
                    "uri": "/Volumes/cache.json",
                },
                "preview": {"ok": True},
            }
        ),
    )

    assert cli.main(["cache", "show", "abc"]) == 0
    assert cli.main(["cache", "peek", "def", "--limit", "3"]) == 0

    assert seen[0][0] == "show" and seen[0][1][-1] == "abc"
    assert seen[1][0] == "peek" and seen[1][1][-1] == "def"
    assert seen[1][2] == {"limit": 3}
    capsys.readouterr()


def test_errors(calls, monkeypatch, capsys):
    monkeypatch.delenv("DBX_CLUSTER_ID", raising=False)
    assert cli.main(["compute"]) == 1
    assert json.loads(capsys.readouterr().out) == {"error": "ValueError: --cluster-id is required"}


def test_workspace_delete_command_is_removed():
    with pytest.raises(SystemExit):
        cli.parser().parse_args(["workspace", "delete", "old.py"])


def test_sql_unity_uses_environment_and_prints_table(calls, monkeypatch, capsys):
    seen = {}
    monkeypatch.setenv("DBX_PROFILE", "team")
    monkeypatch.setenv("DBX_WAREHOUSE_ID", "warehouse-1")

    def run_sql(*args, **kwargs):
        seen.update(args=args, kwargs=kwargs)
        return {
            "statement_id": "statement-1",
            "source": "unity",
            "columns": ["id", "name"],
            "rows": [["1", "order"]],
            "total_rows": 1,
        }

    monkeypatch.setattr(cli.runtime, "run_sql", run_sql)

    assert cli.main(["sql", "--unity", "main.sales.orders"]) == 0
    assert seen == {
        "args": (
            "client:team",
            "SELECT * FROM `main`.`sales`.`orders` LIMIT 5",
            "warehouse-1",
        ),
        "kwargs": {"limit": 5, "catalog": None, "source": "unity"},
    }
    output = capsys.readouterr().out
    assert "| id | name  |" in output
    assert "1 row(s) | unity | statement-1" in output
    assert not output.lstrip().startswith("{")


def test_sql_glue_prefixes_hive_metastore(calls, monkeypatch):
    monkeypatch.setenv("DBX_WAREHOUSE_ID", "warehouse-1")
    args = cli.parser().parse_args(["sql", "--glue", "sales.orders", "--limit", "2"])

    cli.execute(args)

    assert calls[-1] == (
        "run_sql",
        (
            "client:corp",
            "SELECT * FROM `hive_metastore`.`sales`.`orders` LIMIT 2",
            "warehouse-1",
        ),
        {"limit": 2, "catalog": "hive_metastore", "source": "glue"},
    )


@pytest.mark.parametrize("removed", ["--profile", "--file", "--warehouse-id"])
def test_sql_removed_options_are_rejected(removed):
    with pytest.raises(SystemExit):
        cli.parser().parse_args(["sql", "--unity", "main.sales.orders", removed, "value"])


def test_debug_reraises_errors(calls, monkeypatch):
    monkeypatch.delenv("DBX_CLUSTER_ID", raising=False)
    monkeypatch.setattr(cli, "run_debugger", lambda _mode, callback: callback())

    with pytest.raises(ValueError, match="--cluster-id is required"):
        cli.main(["--debug", "console", "compute"])
