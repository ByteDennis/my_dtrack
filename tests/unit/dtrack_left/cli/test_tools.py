import json
import sys
from types import SimpleNamespace

import click
import pytest
from support import pairs_config

from dtrack_left import cli as cli_package
from dtrack_left.cli import main, tools


@pytest.fixture
def config_path(synthetic, tmp_path):
    path = tmp_path / "config.json"
    path.write_text(json.dumps(pairs_config(synthetic.left)))
    return path


def test_help_lists_every_command(capsys):
    assert main(["-h"]) == 0
    out = capsys.readouterr().out
    for command in ("row-check", "col-check", "fetch-report", "doctor", "debug"):
        assert command in out


def test_doctor_preflights_real_csv_sources_and_prints_one_json_line(config_path, capsys):
    assert main(["doctor", "--config", str(config_path)]) == 0
    result = json.loads(capsys.readouterr().out)
    assert set(result) == {"orders", "customers"} and all(row["ok"] for row in result.values())
    assert (
        result["orders"]["right"]["kind"] == "table"
        and result["orders"]["col_type_overrides"] == {}
    )


@pytest.mark.parametrize(
    ("spec", "expected"),
    [
        ({"catalog": "c", "schema": "s", "table": "t"}, ("table", "c.s.t", False)),
        ({"conn_macro": "c.s", "table": "t"}, ("table", "c.s.t", False)),
        ({"conn_macro": "dbx_main", "table": "t"}, ("table", "t", False)),
        ({"kind": "query", "query": "SELECT *\n  FROM x"}, ("query", "SELECT * FROM x", False)),
        (
            {
                "kind": "volume_raw",
                "path": "/V/in/",
                "format": "csv",
                "query": "SELECT * FROM {src}",
            },
            ("files", "read_files(/V/in/, format=csv)", True),
        ),
    ],
)
def test_describe_right_names_what_the_job_reads(spec, expected):
    row = tools.describe_right(spec)
    assert (row["kind"], row["target"], row["patched"]) == expected


def test_doctor_fails_with_exit_code_one_when_a_source_is_missing(config_path, tmp_path, capsys):
    config = json.loads(config_path.read_text())
    config["pairs"]["orders"]["left"]["path"] = str(tmp_path / "missing.csv")
    broken = tmp_path / "broken.json"
    broken.write_text(json.dumps(config))
    assert main(["doctor", "--config", str(broken)]) == 1
    assert json.loads(capsys.readouterr().out)["orders"]["ok"] is False


def test_usage_errors_exit_two(capsys):
    assert main(["row-check", "--config", "nope.json", "--s3", "s3://b/p"]) == 2
    assert main(["debug"]) == 2


@pytest.mark.parametrize(
    ("argv", "attach"),
    [
        (["debug", "--", "doctor", "--config", "c.json"], None),
        (["debug", "--attach", "--", "doctor", "--config", "c.json"], 5678),
        (["debug", "--attach", "6000", "--", "doctor", "--config", "c.json"], 6000),
    ],
)
def test_debug_forwards_the_command_and_attach_port(monkeypatch, argv, attach):
    seen = {}
    monkeypatch.setattr(
        tools,
        "run_under_debugger",
        lambda main, rest, attach: seen.update(rest=rest, attach=attach),
    )
    main(argv)
    assert seen == {"rest": ["doctor", "--config", "c.json"], "attach": attach}


def test_version_and_abort_map_to_exit_codes(monkeypatch, capsys):
    assert main(["--version"]) == 0 and "dtrack, version" in capsys.readouterr().out
    monkeypatch.setattr(cli_package.cli, "main", lambda **_: (_ for _ in ()).throw(click.Abort()))
    assert main(["doctor"]) == 1 and "Aborted!" in capsys.readouterr().err


def test_fetch_report_command_downloads_the_latest_run(s3_bucket, tmp_path, capsys):
    import boto3

    client = boto3.client("s3")
    client.put_object(Bucket=s3_bucket, Key="rep/history/r9/index.html", Body=b"<html>")
    client.put_object(Bucket=s3_bucket, Key="rep/latest/pointer.json", Body=b'{"run_id": "r9"}')
    code = main(["fetch-report", "--report-s3", f"s3://{s3_bucket}/rep", "--out", str(tmp_path)])
    assert code == 0
    assert json.loads(capsys.readouterr().out) == {
        "files": {"index.html": str(tmp_path / "index.html")},
        "run_id": "r9",
    }


class FakeDebugger:
    __name__ = "fake_debugger"

    def __init__(self, fail=False):
        self.fail, self.calls = fail, []

    def runcall(self, func, argv):
        self.calls.append(("runcall", argv))
        if self.fail:
            raise RuntimeError("boom")
        return func(argv)

    def post_mortem(self, tb):
        self.calls.append(("post_mortem", tb is not None))


def test_run_under_debugger_uses_ipdb_and_opens_post_mortem_on_errors(monkeypatch):
    good, bad = FakeDebugger(), FakeDebugger(fail=True)
    monkeypatch.setitem(sys.modules, "ipdb", good)
    assert tools.run_under_debugger(lambda argv: 7, ["doctor"]) == 7
    monkeypatch.setitem(sys.modules, "ipdb", bad)
    assert tools.run_under_debugger(lambda argv: 7, ["doctor"]) == 1
    assert bad.calls == [("runcall", ["doctor"]), ("post_mortem", True)]


def test_run_under_debugger_falls_back_to_pdb(monkeypatch):
    monkeypatch.setitem(sys.modules, "ipdb", None)
    monkeypatch.setitem(sys.modules, "pdb", FakeDebugger())
    assert tools.run_under_debugger(lambda argv: 3, []) == 3


def test_attach_waits_for_vs_code_before_running(monkeypatch):
    events = []
    fake = SimpleNamespace(
        listen=lambda address: events.append(("listen", address)),
        wait_for_client=lambda: events.append("wait"),
        breakpoint=lambda: events.append("breakpoint"),
    )
    monkeypatch.setitem(sys.modules, "debugpy", fake)
    assert tools.run_under_debugger(lambda argv: events.append(argv) or 0, ["x"], attach=5678) == 0
    assert events == [("listen", ("0.0.0.0", 5678)), "wait", "breakpoint", ["x"]]


# >>> dtrack dbx: every subcommand reaches its backend function with the resolved ids <<< #
@pytest.fixture
def dbx_calls(monkeypatch):
    calls = []
    monkeypatch.setattr(tools.databricks, "workspace", lambda profile: f"w:{profile}")
    for name in (
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
    ):
        monkeypatch.setattr(
            tools.databricks,
            name,
            lambda *a, _n=name, **k: calls.append((_n, a, k)) or {"called": _n},
        )
    return calls


@pytest.mark.parametrize(
    ("argv", "call"),
    [
        (["dbx", "login", "--no-open"], ("login", ("w:corp",), {"open_browser": False})),
        (["dbx", "compute", "--cluster-id", "c1"], ("compute_status", ("w:corp", "c1"), {})),
        (
            ["dbx", "compute", "start", "--cluster-id", "c1", "--wait", "--timeout", "5"],
            ("compute_start", ("w:corp", "c1"), {"wait": True, "timeout_minutes": 5}),
        ),
        (
            ["dbx", "compute", "stop", "--cluster-id", "c1", "--profile", "work"],
            ("compute_stop", ("w:work", "c1"), {"wait": False, "timeout_minutes": 20}),
        ),
        (["dbx", "warehouse", "--warehouse-id", "wh"], ("warehouse_status", ("w:corp", "wh"), {})),
        (
            ["dbx", "warehouse", "start", "--warehouse-id", "wh"],
            ("warehouse_start", ("w:corp", "wh"), {"wait": False, "timeout_minutes": 20}),
        ),
        (
            ["dbx", "warehouse", "stop", "--warehouse-id", "wh"],
            ("warehouse_stop", ("w:corp", "wh"), {"wait": False, "timeout_minutes": 20}),
        ),
        (
            ["dbx", "sql", "SELECT 1", "--warehouse-id", "wh", "--limit", "3"],
            ("run_sql", ("w:corp", "SELECT 1", "wh"), {"limit": 3}),
        ),
        (
            ["dbx", "runs", "--job-id", "7", "--active"],
            ("list_runs", ("w:corp",), {"limit": 10, "job_id": 7, "active": True}),
        ),
        (["dbx", "runs", "--run-id", "42"], ("run_detail", ("w:corp", 42), {})),
    ],
)
def test_dbx_commands_call_their_backend(dbx_calls, capsys, argv, call):
    assert main(argv) == 0
    assert dbx_calls == [call]
    assert json.loads(capsys.readouterr().out) == {"called": call[0]}


def test_dbx_ids_and_profile_come_from_the_environment(dbx_calls, monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("DTRACK_DBX_PROFILE", "work")
    monkeypatch.setenv("DTRACK_DBX_CLUSTER_ID", "c9")
    monkeypatch.setenv("DTRACK_DBX_WAREHOUSE_ID", "wh9")
    (tmp_path / "q.sql").write_text("SELECT 2")
    assert (
        main(["dbx", "compute"]) == 0
        and main(["dbx", "sql", "--file", str(tmp_path / "q.sql")]) == 0
    )
    assert dbx_calls[0][1] == ("w:work", "c9") and dbx_calls[1][1] == ("w:work", "SELECT 2", "wh9")


def test_dbx_usage_errors_exit_two_and_sdk_errors_exit_one(dbx_calls, monkeypatch, capsys):
    monkeypatch.delenv("DTRACK_DBX_CLUSTER_ID", raising=False)
    monkeypatch.delenv("DTRACK_DBX_WAREHOUSE_ID", raising=False)
    assert main(["dbx", "compute"]) == 2
    assert main(["dbx", "sql", "--warehouse-id", "wh"]) == 2
    capsys.readouterr()

    def missing(*_args, **_kwargs):
        raise OSError("Cluster nope does not exist")

    monkeypatch.setattr(tools.databricks, "compute_status", missing)
    assert main(["dbx", "compute", "--cluster-id", "nope"]) == 1
    assert json.loads(capsys.readouterr().out) == {"error": "OSError: Cluster nope does not exist"}


def test_dbx_reads_its_defaults_from_dotenv(dbx_calls, monkeypatch, tmp_path, capsys):
    monkeypatch.chdir(tmp_path)
    for name in ("DTRACK_DBX_PROFILE", "DTRACK_DBX_CLUSTER_ID"):
        monkeypatch.delenv(name, raising=False)
    (tmp_path / ".env").write_text("DTRACK_DBX_PROFILE=dotenv\nDTRACK_DBX_CLUSTER_ID=c-dotenv\n")
    assert main(["dbx", "compute"]) == 0
    assert dbx_calls == [("compute_status", ("w:dotenv", "c-dotenv"), {})]
    for name in ("DTRACK_DBX_PROFILE", "DTRACK_DBX_CLUSTER_ID"):
        monkeypatch.delenv(name, raising=False)
