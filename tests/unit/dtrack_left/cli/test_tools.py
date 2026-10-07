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
    for command in ("row-check", "col-check", "tools"):
        assert command in out
    for command in ("fetch-report", "doctor", "rollback", "debug", "dbx"):
        assert f"  {command}" not in out

    assert main(["tools", "-h"]) == 0
    tools_help = capsys.readouterr().out
    for command in ("fetch-report", "doctor", "rollback", "debug", "dbx", "blog"):
        assert command in tools_help


@pytest.mark.parametrize("command", ["fetch-report", "doctor", "rollback", "debug", "dbx", "blog"])
def test_auxiliary_commands_require_the_tools_group(command, capsys):
    assert main([command, "-h"]) == 2
    assert f"No such command '{command}'" in capsys.readouterr().err


def test_doctor_preflights_real_csv_sources_and_prints_one_json_line(config_path, capsys):
    assert main(["tools", "doctor", "--config", str(config_path)]) == 0
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
    assert main(["tools", "doctor", "--config", str(broken)]) == 1
    assert json.loads(capsys.readouterr().out)["orders"]["ok"] is False


def test_usage_errors_exit_two(capsys):
    assert main(["row-check", "--config", "nope.json", "--run-s3", "s3://b/runs"]) == 2
    assert main(["tools", "debug"]) == 2


@pytest.mark.parametrize(
    ("argv", "attach"),
    [
        (["tools", "debug", "--", "tools", "doctor", "--config", "c.json"], None),
        (
            ["tools", "debug", "--attach", "--", "tools", "doctor", "--config", "c.json"],
            5678,
        ),
        (
            [
                "tools",
                "debug",
                "--attach",
                "6000",
                "--",
                "tools",
                "doctor",
                "--config",
                "c.json",
            ],
            6000,
        ),
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
    assert seen == {"rest": ["tools", "doctor", "--config", "c.json"], "attach": attach}


@pytest.mark.parametrize(
    ("debug_options", "attach"),
    [(["--local"], None), (["--attach", "--local"], 5678), (["--attach", "6000", "--local"], 6000)],
)
def test_debug_local_routes_the_local_command_tree_through_the_debugger(
    monkeypatch, debug_options, attach
):
    seen = {}
    monkeypatch.setattr(
        tools,
        "run_under_debugger",
        lambda target, rest, attach: seen.update(target=target, rest=rest, attach=attach),
    )
    argv = ["tools", "debug", *debug_options, "--", "run", "--config", "c.json"]
    assert main(argv) == 0
    assert seen == {
        "target": tools._local_debug_main,
        "rest": ["run", "--config", "c.json"],
        "attach": attach,
    }


def test_local_debug_main_does_not_swallow_runtime_exceptions(monkeypatch):
    from dtrack_local import cli as local_cli

    def fail(**_kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(local_cli.cli, "main", fail)
    with pytest.raises(RuntimeError, match="boom"):
        tools._local_debug_main(["run"])


def test_debug_help_documents_local_and_attach(capsys):
    assert main(["tools", "debug", "-h"]) == 0
    output = capsys.readouterr().out
    assert "--local" in output and "--attach" in output


def test_version_and_abort_map_to_exit_codes(monkeypatch, capsys):
    assert main(["--version"]) == 0 and "dtrack, version" in capsys.readouterr().out
    monkeypatch.setattr(cli_package.cli, "main", lambda **_: (_ for _ in ()).throw(click.Abort()))
    assert main(["tools", "doctor"]) == 1 and "Aborted!" in capsys.readouterr().err


def test_fetch_report_command_downloads_the_latest_run(s3_bucket, tmp_path, capsys):
    import boto3

    client = boto3.client("s3")
    client.put_object(Bucket=s3_bucket, Key="runs/r9/report/index.html", Body=b"<html>")
    client.put_object(
        Bucket=s3_bucket, Key="runs/r9/report/summary.json", Body=b'{"run_id": "r9"}'
    )
    client.put_object(Bucket=s3_bucket, Key="runs/r9/_STATUS.json", Body=b'{"state":"complete"}')
    code = main(
        ["tools", "fetch-report", "--run-s3", f"s3://{s3_bucket}/runs", "--out", str(tmp_path)]
    )
    assert code == 0
    assert json.loads(capsys.readouterr().out) == {
        "files": {
            "index.html": str(tmp_path / "index.html"),
            "summary.json": str(tmp_path / "summary.json"),
        },
        "run_id": "r9",
    }
    nested = tmp_path / "nested"
    code = main(
        [
            "tools",
            "fetch-report",
            "--run-s3",
            f"s3://{s3_bucket}/runs",
            "--run-id",
            "r9",
            "--out",
            str(nested),
        ]
    )
    assert code == 0 and json.loads(capsys.readouterr().out)["run_id"] == "r9"


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


def test_dotenv_feeds_fetch_report_run_and_the_shell_wins(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("DTRACK_RUN_S3", raising=False)
    (tmp_path / ".env").write_text("DTRACK_RUN_S3=missing-runs\n")
    with pytest.raises(FileNotFoundError, match="no completed run"):
        main(["tools", "fetch-report", "--out", str(tmp_path / "out")])
    monkeypatch.setenv("DTRACK_RUN_S3", "also-missing")
    with pytest.raises(FileNotFoundError, match="no completed run"):
        main(["tools", "fetch-report", "--out", str(tmp_path / "out")])
