import json
import sys
from types import SimpleNamespace

import click
import pytest
from support import pairs_config

from dtrack_left import cli as cli_package
from dtrack_left.cli import debugging, main, tools
from dtrack_left.cli.options import debug_option


@pytest.fixture
def config_path(synthetic, tmp_path):
    path = tmp_path / "config.json"
    path.write_text(json.dumps(pairs_config(synthetic.left)))
    return path


def test_help_lists(capsys):
    assert main(["-h"]) == 0
    out = capsys.readouterr().out
    for command in ("row-check", "col-check", "tools"):
        assert command in out
    for command in ("materialize", "doctor", "rollback", "dbx"):
        assert f"  {command}" not in out

    assert main(["tools", "-h"]) == 0
    tools_help = capsys.readouterr().out
    for command in ("materialize", "doctor", "rollback", "dbx", "blog"):
        assert command in tools_help
    assert "fetch-report" not in tools_help and "debug" not in tools_help


@pytest.mark.parametrize(
    "command", ["fetch-report", "materialize", "doctor", "rollback", "debug", "dbx", "blog"]
)
def test_auxiliary_commands(command, capsys):
    assert main([command, "-h"]) == 2
    assert f"No such command '{command}'" in capsys.readouterr().err


def test_doctor_preflights(config_path, capsys):
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
def test_describe_right(spec, expected):
    row = tools.describe_right(spec)
    assert (row["kind"], row["target"], row["patched"]) == expected


def test_doctor_fails(config_path, tmp_path, capsys):
    config = json.loads(config_path.read_text())
    config["pairs"]["orders"]["left"]["path"] = str(tmp_path / "missing.csv")
    broken = tmp_path / "broken.json"
    broken.write_text(json.dumps(config))
    assert main(["tools", "doctor", "--config", str(broken)]) == 1
    assert json.loads(capsys.readouterr().out)["orders"]["ok"] is False


def test_usage_errors(capsys):
    assert main(["row-check", "--config", "nope.json", "--run-s3", "s3://b/runs"]) == 2


def test_version_abort(monkeypatch, capsys):
    assert main(["--version"]) == 0 and "dtrack, version" in capsys.readouterr().out
    monkeypatch.setattr(cli_package.cli, "main", lambda **_: (_ for _ in ()).throw(click.Abort()))
    assert main(["tools", "doctor"]) == 1 and "Aborted!" in capsys.readouterr().err


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


def test_run_debugger(monkeypatch):
    good, bad = FakeDebugger(), FakeDebugger(fail=True)
    monkeypatch.setitem(sys.modules, "ipdb", good)
    assert debugging.run_under_debugger(lambda argv: 7, ["doctor"]) == 7
    monkeypatch.setitem(sys.modules, "ipdb", bad)
    assert debugging.run_under_debugger(lambda argv: 7, ["doctor"]) == 1
    assert bad.calls == [("runcall", ["doctor"]), ("post_mortem", True)]


def test_run_requires(monkeypatch):
    monkeypatch.setitem(sys.modules, "ipdb", None)
    with pytest.raises(RuntimeError, match="ipdb is required"):
        debugging.run_under_debugger(lambda argv: 3, [])


def test_attach_waits(monkeypatch):
    events = []
    fake = SimpleNamespace(
        listen=lambda address: events.append(("listen", address)),
        wait_for_client=lambda: events.append("wait"),
        breakpoint=lambda: events.append("breakpoint"),
    )
    monkeypatch.setitem(sys.modules, "debugpy", fake)
    assert (
        debugging.run_under_debugger(lambda argv: events.append(argv) or 0, ["x"], attach=5678) == 0
    )
    assert events == [("listen", ("0.0.0.0", 5678)), "wait", "breakpoint", ["x"]]


@pytest.mark.parametrize(("mode", "port"), [("ipdb", None), ("attach", 5678)])
def test_debug_option(monkeypatch, mode, port):
    seen = []

    @click.command()
    @debug_option
    def command():
        seen.append("command")
        return 7

    def run(target, argv, *, attach):
        seen.append((argv, attach))
        return target(argv)

    monkeypatch.setattr(debugging, "run_under_debugger", run)
    assert command.main(["--debug", mode], standalone_mode=False) == 7
    assert seen == [([], port), "command"]
