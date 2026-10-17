import json

from dtrack.cli import main


def test_local_group(capsys):
    assert main(["-h"]) == 0
    root_help = capsys.readouterr().out
    assert all(
        f"  {name}" in root_help
        for name in ("left-run", "row-check", "col-check", "local", "tools")
    )
    assert "  fetch-report" not in root_help

    assert main(["tools", "-h"]) == 0
    tools_help = capsys.readouterr().out
    for command in ("materialize", "doctor", "rollback", "dbx", "blog"):
        assert command in tools_help
    assert all(command not in tools_help for command in ("fetch-report", "debug", "key-check"))

    assert main(["local", "-h"]) == 0
    local_help = capsys.readouterr().out
    assert all(command in local_help for command in ("preflight", "run", "key-check"))


def test_root_commands(capsys):
    for command in (
        "fetch-report",
        "materialize",
        "doctor",
        "rollback",
        "debug",
        "dbx",
        "blog",
        "key-check",
    ):
        assert main([command, "-h"]) == 2
        assert f"No such command '{command}'" in capsys.readouterr().err


def test_help_defaults(tmp_path, monkeypatch, capsys):
    config = tmp_path / "config.json"
    config.write_text(
        json.dumps(
            {
                "row-check": {
                    "run-s3": "s3://config/rows",
                    "run-id": "config-row",
                    "workers": 7,
                },
                "col-check": {
                    "run-s3": "s3://config/cols",
                    "run-id": "config-col",
                    "pull-workers": 3,
                },
            }
        )
    )
    monkeypatch.setenv("DTRACK_RUN_S3", "s3://env/runs")
    monkeypatch.setenv("DTRACK_RUN_ID", "env-run")

    assert main(["row-check", "--config", str(config), "--help"]) == 0
    row_help = " ".join(capsys.readouterr().out.split())
    assert "default: s3://config/rows" in row_help
    assert "default: config-row" in row_help
    assert "default: 7" in row_help
    assert "DTRACK_ROW_WORKERS" in row_help
    assert "DTRACK_PAIRS" in row_help
    assert "DTRACK_DEBUG" in row_help
    assert "--force" in row_help and "DTRACK_FORCE" in row_help

    assert main(["col-check", "--config", str(config), "--help"]) == 0
    col_help = " ".join(capsys.readouterr().out.split())
    assert "default: s3://config/cols" in col_help
    assert "default: config-col" in col_help
    assert "default: 3" in col_help
    assert "DTRACK_COL_PULL_WORKERS" in col_help
    assert "DTRACK_COL_WAIT_TIMEOUT" in col_help
    assert "DTRACK_DEBUG" in col_help
    assert "--force" in col_help and "DTRACK_FORCE" in col_help
    assert "default: s3://env/runs" not in row_help + col_help
