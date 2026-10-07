import json

from dtrack.cli import main
from dtrack_left.cli import tools


def test_unified_cli_has_a_local_workflow_group(capsys):
    assert main(["-h"]) == 0
    root_help = capsys.readouterr().out
    assert all(f"  {name}" in root_help for name in ("row-check", "col-check", "local", "tools"))
    assert "  fetch-report" not in root_help

    assert main(["tools", "-h"]) == 0
    tools_help = capsys.readouterr().out
    for command in ("fetch-report", "materialize", "doctor", "rollback", "debug", "dbx", "blog"):
        assert command in tools_help
    assert "key-check" not in tools_help

    assert main(["local", "-h"]) == 0
    local_help = capsys.readouterr().out
    assert all(command in local_help for command in ("preflight", "run", "key-check"))


def test_auxiliary_commands_are_not_available_at_the_root(capsys):
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


def test_debug_can_invoke_the_root_local_group(monkeypatch, capsys):
    seen = {}

    def run(target, argv, *, attach):
        seen.update(argv=argv, attach=attach)
        return target(argv)

    monkeypatch.setattr(tools, "run_under_debugger", run)
    assert main(["tools", "debug", "--", "local", "-h"]) == 0
    assert seen == {"argv": ["local", "-h"], "attach": None}
    assert "Local execution against both connections" in capsys.readouterr().out


def test_row_and_col_help_show_config_defaults_over_environment(tmp_path, monkeypatch, capsys):
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

    assert main(["col-check", "--config", str(config), "--help"]) == 0
    col_help = " ".join(capsys.readouterr().out.split())
    assert "default: s3://config/cols" in col_help
    assert "default: config-col" in col_help
    assert "default: 3" in col_help
    assert "DTRACK_COL_PULL_WORKERS" in col_help
    assert "DTRACK_COL_WAIT_TIMEOUT" in col_help
    assert "DTRACK_DEBUG" in col_help
    assert "default: s3://env/runs" not in row_help + col_help
