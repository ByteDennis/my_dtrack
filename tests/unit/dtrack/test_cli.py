from dtrack.cli import main


def test_unified_tools_group_includes_the_local_key_check(capsys):
    assert main(["-h"]) == 0
    root_help = capsys.readouterr().out
    assert all(f"  {name}" in root_help for name in ("row-check", "col-check", "tools"))
    assert "  fetch-report" not in root_help

    assert main(["tools", "-h"]) == 0
    tools_help = capsys.readouterr().out
    for command in ("fetch-report", "doctor", "rollback", "debug", "dbx", "key-check", "blog"):
        assert command in tools_help


def test_auxiliary_commands_are_not_available_at_the_root(capsys):
    for command in ("fetch-report", "doctor", "rollback", "debug", "dbx", "blog", "key-check"):
        assert main([command, "-h"]) == 2
        assert f"No such command '{command}'" in capsys.readouterr().err
