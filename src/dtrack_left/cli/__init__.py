from __future__ import annotations

import click

from .. import __version__
from .checks import col_check_command, row_check_command
from .options import CONTEXT_SETTINGS
from .tools import dbx_group, debug_command, doctor_command, fetch_report_command


# >>> dtrack: the Left-side CLI; pipeline commands talk only to Left sources and S3, `dbx` is the operator's Databricks helper <<< #
@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option(__version__, "-v", "--version", prog_name="dtrack")
def cli():
    pass


for command in (
    row_check_command,
    col_check_command,
    fetch_report_command,
    doctor_command,
    debug_command,
    dbx_group,
):
    cli.add_command(command)


# >>> console entry point: click parsing with the command's return value as the exit code <<< #
def main(argv: list[str] | None = None) -> int:
    try:
        code = cli.main(args=argv, prog_name="dtrack", standalone_mode=False)
    except click.exceptions.Exit as exc:
        return exc.exit_code
    except click.ClickException as exc:
        exc.show()
        return exc.exit_code
    except click.Abort:
        click.echo("Aborted!", err=True)
        return 1
    return code if isinstance(code, int) else 0
