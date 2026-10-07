from __future__ import annotations

import click
from dotenv import find_dotenv, load_dotenv

from dtrack_left import __version__
from dtrack_left.cli.blog import blog_command, blog_delete_command
from dtrack_left.cli.checks import col_check_command, row_check_command
from dtrack_left.cli.ops import dbx_group
from dtrack_left.cli.options import CONTEXT_SETTINGS, DistributedGroup
from dtrack_left.cli.tools import (
    debug_command,
    doctor_command,
    fetch_report_command,
    rollback_command,
)
from dtrack_local.cli import cli as local_group

AUXILIARY_COMMANDS = (
    fetch_report_command,
    doctor_command,
    rollback_command,
    debug_command,
    blog_command,
    blog_delete_command,
    dbx_group,
)


@click.group("tools", help="operator and diagnostic helpers", context_settings=CONTEXT_SETTINGS)
def tools_group():
    pass


for command in AUXILIARY_COMMANDS:
    tools_group.add_command(command)


@click.group(cls=DistributedGroup, context_settings=CONTEXT_SETTINGS)
@click.version_option(__version__, "-v", "--version", prog_name="dtrack")
def cli():
    load_dotenv(find_dotenv(usecwd=True))


for command in (row_check_command, col_check_command, local_group, tools_group):
    cli.add_command(command)


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
