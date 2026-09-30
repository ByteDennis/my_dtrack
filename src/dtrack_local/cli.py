# python -m dtrack_local <command>
#
#   preflight --config c.json [run options]                        both sides reachable?
#   run       --config c.json [run options] [--gate auto|manual|ratio:x]
#             [--report-s3 s3://.../report] [--backup-s3 s3://.../runs] [--include-data]
#
# Run options: --run-id (resume; default <UTC stamp>-<config hash>), --out (.dtrack-local/runs),
#   --profile (corp), --warehouse-id, --disposition external_links|inline, --push-sample,
#   --pair P (repeatable), --log-level.
# Environment (.env works): DTRACK_DBX_PROFILE, DTRACK_DBX_WAREHOUSE_ID, DTRACK_LOCAL_OUT,
#   DTRACK_LOCAL_DISPOSITION, DTRACK_REPORT_S3, DTRACK_LOCAL_BACKUP_S3, Left credentials.
# Roll one pair back: dtrack rollback --run <out>/<run_id> --pair P --stage dead|col|row [--yes]

from __future__ import annotations

import functools

import click
from dotenv import find_dotenv, load_dotenv

from dtrack_left.cli.options import CONTEXT_SETTINGS, emit_json, log_level_option, pair_option

from . import pipeline
from .warehouse import DISPOSITIONS


# >>> where the run lives and how the Right side is reached; shared by every command <<< #
def run_options(func):
    @click.option(
        "--config",
        "config_path",
        required=True,
        type=click.Path(exists=True, dir_okay=False),
        help="pairs config JSON",
    )
    @click.option("--run-id", help="resume this run; default <UTC stamp>-<config hash>")
    @click.option(
        "--out",
        default=".dtrack-local/runs",
        show_default=True,
        envvar="DTRACK_LOCAL_OUT",
        help="local runs root",
    )
    @click.option(
        "--profile", envvar="DTRACK_DBX_PROFILE", help="~/.databrickscfg profile [default: corp]"
    )
    @click.option(
        "--warehouse-id",
        envvar="DTRACK_DBX_WAREHOUSE_ID",
        help="SQL warehouse the Right side is read through",
    )
    @click.option(
        "--disposition",
        type=click.Choice(sorted(DISPOSITIONS)),
        default="external_links",
        show_default=True,
        envvar="DTRACK_LOCAL_DISPOSITION",
        help="inline rows (<=25 MiB per window) or presigned download links",
    )
    @click.option(
        "--push-sample/--no-push-sample",
        default=False,
        show_default=True,
        help="filter the sample in the warehouse too, to pull fewer rows",
    )
    @pair_option
    @functools.wraps(func)
    def wrapper(
        *args,
        config_path,
        run_id,
        out,
        profile,
        warehouse_id,
        disposition,
        push_sample,
        pairs,
        **kwargs,
    ):
        run = pipeline.open_run(
            config_path,
            run_id=run_id,
            out=out,
            profile=profile,
            warehouse_id=warehouse_id,
            disposition=disposition,
            push_sample=push_sample,
            pairs=pairs,
        )
        return func(*args, run=run, **kwargs)

    return wrapper


# >>> python -m dtrack_local: the backup plan, both sides pulled to this machine and compared here <<< #
@click.group(context_settings=CONTEXT_SETTINGS)
def cli():
    load_dotenv(find_dotenv(usecwd=True))


# >>> preflight: both sides reachable, nothing counted <<< #
@cli.command("preflight", help="check both sides are reachable", context_settings=CONTEXT_SETTINGS)
@run_options
@log_level_option
@emit_json
def preflight_command(run):
    health = pipeline.preflight(run)
    ok = all(row["left"].get("ok") and row["right"].get("ok") for row in health.values())
    return {**health, "exit_code": 0 if ok else 1}


# >>> run: preflight, row check, col check, report, then optional report publish and S3 backup <<< #
@cli.command("run", help="the whole comparison on this machine", context_settings=CONTEXT_SETTINGS)
@run_options
@click.option(
    "--gate",
    default="auto",
    show_default=True,
    help="Stage 1 gate: auto | manual | ratio:<max excluded share>",
)
@click.option("--report-s3", envvar="DTRACK_REPORT_S3", help="also publish the report here")
@click.option(
    "--backup-s3", envvar="DTRACK_LOCAL_BACKUP_S3", help="copy the run folder to <this>/<run_id>/"
)
@click.option(
    "--include-data",
    is_flag=True,
    help="with --backup-s3, also upload the pulled rows (_local/data.duckdb)",
)
@log_level_option
@emit_json
def run_command(run, gate, report_s3, backup_s3, include_data):
    outcome = pipeline.run_all(
        run, gate=gate, report_s3=report_s3, backup_s3=backup_s3, include_data=include_data
    )
    return {
        key: outcome[key]
        for key in ("run_id", "folder", "status", "findings", "published", "backup", "landing")
    }


# >>> console entry point with the command's return value as exit code <<< #
def main(argv: list[str] | None = None) -> int:
    try:
        code = cli.main(args=argv, prog_name="python -m dtrack_local", standalone_mode=False)
    except click.exceptions.Exit as exc:
        return exc.exit_code
    except click.ClickException as exc:
        exc.show()
        return exc.exit_code
    except click.Abort:
        click.echo("Aborted!", err=True)
        return 1
    return code if isinstance(code, int) else 0
