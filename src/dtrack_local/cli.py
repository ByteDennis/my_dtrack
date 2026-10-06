# python -m dtrack_local <command>
#
#   key-check --config c.json --pair P [--from-date D --to-date D] production sample key parity
#   preflight --config c.json [run options]                        both sides reachable?
#   run       --config c.json [run options] [--gate auto|manual|ratio:x]
#             [--report-s3 s3://.../report] [--backup-s3 s3://.../runs] [--include-data]
#
# Run options: --run-id (resume; default <UTC stamp>-<config hash>), --out (.dtrack-local/runs),
#   --profile (corp), --warehouse-id, --disposition external_links|inline, --no-push-sample,
#   --pair P (repeatable), --log-level.
# Environment (.env works): DTRACK_DBX_PROFILE, DTRACK_DBX_WAREHOUSE_ID, DTRACK_LOCAL_OUT,
#   DTRACK_LOCAL_DISPOSITION, DTRACK_REPORT_S3, DTRACK_LOCAL_BACKUP_S3, Left credentials.
# Roll one pair back: dtrack tools rollback --run <out>/<run_id> --pair P --stage dead|col|row [--yes]

from __future__ import annotations

import functools
import json
import logging
import os
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import click
from dotenv import find_dotenv, load_dotenv

from dtrack_left.backends.sources import source_from_config
from dtrack_left.cli.config import load_config
from dtrack_left.cli.options import CONTEXT_SETTINGS, emit_json, log_level_option, pair_option

from . import keycheck, pipeline
from .run_logging import current_error_log, log_exception_once
from .warehouse import DISPOSITIONS

log = logging.getLogger("dtrack.local")


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
        default=True,
        show_default=True,
        help="filter the sample in the warehouse; disable only to retain the full Right window",
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


def _keycheck_target(pair, work, *, profile, warehouse_id, disposition):
    from dtrack_left.backends.databricks import DEFAULT_PROFILE, workspace

    client = workspace(profile or os.getenv("DTRACK_DBX_PROFILE") or DEFAULT_PROFILE)
    warehouse = pipeline.Warehouse(
        client,
        warehouse_id or os.getenv("DTRACK_DBX_WAREHOUSE_ID", ""),
        disposition=disposition,
    )
    return pipeline.WarehouseTarget(warehouse, pair["right"], work)


def _keycheck_window(from_date: str | None, to_date: str | None, days: int) -> tuple[str, str]:
    if days < 1:
        raise click.BadParameter("must be positive", param_hint="--days")
    end = (
        date.fromisoformat(to_date)
        if to_date
        else datetime.now(UTC).astimezone().date() - timedelta(days=1)
    )
    start = date.fromisoformat(from_date) if from_date else end - timedelta(days=days - 1)
    if start > end:
        raise click.BadParameter("must not be after --to-date", param_hint="--from-date")
    return start.isoformat(), end.isoformat()


def _key_overrides(values: tuple[str, ...]) -> list[str]:
    return [item.strip() for value in values for item in value.split(",") if item.strip()]


def _family_overrides(values: tuple[str, ...]) -> dict[str, object]:
    families = {}
    for value in values:
        if "=" not in value:
            raise click.BadParameter("use LEFT_KEY=FAMILY", param_hint="--family")
        key, spec = (part.strip() for part in value.split("=", 1))
        if not key or not spec:
            raise click.BadParameter("use LEFT_KEY=FAMILY", param_hint="--family")
        if spec.startswith(("[", "{")):
            try:
                spec = json.loads(spec)
            except json.JSONDecodeError as exc:
                raise click.BadParameter(str(exc), param_hint="--family") from None
        families[key] = spec
    return families


# >>> compare the keys selected by the pair's real sample predicate on Left and Databricks Right <<< #
@cli.command(
    "key-check",
    help="verify that the production sample selects the same keys on both sides",
    context_settings=CONTEXT_SETTINGS,
)
@click.option(
    "--config",
    "config_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="pairs config JSON",
)
@click.option("--pair", "pair_name", required=True, help="one pair to check")
@click.option(
    "--key",
    "keys",
    multiple=True,
    help="override Left key columns; repeat or use comma-separated names",
)
@click.option(
    "--family",
    "family_values",
    multiple=True,
    help="override a family as LEFT_KEY=name|JSON (repeatable)",
)
@click.option(
    "--sample", help="override pair sample: fraction such as 0.1 or fixed count such as 100"
)
@click.option("--from-date", help="first partition, YYYY-MM-DD")
@click.option("--to-date", help="last partition, YYYY-MM-DD; default yesterday")
@click.option(
    "--days", type=int, default=3, show_default=True, help="window when --from-date is absent"
)
@click.option("--out", default=".dtrack-local/keycheck", show_default=True, help="artifact root")
@click.option("--profile", envvar="DTRACK_DBX_PROFILE", help="Databricks profile")
@click.option("--warehouse-id", envvar="DTRACK_DBX_WAREHOUSE_ID", help="SQL warehouse id")
@click.option(
    "--disposition",
    type=click.Choice(sorted(DISPOSITIONS)),
    default="external_links",
    show_default=True,
)
@click.option("--max-examples", type=click.IntRange(min=1), default=200, show_default=True)
@log_level_option
@emit_json
def key_check_command(
    config_path,
    pair_name,
    keys,
    family_values,
    sample,
    from_date,
    to_date,
    days,
    out,
    profile,
    warehouse_id,
    disposition,
    max_examples,
):
    config = load_config(config_path)
    if pair_name not in config["pairs"]:
        raise click.BadParameter(f"unknown pair {pair_name!r}", param_hint="--pair")
    pair = {**config["pairs"][pair_name]}
    selected_keys = _key_overrides(keys)
    if selected_keys:
        pair["key"] = ",".join(selected_keys)
    family_overrides = _family_overrides(family_values)
    active_keys = _key_overrides((str(pair.get("key") or ""),))
    unknown = sorted(set(family_overrides) - set(active_keys))
    if unknown:
        raise click.BadParameter(
            "families name non-key columns: " + ", ".join(unknown), param_hint="--family"
        )
    if family_overrides:
        pair["key_families"] = {**(pair.get("key_families") or {}), **family_overrides}
    if sample is not None:
        pair["sample"] = sample
    start, end = _keycheck_window(from_date, to_date, days)
    source = source_from_config(pair["left"])
    counts = source.partition_counts(start, end)
    partitions = sorted(counts)
    work = Path(out) / pair_name
    target = _keycheck_target(
        pair, work, profile=profile, warehouse_id=warehouse_id, disposition=disposition
    )
    check = keycheck.run_sample_check(
        pair_name,
        pair,
        source,
        target,
        partitions,
        work,
        window=(start, end),
        max_examples=max_examples,
    )
    summary = check.summary()
    return {
        **summary,
        "partitions": partitions,
        "left window rows": sum(counts.values()),
        "sample": check.sample,
        "selected keys": check.selected_keys,
        "artifacts": {name: str(Path(path).resolve()) for name, path in check.artifacts.items()},
        "exit_code": 0 if summary["ok"] else 1,
    }


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
        for key in (
            "run_id",
            "folder",
            "status",
            "findings",
            "published",
            "backup",
            "landing",
            "error_log",
        )
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
    except Exception as exc:  # noqa: BLE001 - CLI boundary writes the traceback to the run log
        log_exception_once(log, "local command failed", exc)
        path = current_error_log()
        click.echo(f"Command failed; details: {path}" if path else "Command failed", err=True)
        return 1
    return code if isinstance(code, int) else 0
