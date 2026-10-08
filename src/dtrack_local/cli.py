# python -m dtrack_local <command>
#
#   key-check --config c.json --pair P [--from-date D --to-date D] production sample key parity
#   preflight --config c.json [run options]                        both sides reachable?
#   run       --config c.json [run options] [--gate auto|manual|ratio:x]
#             [--run-s3 s3://.../runs]
#
# Run options: --run-id (resume; default <UTC stamp>-<config hash>), --out (.dtrack-local/runs),
#   --profile (corp), --warehouse-id,
#   --pair P (repeatable), --log-level.
# Environment (.env works): DTRACK_DBX_PROFILE, DTRACK_DBX_WAREHOUSE_ID, DTRACK_LOCAL_OUT,
#   DTRACK_RUN_S3, USERNAME/PASSWORD/AWS_HOST (proxy for Right chunk downloads), Left credentials.
# Roll back: dtrack tools rollback --run-s3 <out> --run-id ID --pair P [--pair Q|--pair all]

from __future__ import annotations

import functools
import json
import logging
import os
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import click
from click.core import ParameterSource
from dotenv import find_dotenv, load_dotenv

from dtrack_left.backends.sources import source_from_config
from dtrack_left.cli.config import load_config
from dtrack_left.cli.options import (
    COMMA_TEXT,
    CONTEXT_SETTINGS,
    SEMICOLON_TEXT,
    ConfigDefaultsGroup,
    debug_option,
    emit_json,
    log_level_option,
    normalized_options,
    pair_option,
    runtime_option,
)
from dtrack_left.common.audit import SqlAudit, attach_sql_audit
from dtrack_right.backends.store import VolumeStore

from . import keycheck, pipeline
from .run_logging import current_error_log, log_exception_once

log = logging.getLogger("dtrack.local")

LOCAL_OPTIONS = {
    "run": {
        "run_id",
        "out",
        "profile",
        "warehouse_id",
        "pairs",
        "gate",
        "run_s3",
        "log_level",
        "debug",
    },
    "preflight": {
        "run_id",
        "out",
        "profile",
        "warehouse_id",
        "pairs",
        "log_level",
        "debug",
    },
    "key-check": {
        "keys",
        "family_values",
        "sample",
        "from_date",
        "to_date",
        "days",
        "out",
        "profile",
        "warehouse_id",
        "max_examples",
        "log_level",
        "debug",
    },
}
LOCAL_ALIASES = {"pair": "pairs", "key": "keys", "family": "family_values"}


def _local_default_map(document: dict) -> dict[str, dict]:
    section = document.get("local") or {}
    if not isinstance(section, dict):
        raise click.UsageError("config local must be an object")
    nested = {name: section.get(name) or {} for name in LOCAL_OPTIONS}
    if any(not isinstance(value, dict) for value in nested.values()):
        raise click.UsageError("config local command sections must be objects")
    flat = {key: value for key, value in section.items() if key not in LOCAL_OPTIONS}
    known = set().union(*LOCAL_OPTIONS.values())
    shared = normalized_options(
        flat,
        allowed=known,
        label="local",
        aliases=LOCAL_ALIASES,
        comma_lists={"pairs"},
        repeatables={"keys", "family_values"},
    )
    result = {}
    for command, allowed in LOCAL_OPTIONS.items():
        command_values = normalized_options(
            nested[command],
            allowed=allowed,
            label=f"local.{command}",
            aliases=LOCAL_ALIASES,
            comma_lists={"pairs"},
            repeatables={"keys", "family_values"},
        )
        result[command] = {
            **{key: value for key, value in shared.items() if key in allowed},
            **command_values,
        }
    return result


class LocalGroup(ConfigDefaultsGroup):
    def config_defaults(self, document: dict) -> dict[str, dict]:
        return _local_default_map(document)


# >>> where the run lives and how the Right side is reached; shared by every command <<< #
def run_options(func):
    @runtime_option(
        "--config",
        "config_path",
        required=True,
        type=click.Path(exists=True, dir_okay=False),
        envvar="DTRACK_CONFIG",
        help="pairs config JSON",
    )
    @runtime_option(
        "--run-id",
        envvar="DTRACK_RUN_ID",
        help="resume this run; default <UTC stamp>-<config hash>",
    )
    @runtime_option(
        "--out",
        default=".dtrack-local/runs",
        envvar="DTRACK_LOCAL_OUT",
        help="local runs root",
    )
    @runtime_option(
        "--profile", envvar="DTRACK_DBX_PROFILE", help="~/.databrickscfg profile [default: corp]"
    )
    @runtime_option(
        "--warehouse-id",
        envvar="DTRACK_DBX_WAREHOUSE_ID",
        help="SQL warehouse the Right side is read through",
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
        pairs,
        **kwargs,
    ):
        run = pipeline.open_run(
            config_path,
            run_id=run_id,
            out=out,
            profile=profile,
            warehouse_id=warehouse_id,
            pairs=pairs,
        )
        return func(*args, run=run, **kwargs)

    return wrapper


# >>> python -m dtrack_local: the backup plan, both sides pulled to this machine and compared here <<< #
@click.group(
    "local",
    cls=LocalGroup,
    help="Local execution against both connections",
    context_settings=CONTEXT_SETTINGS,
)
def cli():
    load_dotenv(find_dotenv(usecwd=True))


def _keycheck_target(pair, work, *, profile, warehouse_id, sql_audit=None):
    from dtrack_left.backends.databricks import DEFAULT_PROFILE, workspace

    client = workspace(profile or os.getenv("DTRACK_DBX_PROFILE") or DEFAULT_PROFILE)
    warehouse = pipeline.Warehouse(
        client,
        warehouse_id or os.getenv("DTRACK_DBX_WAREHOUSE_ID", ""),
        sql_audit=sql_audit,
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


def _keycheck_bounds(
    from_date: str | None, to_date: str | None, pair: dict[str, Any]
) -> tuple[str | None, str | None]:
    """Resolve both bounds from one precedence layer so unrelated windows never get mixed."""
    context = click.get_current_context()
    explicit = {ParameterSource.COMMANDLINE, ParameterSource.DEFAULT_MAP}
    from_source = context.get_parameter_source("from_date")
    to_source = context.get_parameter_source("to_date")
    if from_source in explicit or to_source in explicit:
        return (
            from_date if from_source in explicit else None,
            to_date if to_source in explicit else None,
        )
    pair_bounds = pair.get("fromDate"), pair.get("toDate")
    return pair_bounds if any(pair_bounds) else (from_date, to_date)


def _compact_preflight_health(
    health: dict[str, dict[str, Any]], *, column_limit: int = 20
) -> dict[str, dict[str, Any]]:
    compact = {}
    edge = max(1, column_limit // 2)
    for pair, sides in health.items():
        compact[pair] = {}
        for side, result in sides.items():
            rendered = dict(result)
            columns = rendered.get("columns")
            if isinstance(columns, list) and len(columns) > column_limit:
                omitted = len(columns) - edge * 2
                rendered["column_count"] = len(columns)
                rendered["columns"] = [
                    *columns[:edge],
                    f"... ({omitted} omitted) ...",
                    *columns[-edge:],
                ]
            compact[pair][side] = rendered
    return compact


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
@debug_option
@runtime_option(
    "--config",
    "config_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    envvar="DTRACK_CONFIG",
    help="pairs config JSON",
)
@runtime_option(
    "--pair", "pair_name", required=True, envvar="DTRACK_PAIR", help="one pair to check"
)
@runtime_option(
    "--key",
    "keys",
    multiple=True,
    type=COMMA_TEXT,
    envvar="DTRACK_KEYS",
    help="override Left key columns; repeat or use comma-separated names",
)
@runtime_option(
    "--family",
    "family_values",
    multiple=True,
    type=SEMICOLON_TEXT,
    envvar="DTRACK_KEY_FAMILIES",
    help="override a family as LEFT_KEY=name|JSON (repeatable)",
)
@runtime_option(
    "--sample",
    envvar="DTRACK_SAMPLE",
    help="override pair sample: fraction such as 0.1 or fixed count such as 100",
)
@runtime_option(
    "--from-date", envvar="DTRACK_KEY_CHECK_FROM_DATE", help="first partition, YYYY-MM-DD"
)
@runtime_option(
    "--to-date",
    envvar="DTRACK_KEY_CHECK_TO_DATE",
    help="last partition, YYYY-MM-DD; default yesterday",
)
@runtime_option(
    "--days",
    type=int,
    default=3,
    envvar="DTRACK_KEY_CHECK_DAYS",
    help="window when --from-date is absent",
)
@runtime_option(
    "--out",
    default=".dtrack-local/keycheck",
    envvar="DTRACK_KEY_CHECK_OUT",
    help="artifact root",
)
@runtime_option("--profile", envvar="DTRACK_DBX_PROFILE", help="Databricks profile")
@runtime_option("--warehouse-id", envvar="DTRACK_DBX_WAREHOUSE_ID", help="SQL warehouse id")
@runtime_option(
    "--max-examples",
    type=click.IntRange(min=1),
    default=200,
    envvar="DTRACK_KEY_CHECK_MAX_EXAMPLES",
)
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
    from_date, to_date = _keycheck_bounds(from_date, to_date, pair)
    start, end = _keycheck_window(from_date, to_date, days)
    work = Path(out) / pair_name
    audit_store = VolumeStore(work)
    source = attach_sql_audit(
        source_from_config(pair["left"]), SqlAudit(audit_store, "left", pair=pair_name)
    )
    counts = source.partition_counts(start, end)
    partitions = sorted(counts)
    target = _keycheck_target(
        pair,
        work,
        profile=profile,
        warehouse_id=warehouse_id,
        sql_audit=SqlAudit(audit_store, "right", pair=pair_name),
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
    SqlAudit(audit_store, "key-check").refresh()
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
@debug_option
@run_options
@log_level_option
@emit_json
def preflight_command(run):
    health = pipeline.preflight(run)
    ok = all(row["left"].get("ok") and row["right"].get("ok") for row in health.values())
    return {
        **_compact_preflight_health(health),
        "error_log": str(run.error_log),
        "exit_code": 0 if ok else 1,
    }


# >>> run: preflight, row check, col check, report, then optional S3 backup <<< #
@cli.command("run", help="the whole comparison on this machine", context_settings=CONTEXT_SETTINGS)
@debug_option
@run_options
@runtime_option(
    "--gate",
    default="auto",
    envvar="DTRACK_GATE",
    help="Stage 1 gate: auto | manual | ratio:<max excluded share>",
)
@runtime_option(
    "--run-s3",
    envvar="DTRACK_RUN_S3",
    help="stable runs root; copy this local run to <root>/<run_id>/",
)
@log_level_option
@emit_json
def run_command(run, gate, run_s3):
    outcome = pipeline.run_all(run, gate=gate, run_s3=run_s3)
    return {
        key: outcome[key]
        for key in (
            "run_id",
            "folder",
            "status",
            "findings",
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
