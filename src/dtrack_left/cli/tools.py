from __future__ import annotations

from pathlib import Path

import click

from ..backends.sources import source_from_config
from ..backends.store import validate_run_id
from ..checks import rollback as rollbacks
from .config import load_config
from .options import CONTEXT_SETTINGS, config_option, emit_json, log_level_option


@click.group(
    "tools",
    help="operator and diagnostic helpers",
    context_settings=CONTEXT_SETTINGS,
)
def tools_group():
    pass


# >>> what the Databricks job will read for a pair; the Left side never connects to it <<< #
def describe_right(spec: dict) -> dict:
    kind = {"volume_raw": "files"}.get(spec.get("kind"), spec.get("kind") or "table")
    if kind == "files":
        target = f"read_files({spec.get('path')}, format={spec.get('format')})"
    elif kind == "query":
        target = " ".join(str(spec.get("query")).split())[:120]
    else:
        macro = str(spec.get("conn_macro") or "")
        prefix = [spec.get("catalog"), spec.get("schema")]
        prefix = prefix if any(prefix) else macro.split(".", 1) if "." in macro else []
        target = ".".join(str(part) for part in [*prefix, spec.get("table")] if part)
    patched = kind == "files" and "{src}" in str(spec.get("query") or "")
    return {"kind": kind, "target": target, "patched": patched}


# >>> dtrack tools doctor: preflight every Left source and show what the Right side will read <<< #
@click.command(
    "doctor",
    help="preflight every Left source; show each pair's Right relation",
    context_settings=CONTEXT_SETTINGS,
)
@config_option
@log_level_option
@emit_json
def doctor_command(config):
    results = {
        name: {
            **source_from_config(
                {
                    **pair["left"],
                    "partition_mode": pair.get("partition_mode", "date"),
                }
            ).preflight(),
            "right": describe_right(pair["right"]),
            "col_type_overrides": dict(pair.get("col_type_overrides") or {}),
        }
        for name, pair in config["pairs"].items()
        if not pair.get("skip")
    }
    return {**results, "exit_code": 0 if all(row["ok"] for row in results.values()) else 1}


# >>> dtrack tools materialize: replace one huge local CSV with verified hash-sorted Parquet <<< #
@click.command(
    "materialize",
    help="convert one pair's local CSV to reusable partitioned Parquet",
    context_settings=CONTEXT_SETTINGS,
)
@click.option(
    "--config",
    "config_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    envvar="DTRACK_CONFIG",
    help="pairs config JSON; updated atomically after validation",
)
@click.option("--pair", "pair_name", required=True, help="one source=file/csv pair")
@click.option(
    "--out",
    type=click.Path(file_okay=False, path_type=Path),
    help="Parquet dataset; default <csv-stem>.dtrack-parquet beside the CSV",
)
@click.option(
    "--partition",
    type=click.Choice(("auto", "day", "month")),
    default="auto",
    show_default=True,
    help="physical partitioning; independent of vintage",
)
@click.option(
    "--delete-source",
    is_flag=True,
    help="delete the CSV only after Parquet, counts, dates, hashes, and config validate",
)
@log_level_option
@emit_json
def materialize_command(config_path, pair_name, out, partition, delete_source):
    from ..backends.materialize import materialize_pair

    try:
        return materialize_pair(
            config_path,
            pair_name,
            output=out,
            partition=partition,
            delete_source=delete_source,
        )
    except (FileExistsError, FileNotFoundError, KeyError, TypeError, ValueError) as exc:
        raise click.UsageError(str(exc)) from None


# >>> dtrack tools rollback: safely reset one, several, or all pairs (dry run unless --yes) <<< #
@click.command(
    "rollback",
    help="delete selected pairs' Stage 1/2 signals so they are produced again; dry run unless --yes",
    context_settings=CONTEXT_SETTINGS,
)
@click.option(
    "--run-s3",
    "runs_uri",
    required=True,
    envvar="DTRACK_RUN_S3",
    help="stable runs root (S3, Volume, or local directory)",
)
@click.option("--run-id", required=True, envvar="DTRACK_RUN_ID", help="exact run to modify")
@click.option(
    "--pair",
    "pairs",
    multiple=True,
    required=True,
    help="pair to roll back (repeatable or comma-separated); use 'all' alone for every run pair",
)
@click.option(
    "--stage",
    type=click.Choice(rollbacks.STAGES),
    required=True,
    help="dead: retry dead units | col: redo Stage 2 | row: redo Stage 1 and 2",
)
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True, dir_okay=False),
    help="col: rebuild manifests; row: immediately rerun Left row-check with this config",
)
@click.option("--yes", "apply", is_flag=True, help="delete; without it only the plan is printed")
@log_level_option
@emit_json
def rollback_command(runs_uri, run_id, pairs, stage, config_path, apply):
    try:
        run_id = validate_run_id(run_id)
    except ValueError as exc:
        raise click.UsageError(str(exc)) from None
    location = runs_uri.rstrip("/\\") + "/" + run_id
    config = load_config(config_path) if config_path else None
    store = rollbacks.open_location(location)
    try:
        outcome = rollbacks.rollback_many(store, pairs, stage, config=config, apply=apply)
    except (KeyError, ValueError) as exc:
        raise click.UsageError(str(exc)) from None
    if not apply:
        outcome["dry_run"] = "nothing deleted; add --yes (stop the Right job first)"
    elif outcome["failed"]:
        outcome["exit_code"] = 1
    return outcome
