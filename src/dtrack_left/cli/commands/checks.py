from __future__ import annotations

import json
import os
from pathlib import Path

import click

from ...backends.store import S3Store, validate_run_id
from ...common.checks.col_check import (
    ColCheckPipeline,
    LeftRuntime,
    publish_fixed_keys,
    wait_for_manifests,
)
from ...common.checks.rollback import reset_for_rerun
from ...common.checks.row_check import active_pairs, make_run_id, reuse_row_check, run_row_check
from ...common.checks.sas import export_sas_bundle, import_sas_bundle
from ...common.model.audit import prune_audit_history
from ...common.model.protocol import get_run_status, list_left_documents
from ...common.runtime.run_logging import configure_left_logging
from ..options import (
    CONTEXT_SETTINGS,
    config_option,
    debug_option,
    emit_json,
    log_level_option,
    pair_option,
    runtime_option,
    store_option,
)


def _configure_left_run_log(run_id: str) -> Path:
    configured = os.getenv("DTRACK_LEFT_LOG")
    return configure_left_logging(configured or Path(".dtrack-work/logs") / f"{run_id}.log")


def _active_run(root, run_id: str | None):
    if run_id:
        try:
            run_id = validate_run_id(run_id)
        except ValueError as exc:
            raise click.UsageError(str(exc)) from None
        return root.child(run_id), run_id
    candidates = []
    for name in root.list_prefixes():
        store = root.child(name)
        status = get_run_status(store)
        complete = status.get("state") == "complete"
        if not complete and (list_left_documents(store) or status):
            candidates.append(name)
    if not candidates:
        raise click.UsageError("--run-s3 has no unfinished run; pass --run-id")
    resolved = max(candidates)
    return root.child(resolved), resolved


# >>> dtrack row-check: create <run-s3>/<run-id>, count Left partitions, hand off to Right <<< #
@click.command(
    "row-check",
    help="count the Left side per partition and hand off to the Right side",
    context_settings=CONTEXT_SETTINGS,
)
@debug_option
@config_option
@store_option()
@runtime_option(
    "--run-id",
    envvar="DTRACK_RUN_ID",
    help="folder below --run-s3; default <UTC stamp>-<config hash>",
)
@pair_option
@runtime_option(
    "--workers", default=4, envvar="DTRACK_ROW_WORKERS", help="pairs counted in parallel"
)
@runtime_option(
    "--force",
    is_flag=True,
    envvar="DTRACK_FORCE",
    help="discard this run's saved row/column results and recompute them",
)
@log_level_option
@emit_json
def row_check_command(config, store, run_id, pairs, workers, force):
    run_id = run_id or make_run_id(config)
    try:
        run_id = validate_run_id(run_id)
    except ValueError as exc:
        raise click.UsageError(str(exc)) from None
    left_log = _configure_left_run_log(run_id)
    run_store = store.child(run_id)
    selected = list(active_pairs(config, pairs))
    if force:
        reset_for_rerun(run_store, selected, "row")
    reused = (
        None if force else reuse_row_check(config, run_store, run_id=run_id, pair_names=selected)
    )
    if reused is None:
        run_id, documents = run_row_check(
            config,
            run_store,
            run_id=run_id,
            pair_names=selected,
            pair_workers=workers,
        )
    else:
        run_id, documents = reused
    prune_audit_history(store, keep=5)
    return {
        "run_id": run_id,
        "run": run_store.root_uri,
        "pairs": len(documents),
        "left_log": str(left_log),
    }


# >>> dtrack col-check: wait for the Right side's manifests, then publish left-side units <<< #
@click.command(
    "col-check",
    help="wait for the Right side's manifests, then publish left-side statistics units",
    context_settings=CONTEXT_SETTINGS,
)
@debug_option
@runtime_option(
    "--config",
    "config_path",
    type=click.Path(exists=True, dir_okay=False),
    envvar="DTRACK_CONFIG",
    expose_value=False,
    help="pairs config JSON; used here for command defaults",
)
@store_option()
@runtime_option(
    "--run-id", envvar="DTRACK_RUN_ID", help="run below --run-s3; default newest unfinished"
)
@pair_option
@runtime_option(
    "--work-dir",
    default=".dtrack-work/col_check",
    envvar="DTRACK_COL_WORK_DIR",
    type=click.Path(file_okay=False),
    help="scratch space for pulled windows",
)
@runtime_option(
    "--pull-workers", default=2, envvar="DTRACK_COL_PULL_WORKERS", help="concurrent window pulls"
)
@runtime_option(
    "--stat-workers",
    default=1,
    envvar="DTRACK_COL_STAT_WORKERS",
    help="compatibility option; vintages run serially (use DTRACK_DUCKDB_WORKERS for shards)",
)
@runtime_option(
    "--wip", default=200, envvar="DTRACK_COL_WIP", help="max published but unclaimed units"
)
@runtime_option(
    "--wait-timeout",
    default=1800.0,
    envvar="DTRACK_COL_WAIT_TIMEOUT",
    help="seconds to wait for manifests",
)
@runtime_option(
    "--poll-interval",
    default=3.0,
    envvar="DTRACK_COL_POLL_INTERVAL",
    help="seconds between S3 polls",
)
@runtime_option(
    "--force",
    is_flag=True,
    envvar="DTRACK_FORCE",
    help="discard this run's saved column results and recompute them",
)
@log_level_option
@emit_json
def col_check_command(
    store,
    run_id,
    pairs,
    work_dir,
    pull_workers,
    stat_workers,
    wip,
    wait_timeout,
    poll_interval,
    force,
):
    store, run_id = _active_run(store, run_id)
    left_log = _configure_left_run_log(run_id)
    manifests = wait_for_manifests(
        store, timeout=wait_timeout, poll_interval=poll_interval, pair_names=pairs
    )
    if force:
        reset_for_rerun(store, [manifest["pair"] for manifest in manifests], "col")
    for manifest in manifests:
        publish_fixed_keys(store, manifest, work_dir)
    runtime = LeftRuntime(work_dir, store, force=force)
    published = ColCheckPipeline(
        store,
        pull_window=runtime.pull_window,
        compute_stats=runtime.compute_stats,
        pull_workers=pull_workers,
        stat_workers=stat_workers,
        wip=wip,
    ).run(manifests)
    return {
        "run_id": run_id,
        "run": store.root_uri,
        "published_units": published,
        "pairs": [m["pair"] for m in manifests],
        "left_log": str(left_log),
    }


# >>> dtrack left-run: version 2 count + plan + statistics in one on-prem invocation <<< #
@click.command(
    "left-run",
    help="run version 2 Left counts and statistics without waiting for the Right side",
    context_settings=CONTEXT_SETTINGS,
)
@debug_option
@config_option
@runtime_option(
    "--run-s3",
    "store_uri",
    envvar="DTRACK_RUN_S3",
    help="runs root, s3://bucket/prefix/runs (run_id is appended)",
)
@runtime_option("--run-id", envvar="DTRACK_RUN_ID", help="folder below --run-s3")
@pair_option
@runtime_option(
    "--export-sas",
    type=click.Path(file_okay=False),
    help="write a portable SAS bundle directory instead of connecting to Oracle",
)
@runtime_option(
    "--import-sas",
    type=click.Path(exists=True, file_okay=False),
    help="validate a completed SAS bundle, compute Left statistics, and upload them",
)
@runtime_option(
    "--workers", default=4, envvar="DTRACK_ROW_WORKERS", help="pairs counted in parallel"
)
@runtime_option(
    "--work-dir",
    default=".dtrack-work/col_check",
    envvar="DTRACK_COL_WORK_DIR",
    type=click.Path(file_okay=False),
    help="scratch space for pulled windows",
)
@runtime_option(
    "--pull-workers", default=1, envvar="DTRACK_COL_PULL_WORKERS", help="concurrent window pulls"
)
@runtime_option(
    "--stat-workers",
    default=1,
    envvar="DTRACK_COL_STAT_WORKERS",
    help="compatibility option; vintages run serially (use DTRACK_DUCKDB_WORKERS for shards)",
)
@runtime_option(
    "--wip", default=200, envvar="DTRACK_COL_WIP", help="max published but unclaimed units"
)
@runtime_option(
    "--force",
    is_flag=True,
    envvar="DTRACK_FORCE",
    help="discard this run's saved results and recompute every Left stage",
)
@log_level_option
@emit_json
def left_run_command(
    config,
    store_uri,
    run_id,
    pairs,
    export_sas,
    import_sas,
    workers,
    work_dir,
    pull_workers,
    stat_workers,
    wip,
    force,
):
    if int(config.get("version") or 1) != 2:
        raise click.UsageError("left-run requires top-level config version: 2")
    if export_sas and import_sas:
        raise click.UsageError("--export-sas and --import-sas are mutually exclusive")
    if export_sas and force:
        raise click.UsageError("--force applies to saved run results, not --export-sas bundles")
    if import_sas and not run_id:
        try:
            run_id = json.loads((Path(import_sas) / "request.json").read_text(encoding="utf-8"))[
                "run_id"
            ]
        except (OSError, KeyError, json.JSONDecodeError) as exc:
            raise click.UsageError(f"cannot read SAS bundle request.json: {exc}") from None
    run_id = run_id or make_run_id(config)
    try:
        run_id = validate_run_id(run_id)
    except ValueError as exc:
        raise click.UsageError(str(exc)) from None
    left_log = _configure_left_run_log(run_id)
    if export_sas:
        return {
            "mode": "export-sas",
            "left_log": str(left_log),
            **export_sas_bundle(config, export_sas, run_id=run_id, pair_names=pairs),
        }
    if not store_uri:
        raise click.UsageError("--run-s3 is required unless --export-sas is used")
    store = S3Store(store_uri)
    run_store = store.child(run_id)
    selected = list(active_pairs(config, pairs))
    if force:
        reset_for_rerun(run_store, selected, "row")
    if import_sas:
        outcome = import_sas_bundle(
            config,
            import_sas,
            run_store,
            run_id=run_id,
            pair_names=selected,
            pull_workers=pull_workers,
            stat_workers=stat_workers,
            wip=wip,
            work_dir=work_dir,
        )
        prune_audit_history(store, keep=5)
        return {
            "mode": "import-sas",
            "run": run_store.root_uri,
            "left_log": str(left_log),
            **outcome,
        }
    reused = (
        None if force else reuse_row_check(config, run_store, run_id=run_id, pair_names=selected)
    )
    if reused is None:
        run_id, documents = run_row_check(
            config,
            run_store,
            run_id=run_id,
            pair_names=selected,
            pair_workers=workers,
        )
    else:
        run_id, documents = reused
    manifests = wait_for_manifests(run_store, timeout=0, poll_interval=0, pair_names=selected)
    for manifest in manifests:
        publish_fixed_keys(run_store, manifest, work_dir)
    runtime = LeftRuntime(work_dir, run_store, force=force)
    published = ColCheckPipeline(
        run_store,
        pull_window=runtime.pull_window,
        compute_stats=runtime.compute_stats,
        pull_workers=pull_workers,
        stat_workers=stat_workers,
        wip=wip,
    ).run(manifests)
    prune_audit_history(store, keep=5)
    return {
        "run_id": run_id,
        "run": run_store.root_uri,
        "pairs": [document["pair"] for document in documents],
        "published_units": published,
        "left_log": str(left_log),
    }
