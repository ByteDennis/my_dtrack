from __future__ import annotations

import click

from ..backends.store import validate_run_id
from ..checks.col_check import ColCheckPipeline, LeftRuntime, publish_fixed_keys, wait_for_manifests
from ..checks.row_check import make_run_id, run_row_check
from ..common.audit import prune_audit_history
from ..common.protocol import get_run_status, list_left_documents
from .options import (
    CONTEXT_SETTINGS,
    config_option,
    debug_option,
    emit_json,
    log_level_option,
    pair_option,
    runtime_option,
    store_option,
)


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
@log_level_option
@emit_json
def row_check_command(config, store, run_id, pairs, workers):
    run_id = run_id or make_run_id(config)
    try:
        run_id = validate_run_id(run_id)
    except ValueError as exc:
        raise click.UsageError(str(exc)) from None
    run_store = store.child(run_id)
    run_id, documents = run_row_check(
        config, run_store, run_id=run_id, pair_names=pairs, pair_workers=workers
    )
    prune_audit_history(store, keep=5)
    return {"run_id": run_id, "run": run_store.root_uri, "pairs": len(documents)}


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
    default=2,
    envvar="DTRACK_COL_STAT_WORKERS",
    help="concurrent statistics jobs",
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
@log_level_option
@emit_json
def col_check_command(
    store, run_id, pairs, work_dir, pull_workers, stat_workers, wip, wait_timeout, poll_interval
):
    store, run_id = _active_run(store, run_id)
    manifests = wait_for_manifests(
        store, timeout=wait_timeout, poll_interval=poll_interval, pair_names=pairs
    )
    for manifest in manifests:
        publish_fixed_keys(store, manifest, work_dir)
    runtime = LeftRuntime(work_dir, store)
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
    }
