from __future__ import annotations

import click

from ..checks.col_check import ColCheckPipeline, LeftRuntime, publish_fixed_keys, wait_for_manifests
from ..checks.row_check import run_row_check
from .options import (
    CONTEXT_SETTINGS,
    config_option,
    emit_json,
    log_level_option,
    pair_option,
    store_option,
)


# >>> dtrack row-check: count the Left side per partition and hand off to the Right side <<< #
@click.command(
    "row-check",
    help="count the Left side per partition and hand off to the Right side",
    context_settings=CONTEXT_SETTINGS,
)
@config_option
@store_option()
@click.option("--run-id", help="must equal the last folder of --s3 (the default)")
@pair_option
@click.option("--workers", default=4, show_default=True, help="pairs counted in parallel")
@log_level_option
@emit_json
def row_check_command(config, store, run_id, pairs, workers):
    folder = store.location.prefix.rstrip("/").rsplit("/", 1)[-1]
    if not folder:
        raise click.UsageError("--s3 must name the run folder, e.g. s3://bucket/runs/<run_id>")
    if run_id and run_id != folder:
        raise click.UsageError(f"--run-id {run_id!r} differs from the --s3 run folder {folder!r}")
    run_id, documents = run_row_check(
        config, store, run_id=folder, pair_names=pairs, pair_workers=workers
    )
    return {"run_id": run_id, "pairs": len(documents)}


# >>> dtrack col-check: wait for the Right side's manifests, then publish left-side units <<< #
@click.command(
    "col-check",
    help="wait for the Right side's manifests, then publish left-side statistics units",
    context_settings=CONTEXT_SETTINGS,
)
@store_option()
@pair_option
@click.option(
    "--work-dir",
    default=".dtrack-work/col_check",
    show_default=True,
    type=click.Path(file_okay=False),
    help="scratch space for pulled windows",
)
@click.option("--pull-workers", default=2, show_default=True, help="concurrent window pulls")
@click.option("--stat-workers", default=2, show_default=True, help="concurrent statistics jobs")
@click.option("--wip", default=200, show_default=True, help="max published but unclaimed units")
@click.option(
    "--wait-timeout", default=1800.0, show_default=True, help="seconds to wait for manifests"
)
@click.option("--poll-interval", default=3.0, show_default=True, help="seconds between S3 polls")
@log_level_option
@emit_json
def col_check_command(
    store, pairs, work_dir, pull_workers, stat_workers, wip, wait_timeout, poll_interval
):
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
    return {"published_units": published, "pairs": [m["pair"] for m in manifests]}
