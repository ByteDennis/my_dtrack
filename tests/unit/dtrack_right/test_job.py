import json

import pytest
from support import pairs_config

from dtrack_left.checks.col_check import (
    ColCheckPipeline,
    LeftRuntime,
    publish_fixed_keys,
    wait_for_manifests,
)
from dtrack_left.checks.row_check import run_row_check
from dtrack_right.__main__ import main as right_main
from dtrack_right.backends.store import VolumeStore
from dtrack_right.backends.targets import CsvAnchorTarget
from dtrack_right.checks.row_check import run_row_check as run_right_row_check
from dtrack_right.job import run


# >>> a run folder where the Left side has published every unit and the Right side has only reconciled counts <<< #
def _published_run(synthetic, runs, work, run_id="r1"):
    store = VolumeStore(runs / run_id)
    run_row_check(pairs_config(synthetic.left), store, run_id=run_id)
    factory = lambda config: CsvAnchorTarget(config, synthetic.right, work)
    assert run_right_row_check(store, factory) == 2
    manifests = wait_for_manifests(store, timeout=1, poll_interval=0)
    for manifest in manifests:
        publish_fixed_keys(store, manifest, work / "left")
    runtime = LeftRuntime(work / "left", store)
    ColCheckPipeline(
        store, pull_window=runtime.pull_window, compute_stats=runtime.compute_stats
    ).run(manifests)
    return store, factory


def test_run_resolves_the_root_publishes_the_report_and_reruns_without_rework(synthetic, tmp_path):
    runs = tmp_path / "runs"
    store, factory = _published_run(synthetic, runs, tmp_path / "work")
    options = {"poll_interval": 0, "idle_timeout": 5, "wait_timeout": 5}
    first = run(
        str(runs), factory, report=str(tmp_path / "report"), out_dir=tmp_path / "o1", **options
    )
    assert first["results"] == 11 and first["summary"]["status"] == "DONE"
    assert VolumeStore(tmp_path / "report").get_json("latest/pointer.json")["run_id"] == "r1"
    assert store.get_json("_STATUS.json") == {
        "state": "complete",
        "role": "right",
        "run_id": "r1",
        "results": 11,
    }
    again = run(str(runs), factory, run_id="r1", out_dir=tmp_path / "o2", **options)
    assert again["results"] == 0 and again["summary"]["units_completed"] == 11
    assert again["published"] == []


def test_run_gives_up_when_no_left_counts_arrive(tmp_path):
    with pytest.raises(TimeoutError, match="wait_timeout"):
        run(str(tmp_path / "runs"), None, poll_interval=0, wait_timeout=0)


def test_local_entry_point_runs_the_job_with_the_csv_anchor(synthetic, tmp_path, capsys):
    runs = tmp_path / "runs"
    _published_run(synthetic, runs, tmp_path / "work", run_id="r7")
    argv = ["--work", str(runs), "--right-csv-dir", str(synthetic.right)]
    argv += ["--out", str(tmp_path / "out"), "--work-dir", str(tmp_path / "rw")]
    argv += ["--report", str(tmp_path / "report"), "--poll-interval", "0", "--log-level", "debug"]
    assert right_main(argv) == 0
    assert json.loads(capsys.readouterr().out) == {
        "published": [f"history/r7/{name}" for name in sorted(_report_files())]
        + ["latest/pointer.json"],
        "results": 11,
        "status": "DONE",
    }


def _report_files():
    return [
        "customers_compare_col.csv",
        "customers_compare_row.csv",
        "index.html",
        "orders_compare_col.csv",
        "orders_compare_row.csv",
        "report.xlsx",
        "summary.json",
    ]
