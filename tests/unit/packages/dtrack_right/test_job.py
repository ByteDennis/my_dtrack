import json

import pytest
from support import pairs_config

from dtrack_left.common.checks.col_check import (
    ColCheckPipeline,
    LeftRuntime,
    publish_fixed_keys,
    wait_for_manifests,
)
from dtrack_left.common.checks.row_check import run_row_check
from dtrack_local.support.csv_target import CsvAnchorTarget
from dtrack_local.support.right_job import main as local_right_main
from dtrack_right.engine.backends.store import VolumeStore
from dtrack_right.engine.checks.row_check import run_row_check as run_right_row_check
from dtrack_right.engine.job import run


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


@pytest.mark.slow
def test_rerun(synthetic, tmp_path):
    runs = tmp_path / "runs"
    store, factory = _published_run(synthetic, runs, tmp_path / "work")
    options = {"poll_interval": 0, "idle_timeout": 5, "wait_timeout": 5}
    first = run(str(runs), factory, out_dir=tmp_path / "o1", **options)
    assert first["results"] == 11 and first["summary"]["status"] == "DONE"
    assert first["audit"] == "_status/audit.sql" and store.exists("_status/audit.sql")
    assert any("/audit/" in key for key in store.list_keys("pairs/"))
    assert store.get_json("report/summary.json")["run_id"] == "r1"
    assert first["published"] == [f"report/{name}" for name in sorted(_report_files())]
    status = store.get_json("_status/run.json")
    assert {key: status[key] for key in ("state", "role", "run_id", "results")} == {
        "state": "complete",
        "role": "right",
        "run_id": "r1",
        "results": 11,
    }
    again = run(str(runs), factory, run_id="r1", out_dir=tmp_path / "o2", **options)
    assert again["results"] == 0 and again["summary"]["units_completed"] == 11
    assert again["published"] == first["published"]
    status = store.get_json("_status/run.json")
    assert status["results"] == 11 and status["new_results"] == 0


def test_missing_counts(tmp_path):
    with pytest.raises(TimeoutError, match="wait_timeout"):
        run(str(tmp_path / "runs"), None, poll_interval=0, wait_timeout=0)


@pytest.mark.slow
def test_csv_launcher(synthetic, tmp_path, capsys):
    runs = tmp_path / "runs"
    _published_run(synthetic, runs, tmp_path / "work", run_id="r7")
    argv = ["--run-s3", str(runs), "--right-csv-dir", str(synthetic.right)]
    argv += ["--out", str(tmp_path / "out"), "--work-dir", str(tmp_path / "rw")]
    argv += ["--poll-interval", "0", "--log-level", "debug"]
    assert local_right_main(argv) == 0
    assert json.loads(capsys.readouterr().out) == {
        "published": [f"report/{name}" for name in sorted(_report_files())],
        "results": 11,
        "status": "DONE",
    }


@pytest.mark.slow
def test_run_selection(synthetic, tmp_path, capsys):
    runs = tmp_path / "runs"
    selected, _ = _published_run(synthetic, runs, tmp_path / "work-r7", run_id="r7")
    other, _ = _published_run(synthetic, runs, tmp_path / "work-r8", run_id="r8")
    argv = [
        "--run-s3",
        str(runs),
        "--run-id",
        "r7",
        "--right-csv-dir",
        str(synthetic.right),
        "--out",
        str(tmp_path / "out"),
        "--work-dir",
        str(tmp_path / "rw"),
        "--poll-interval",
        "0",
    ]
    assert local_right_main(argv) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "DONE"
    assert selected.get_json("_status/run.json")["state"] == "complete"
    assert other.get_json("_status/run.json")["state"] != "complete"


def _report_files():
    return [
        "pairs/customers/col_check.csv",
        "pairs/customers/row_check.csv",
        "index.html",
        "pairs/orders/col_check.csv",
        "pairs/orders/row_check.csv",
        "report.xlsx",
        "summary.json",
    ]
