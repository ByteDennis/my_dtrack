import json
from pathlib import Path

import pytest
from support import pairs_config, synthetic_data

from dtrack_left.col_check import (
    ColCheckPipeline,
    LeftRuntime,
    publish_fixed_keys,
    wait_for_manifests,
)
from dtrack_left.fetch import fetch_report
from dtrack_left.row_check import run_row_check
from dtrack_right.app import run as run_right
from dtrack_right.col_check import ColCheck
from dtrack_right.report import build_report, publish_report
from dtrack_right.row_check import run_row_check as run_right_row_check
from dtrack_right.store import VolumeStore
from dtrack_right.targets import CsvAnchorTarget


@pytest.fixture(scope="module")
def data(tmp_path_factory):
    root = tmp_path_factory.mktemp("synthetic")
    synthetic_data(root)
    return root


def _target_factory(root: Path, work: Path):
    return lambda config: CsvAnchorTarget(config, root / "right", work)


# >>> the whole protocol in one process on real CSV files and a real directory store, no mocks <<< #
def test_full_pipeline_on_real_files(data, tmp_path):
    root = data
    store = VolumeStore(tmp_path / "runs" / "real")
    report_store = VolumeStore(tmp_path / "report")
    run_id, _ = run_row_check(pairs_config(root / "left"), store, run_id="real")

    assert run_right_row_check(store, _target_factory(root, tmp_path / "rw")) == 2
    orders = store.get_json("row_check/orders.json")
    assert orders["summary"] == {"match": 56, "count_diff": 1, "left_only": 1, "right_only": 1}
    assert orders["partitions"]["2026-01-15"] == {
        "left": 30,
        "right": 27,
        "verdict": "count_diff",
        "delta": -3,
    }
    assert store.exists("row_check/_CLOSED") and store.exists("reports/orders_compare_row.csv")
    manifest = store.get_json("manifest/orders.json")
    for hole in ("2026-01-15", "2026-01-31", "2026-02-10"):
        assert all(hole not in window["partitions"] for window in manifest["windows"])

    manifests = wait_for_manifests(store, timeout=1, poll_interval=0)
    for manifest in manifests:
        publish_fixed_keys(store, manifest, tmp_path / "lw")
    assert len(store.get_json("manifest/customers.keys.json")["windows"]["2026-01-01"]) == 40
    runtime = LeftRuntime(tmp_path / "lw", store)
    assert (
        ColCheckPipeline(
            store, pull_window=runtime.pull_window, compute_stats=runtime.compute_stats
        ).run(manifests)
        == 11
    )

    results = ColCheck(store, _target_factory(root, tmp_path / "rw"), poll_interval=0).run()
    verdicts = {(row["pair"], row["vintage"], row["column"]): row["verdict"] for row in results}
    assert len(verdicts) == 11 and not store.list_keys("dead/")
    assert verdicts[("orders", "2026-01-01", "STATUS")] == "diff"
    assert verdicts[("orders", "2026-02-01", "AMOUNT")] == "diff"
    assert all(
        v == "match"
        for k, v in verdicts.items()
        if k not in {("orders", "2026-01-01", "STATUS"), ("orders", "2026-02-01", "AMOUNT")}
    )
    amount = next(
        row for row in results if row["column"] == "AMOUNT" and row["vintage"] == "2026-02-01"
    )
    assert amount["n_total_left"] == amount["n_total_right"] and amount["hash_sum_match"] is False

    files = build_report(store, tmp_path / "out")
    summary = json.loads(files["summary.json"].read_text())
    assert summary["status"] == "DONE" and summary["findings"] == {
        "row_count_diff": 1,
        "partition_missing": 2,
        "col_stat_diff": 2,
    }
    publish_report(report_store, run_id, files, summary)
    fetched = fetch_report(report_store, tmp_path / "fetched")
    assert fetched["run_id"] == run_id and (tmp_path / "fetched" / "report.xlsx").stat().st_size > 0


# >>> the Right job entry point resolves the run under a root and is idempotent on a finished run <<< #
def test_right_app_resolves_run_root_and_reruns_without_rework(data, tmp_path):
    root = data
    runs = tmp_path / "runs"
    store = VolumeStore(runs / "r1")
    run_row_check(pairs_config(root / "left"), store, run_id="r1")
    factory = _target_factory(root, tmp_path / "rw")
    assert run_right_row_check(store, factory) == 2
    manifests = wait_for_manifests(store, timeout=1, poll_interval=0)
    for manifest in manifests:
        publish_fixed_keys(store, manifest, tmp_path / "lw")
    runtime = LeftRuntime(tmp_path / "lw", store)
    ColCheckPipeline(
        store, pull_window=runtime.pull_window, compute_stats=runtime.compute_stats
    ).run(manifests)
    first = run_right(
        str(runs),
        factory,
        report=str(tmp_path / "report"),
        out_dir=tmp_path / "out",
        poll_interval=0,
        idle_timeout=5,
        wait_timeout=5,
    )
    assert first["results"] == 11 and first["summary"]["status"] == "DONE"
    assert VolumeStore(tmp_path / "report").get_json("latest/pointer.json")["run_id"] == "r1"
    again = run_right(
        str(runs),
        factory,
        run_id="r1",
        report=None,
        out_dir=tmp_path / "out2",
        poll_interval=0,
        idle_timeout=5,
        wait_timeout=5,
    )
    assert again["results"] == 0 and again["summary"]["units_completed"] == 11
