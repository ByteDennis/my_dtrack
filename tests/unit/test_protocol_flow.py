import json
from pathlib import Path

from support import pairs_config

from dtrack_left.checks.col_check import (
    ColCheckPipeline,
    LeftRuntime,
    publish_fixed_keys,
    wait_for_manifests,
)
from dtrack_left.checks.row_check import run_row_check
from dtrack_local.csv_target import CsvAnchorTarget
from dtrack_right.backends.store import VolumeStore
from dtrack_right.checks.col_check import ColCheck
from dtrack_right.checks.row_check import run_row_check as run_right_row_check
from dtrack_right.common.protocol import list_failures, write_run_status
from dtrack_right.report import build_report


def _target_factory(root: Path, work: Path):
    return lambda config: CsvAnchorTarget(config, root / "right", work)


# >>> the whole protocol in one process on real CSV files and a real directory store, no mocks <<< #
def test_full_pipeline_on_real_files(synthetic, tmp_path):
    root = synthetic.root
    store = VolumeStore(tmp_path / "runs" / "real")
    _run_id, _ = run_row_check(pairs_config(root / "left"), store, run_id="real")

    assert run_right_row_check(store, _target_factory(root, tmp_path / "rw")) == 2
    orders = store.get_json("pairs/orders/row_check/result.json")
    assert orders["summary"] == {"match": 56, "count_diff": 1, "left_only": 1, "right_only": 1}
    assert orders["partitions"]["2026-01-15"] == {
        "left": 30,
        "right": 27,
        "verdict": "count_diff",
        "delta": -3,
    }
    assert store.exists("_status/row_check.closed.json")
    assert store.exists("report/pairs/orders/row_check.csv")
    manifest = store.get_json("pairs/orders/plan.json")
    detected = {"type": "string", "format": "dash"}
    assert manifest["left"]["date_type"] == detected
    assert manifest["right"]["date_type"] == detected
    for hole in ("2026-01-15", "2026-01-31", "2026-02-10"):
        assert all(hole not in window["partitions"] for window in manifest["windows"])

    manifests = wait_for_manifests(store, timeout=1, poll_interval=0)
    for manifest in manifests:
        publish_fixed_keys(store, manifest, tmp_path / "lw")
    assert len(store.get_json("pairs/customers/fixed_keys.json")["windows"]["2026-01-01"]) == 40
    runtime = LeftRuntime(tmp_path / "lw", store)
    assert (
        ColCheckPipeline(
            store, pull_window=runtime.pull_window, compute_stats=runtime.compute_stats
        ).run(manifests)
        == 11
    )

    results = ColCheck(store, _target_factory(root, tmp_path / "rw"), poll_interval=0).run()
    verdicts = {(row["pair"], row["vintage"], row["column"]): row["verdict"] for row in results}
    assert len(verdicts) == 11 and not list_failures(store)
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
    for name, path in files.items():
        store.put_bytes(f"report/{name}", path.read_bytes())
    write_run_status(store, {"state": "complete"})
    assert store.get_bytes("report/report.xlsx") == files["report.xlsx"].read_bytes()
