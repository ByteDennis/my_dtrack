import json

import pytest
from support import STATS, MemoryStore

from pair_recon_right.engine.checks.col_check import compare_statistics
from pair_recon_right.engine.common.model.manifest import build_manifest, unit_id_for
from pair_recon_right.engine.report import build_report
from pair_recon_right.engine.report.build import collect


def _store():
    pair = {
        "version": 2,
        "left": {"source": "oracle", "table": "A", "date_col": "DT"},
        "right": {
            "source": "databricks",
            "catalog": "c",
            "schema": "s",
            "table": "a",
            "date_col": "dt",
        },
        "col_map": {"x": "x", "y": "y_r"},
        "excluded_partitions": ["2026-01-02", "2026-01-03"],
    }
    row_document = {
        "run_id": "run",
        "pair": "p",
        "left": pair["left"],
        "right": pair["right"],
        "bounds": {"from": "2026-01-01", "to": "2026-01-03"},
        "partition_mode": "date",
        "partitions": {
            "2026-01-01": {"left": 1, "right": 1, "result": "match"},
            "2026-01-02": {"left": 1, "right": 2, "result": "count_diff", "delta": 1},
            "2026-01-03": {"left": 1, "right": None, "result": "left_only"},
        },
        "summary": {"match": 1, "count_diff": 1, "left_only": 1, "right_only": 0},
        "gate_decision": {"policy": "auto", "passed": True},
        "timing": {"left_count_s": 1.5, "right_count_s": 0.5},
    }
    manifest = build_manifest("run", "p", pair, row_document)
    window = manifest["windows"][0]
    store = MemoryStore()
    store.put_json("row_check/result.json", row_document)
    store.put_json("plan.json", manifest)
    store.close_manifests(["p"], manifest["expected_units"], {"p": manifest["plan_id"]})

    same = compare_statistics(
        pair="p", window=window["bucket"], column="x", left=STATS, right=STATS
    )
    same.update(unit_id=unit_id_for(manifest, window, "x"), right_col="x")
    changed = compare_statistics(
        pair="p",
        window=window["bucket"],
        column="y",
        left=STATS,
        right={**STATS, "mean": 9.0, "n_unique": 2},
    )
    changed.update(unit_id=unit_id_for(manifest, window, "y"), right_col="y_r")
    store.commit_results([same])
    store.put_result(changed["unit_id"], changed)
    return store


def test_report_builds_flat_files_from_batch_and_per_unit_results(tmp_path):
    files = build_report(_store(), tmp_path / "out", title="T")

    assert set(files) == {
        "index.html",
        "report.xlsx",
        "summary.json",
    }
    summary = json.loads(files["summary.json"].read_text())
    assert summary["status"] == "DONE" and summary["pairs"] == 1
    assert summary["findings"] == {
        "row_count_diff": 1,
        "partition_missing": 1,
        "col_stat_diff": 1,
    }
    assert summary["files"] == ["index.html", "report.xlsx"]
    page = files["index.html"].read_text()
    assert "days mismatch: 1" in page and "y → y_r" in page and "mean (1.5000→9)" in page

    from openpyxl import load_workbook

    workbook = load_workbook(files["report.xlsx"])
    assert workbook.sheetnames == ["Row Check", "Column Check"]
    assert workbook["Column Check"]["B1"].value == "FAIL"


def test_collect_filters_stale_outcomes_against_active_plan():
    store = _store()
    store.put_json(
        "col_check/results/stale.json",
        {"unit_id": "stale", "pair": "p", "verdict": "diff"},
    )
    store.put_json(
        "col_check/failures/stale.json",
        {"unit_id": "stale-failure", "pair": "p", "error": "old"},
    )
    store.put_json(
        "col_check/failures/source.json",
        {"unit_id": "source", "source": "databricks", "error": "runtime"},
    )

    state = collect(store)

    assert {row["unit_id"] for row in state["results"]} != {"stale"}
    assert {row["unit_id"] for row in state["dead"]} == {"source"}


def test_rows_only_is_complete_and_omits_column_sheet(tmp_path):
    store = _store()
    store.objects.pop("plan.json")
    store.objects.pop("_state/left.json")
    for key in list(store.objects):
        if key.startswith("col_check/"):
            store.objects.pop(key)

    files = build_report(store, tmp_path)
    summary = json.loads(files["summary.json"].read_text())

    assert summary["status"] == "DONE"
    assert "Stage 2" not in files["index.html"].read_text()

    from openpyxl import load_workbook

    assert load_workbook(files["report.xlsx"]).sheetnames == ["Row Check"]


def test_rows_required(tmp_path):
    with pytest.raises(ValueError, match="no row_check documents"):
        build_report(MemoryStore(), tmp_path)
