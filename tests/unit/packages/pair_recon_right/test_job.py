import pytest
from support import FakeTarget, MemoryStore

from pair_recon_right.engine.backends.store import VolumeStore
from pair_recon_right.engine.job import run, summary


def document(checks):
    side = {
        "left": {"source": "oracle"},
        "right": {"source": "databricks", "table": "R"},
    }
    return {
        "run_id": "r1",
        "pair": "p",
        **side,
        "bounds": {"from": None, "to": None},
        "partition_mode": "all",
        "counts": {"all": 2},
        "pair_config": {
            "version": 2,
            "partition_mode": "all",
            **side,
            "col_map": {"A": "a"},
            "checks": checks,
        },
        "preflight": {"ok": True},
        "timing": {},
    }


def test_rowonly(tmp_path):
    store = VolumeStore(tmp_path / "runs" / "r1")
    store.put_json("row_check/left.json", document(["row_count"]))
    store.put_json("_status/left_row_check.closed.json", {"pairs": ["p"]})
    result = run(
        tmp_path / "runs",
        lambda _config: FakeTarget(counts={"all": 2}),
        run_id="r1",
        poll_interval=0,
        wait_timeout=1,
    )
    assert result == {"row_errors": 0, "column_errors": 0, "failures": 0}
    assert {
        "report/index.html",
        "report/report.xlsx",
        "report/row_check.csv",
        "report/summary.json",
    } <= set(store.list_keys("report/"))


def test_timeout(tmp_path):
    with pytest.raises(TimeoutError):
        run(tmp_path, None, poll_interval=0, wait_timeout=0)


def test_summary_excludes_stale_outcomes_from_an_empty_current_plan():
    store = MemoryStore()
    store.put_json("plan.json", {"pair": "p", "windows": [], "columns": []})
    store.put_json(
        "col_check/results/stale.json",
        {"unit_id": "stale", "pair": "p", "verdict": "diff"},
    )
    store.put_json(
        "col_check/failures/stale.json",
        {"unit_id": "stale-failure", "pair": "p", "error": "old"},
    )

    assert summary(store) == {"row_errors": 0, "column_errors": 0, "failures": 0}
