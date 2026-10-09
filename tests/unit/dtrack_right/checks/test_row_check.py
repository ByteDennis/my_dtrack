import pytest
from support import FakeTarget, MemoryStore, left_document

from dtrack_right.checks.row_check import (
    evaluate_gate,
    pending_left_documents,
    reconcile_pair,
    reconcile_partitions,
    row_check_csv_bytes,
    run_row_check,
    summarize_partitions,
)


def test_reconcile_partitions_classifies_every_day_on_either_side():
    rows = reconcile_partitions(
        {"2026-01-01": 10, "2026-01-02": 5, "20260104": 3},
        {"2026-01-01": 10, "2026-01-03": 5, "2026-01-04": 4},
    )
    assert {day: row["verdict"] for day, row in rows.items()} == {
        "2026-01-01": "match",
        "2026-01-02": "left_only",
        "2026-01-03": "right_only",
        "2026-01-04": "count_diff",
    }
    assert rows["2026-01-04"]["delta"] == 1 and "delta" not in rows["2026-01-01"]
    assert summarize_partitions(rows) == {
        "match": 1,
        "count_diff": 1,
        "left_only": 1,
        "right_only": 1,
    }


def test_reconcile_partitions_folds_date_synonyms_on_both_sides():
    rows = reconcile_partitions(
        {"3000-01-01": 2}, {"2026-07-01": 2}, date_synonyms={"3000-01-01": "2026-07-01"}
    )
    assert rows == {"2026-07-01": {"left": 2, "right": 2, "verdict": "match"}}


ROWS = {
    "a": {"verdict": "match"},
    "b": {"verdict": "match"},
    "c": {"verdict": "count_diff"},
    "d": {"verdict": "left_only"},
}


@pytest.mark.parametrize(
    ("policy", "passed"),
    [
        ("auto", True),
        (" AUTO ", True),
        ("manual", False),
        ("ratio:0.5", True),
        ("ratio:0.4", False),
    ],
)
def test_gate_policies(policy, passed):
    decision = evaluate_gate(ROWS, policy)
    assert decision["passed"] is passed and decision["excluded_ratio"] == 0.5


def test_gate_auto_fails_when_nothing_matched_and_handles_empty_input():
    assert evaluate_gate({"a": {"verdict": "count_diff"}})["passed"] is False
    assert evaluate_gate({}, None) == {
        "policy": "auto",
        "excluded_ratio": 0.0,
        "matched_partitions": 0,
        "total_partitions": 0,
        "passed": False,
        "reason": "auto",
    }


@pytest.mark.parametrize("policy", ["ratio:1.5", "strict"])
def test_gate_rejects_unknown_or_out_of_range_policies(policy):
    with pytest.raises(ValueError):
        evaluate_gate(ROWS, policy)


def test_row_check_csv_is_sorted_with_blank_delta_for_non_diffs():
    document = {
        "pair": "p",
        "partitions": {
            "2026-01-02": {"left": 1, "right": 3, "verdict": "count_diff", "delta": 2},
            "2026-01-01": {"left": 1, "right": 1, "verdict": "match"},
        },
    }
    assert row_check_csv_bytes(document).decode().splitlines() == [
        "pair,partition,left,right,delta,verdict",
        "p,2026-01-01,1,1,,match",
        "p,2026-01-02,1,3,2,count_diff",
    ]


def test_pending_documents_skip_reconciled_pairs_and_non_left_files():
    store = MemoryStore()
    store.put_json("row_check/a.left.json", left_document("a"))
    store.put_json("row_check/b.left.json", left_document("b"))
    store.put_json("row_check/b.json", {"pair": "b"})
    store.put_json("row_check/_LEFT_CLOSED", {"pairs": ["a", "b"]})
    assert [doc["pair"] for doc in pending_left_documents(store)] == ["a"]


def test_right_row_check_reconciles_pending_left_documents_and_writes_manifest_with_holes():
    store = MemoryStore()
    store.put_json("row_check/p.left.json", left_document())
    store.put_json("row_check/_LEFT_CLOSED", {"pairs": ["p"]})
    assert run_row_check(store, lambda _cfg: FakeTarget()) == 1
    doc = store.get_json("pairs/p/row_check/result.json")
    assert doc["summary"] == {"match": 1, "count_diff": 1, "left_only": 1, "right_only": 0}
    assert doc["timing"]["left_count_s"] == 0.1
    manifest = store.get_json("pairs/p/plan.json")
    assert manifest["windows"][0]["partitions"] == ["2026-01-01"]
    assert store.exists("_status/row_check.closed.json")
    assert store.exists("report/pairs/p/row_check.csv")
    assert store.exists("_status/units.csv")
    assert run_row_check(store, lambda _cfg: FakeTarget()) == 0


# >>> the verdict is the commit marker: a crash before it leaves the pair pending, never half-released <<< #
def test_a_crash_before_the_manifest_lands_is_redone_on_restart():
    store = MemoryStore()
    store.put_json("row_check/p.left.json", left_document())
    store.put_json("row_check/_LEFT_CLOSED", {"pairs": ["p"]})
    real_put = store.put_json

    def crash_on_manifest(key, value):
        if key.endswith("/plan.json"):
            raise ConnectionError("killed")
        return real_put(key, value)

    store.put_json = crash_on_manifest
    with pytest.raises(ConnectionError):
        run_row_check(store, lambda _cfg: FakeTarget())
    store.put_json = real_put
    assert not store.exists("pairs/p/row_check/result.json")
    assert run_row_check(store, lambda _cfg: FakeTarget()) == 1
    assert store.exists("pairs/p/plan.json")
    assert store.exists("_status/row_check.closed.json")


def test_row_check_stays_open_until_every_announced_pair_is_reconciled():
    store = MemoryStore()
    store.put_json("row_check/a.left.json", left_document("a"))
    store.put_json("row_check/_LEFT_CLOSED", {"pairs": ["a", "b"]})
    run_row_check(store, lambda _cfg: FakeTarget())
    assert not store.exists("_status/row_check.closed.json")


def test_manual_gate_reports_without_releasing_a_manifest():
    store = MemoryStore()
    store.put_json("row_check/p.left.json", left_document())
    run_row_check(store, lambda _cfg: FakeTarget(), gate="manual")
    assert store.exists("pairs/p/row_check/result.json")
    assert not store.exists("pairs/p/plan.json")


def test_a_failed_right_preflight_stops_the_pair():
    store = MemoryStore()
    with pytest.raises(RuntimeError, match="TABLE_OR_VIEW_NOT_FOUND"):
        reconcile_pair(store, left_document(), FakeTarget(healthy=False))
    assert not store.exists("pairs/p/row_check/result.json")
