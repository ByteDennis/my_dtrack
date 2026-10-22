import pytest
from support import FakeTarget, MemoryStore, left_document

from pair_recon_right.engine.checks.row_check import (
    evaluate_gate,
    pending_left_documents,
    reconcile_pair,
    reconcile_partitions,
    reconcile_snapshot,
    row_check_csv_bytes,
    run_row_check,
    summarize_partitions,
)
from pair_recon_right.engine.common.model.manifest import build_manifest


def test_partition():
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


def test_date():
    rows = reconcile_partitions(
        {"3000-01-01": 2}, {"2026-07-01": 2}, date_synonyms={"3000-01-01": "2026-07-01"}
    )
    assert rows == {"2026-07-01": {"left": 2, "right": 2, "verdict": "match"}}


def test_snapshot():
    counts = {"2026-01-01": 9, "2026-01-02": 10, "2026-01-03": 10}
    rows = reconcile_snapshot({"all": 10}, counts)
    assert rows == {"2026-01-03": {"left": 10, "right": 10, "verdict": "match"}}
    store = MemoryStore()
    left = left_document(counts={"all": 10})
    left["left"] = {"source": "oracle", "table": "CURRENT"}
    left["pair_config"].update(
        version=2,
        snapshot=True,
        checks=["hash_sum"],
        left=left["left"],
    )
    store.put_json("row_check/left.json", left)
    store.put_json("_status/left_row_check.closed.json", {"pairs": ["p"]})
    run_row_check(store, lambda _config: FakeTarget(counts=counts))
    plan = store.get_json("plan.json")
    assert plan["left_snapshot"] is True
    assert plan["windows"][0]["partitions"] == ["2026-01-03"]


def test_all():
    rows = reconcile_partitions({"all": 9}, {"all": 9}, partition_mode="all")
    assert rows == {"all": {"left": 9, "right": 9, "verdict": "match"}}


@pytest.mark.parametrize(
    ("key", "mode", "family", "right_pre_sampled"),
    [("id", "all", "string", False), ("dt", "date", "date_type", False), ("id", "all", None, True)],
)
def test_sample(key, mode, family, right_pre_sampled):
    seen = {}

    class Target(FakeTarget):
        def partition_counts(self, *_args, **options):
            seen.update(options)
            return self.counts

    counts = {"all": 5} if mode == "all" else {"2026-01-01": 5}
    left = left_document(counts=counts, sample=0.1)
    left["partition_mode"] = mode
    left["bounds"] = (
        {"from": None, "to": None} if mode == "all" else {"from": "2026-01-01", "to": "2026-01-01"}
    )
    left["pair_config"].update(
        version=2,
        checks=["row_count"],
        partition_mode=mode,
        pre_sampled=True,
        right_pre_sampled=right_pre_sampled,
        key=key,
    )
    if mode == "date":
        left["pair_config"]["left"]["date_type"] = "date"
        left["pair_config"]["right"]["date_type"] = "date"
    reconcile_pair(MemoryStore(), left, Target(counts=counts))
    if right_pre_sampled:
        assert seen == {}
    else:
        assert seen["sample"]["mode"] == "fraction"
        assert seen["key_columns"] == [key]
        resolved = seen["key_families"][0]
        assert (resolved if isinstance(resolved, str) else resolved["family"]) == family


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
def test_gate(policy, passed):
    decision = evaluate_gate(ROWS, policy)
    assert decision["passed"] is passed and decision["excluded_ratio"] == 0.5


def test_gate2():
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
def test_gate3(policy):
    with pytest.raises(ValueError):
        evaluate_gate(ROWS, policy)


def test_csv():
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


def test_pending():
    store = MemoryStore()
    store.put_json("row_check/left.json", left_document())
    assert [doc["pair"] for doc in pending_left_documents(store)] == ["p"]
    store.put_json("row_check/result.json", {"pair": "p"})
    assert pending_left_documents(store) == []


def test_reconcile(caplog):
    store = MemoryStore()
    store.put_json("row_check/left.json", left_document())
    store.put_json("_status/left_row_check.closed.json", {"pairs": ["p"]})
    with caplog.at_level("INFO"):
        assert run_row_check(store, lambda _cfg: FakeTarget()) == 1
    doc = store.get_json("row_check/result.json")
    assert doc["summary"] == {"match": 1, "count_diff": 1, "left_only": 1, "right_only": 0}
    assert doc["timing"]["left_count_s"] == 0.1
    manifest = store.get_json("plan.json")
    assert manifest["windows"][0]["partitions"] == ["2026-01-01"]
    assert "plan released: memory://run/plan.json (1 windows, 1 columns, 1 units)" in caplog.text
    assert "right count p: 2 partitions, 3 rows" in caplog.text
    assert "first ('2026-01-01', 1)" in caplog.text
    assert "finding p 2026-01-02 count_diff left=" in caplog.text
    assert "more finding(s)" not in caplog.text


def test_reconcile_caps_logged_findings(caplog):
    store = MemoryStore()
    doc = left_document()
    doc["counts"] = {f"2026-01-{day:02d}": 1 for day in range(1, 16)}
    store.put_json("row_check/left.json", doc)
    store.put_json("_status/left_row_check.closed.json", {"pairs": ["p"]})
    with caplog.at_level("INFO", logger="pair_recon.right.row_check"):
        run_row_check(store, lambda _cfg: FakeTarget(counts={"2026-01-01": 1}))
    assert caplog.text.count("  finding p ") == 10
    assert "  ... 4 more finding(s) in report/row_check.csv" in caplog.text
    assert store.get_bytes("mapping.csv").decode().splitlines() == [
        "left_column,right_column,usage,data_type",
        "amount,amount,compare,",
    ]
    assert store.exists("_status/row_check.closed.json")
    assert store.exists("report/row_check.csv")
    assert store.exists("_status/units.csv")
    assert run_row_check(store, lambda _cfg: FakeTarget()) == 0


def test_restart():
    store = MemoryStore()
    store.put_json("row_check/left.json", left_document())
    store.put_json("_status/left_row_check.closed.json", {"pairs": ["p"]})
    real_put = store.put_json

    def crash_on_manifest(key, value):
        if key == "plan.json":
            raise ConnectionError("killed")
        return real_put(key, value)

    store.put_json = crash_on_manifest
    with pytest.raises(ConnectionError):
        run_row_check(store, lambda _cfg: FakeTarget())
    store.put_json = real_put
    assert not store.exists("row_check/result.json")
    assert run_row_check(store, lambda _cfg: FakeTarget()) == 1
    assert store.exists("plan.json")
    assert store.exists("_status/row_check.closed.json")


def test_open():
    store = MemoryStore()
    store.put_json("row_check/left.json", left_document())
    run_row_check(store, lambda _cfg: FakeTarget())
    assert not store.exists("_status/row_check.closed.json")


def test_manual():
    store = MemoryStore()
    store.put_json("row_check/left.json", left_document())
    run_row_check(store, lambda _cfg: FakeTarget(), gate="manual")
    assert store.exists("row_check/result.json")
    assert not store.exists("plan.json")


def test_preflight():
    store = MemoryStore()
    with pytest.raises(RuntimeError, match="TABLE_OR_VIEW_NOT_FOUND"):
        reconcile_pair(store, left_document(), FakeTarget(healthy=False))
    assert not store.exists("row_check/result.json")


def test_v2():
    store = MemoryStore()
    left = left_document()
    left["pair_config"]["version"] = 2
    left["pair_config"]["checks"] = ["hash_sum"]
    baseline = {
        "gate_decision": {"passed": True},
        "partitions": {
            day: {"left": count, "right": None, "verdict": "left_only"}
            for day, count in left["counts"].items()
        },
    }
    store.put_json("row_check/left.json", left)
    store.put_json("_status/left_row_check.closed.json", {"pairs": ["p"]})

    with pytest.raises(RuntimeError, match=r"p: 2026-01-02, 2026-01-03"):
        run_row_check(store, lambda _cfg: FakeTarget())
    assert store.exists("row_check/result.json")
    assert store.exists("report/row_check.csv")
    assert store.get_json("plan.json")["version"] == 2

    updated = build_manifest(
        "run",
        "p",
        {**left["pair_config"], "excluded_partitions": ["2026-01-02", "2026-01-03"]},
        baseline,
    )
    store.put_json("plan.json", updated)
    assert run_row_check(store, lambda _cfg: FakeTarget()) == 0


def test_all2():
    store = MemoryStore()
    left = left_document(counts={"all": 5})
    left["partition_mode"] = "all"
    left["bounds"] = {"from": None, "to": None}
    left["left"] = {"source": "oracle", "table": "A"}
    left["right"] = {"source": "databricks", "table": "a"}
    left["pair_config"] = {
        "version": 2,
        "checks": ["hash_sum"],
        "partition_mode": "all",
        "left": left["left"],
        "right": left["right"],
        "col_map": {"amount": "amount"},
    }
    store.put_json("row_check/left.json", left)
    store.put_json("_status/left_row_check.closed.json", {"pairs": ["p"]})
    with pytest.raises(RuntimeError, match="partition_mode all row counts differ"):
        run_row_check(store, lambda _cfg: FakeTarget(counts={"all": 6}))
