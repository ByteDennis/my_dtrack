import pytest
from support import MemoryStore

from pair_recon_left.common.checks.row_check import run_row_check


class Source:
    def __init__(self, counts):
        self.counts = counts

    def preflight(self):
        return {"ok": True}

    def partition_counts(self, *_args):
        return self.counts


def test_run_requires_exactly_one_pair():
    with pytest.raises(ValueError, match="exactly one active pair"):
        run_row_check(
            {"pairs": {"a": {}, "b": {}}},
            MemoryStore(),
            run_id="run",
        )


@pytest.mark.parametrize(
    ("mode", "counts"),
    [("date", {"2026-01-01": 2}), ("all", {"all": 2})],
)
def test_counts(mode, counts):
    dates = {} if mode == "all" else {"date_col": "D", "date_type": "date"}
    pair = {
        "partition_mode": mode,
        "left": {"source": "oracle", "table": "L", **dates},
        "right": {"source": "databricks", "table": "R", **dates},
        "col_map": {"A": "a"},
        "checks": ["row_count"],
        "version": 2,
    }
    store = MemoryStore()
    run_row_check(
        {"version": 2, "pairs": {"p": pair}},
        store,
        run_id="run",
        source_factory=lambda _value: Source(counts),
    )
    assert store.get_json("row_check/left.json")["counts"] == counts
    assert not store.exists("plan.json")


def test_plan():
    pair = {
        "partition_mode": "all",
        "left": {"source": "oracle", "table": "L"},
        "right": {"source": "databricks", "table": "R"},
        "col_map": {"A": "a"},
        "checks": ["hash_sum", "n_unique"],
        "statistics": ["n_unique", "hash_sum"],
        "version": 2,
    }
    store = MemoryStore()
    run_row_check(
        {"version": 2, "pairs": {"p": pair}},
        store,
        run_id="run",
        source_factory=lambda _value: Source({"all": 2}),
    )
    assert store.get_json("plan.json")["statistics"] == ["n_unique", "hash_sum"]


def test_vintage_sampling_keeps_full_row_counts():
    counts = {"2026-01-01": 2, "2026-02-01": 3, "2026-03-01": 4}
    pair = {
        "left": {"source": "oracle", "table": "L", "date_col": "D", "date_type": "date"},
        "right": {
            "source": "databricks",
            "table": "R",
            "date_col": "D",
            "date_type": "date",
        },
        "col_map": {"A": "a"},
        "checks": ["hash_sum"],
        "vintage": "month",
        "vintage_sample_size": 1,
        "version": 2,
    }
    store = MemoryStore()
    run_row_check(
        {"version": 2, "pairs": {"p": pair}},
        store,
        run_id="run",
        source_factory=lambda _value: Source(counts),
    )
    assert store.get_json("row_check/left.json")["counts"] == counts
    plan = store.get_json("plan.json")
    assert len(plan["windows"]) == 1
    assert plan["vintage_selection"]["available"] == 3


def test_snapshot():
    seen = []
    pair = {
        "snapshot": True,
        "left": {"source": "oracle", "table": "CURRENT"},
        "right": {"source": "databricks", "table": "HISTORY", "date_col": "D"},
        "col_map": {"A": "a"},
        "checks": ["hash_sum"],
        "version": 2,
    }
    store = MemoryStore()

    def factory(config):
        seen.append(config)
        return Source({"all": 2})

    run_row_check(
        {"version": 2, "pairs": {"p": pair}},
        store,
        run_id="run",
        source_factory=factory,
    )
    assert seen[0]["partition_mode"] == "all"
    assert not store.exists("plan.json")
