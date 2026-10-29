import threading
import time

import pytest
from support import MemoryStore

from pair_recon_left.common.checks import row_check
from pair_recon_left.common.checks.row_check import (
    public_pair_config,
    reuse_row_check,
    run_row_check,
)
from pair_recon_left.common.model.protocol import (
    get_left_state,
    get_right_state,
    left_row_path,
    mapping_path,
    plan_path,
    row_result_path,
)


class Source:
    def __init__(self, counts):
        self.counts = counts

    def health_check(self):
        return {"ok": True}

    def partition_counts(self, *_args):
        return self.counts


def test_public_pair_config_excludes_local_sas_handoff_path():
    pair = {
        "left": {
            "source": "oracle",
            "table": "orders",
            "sas_sample_csv": "C:/PairRecon/samples/orders/left_sample.csv",
        },
        "right": {"source": "databricks", "table": "orders"},
    }

    public = public_pair_config(pair)

    assert public["left"] == {"source": "oracle", "table": "orders"}


def test_run_requires_exactly_one_pair():
    with pytest.raises(ValueError, match="exactly one active pair"):
        run_row_check(
            {"pairs": {"a": {}, "b": {}}},
            MemoryStore(),
            run_id="run",
        )


@pytest.mark.parametrize(
    ("mode", "counts"),
    [("day", {"2026-01-01": 2}), ("all", {"all": 2})],
)
def test_counts(mode, counts):
    dates = {} if mode == "all" else {"date_col": "D", "date_type": "date"}
    pair = {
        "partition": mode,
        "left": {"source": "oracle", "table": "L", **dates},
        "right": {"source": "databricks", "table": "R", **dates},
        "col_map": {"A": "a"},
        "checks": ["row_count"],
    }
    store = MemoryStore()
    run_row_check(
        {"pairs": {"p": pair}},
        store,
        run_id="run",
        source_factory=lambda _value: Source(counts),
    )
    assert store.get_json(left_row_path("p"))["counts"] == counts
    assert not store.exists(plan_path("p"))


def test_row_count_query_logs_start_heartbeat_and_completion(monkeypatch, caplog):
    class SlowSource(Source):
        def partition_counts(self, *_args):
            time.sleep(0.03)
            return self.counts

    pair = {
        "partition": "day",
        "fromDate": "2026-01-01",
        "toDate": "2026-01-31",
        "left": {"source": "oracle", "table": "L", "date_col": "D", "date_type": "date"},
        "right": {
            "source": "databricks",
            "table": "R",
            "date_col": "D",
            "date_type": "date",
        },
        "col_map": {"A": "a"},
        "checks": ["row_count"],
    }
    monkeypatch.setattr(row_check, "ROW_COUNT_LOG_INTERVAL", 0.005)

    with caplog.at_level("INFO"):
        run_row_check(
            {"pairs": {"p": pair}},
            MemoryStore(),
            run_id="run",
            source_factory=lambda _value: SlowSource({"2026-01-01": 2}),
        )

    assert (
        "left row count query started: pair=p range=2026-01-01..2026-01-31 partition=day"
    ) in caplog.text
    assert "left row count query still running: pairs=['p']" in caplog.text
    assert "left row count query completed: pair=p partitions=1 rows=2" in caplog.text


def test_plan(caplog):
    pair = {
        "partition": "all",
        "left": {"source": "oracle", "table": "L"},
        "right": {"source": "databricks", "table": "R"},
        "col_map": {"A": "a"},
        "checks": ["hash_sum", "n_unique"],
        "statistics": ["n_unique", "hash_sum"],
    }
    store = MemoryStore()
    with caplog.at_level("INFO"):
        run_row_check(
            {"pairs": {"p": pair}},
            store,
            run_id="run",
            source_factory=lambda _value: Source({"all": 2}),
        )
    assert store.get_json(plan_path("p"))["statistics"] == ["n_unique", "hash_sum"]
    assert (
        "plan released: memory://run/config/manifest.json (1 windows, 1 columns, 1 units)"
        in caplog.text
    )
    assert "version=" not in caplog.text
    assert store.get_bytes(mapping_path("p")).decode().splitlines() == [
        "left_column,right_column,usage,data_type",
        "A,a,compare,",
    ]


def test_reused_row_check_reopens_column_stage():
    pair = {
        "partition": "all",
        "left": {"source": "oracle", "table": "L"},
        "right": {"source": "databricks", "table": "R"},
        "col_map": {"A": "a"},
        "checks": ["hash_sum"],
    }
    config = {"pairs": {"p": pair}}
    store = MemoryStore()
    run_row_check(
        config,
        store,
        run_id="run",
        source_factory=lambda _value: Source({"all": 2}),
    )
    store.close_manifests(["p"], 1, {"p": store.get_json(plan_path("p"))["plan_id"]})

    assert reuse_row_check(config, store, run_id="run", pair_names=["p"])

    assert get_left_state(store)["col_check_closed_at"] is None


def test_reused_left_counts_invalidate_right_when_effective_config_changes():
    pair = {
        "partition": "all",
        "left": {"source": "oracle", "table": "L"},
        "right": {"source": "databricks", "table": "R"},
        "col_map": {"A": "a"},
        "checks": ["hash_sum"],
    }
    config = {"pairs": {"p": pair}}
    store = MemoryStore()
    run_row_check(
        config,
        store,
        run_id="run",
        source_factory=lambda _value: Source({"all": 2}),
    )
    store.put_json(row_result_path("p"), {"pair": "p", "partitions": {}})
    store.close_row_check(["p"])
    pair["normalization_rules"] = [{"rule_name": "trim", "source": "all"}]

    assert reuse_row_check(config, store, run_id="run", pair_names=["p"])

    assert not store.exists(row_result_path("p"))
    assert get_right_state(store).get("row_check_closed_at") is None
    assert store.get_json(left_row_path("p"))["counts"] == {"all": 2}
    assert store.get_json(plan_path("p"))["normalization_rules"] == [
        {"rule_name": "trim", "source": "all"}
    ]


def test_reused_left_counts_allow_non_key_column_type_changes():
    pair = {
        "partition": "all",
        "left": {"source": "oracle", "table": "L"},
        "right": {"source": "databricks", "table": "R"},
        "key": "ID",
        "col_map": {"ID": "id", "AMOUNT": "amount"},
        "checks": ["hash_sum"],
    }
    config = {"pairs": {"p": pair}}
    store = MemoryStore()
    run_row_check(
        config,
        store,
        run_id="run",
        source_factory=lambda _value: Source({"all": 2}),
    )
    pair["column_types"] = {"AMOUNT": "numeric"}

    assert reuse_row_check(config, store, run_id="run", pair_names=["p"])
    assert store.get_json(left_row_path("p"))["counts"] == {"all": 2}
    assert store.get_json(plan_path("p"))["column_types"] == {"AMOUNT": "numeric"}


def test_reused_left_counts_reject_sampled_key_type_changes():
    pair = {
        "partition": "all",
        "left": {"source": "oracle", "table": "L"},
        "right": {"source": "databricks", "table": "R"},
        "key": "ID",
        "col_map": {"ID": "id", "AMOUNT": "amount"},
        "checks": ["hash_sum"],
    }
    config = {"pairs": {"p": pair}}
    store = MemoryStore()
    run_row_check(
        config,
        store,
        run_id="run",
        source_factory=lambda _value: Source({"all": 2}),
    )
    pair["column_types"] = {"ID": "numeric"}

    assert reuse_row_check(config, store, run_id="run", pair_names=["p"]) is None


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
    }
    store = MemoryStore()
    run_row_check(
        {"pairs": {"p": pair}},
        store,
        run_id="run",
        source_factory=lambda _value: Source(counts),
    )
    assert store.get_json(left_row_path("p"))["counts"] == counts
    plan = store.get_json(plan_path("p"))
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
    }
    store = MemoryStore()

    def factory(config):
        seen.append(config)
        return Source({"all": 2})

    run_row_check(
        {"pairs": {"p": pair}},
        store,
        run_id="run",
        source_factory=factory,
    )
    assert seen[0]["partition"] == "all"
    assert not store.exists(plan_path("p"))


def test_run_row_check_aborts_while_the_count_is_running(monkeypatch):
    started, release = threading.Event(), threading.Event()

    class BlockedSource(Source):
        def partition_counts(self, *_args, **_kwargs):
            started.set()
            release.wait(5)
            return self.counts

    def abort():
        if started.is_set():
            raise RuntimeError("Right failed")

    pair = {
        "partition": "all",
        "left": {"source": "oracle", "table": "L"},
        "right": {"source": "databricks", "table": "R"},
        "col_map": {"A": "a"},
        "checks": ["row_count"],
    }
    monkeypatch.setattr(row_check, "ROW_COUNT_LOG_INTERVAL", 0.01)
    outcome = {}
    store = MemoryStore()

    def run():
        try:
            run_row_check(
                {"pairs": {"p": pair}},
                store,
                run_id="run",
                source_factory=lambda _value: BlockedSource({"all": 2}),
                abort_if=abort,
            )
        except RuntimeError as exc:
            outcome["error"] = str(exc)

    worker = threading.Thread(target=run)
    worker.start()
    worker.join(5)
    assert not worker.is_alive() and outcome["error"] == "Right failed"
    assert not release.is_set()
    release.set()


def test_run_row_check_heartbeats_while_the_count_is_running(monkeypatch):
    ticks = threading.Semaphore(0)

    class SlowSource(Source):
        def partition_counts(self, *_args, **_kwargs):
            for _ in range(3):
                ticks.acquire(timeout=5)
            return self.counts

    monkeypatch.setattr(row_check, "ROW_COUNT_LOG_INTERVAL", 0.01)
    store = MemoryStore()
    beats = []
    original = store.heartbeat

    def heartbeat(now=None):
        beats.append(1)
        ticks.release()
        return original(now)

    monkeypatch.setattr(store, "heartbeat", heartbeat)
    pair = {
        "partition": "all",
        "left": {"source": "oracle", "table": "L"},
        "right": {"source": "databricks", "table": "R"},
        "col_map": {"A": "a"},
        "checks": ["row_count"],
    }
    run_row_check(
        {"pairs": {"p": pair}},
        store,
        run_id="run",
        source_factory=lambda _value: SlowSource({"all": 2}),
    )
    assert len(beats) >= 3 and get_left_state(store).get("heartbeat_epoch_s") is not None
