import pytest
from conftest import MemoryStore

from dtrack_distributed.domain.manifest import unit_id
from dtrack_distributed.workflows.producer import ProducerPipeline, wait_for_manifests


def manifest():
    return {
        "protocol_version": 1,
        "run_id": "run",
        "pair": "pair",
        "sample": {"mode": "all"},
        "vintage": "month",
        "key_cols": [],
        "right_key_cols": [],
        "columns": ["amount"],
        "col_map": {"amount": "amount"},
        "windows": [{"bucket": "2026-01", "partitions": ["2026-01-01"], "n_left": 2}],
        "left": {"source": "csv", "date_col": "dt"},
        "right": {"source": "databricks", "date_col": "dt"},
        "strict": False,
        "expected_units": 1,
    }


STATS = {"col_type": "numeric", "n_total": 2, "n_missing": 0, "n_unique": 2, "hash_sum": "3"}


def test_producer_commits_payload_before_pointer_and_closes_only_when_complete():
    store = MemoryStore()
    row = manifest()
    store.put_json("manifest/pair.json", row)
    published = ProducerPipeline(
        store,
        pull_window=lambda _manifest, _window: "pulled",
        compute_stats=lambda _manifest, _window, _pulled: {"amount": STATS},
    ).run([row])
    uid = unit_id("pair", "2026-01", "amount")
    assert published == 1
    assert store.events.index(("put", f"units/{uid}.json")) < next(
        i for i, event in enumerate(store.events) if event[1].startswith("_INDEX/")
    )
    assert store.exists("manifest/_CLOSED")


def test_producer_skips_units_already_indexed():
    store = MemoryStore()
    row = manifest()
    store.put_json("manifest/pair.json", row)
    args = {
        "pull_window": lambda _m, _w: "pulled",
        "compute_stats": lambda _m, _w, _p: {"amount": STATS},
    }
    assert ProducerPipeline(store, **args).run([row]) == 1
    assert ProducerPipeline(store, **args).run([row]) == 0


def test_wait_for_manifests_returns_only_after_right_side_closes_stage1():
    store = MemoryStore()
    store.put_json("manifest/pair.json", manifest())
    store.put_json("manifest/pair.keys.json", {"pair": "pair", "windows": {}})
    with pytest.raises(TimeoutError):
        wait_for_manifests(store, timeout=0, poll_interval=0)
    store.put_json("stage1/_CLOSED", {"pairs": ["pair"]})
    assert [m["pair"] for m in wait_for_manifests(store, timeout=0, poll_interval=0)] == ["pair"]


def test_wait_for_manifests_fails_fast_on_right_preflight_failure():
    store = MemoryStore()
    store.put_json("_PREFLIGHT.json", {"ok": False, "pairs": {"pair": {"error": "no table"}}})
    with pytest.raises(RuntimeError, match="preflight failed"):
        wait_for_manifests(store, timeout=0, poll_interval=0)
