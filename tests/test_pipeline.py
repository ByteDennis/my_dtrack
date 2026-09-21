from conftest import MemoryStore

from dtrack_distributed.domain.manifest import unit_id
from dtrack_distributed.workflows.consumer import Consumer
from dtrack_distributed.workflows.producer import ProducerPipeline
from dtrack_distributed.workflows.scheduler import DrainScheduler


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


STATS = {
    "col_type": "numeric",
    "n_total": 2,
    "n_missing": 0,
    "n_unique": 2,
    "hash_sum": "3",
    "mean": 1.5,
    "std": 0.7,
    "min_val": "1",
    "max_val": "2",
    "length_avg": None,
    "length_max": None,
}


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


def test_scheduler_prioritizes_ready_compare_over_prefetch():
    scheduler = DrainScheduler()
    row = manifest()
    scheduler.add_manifest(row)
    pointer = {"pair": "pair", "window": "2026-01", "unit_id": "u"}
    scheduler.add_unit(pointer)
    first = scheduler.next_task()
    assert first.kind == "prefetch"
    scheduler.mark_right_ready("pair", "2026-01", {"amount": STATS})
    assert scheduler.next_task().kind == "compare"


def test_consumer_completes_a_matching_unit():
    store = MemoryStore()
    row = manifest()
    store.put_json("manifest/pair.json", row)
    uid = unit_id("pair", "2026-01", "amount")
    store.commit_unit(1, uid, STATS, {"pair": "pair", "window": "2026-01", "column": "amount"})
    store.close_manifests(["pair"], 1)
    store.heartbeat()
    consumer = Consumer(
        store, fetch_right=lambda _manifest, _window: {"amount": STATS}, poll_interval=0
    )
    results = consumer.run()
    assert results[0]["verdict"] == "match"
    assert store.exists(f"results/{uid}.json")
