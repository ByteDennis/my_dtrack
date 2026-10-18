from support import MemoryStore

from pair_recon_left.backends.sources import write_rows
from pair_recon_left.common.checks.col_check import ColCheckPipeline, LeftRuntime
from pair_recon_left.common.model.manifest import unit_id


def manifest():
    return {
        "protocol_version": 2,
        "run_id": "run",
        "pair": "pair",
        "sample": {"mode": "all"},
        "vintage": "all",
        "partition_mode": "all",
        "key_cols": [],
        "right_key_cols": [],
        "key_families": [],
        "columns": ["amount"],
        "col_map": {"amount": "amount"},
        "windows": [{"bucket": "all", "partitions": ["all"], "n_left": 2}],
        "left": {"source": "oracle"},
        "right": {"source": "databricks", "table": "orders"},
        "expected_units": 1,
    }


def test_pipeline():
    store = MemoryStore()
    row = manifest()
    store.put_json("pairs/pair/plan.json", row)
    stats = {"amount": {"col_type": "numeric", "n_unique": 2, "hash_sum": "3"}}
    count = ColCheckPipeline(
        store,
        pull_window=lambda *_args: "pulled",
        compute_stats=lambda *_args: stats,
    ).run([row])
    uid = unit_id("pair", "all", "amount")
    assert count == 1 and store.exists(f"pairs/pair/units/00000001-{uid}.json")


def test_cache(tmp_path):
    row = {**manifest(), "version": 2, "plan_id": "one"}
    calls = []

    class Source:
        def pull_window(self, _partitions, columns, output, **_options):
            calls.append(columns)
            return write_rows(output, columns, [["1"]])

    runtime = LeftRuntime(tmp_path)
    runtime.sources["pair"] = Source()
    window = row["windows"][0]
    runtime.pull_window(row, window)
    runtime.pull_window(row, window)
    assert calls == [["amount"]]
