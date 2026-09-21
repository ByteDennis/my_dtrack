from conftest import MemoryStore

from dtrack_distributed.workflows.stage1 import run_stage1


def test_stage1_orders_all_preflights_then_right_counts_then_left_counts():
    calls = []
    config = {
        "pairs": {
            name: {
                "left": {"source": "csv", "path": "unused", "date_col": "dt"},
                "right": {"source": "databricks", "table": name, "date_col": "dt"},
                "col_map": {"amount": "amount"},
            }
            for name in ("a", "b")
        }
    }

    class Fake:
        def __init__(self, side, name):
            self.side, self.name = side, name

        def preflight(self):
            calls.append(("preflight", self.name))
            return {"ok": True}

        def partition_counts(self, *_args):
            calls.append((self.side, self.name))
            return {"2026-01-01": 1}

    store = MemoryStore()
    run_stage1(
        config,
        store,
        run_id="run",
        source_factory=lambda row: Fake("left", row.get("table", "left")),
        target_factory=lambda row: Fake("right", row["table"]),
    )
    kinds = [kind for kind, _name in calls]
    assert max(i for i, kind in enumerate(kinds) if kind == "preflight") < min(
        i for i, kind in enumerate(kinds) if kind == "right"
    )
    assert max(i for i, kind in enumerate(kinds) if kind == "right") < min(
        i for i, kind in enumerate(kinds) if kind == "left"
    )
    assert store.exists("manifest/a.json")
    assert store.exists("stage1/_CLOSED")


def test_stage1_materializes_fixed_sample_keys_before_publishing_manifest(tmp_path):
    pair = {
        "left": {"source": "csv", "path": "unused", "date_col": "dt"},
        "right": {"source": "databricks", "table": "right", "date_col": "dt"},
        "key": "id",
        "sample": 1,
        "col_map": {"id": "id", "amount": "amount"},
    }

    class Fake:
        def preflight(self):
            return {"ok": True}

        def partition_counts(self, *_args):
            return {"2026-01-01": 2}

        def pull_window(self, _partitions, _columns, output, *, key_columns):
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text("dt,id\n2026-01-01,10\n2026-01-01,20\n")
            return output

    store = MemoryStore()
    run_stage1(
        {"pairs": {"p": pair}},
        store,
        run_id="run",
        source_factory=lambda _row: Fake(),
        target_factory=lambda _row: Fake(),
        work_dir=tmp_path,
    )
    window = store.get_json("manifest/p.json")["windows"][0]
    assert len(window["key_tuples"]) == 1
