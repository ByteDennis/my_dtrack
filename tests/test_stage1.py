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
