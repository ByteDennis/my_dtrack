import json

from pair_recon_right.engine.common.model.protocol import (
    COL_CHECK_CLOSED,
    HEARTBEAT,
    ROW_CHECK_CLOSED,
    failure_path,
    list_failures,
    list_results,
    list_units,
    result_path,
    timestamp,
    unit_path,
)


class MemoryStore:
    root_uri = "memory://run"

    def __init__(self):
        self.objects = {}
        self.events = []

    def put_json(self, key, value):
        self.events.append(("put", key))
        self.objects[key] = json.loads(json.dumps(value))
        return key

    def put_bytes(self, key, body, *, content_type=""):
        self.events.append(("put", key))
        self.objects[key] = bytes(body)
        return key

    def get_json(self, key):
        return json.loads(json.dumps(self.objects[key]))

    def get_bytes(self, key):
        value = self.objects[key]
        return value if isinstance(value, bytes) else json.dumps(value).encode()

    def exists(self, key):
        return key in self.objects

    def list_keys(self, prefix, *, start_after=None):
        rows = sorted(key for key in self.objects if key.startswith(prefix))
        return [key for key in rows if start_after is None or key > start_after]

    def list_json(self, prefix, *, start_after=None):
        return [
            (key, self.get_json(key))
            for key in self.list_keys(prefix, start_after=start_after)
            if isinstance(self.objects[key], dict)
        ]

    def list_prefixes(self, prefix=""):
        start = f"{prefix.strip('/')}/" if prefix.strip("/") else ""
        return sorted(
            {
                key.removeprefix(start).split("/", 1)[0]
                for key in self.objects
                if key.startswith(start) and "/" in key.removeprefix(start)
            }
        )

    def delete_keys(self, keys):
        deleted = 0
        for key in keys:
            if key in self.objects:
                self.objects.pop(key)
                self.events.append(("delete", key))
                deleted += 1
        return deleted

    def clear(self):
        return self.delete_keys(list(self.objects))

    def heartbeat(self, now=None):
        return self.put_json(
            HEARTBEAT,
            {"epoch_s": 0 if now is None else now, "updated_at": timestamp(self)},
        )

    def heartbeat_age(self, now=None):
        return 0

    def commit_unit(self, sequence, unit_id, payload, pointer):
        row = {
            **pointer,
            "unit_id": unit_id,
            "sequence": sequence,
            "left_ready_at": timestamp(self),
            "left_statistics": payload,
        }
        self.put_json(unit_path(pointer["pair"], sequence, unit_id), row)
        return row

    def ready_unclaimed(self):
        return max(0, len(list_units(self)) - len(list_results(self)) - len(list_failures(self)))

    def close_row_check(self, pairs):
        return self.put_json(
            ROW_CHECK_CLOSED, {"pairs": sorted(pairs), "closed_at": timestamp(self)}
        )

    def close_manifests(self, pairs, expected_units, plan_ids=None):
        return self.put_json(
            COL_CHECK_CLOSED,
            {
                "pairs": sorted(pairs),
                "expected_units": expected_units,
                "plan_ids": plan_ids or {},
                "closed_at": timestamp(self),
            },
        )

    def put_result(self, unit_id, result):
        pair = result.get("pair") or next(
            value["pair"] for _key, value in list_units(self) if value["unit_id"] == unit_id
        )
        return self.put_json(
            result_path(pair, unit_id),
            {**result, "unit_id": unit_id, "pair": pair, "completed_at": timestamp(self)},
        )

    def put_dead(self, unit_id, result):
        pair = result.get("pair") or next(
            value["pair"] for _key, value in list_units(self) if value["unit_id"] == unit_id
        )
        return self.put_json(
            failure_path(pair, unit_id),
            {**result, "unit_id": unit_id, "pair": pair, "failed_at": timestamp(self)},
        )


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.description = None
        self.rows = []

    def execute(self, sql):
        self.connection.statements.append(sql)
        columns, self.rows = self.connection.responder(sql)
        self.description = [(name,) for name in columns] if columns else None

    def fetchall(self):
        return list(self.rows)

    def fetchmany(self, size):
        rows, self.rows = self.rows[:size], self.rows[size:]
        return rows

    def close(self):
        self.connection.cursors_closed += 1


class FakeConnection:
    def __init__(self, responder=lambda _sql: ([], [])):
        self.responder = responder
        self.statements = []
        self.cursors_closed = 0
        self.closed = False

    def cursor(self):
        return FakeCursor(self)

    def close(self):
        self.closed = True


class FakeRow(dict):
    def asDict(self):
        return dict(self)


class FakeFrame:
    def __init__(self, rows):
        self.rows = rows

    def collect(self):
        return list(self.rows)


class FakeSpark:
    def __init__(self, responder=lambda _sql: []):
        self.responder = responder
        self.statements = []

    def sql(self, text):
        self.statements.append(text)
        return FakeFrame(self.responder(text))


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
    "top_10": "",
}


def left_document(pair="p", counts=None, sample=0.5):
    side = {"left": {"source": "oracle", "table": "A", "date_col": "dt"}}
    side["right"] = {"source": "databricks", "table": "a", "date_col": "dt"}
    return {
        "run_id": "run",
        "pair": pair,
        **side,
        "bounds": {"from": "2026-01-01", "to": "2026-01-03"},
        "date_synonyms": {},
        "counts": counts or {"2026-01-01": 1, "2026-01-02": 1, "2026-01-03": 5},
        "pair_config": {
            **side,
            "col_map": {"amount": "amount"},
            "vintage": "month",
            "key": "id",
            "sample": sample,
        },
        "timing": {"left_count_s": 0.1},
    }


class FakeTarget:
    def __init__(self, counts=None, stats=None, healthy=True):
        self.counts = counts or {"2026-01-01": 1, "2026-01-02": 2}
        self.stats = stats or {"amount": STATS}
        self.healthy = healthy
        self.calls = []

    def preflight(self):
        return {"ok": True} if self.healthy else {"ok": False, "error": "TABLE_OR_VIEW_NOT_FOUND"}

    def partition_counts(self, *_args):
        return self.counts

    def window_statistics(self, manifest, window):
        self.calls.append((manifest["pair"], window["bucket"], window.get("key_tuples")))
        return self.stats
