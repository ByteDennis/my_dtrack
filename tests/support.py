from __future__ import annotations

import importlib.util
import json
from datetime import datetime
from pathlib import Path

from dtrack_right.common.protocol import (
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

ROOT = Path(__file__).resolve().parents[1]

PAIRS_CONFIG = {
    "pairs": {
        "orders": {
            "left": {
                "source": "csv",
                "path": "<left>/ORDERS.csv",
                "date_col": "BUSINESS_DATE",
                "date_type": "string",
            },
            "right": {
                "source": "databricks",
                "catalog": "main",
                "schema": "sales",
                "table": "orders",
                "date_col": "business_date",
                "date_type": "string",
            },
            "fromDate": "2026-01-01",
            "toDate": "2026-02-28",
            "vintage": "month",
            "key": "ORDER_ID",
            "key_families": {"ORDER_ID": "numeric"},
            "sample": 0.25,
            "col_map": {
                "ORDER_ID": "order_id",
                "AMOUNT": "amount",
                "STATUS": "status",
                "REGION": "region",
            },
        },
        "customers": {
            "left": {
                "source": "csv",
                "path": "<left>/CUSTOMERS.csv",
                "date_col": "SNAPSHOT_DT",
                "date_type": "string",
            },
            "right": {
                "source": "databricks",
                "catalog": "risk",
                "schema": "crm",
                "table": "customers",
                "date_col": "snapshot_dt",
                "date_type": "string",
            },
            "fromDate": "2026-01-01",
            "toDate": "2026-01-31",
            "vintage": "month",
            "key": "CUST_ID",
            "key_families": {"CUST_ID": "numeric"},
            "sample": 40,
            "strict": True,
            "col_map": {"CUST_ID": "cust_id", "TIER": "tier", "BALANCE": "balance"},
        },
    }
}


# >>> synthetic left/right CSVs from examples/e2e/make_synthetic.py written under `root` <<< #
def synthetic_data(root: Path) -> dict:
    spec = importlib.util.spec_from_file_location(
        "make_synthetic", ROOT / "examples/e2e/make_synthetic.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.build(root)


# >>> the e2e pair config with the left CSV paths pointed at `left_dir` <<< #
def pairs_config(left_dir: Path) -> dict:
    config = json.loads(json.dumps(PAIRS_CONFIG))
    for pair in config["pairs"].values():
        pair["left"]["path"] = pair["left"]["path"].replace("<left>", str(left_dir))
    return config


class MemoryStore:
    root_uri = "s3://test/run"

    def __init__(self):
        self.objects = {}
        self.events = []

    def put_json(self, key, value):
        self.events.append(("put", key))
        self.objects[key] = json.loads(json.dumps(value))
        return key

    def put_bytes(self, key, body, *, content_type):
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
        base = prefix.strip("/")
        start = f"{base}/" if base else ""
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
        return max(
            0,
            len(list_units(self)) - len(list_results(self)) - len(list_failures(self)),
        )

    def close_row_check(self, pairs):
        return self.put_json(
            ROW_CHECK_CLOSED,
            {"pairs": sorted(pairs), "closed_at": timestamp(self)},
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


# >>> DB-API stand-in: records every statement, answers with responder(sql) -> (columns, rows) <<< #
class FakeConnection:
    def __init__(self, responder=lambda sql: ([], [])):
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


# >>> SparkSession stand-in: records spark.sql text, answers with responder(sql) -> rows <<< #
class FakeSpark:
    def __init__(self, responder=lambda sql: []):
        self.responder = responder
        self.statements = []

    def sql(self, text):
        self.statements.append(text)
        return FakeFrame(self.responder(text))


# >>> the store contract both S3Store and VolumeStore must honour <<< #
def assert_store_contract(store, *, expected_timestamp=None):
    def without_timestamp(document, field):
        value = document.pop(field)
        if expected_timestamp is None:
            assert datetime.fromisoformat(value).tzinfo is not None
        else:
            assert value == expected_timestamp
        return document

    assert store.json_bytes({"x": 1, "a": 2}) == b'{"a":2,"x":1}'
    assert store.put_json("a/b.json", {"x": 1}).endswith("a/b.json")
    store.put_bytes("a/c.csv", b"h\n1\n", content_type="text/csv")
    assert store.get_json("a/b.json") == {"x": 1} and store.get_bytes("a/c.csv") == b"h\n1\n"
    assert store.exists("a/b.json") and not store.exists("a/missing.json")
    assert store.list_keys("a/") == ["a/b.json", "a/c.csv"]
    assert store.list_keys("a/", start_after="a/b.json") == ["a/c.csv"]
    assert store.list_keys("nothing/") == []
    first = store.commit_unit(1, "u1", {"v": 1}, {"pair": "p"})
    store.commit_unit(2, "u2", {"v": 2}, {"pair": "p"})
    assert without_timestamp(first, "left_ready_at") == {
        "pair": "p",
        "unit_id": "u1",
        "sequence": 1,
        "left_statistics": {"v": 1},
    }
    assert [key for key, _ in store.list_json("pairs/p/units/")] == [
        "pairs/p/units/00000001-u1.json",
        "pairs/p/units/00000002-u2.json",
    ]
    assert store.ready_unclaimed() == 2
    assert store.put_result("u1", {"verdict": "match"}).endswith(
        "pairs/p/col_check/results/u1.json"
    )
    assert without_timestamp(
        store.get_json("pairs/p/col_check/results/u1.json"), "completed_at"
    ) == {
        "pair": "p",
        "unit_id": "u1",
        "verdict": "match",
    }
    assert store.put_dead("u2", {"error": "x"}).endswith("pairs/p/col_check/failures/u2.json")
    assert without_timestamp(store.get_json("pairs/p/col_check/failures/u2.json"), "failed_at") == {
        "error": "x",
        "pair": "p",
        "unit_id": "u2",
    }
    assert store.ready_unclaimed() == 0
    assert store.heartbeat_age() is None
    assert store.heartbeat(now=100.0).endswith("_status/heartbeat.json")
    assert without_timestamp(store.get_json("_status/heartbeat.json"), "updated_at") == {
        "epoch_s": 100.0,
    }
    assert store.heartbeat_age(now=130.0) == 30.0 and store.heartbeat_age(now=50.0) == 0.0
    assert store.heartbeat_age() > 0
    assert store.close_row_check(["b", "a"]).endswith("_status/row_check.closed.json")
    assert store.close_manifests(["b", "a"], 4, {"p": "plan-1"}).endswith(
        "_status/col_check.closed.json"
    )
    assert without_timestamp(store.get_json("_status/row_check.closed.json"), "closed_at") == {
        "pairs": ["a", "b"],
    }
    assert without_timestamp(store.get_json("_status/col_check.closed.json"), "closed_at") == {
        "expected_units": 4,
        "pairs": ["a", "b"],
        "plan_ids": {"p": "plan-1"},
    }


# >>> a manifest whose every partition matched, built from real left counts <<< #
def all_match_manifest(build_manifest, run_id, name, pair, counts):
    verdict = {
        "gate_decision": {"passed": True},
        "partitions": {
            day: {"left": n, "right": n, "verdict": "match"} for day, n in counts.items()
        },
    }
    return build_manifest(run_id, name, pair, verdict)


# >>> fake AWS credentials and no Moto endpoint, so moto.mock_aws intercepts boto3 in-process <<< #
def mock_aws_env(monkeypatch):
    for name, value in {
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_DEFAULT_REGION": "us-east-1",
    }.items():
        monkeypatch.setenv(name, value)
    for name in ("DTRACK_S3_ENDPOINT_URL", "AWS_PROFILE", "AWS_SESSION_TOKEN"):
        monkeypatch.delenv(name, raising=False)


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


# >>> a pairs/<pair>/row_check/left.json document as the Left side writes it <<< #
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


# >>> Right target stand-in: fixed counts and statistics, records every window it was asked for <<< #
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


# >>> boto3 S3 client stand-in: records calls, serves canned list pages, raises on head_object <<< #
class RecordingClient:
    def __init__(self, pages=None, head_error=None):
        self.calls = []
        self.pages = list(pages or [])
        self.head_error = head_error

    def put_object(self, **kwargs):
        self.calls.append(("put_object", kwargs))

    def head_object(self, **kwargs):
        raise self.head_error

    def list_objects_v2(self, **kwargs):
        self.calls.append(("list_objects_v2", dict(kwargs)))
        return self.pages.pop(0)


ORACLE_IMAGE = "gvenzl/oracle-free:slim-faststart"
ORACLE_TABLES = {
    "ORDERS": (
        (
            "CREATE TABLE ORDERS (BUSINESS_DATE DATE, ORDER_ID NUMBER(12), AMOUNT NUMBER(10,2), "
            "STATUS VARCHAR2(20), REGION VARCHAR2(4))"
        ),
        "INSERT INTO ORDERS VALUES (TO_DATE(:1, 'YYYY-MM-DD'), :2, :3, :4, :5)",
    ),
    "CUSTOMERS": (
        (
            "CREATE TABLE CUSTOMERS (SNAPSHOT_DT DATE, CUST_ID NUMBER(12), TIER VARCHAR2(20), "
            "BALANCE NUMBER(12,2))"
        ),
        "INSERT INTO CUSTOMERS VALUES (TO_DATE(:1, 'YYYY-MM-DD'), :2, :3, :4)",
    ),
}


# >>> an Oracle Free container via testcontainers; yields connect kwargs (user, password, dsn) <<< #
def oracle_container():
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.waiting_utils import wait_for_logs

    container = (
        DockerContainer(ORACLE_IMAGE)
        .with_env("ORACLE_PASSWORD", "dtrack")
        .with_env("APP_USER", "dtrack")
        .with_env("APP_USER_PASSWORD", "dtrack")
        .with_exposed_ports(1521)
    )
    container.start()
    try:
        wait_for_logs(container, "DATABASE IS READY TO USE!", timeout=600)
        host, port = container.get_container_host_ip(), container.get_exposed_port(1521)
        yield {"user": "dtrack", "password": "dtrack", "dsn": f"{host}:{port}/FREEPDB1"}
    finally:
        container.stop()


# >>> load the synthetic left CSVs into Oracle tables of the same name <<< #
def load_oracle_tables(oracle: dict, left_dir: Path, tables=("ORDERS", "CUSTOMERS")) -> None:
    import csv
    import time

    import oracledb

    for attempt in range(30):
        try:
            connection = oracledb.connect(**oracle)
            break
        except oracledb.Error:
            if attempt == 29:
                raise
            time.sleep(2)
    with connection:
        cursor = connection.cursor()
        for table in tables:
            create, insert = ORACLE_TABLES[table]
            cursor.execute(create)
            with (left_dir / f"{table}.csv").open() as handle:
                cursor.executemany(insert, [list(row.values()) for row in csv.DictReader(handle)])
        connection.commit()


EXACT_FIELDS = ("col_type", "n_total", "n_missing", "n_unique", "hash_sum")
EXACT_FIELDS += ("min_val", "max_val", "length_max")
CLOSE_FIELDS = ("mean", "std", "length_avg")


# >>> two statistics dicts agree: exact for counts/hashes/extremes, 1e-9 relative for floats <<< #
def assert_stats_agree(columns, got, want):
    for column in columns:
        for field in EXACT_FIELDS:
            assert str(got[column][field]) == str(want[column][field]), (
                column,
                field,
                got[column][field],
                want[column][field],
            )
        for field in CLOSE_FIELDS:
            a, b = got[column][field], want[column][field]
            assert (a is None) == (b is None), (column, field)
            if a is not None:
                assert abs(float(a) - float(b)) <= 1e-9 * max(1.0, abs(float(b))), (column, field)


# >>> the Left pairs read from the Oracle container; the Right side is only named, never contacted <<< #
def oracle_config(oracle, right_tables=None):
    right = right_tables or {"orders": "main.sales.orders", "customers": "main.crm.customers"}

    def side(table):
        catalog, schema, name = right[table].split(".")
        return {"source": "databricks", "catalog": catalog, "schema": schema, "table": name}

    return {
        "pairs": {
            "orders": {
                "left": {"source": "oracle", "table": "ORDERS", "date_col": "BUSINESS_DATE"}
                | {"date_type": "date", **oracle},
                "right": side("orders") | {"date_col": "business_date", "date_type": "string"},
                "fromDate": "2026-01-01",
                "toDate": "2026-02-28",
                "vintage": "month",
                "key": "ORDER_ID",
                "key_families": {"ORDER_ID": "numeric"},
                "sample": 0.25,
                "col_map": {
                    "ORDER_ID": "order_id",
                    "AMOUNT": "amount",
                    "STATUS": "status",
                    "REGION": "region",
                },
            },
            "customers": {
                "left": {"source": "oracle", "table": "CUSTOMERS", "date_col": "SNAPSHOT_DT"}
                | {"date_type": "date", **oracle},
                "right": side("customers") | {"date_col": "snapshot_dt", "date_type": "string"},
                "fromDate": "2026-01-01",
                "toDate": "2026-01-31",
                "vintage": "month",
                "key": "CUST_ID",
                "key_families": {"CUST_ID": "numeric"},
                "sample": 40,
                "strict": True,
                "col_map": {"CUST_ID": "cust_id", "TIER": "tier", "BALANCE": "balance"},
            },
        }
    }
