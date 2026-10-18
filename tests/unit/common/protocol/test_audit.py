from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

from pair_recon_left.common.model import audit as audit_module
from pair_recon_left.common.model.audit import SqlAudit as LeftSqlAudit
from pair_recon_right.engine.backends.store import VolumeStore
from pair_recon_right.engine.common.model.audit import SqlAudit as RightSqlAudit


def test_event(tmp_path):
    store = VolumeStore(tmp_path / "run")
    left = LeftSqlAudit(store, "left", pair="orders")
    right = RightSqlAudit(store, "right", pair="orders")

    left_key = left.record("SELECT * FROM APP.ORDERS", backend="oracle")
    right_key = right.record("SELECT * FROM main.sales.orders", backend="spark")
    assert not store.exists("_status/audit.sql")
    right.refresh()

    assert left_key and right_key
    assert len(store.list_keys("pairs/orders/audit/")) == 2
    text = store.get_bytes("_status/audit.sql").decode()
    assert text == "SELECT * FROM APP.ORDERS\n\nSELECT * FROM main.sales.orders\n\n"
    assert "-- timestamp=" not in text


def test_failure():
    failures = {}

    class BrokenStore:
        root_uri = "broken"

        def put_bytes(self, *_args, **_kwargs):
            raise OSError("audit unavailable")

        def put_json(self, key, value):
            failures[key] = value

    result = LeftSqlAudit(BrokenStore(), "left", pair="orders").record(
        "SELECT expensive FROM source", backend="oracle"
    )
    assert result is None
    assert len(failures) == 1
    failure = next(iter(failures.values()))
    assert failure["error"] == "audit unavailable" and failure["pair"] == "orders"


def test_metadata(tmp_path):
    store = VolumeStore(tmp_path / "run")
    audit = LeftSqlAudit(store, "local")
    assert audit.record("SHOW TABLES", backend="duckdb", operation="inspect")
    assert audit.refresh()
    assert SimpleNamespace(text=store.get_bytes("_status/audit.sql").decode()).text.endswith(
        "SHOW TABLES\n\n"
    )


def test_audit(tmp_path):
    store = VolumeStore(tmp_path / "run")
    store.put_json("_status/run.json", {"timezone": "Asia/Tokyo"})
    key = LeftSqlAudit(store, "left", pair="orders").record(
        "SELECT * FROM APP.ORDERS", backend="oracle", operation="pull_window"
    )
    header = store.get_bytes(key).decode().splitlines()[0]
    assert header.startswith("-- timestamp=") and "+09:00 side=left" in header
    assert " backend=oracle operation=pull_window" in header
    assert not any(
        field in header for field in ("event_id=", "audit_key=", "shape=", "policy=", "pair=")
    )


def test_shape(tmp_path):
    store = VolumeStore(tmp_path / "run")
    audit = LeftSqlAudit(store, "left", pair="orders")
    first = audit.record(
        "SELECT A FROM T WHERE DT BETWEEN '2026-01-01' AND '2026-01-31'",
        backend="oracle",
        operation="pull_window",
    )
    repeated = audit.record(
        "SELECT A FROM T WHERE DT BETWEEN '2026-02-01' AND '2026-02-28'",
        backend="oracle",
        operation="pull_window",
    )
    changed = audit.record(
        "SELECT A, B FROM T WHERE DT BETWEEN '2026-02-01' AND '2026-02-28'",
        backend="oracle",
        operation="pull_window",
    )
    assert first and repeated is None and changed
    assert len(store.list_keys("pairs/orders/audit/")) == 2
    audit.refresh()
    text = store.get_bytes("_status/audit.sql").decode()
    assert "2026-01-01" in text and "2026-02-01" in text


def test_shape2(tmp_path):
    store = VolumeStore(tmp_path / "run")
    audit = LeftSqlAudit(store, "left", pair="orders")
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(
            pool.map(
                lambda day: audit.record(
                    f"SELECT A FROM T WHERE DT = '{day}'",
                    backend="oracle",
                    operation="pull_window",
                ),
                [f"2026-01-{day:02d}" for day in range(1, 21)],
            )
        )
    assert sum(result is not None for result in results) == 1
    assert len(store.list_keys("pairs/orders/audit/")) == 1


def test_shape3(tmp_path):
    store = VolumeStore(tmp_path / "run")
    first = LeftSqlAudit(store, "left", pair="orders").record(
        "SELECT A FROM T WHERE DT = '2026-01-01'",
        backend="oracle",
        operation="pull_window",
    )
    audit_module._CAPTURED.clear()
    repeated = LeftSqlAudit(store, "left", pair="orders").record(
        "SELECT A FROM T WHERE DT = '2026-02-01'",
        backend="oracle",
        operation="pull_window",
    )

    assert first and repeated is None
    assert first.rsplit("/", 1)[-1].count("-") == 0
    assert store.list_keys("pairs/orders/audit/") == [first]


def test_long(tmp_path):
    store = VolumeStore(tmp_path / "run")
    sql = "SELECT " + ", ".join(f"source_column_{index} AS C{index}" for index in range(1001))
    sql += " FROM VERY_WIDE_RUNTIME_QUERY"

    key = LeftSqlAudit(store, "left", pair="wide").record(
        sql, backend="oracle", operation="pull_window"
    )
    recorded = store.get_bytes(key).decode()

    assert len(recorded) < 8000
    assert "997 attributes omitted from audit" in recorded
    assert "sha256" not in recorded
    assert "source_column_0 AS C0" in recorded and "source_column_1 AS C1" in recorded
    assert "source_column_999 AS C999" in recorded
    assert "source_column_1000 AS C1000" in recorded
    assert "source_column_500 AS C500" not in recorded


def test_sql(tmp_path):
    store = VolumeStore(tmp_path / "run")
    audit = LeftSqlAudit(store, "local")
    assert audit.record("DELETE FROM T WHERE ID = 1", backend="duckdb")
    assert audit.record("DELETE FROM T WHERE ID = 2", backend="duckdb")
    assert audit.record("SHOW TABLES", backend="duckdb", operation="landing_inspect") is None
    assert len(store.list_keys("_status/audit/")) == 2
