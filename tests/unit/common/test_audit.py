from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

from dtrack_left.common.audit import SqlAudit as LeftSqlAudit
from dtrack_right.backends.store import VolumeStore
from dtrack_right.common.audit import SqlAudit as RightSqlAudit


def test_left_and_right_sql_events_merge_into_one_run_audit(tmp_path):
    store = VolumeStore(tmp_path / "run")
    left = LeftSqlAudit(store, "left", pair="orders")
    right = RightSqlAudit(store, "right", pair="orders")

    left_key = left.record("SELECT * FROM APP.ORDERS", backend="oracle")
    right_key = right.record("SELECT * FROM main.sales.orders", backend="spark")
    assert not store.exists("audit.txt")
    right.refresh()

    assert left_key and right_key
    assert len(store.list_keys("audit/")) == 2
    text = store.get_bytes("audit.txt").decode()
    assert "side=left" in text and "backend=oracle" in text
    assert "side=right" in text and "backend=spark" in text
    assert "operation=query" in text and "pair=orders" in text
    assert "policy=first_per_shape" in text
    assert "SELECT * FROM APP.ORDERS" in text
    assert "SELECT * FROM main.sales.orders" in text


def test_sql_audit_failure_is_recorded_but_never_raises():
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


def test_sql_audit_metadata_is_plain_and_serializable(tmp_path):
    store = VolumeStore(tmp_path / "run")
    audit = LeftSqlAudit(store, "local")
    assert audit.record("SHOW TABLES", backend="duckdb", operation="inspect")
    assert audit.refresh()
    assert SimpleNamespace(text=store.get_bytes("audit.txt").decode()).text.endswith(
        "SHOW TABLES\n\n"
    )


def test_repeated_window_literals_keep_only_the_first_query_shape(tmp_path):
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
    assert len(store.list_keys("audit/")) == 2
    audit.refresh()
    text = store.get_bytes("audit.txt").decode()
    assert "2026-01-01" in text and "2026-02-01" in text


def test_multithreaded_same_shape_has_one_first_winner(tmp_path):
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
    assert len(store.list_keys("audit/")) == 1


def test_mutating_sql_is_never_sampled_and_internal_maintenance_is_omitted(tmp_path):
    store = VolumeStore(tmp_path / "run")
    audit = LeftSqlAudit(store, "local")
    assert audit.record("DELETE FROM T WHERE ID = 1", backend="duckdb")
    assert audit.record("DELETE FROM T WHERE ID = 2", backend="duckdb")
    assert audit.record("SHOW TABLES", backend="duckdb", operation="landing_inspect") is None
    assert len(store.list_keys("audit/")) == 2
