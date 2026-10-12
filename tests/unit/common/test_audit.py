from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

from dtrack_left.common.audit import SqlAudit as LeftSqlAudit
from dtrack_left.common.audit import prune_audit_history
from dtrack_right.backends.store import VolumeStore
from dtrack_right.common.audit import SqlAudit as RightSqlAudit


def test_left_and_right_sql_events_merge_into_one_run_audit(tmp_path):
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
    assert SimpleNamespace(text=store.get_bytes("_status/audit.sql").decode()).text.endswith(
        "SHOW TABLES\n\n"
    )


def test_detailed_audit_header_is_minimal_and_uses_the_run_timezone(tmp_path):
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
    assert len(store.list_keys("pairs/orders/audit/")) == 2
    audit.refresh()
    text = store.get_bytes("_status/audit.sql").decode()
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
    assert len(store.list_keys("pairs/orders/audit/")) == 1


def test_mutating_sql_is_never_sampled_and_internal_maintenance_is_omitted(tmp_path):
    store = VolumeStore(tmp_path / "run")
    audit = LeftSqlAudit(store, "local")
    assert audit.record("DELETE FROM T WHERE ID = 1", backend="duckdb")
    assert audit.record("DELETE FROM T WHERE ID = 2", backend="duckdb")
    assert audit.record("SHOW TABLES", backend="duckdb", operation="landing_inspect") is None
    assert len(store.list_keys("_status/audit/")) == 2


def test_detailed_audit_events_are_retained_for_only_the_latest_five_runs(tmp_path):
    runs = VolumeStore(tmp_path / "runs")
    for index in range(7):
        run = runs.child(f"run-{index}")
        run.put_bytes(
            f"_status/audit/2026010{index + 1}T000000000000Z-event.sql",
            f"-- audit_key={index}\nSELECT {index}\n\n".encode(),
            content_type="text/plain",
        )
        run.put_bytes(
            "_status/audit.sql", f"SELECT {index}\n".encode(), content_type="text/plain"
        )

    assert prune_audit_history(runs, keep=5) == ["run-1", "run-0"]
    assert not runs.child("run-0").list_keys("_status/audit/")
    assert not runs.child("run-1").list_keys("_status/audit/")
    assert not (tmp_path / "runs/run-0/_status/audit").exists()
    assert not (tmp_path / "runs/run-1/_status/audit").exists()
    assert all(
        runs.child(f"run-{index}").list_keys("_status/audit/") for index in range(2, 7)
    )
    assert all(runs.child(f"run-{index}").exists("_status/audit.sql") for index in range(7))
