import sys
import threading
from types import SimpleNamespace

import pytest
from support import FakeConnection

from pair_recon_left.backends import sources
from pair_recon_left.backends.sources import (
    CsvSource,
    HadoopSource,
    OracleSource,
    source_from_config,
)
from pair_recon_left.common.model.hashing import HASH_BUCKET_SPACE
from pair_recon_left.common.runtime.stop import StopRequested, StopSignal

LDAP = "ldap://ldap.corp:389/PRODSVC,cn=OracleContext,dc=corp,dc=com"
ORACLE_ENV = ("ORACLE_SVC", "ORACLE_DSN", "LDAP_DSN")


@pytest.mark.parametrize(
    ("source", "kind"),
    [("oracle", OracleSource), ("hadoop", HadoopSource), ("csv", CsvSource)],
)
def test_factory(source, kind):
    config = {"source": source, "path": "rows.csv"} if source == "csv" else {"source": source}
    assert isinstance(source_from_config(config), kind)


def test_hadoop(monkeypatch):
    calls = []
    monkeypatch.setitem(
        sys.modules,
        "pyodbc",
        SimpleNamespace(
            connect=lambda value, **options: calls.append((value, options)) or object()
        ),
    )
    source = HadoopSource({"source": "hadoop", "dsn": "Hive_Prod"})
    source.connect()
    assert calls == [("DSN=Hive_Prod", {"autocommit": True})]


def test_csv(tmp_path):
    path = tmp_path / "rows.csv"
    path.write_text("ID,VALUE\n1,文字\n2,✓\n", encoding="utf-8")
    source = CsvSource({"source": "csv", "path": str(path), "partition": "all"})
    assert source.health_check()["ok"]
    assert source.partition_counts() == {"all": 2}
    columns, rows = source.query(f"SELECT * FROM {source.relation()}")
    assert columns == ["ID", "VALUE"] and rows == [("1", "文字"), ("2", "✓")]
    output, count = source.query_to_csv(
        f"SELECT * FROM {source.relation()}", tmp_path / "copy.csv", columns
    )
    assert count == 2 and "文字" in output.read_text(encoding="utf-8")


def test_credentials(monkeypatch):
    monkeypatch.setenv("ORACLE_SVC", "pb30:svc30")
    monkeypatch.setenv("pb30_USR", "reader")
    monkeypatch.setenv("pb30_PWD", "secret")
    source = OracleSource({"source": "oracle", "conn_macro": "pb30", "table": "T"})
    assert (source.user, source.password, source.resolve_dsn()) == ("reader", "secret", "svc30")


def test_credentials_ignore_retired_fallbacks(monkeypatch):
    for name in ORACLE_ENV + ("ORACLE_USER", "ORACLE_PASSWORD", "pb30_USR", "pb30_PWD"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("PAIR_RECON_ORACLE_MACROS", "pb30:svc30")
    monkeypatch.setenv("PCDS_USR", "old-user")
    monkeypatch.setenv("pb30_pwd", "old-secret")
    source = OracleSource({"source": "oracle", "conn_macro": "pb30", "table": "T"})
    assert (source.user, source.password, source.service, source.resolve_dsn()) == (
        None,
        None,
        None,
        None,
    )


def test_ldap_dsn_resolves_once_without_a_service(monkeypatch):
    for name in ORACLE_ENV:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("LDAP_DSN", LDAP)
    lookups = []
    monkeypatch.setattr(
        sources, "solve_ldap", lambda value: lookups.append(value) or "(DESCRIPTION=x)"
    )
    connects = []
    monkeypatch.setitem(
        sys.modules,
        "oracledb",
        SimpleNamespace(connect=lambda **options: connects.append(options) or object()),
    )
    source = OracleSource(
        {"source": "oracle", "conn_macro": "pb30", "user": "reader", "password": "secret"}
    )
    assert source.service is None and source.dsn is None
    assert source.resolve_dsn() == "(DESCRIPTION=x)"
    source.connect()
    assert connects == [{"user": "reader", "password": "secret", "dsn": "(DESCRIPTION=x)"}]
    assert lookups == [LDAP]


def test_ldap_dsn_template_takes_service_from_macro(monkeypatch):
    for name in ORACLE_ENV:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("ORACLE_SVC", "pb30:svc30")
    monkeypatch.setenv("pb30_LDAP_DSN", "ldap://h/{service},cn=OracleContext,dc=x")
    monkeypatch.setattr(sources, "solve_ldap", lambda value: f"resolved:{value}")
    source = OracleSource({"source": "oracle", "conn_macro": "pb30", "user": "u", "password": "p"})
    assert source.resolve_dsn() == "resolved:ldap://h/svc30,cn=OracleContext,dc=x"
    bare = OracleSource(
        {
            "source": "oracle",
            "conn_macro": "other",
            "user": "u",
            "password": "p",
            "ldap_dsn": "ldap://h/{service},cn=OracleContext,dc=x",
        }
    )
    with pytest.raises(ValueError, match="no Oracle service"):
        bare.resolve_dsn()


def test_explicit_dsn_wins_over_ldap(monkeypatch):
    for name in ORACLE_ENV:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("LDAP_DSN", LDAP)
    monkeypatch.setattr(
        sources, "solve_ldap", lambda _value: pytest.fail("explicit dsn must skip LDAP")
    )
    source = OracleSource({"source": "oracle", "dsn": "db:1521/svc", "user": "u", "password": "p"})
    assert source.resolve_dsn() == "db:1521/svc"
    monkeypatch.delenv("LDAP_DSN")
    missing = OracleSource({"source": "oracle", "user": "u", "password": "p"})
    with pytest.raises(ValueError, match="dsn, service, or LDAP_DSN"):
        missing.connect()


def test_solve_ldap_reads_the_tns_descriptor(monkeypatch):
    searches = []
    directory = {
        "bound": True,
        "entries": [SimpleNamespace(orclNetDescString=SimpleNamespace(value="(DESCRIPTION=d)"))],
    }

    class Server:
        def __init__(self, host, get_info=None):
            self.host = host

    class Connection:
        result = "invalidCredentials"

        def __init__(self, server):
            self.server = server

        def bind(self):
            return directory["bound"]

        @property
        def entries(self):
            return directory["entries"]

        def search(self, base, query, attributes):
            searches.append((self.server.host, base, query, attributes))

    monkeypatch.setitem(
        sys.modules, "ldap3", SimpleNamespace(ALL="ALL", Connection=Connection, Server=Server)
    )
    assert sources.solve_ldap(LDAP) == "(DESCRIPTION=d)"
    assert searches == [
        ("ldap.corp:389", "cn=OracleContext,dc=corp,dc=com", "(cn=PRODSVC)", ["orclNetDescString"])
    ]
    directory["entries"] = []
    with pytest.raises(RuntimeError, match="no orclNetDescString for service 'PRODSVC'"):
        sources.solve_ldap(LDAP)
    directory["bound"] = False
    with pytest.raises(RuntimeError, match="LDAP bind to ldap.corp:389 failed"):
        sources.solve_ldap(LDAP)
    with pytest.raises(ValueError, match="unsupported LDAP_DSN"):
        sources.solve_ldap("ldaps://host/svc")


def test_query(tmp_path):
    connection = FakeConnection(lambda _sql: (["ID", "文字"], [(1, "✓")]))
    source = OracleSource({"source": "oracle", "table": "T"})
    source.connect = lambda: connection
    columns, rows = source.query("SELECT * FROM T")
    assert columns == ["ID", "文字"] and rows == [(1, "✓")]
    path, count = source.query_to_csv("SELECT * FROM T", tmp_path / "rows.csv", columns)
    assert count == 1 and "文字" in path.read_text(encoding="utf-8")


def test_oracle_query_to_csv_tunes_fetch_and_retries_connection_loss(monkeypatch, tmp_path, caplog):
    connections = []

    class Cursor:
        def __init__(self, fail):
            self.fail = fail
            self.rows = [(1,), (2,)]
            self.arraysize = None
            self.prefetchrows = None

        def execute(self, _sql):
            pass

        def fetchmany(self, size):
            assert size == 500 and str(tmp_path / "rows.csv") in sources.FETCH_PROGRESS
            if self.fail:
                self.fail = False
                raise RuntimeError("DPY-4011: database or network closed the connection")
            rows, self.rows = self.rows[:size], self.rows[size:]
            return rows

        def close(self):
            pass

    class Connection:
        def __init__(self, fail):
            self.cursor_value = Cursor(fail)
            self.closed = False

        def cursor(self):
            return self.cursor_value

        def close(self):
            self.closed = True

    def connect():
        connection = Connection(fail=not connections)
        connections.append(connection)
        return connection

    sleeps = []
    monkeypatch.setattr(sources.time, "sleep", sleeps.append)
    monkeypatch.setattr(sources.random, "uniform", lambda _start, _end: 0)
    source = OracleSource(
        {
            "source": "oracle",
            "table": "T",
            "arraysize": 500,
            "prefetchrows": 250,
            "fetch_batch_size": 500,
            "fetch_retries": 1,
        }
    )
    source.connect = connect

    with caplog.at_level("INFO", logger="pair_recon.left.sources"):
        path, count = source.query_to_csv("SELECT ID FROM T", tmp_path / "rows.csv", ["ID"])

    assert count == 2 and path.read_text(encoding="utf-8") == "ID\n1\n2\n"
    assert len(connections) == 2 and all(connection.closed for connection in connections)
    assert connections[1].cursor_value.arraysize == 500
    assert connections[1].cursor_value.prefetchrows == 250
    assert sleeps == [5]
    assert "retrying from the beginning" in caplog.text
    assert "throughput=" in caplog.text and "first_batch=" in caplog.text
    assert not sources.FETCH_PROGRESS


def test_oracle_query_to_csv_does_not_retry_non_connection_errors(monkeypatch, tmp_path):
    source = OracleSource({"source": "oracle", "table": "T"})
    calls = []

    def connect():
        calls.append(True)
        raise RuntimeError("ORA-00942: table or view does not exist")

    source.connect = connect
    monkeypatch.setattr(
        sources.time, "sleep", lambda _delay: pytest.fail("non-transient errors must not retry")
    )
    with pytest.raises(RuntimeError, match="ORA-00942"):
        source.query_to_csv("SELECT * FROM T", tmp_path / "rows.csv", ["ID"])
    assert len(calls) == 1


@pytest.mark.parametrize(
    ("config", "expected"),
    [
        ({"partition": "all"}, {"all": 7}),
        ({"date_col": "D", "date_type": "date"}, {"2026-01-01": 7}),
        (
            {"date_col": "D", "date_type": "date", "partition": "month"},
            {"2026-01-01": 7},
        ),
    ],
)
def test_counts(config, expected):
    def answer(sql):
        if "GROUP BY" in sql:
            return ["D", "N"], [("2026-01-01", 7)]
        return ["N"], [(7,)]

    connection = FakeConnection(answer)
    source = OracleSource({"source": "oracle", "table": "T", **config})
    source.connect = lambda: connection
    assert source.partition_counts() == expected
    source.partition_counts(
        sample={"mode": "fraction", "threshold": 10},
        key_columns=["ID"],
        key_families=["string"],
    )
    assert "STANDARD_HASH" in connection.statements[-1]


def test_query_to_csv_cancels_the_running_statement_on_stop(tmp_path):
    started, release = threading.Event(), threading.Event()

    class BlockedCursor:
        def __init__(self):
            self.description = [("ID",)]

        def execute(self, _sql):
            pass

        def fetchmany(self, _size):
            started.set()
            release.wait(5)
            raise RuntimeError("ORA-01013: user requested cancel of current operation")

        def close(self):
            pass

    class BlockedConnection:
        def __init__(self):
            self.cancels = 0

        def cursor(self):
            return BlockedCursor()

        def cancel(self):
            self.cancels += 1
            release.set()

        def close(self):
            pass

    connection = BlockedConnection()
    source = OracleSource({"source": "oracle", "table": "T"})
    source.connect = lambda: connection
    stop = StopSignal()
    source.set_stop(stop)
    outcome = {}

    def run():
        try:
            source.query_to_csv("SELECT ID FROM T", tmp_path / "rows.csv", ["ID"])
        except StopRequested as exc:
            outcome["error"] = exc

    worker = threading.Thread(target=run)
    worker.start()
    assert started.wait(2)
    stop.trigger("Right failed")
    worker.join(2)
    assert not worker.is_alive() and connection.cancels == 1
    assert "Right failed" in str(outcome["error"])
    assert not (tmp_path / "rows.csv.part").exists()


def test_oracle_connect_waits_out_the_session_limit(monkeypatch):
    for name in ORACLE_ENV:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("ORACLE_SESSION_RETRIES", "2")
    monkeypatch.setenv("ORACLE_SESSION_COOLDOWN_SECONDS", "7")
    sleeps = []
    monkeypatch.setattr(sources.time, "sleep", sleeps.append)
    attempts = []

    class Connection:
        closed = False

        def close(self):
            self.closed = True

    def connect(**_options):
        attempts.append(1)
        if len(attempts) < 3:
            raise RuntimeError("ORA-02391: exceeded simultaneous SESSIONS_PER_USER limit")
        return Connection()

    monkeypatch.setitem(sys.modules, "oracledb", SimpleNamespace(connect=connect))
    source = OracleSource(
        {
            "source": "oracle",
            "table": "T",
            "user": "u",
            "password": "p",
            "dsn": "d",
            "max_sessions": 1,
        }
    )
    connection = source.connect()
    assert len(attempts) == 3 and sleeps == [7.0, 7.0] and source._sessions._value == 0
    connection.close()
    connection.close()
    assert connection.closed and source._sessions._value == 1
    monkeypatch.setenv("ORACLE_SESSION_RETRIES", "0")
    attempts.clear()
    with pytest.raises(RuntimeError, match="ORA-02391"):
        source.connect()
    assert len(attempts) == 1 and source._sessions._value == 1


def test_ldap_failures_count_as_transient():
    class LDAPSocketOpenError(Exception):
        pass

    assert sources._transient_oracle_error(LDAPSocketOpenError("invalid server address"))
    assert not sources._transient_oracle_error(RuntimeError("ORA-00942: table does not exist"))


def test_csv_sample_rows_and_count_keys(tmp_path):
    path = tmp_path / "rows.csv"
    path.write_text("ID,VALUE\n1,a\n2,b\n3,c\n", encoding="utf-8")
    source = CsvSource({"source": "csv", "path": str(path), "partition": "all"})
    sample = {"mode": "fraction", "rate": 1.0, "threshold": HASH_BUCKET_SPACE}
    columns, rows = source.sample_rows(
        None, None, ["VALUE"], key_columns=["ID"], key_families=["numeric"], sample=sample, limit=2
    )
    assert columns == ["ID", "VALUE"] and len(rows) == 2
    matched = source.count_keys(
        None,
        None,
        key_columns=["ID"],
        key_families=["numeric"],
        sample=sample,
        key_tuples=[[row[0]] for row in rows] + [["9"]],
    )
    assert matched == 2
