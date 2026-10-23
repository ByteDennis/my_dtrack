import sys
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
    source = CsvSource({"source": "csv", "path": str(path), "partition_mode": "all"})
    assert source.preflight()["ok"]
    assert source.partition_counts() == {"all": 2}
    columns, rows = source.query(f"SELECT * FROM {source.relation()}")
    assert columns == ["ID", "VALUE"] and rows == [("1", "文字"), ("2", "✓")]
    output = source.query_to_csv(
        f"SELECT * FROM {source.relation()}", tmp_path / "copy.csv", columns
    )
    assert "文字" in output.read_text(encoding="utf-8")


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
    path = source.query_to_csv("SELECT * FROM T", tmp_path / "rows.csv", columns)
    assert "文字" in path.read_text(encoding="utf-8")


@pytest.mark.parametrize(
    ("config", "expected"),
    [
        ({"partition_mode": "all"}, {"all": 7}),
        ({"date_col": "D", "date_type": "date"}, {"2026-01-01": 7}),
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
