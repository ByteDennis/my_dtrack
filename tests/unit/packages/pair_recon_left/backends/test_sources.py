import sys
from types import SimpleNamespace

import pytest
from support import FakeConnection

from pair_recon_left.backends.sources import (
    CsvSource,
    HadoopSource,
    OracleSource,
    source_from_config,
)


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
    monkeypatch.setenv("PAIR_RECON_ORACLE_MACROS", "pb30:svc30")
    monkeypatch.setenv("pb30_USR", "reader")
    monkeypatch.setenv("pb30_PWD", "secret")
    source = OracleSource({"source": "oracle", "conn_macro": "pb30", "table": "T"})
    assert (source.user, source.password, source.resolve_dsn()) == ("reader", "secret", "svc30")


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
