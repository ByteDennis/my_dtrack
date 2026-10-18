import pytest
from support import FakeConnection

from pair_recon_left.backends.sources import OracleSource, source_from_config


@pytest.mark.parametrize("source", ["hadoop", "athena", "csv", "duckdb"])
def test_factory(source):
    with pytest.raises(ValueError, match="unsupported"):
        source_from_config({"source": source})


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

    source = OracleSource({"source": "oracle", "table": "T", **config})
    source.connect = lambda: FakeConnection(answer)
    assert source.partition_counts() == expected
