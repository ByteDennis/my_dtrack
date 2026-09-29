import sys
from types import SimpleNamespace

import duckdb
import pandas as pd
import pytest
from support import FakeConnection

from dtrack_left.backends import sources
from dtrack_left.backends.sources import (
    AthenaSource,
    FileSource,
    HadoopSource,
    OracleSource,
    source_from_config,
    write_rows,
)

ORACLE = {"source": "oracle", "table": "ORDERS", "date_col": "BUSINESS_DATE", "date_type": "date"}


def _oracle(monkeypatch, responder, **config):
    connection = FakeConnection(responder)
    captured = {}

    def connect(**kwargs):
        captured.update(kwargs)
        return connection

    monkeypatch.setitem(sys.modules, "oracledb", SimpleNamespace(connect=connect))
    source = OracleSource({**ORACLE, "user": "u", "password": "p", "dsn": "h:1521/S", **config})
    return source, connection, captured


def test_write_rows_creates_parents_and_writes_a_header(tmp_path):
    path = write_rows(tmp_path / "a" / "b.csv", ["x", "y"], [(1, "a"), (2, None)])
    assert path.read_text().splitlines() == ["x,y", "1,a", "2,"]


def test_relation_quotes_parts_wraps_queries_and_needs_a_table():
    assert OracleSource({**ORACLE, "schema": "OWN"}).relation() == '"OWN"."ORDERS"'
    assert OracleSource({"query": "SELECT 1 FROM dual"}).relation() == (
        "(SELECT 1 FROM dual) dtrack_source"
    )
    with pytest.raises(ValueError, match="table or query"):
        OracleSource({"source": "oracle"}).relation()


def test_oracle_credentials_come_from_config_or_named_environment(monkeypatch):
    monkeypatch.setenv("PCDS_USER", "env-user")
    monkeypatch.setenv("ORACLE_PASSWORD", "env-pass")
    monkeypatch.setenv("ORACLE_DSN", "env:1521/X")
    source = OracleSource({**ORACLE, "user_env": "PCDS_USER"})
    assert (source.user, source.password, source.dsn) == ("env-user", "env-pass", "env:1521/X")


def test_oracle_connect_requires_all_three_credentials(monkeypatch):
    monkeypatch.setitem(sys.modules, "oracledb", SimpleNamespace(connect=None))
    for name in ("ORACLE_USER", "ORACLE_PASSWORD", "ORACLE_DSN", "PCDS_USR"):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(ValueError, match="user, password, and dsn"):
        OracleSource(ORACLE).connect()


def test_preflight_runs_an_empty_select_and_reports_failures(monkeypatch):
    source, connection, captured = _oracle(monkeypatch, lambda sql: (["A"], []))
    assert source.preflight() == {"ok": True, "source": "oracle"}
    assert connection.statements == ['SELECT * FROM "ORDERS" WHERE 1=0']
    assert captured == {"user": "u", "password": "p", "dsn": "h:1521/S"}
    assert connection.closed and connection.cursors_closed == 1

    def fail(sql):
        raise RuntimeError("ORA-00942: table or view does not exist")

    broken, _, _ = _oracle(monkeypatch, fail)
    assert broken.preflight() == {
        "ok": False,
        "source": "oracle",
        "error": "ORA-00942: table or view does not exist",
    }


def test_partition_counts_groups_by_day_within_bounds_and_extra_where(monkeypatch):
    rows = [(pd.Timestamp("2026-01-01"), 3), ("20260102", "4")]
    source, connection, _ = _oracle(
        monkeypatch, lambda sql: (["P", "N"], rows), where="REGION = 'EU'"
    )
    assert source.partition_counts("2026-01-01", "2026-01-31") == {
        "2026-01-01": 3,
        "2026-01-02": 4,
    }
    assert connection.statements == [
        (
            'SELECT "BUSINESS_DATE" AS dtrack_partition, COUNT(*) AS dtrack_count FROM "ORDERS" '
            "WHERE (REGION = 'EU') AND (\"BUSINESS_DATE\" >= DATE '2026-01-01' AND "
            '"BUSINESS_DATE" <= DATE \'2026-01-31\') GROUP BY "BUSINESS_DATE"'
        )
    ]


def test_query_without_a_result_set_returns_no_rows(monkeypatch):
    source, _, _ = _oracle(monkeypatch, lambda sql: ([], [("ignored",)]))
    assert source.query("BEGIN NULL; END;") == ([], [])


def test_pull_window_selects_date_keys_and_columns_once_over_the_admitted_days(
    monkeypatch, tmp_path
):
    source, connection, _ = _oracle(
        monkeypatch, lambda sql: (["D", "K", "A"], [("2026-01-01", 1, 2.5)])
    )
    out = source.pull_window(
        ["2026-01-01", "2026-01-03"],
        ["AMOUNT", "ORDER_ID"],
        tmp_path / "w.csv",
        key_columns=["ORDER_ID"],
    )
    assert out.read_text().splitlines() == ["BUSINESS_DATE,ORDER_ID,AMOUNT", "2026-01-01,1,2.5"]
    assert connection.statements[0].startswith(
        'SELECT "BUSINESS_DATE", "ORDER_ID", "AMOUNT" FROM "ORDERS" WHERE (('
    )
    assert "\"BUSINESS_DATE\" = DATE '2026-01-03'" in connection.statements[0]


def test_athena_and_hadoop_connect_through_their_optional_drivers(monkeypatch):
    calls = []
    fake = SimpleNamespace(connect=lambda **kwargs: calls.append(kwargs) or "conn")
    monkeypatch.setitem(sys.modules, "pyathena", fake)
    monkeypatch.setitem(sys.modules, "impala", SimpleNamespace(dbapi=fake))
    monkeypatch.setitem(sys.modules, "impala.dbapi", fake)
    athena = AthenaSource(
        {"source": "athena", "schema": "db", "s3_staging_dir": "s3://q/", "region": "us-east-1"}
    )
    hadoop = HadoopSource({"source": "hadoop", "host": "hive", "port": "10001"})
    assert athena.connect() == "conn" and hadoop.connect() == "conn"
    assert calls == [
        {"s3_staging_dir": "s3://q/", "region_name": "us-east-1", "schema_name": "db"},
        {"host": "hive", "port": 10001, "database": "default", "auth_mechanism": "PLAIN"},
    ]


def test_optional_drivers_explain_the_extra_to_install(monkeypatch):
    monkeypatch.setitem(sys.modules, "pyathena", None)
    monkeypatch.setitem(sys.modules, "impala", None)
    monkeypatch.setitem(sys.modules, "impala.dbapi", None)
    with pytest.raises(ImportError, match=r"dtrack\[athena\]"):
        AthenaSource({"source": "athena"}).connect()
    with pytest.raises(ImportError, match=r"dtrack\[hadoop\]"):
        HadoopSource({"source": "hadoop"}).connect()


@pytest.mark.parametrize(
    ("name", "cls"),
    [
        ("oracle", OracleSource),
        ("ATHENA", AthenaSource),
        ("aws", AthenaSource),
        ("hadoop", HadoopSource),
        ("csv", FileSource),
        ("file", FileSource),
    ],
)
def test_source_from_config_dispatches_on_source(name, cls):
    assert isinstance(source_from_config({"source": name}), cls)


def test_source_from_config_rejects_unknown_sources():
    with pytest.raises(ValueError, match="unsupported Left source: sas"):
        source_from_config({"source": "sas"})


def _orders(synthetic, **config):
    path = str(synthetic.left / "ORDERS.csv")
    return FileSource({"source": "csv", "path": path, "date_col": "BUSINESS_DATE", **config})


def test_file_source_counts_every_real_partition_exactly(synthetic):
    source = _orders(synthetic, date_type="string_dash")
    assert source.preflight()["ok"]
    counts = source.partition_counts("2026-01-01", "2026-02-28")
    assert len(counts) == 58 and sum(counts.values()) == synthetic.summary["orders"]["left_rows"]
    assert "2026-01-31" not in counts and counts["2026-01-15"] == 30
    assert source.partition_counts("2026-02-01") == {
        k: v for k, v in counts.items() if k >= "2026-02-01"
    }


# >>> parity with the pandas reader it replaced: same rows, same raw text, byte for byte <<< #
def test_file_source_pull_window_matches_the_old_pandas_reader_byte_for_byte(synthetic, tmp_path):
    days = [f"2026-01-{d:02d}" for d in range(1, 31) if d not in (15, 31)]
    columns = ["BUSINESS_DATE", "ORDER_ID", "AMOUNT", "STATUS"]
    out = _orders(synthetic, date_type="string_dash").pull_window(
        days, ["AMOUNT", "STATUS"], tmp_path / "x" / "w.csv", key_columns=["ORDER_ID"]
    )
    frame = pd.read_csv(synthetic.left / "ORDERS.csv", dtype=object)
    old = tmp_path / "old.csv"
    frame.loc[frame["BUSINESS_DATE"].isin(days), columns].to_csv(old, index=False)
    assert out.read_bytes() == old.read_bytes()
    assert len(pd.read_csv(out)) == 29 * 30


def test_file_source_preflight_reports_missing_file_and_missing_date_column(tmp_path):
    missing = FileSource({"source": "csv", "path": str(tmp_path / "nope.csv"), "date_col": "D"})
    assert missing.preflight()["ok"] is False
    path = tmp_path / "a.csv"
    path.write_text("X\n1\n")
    result = FileSource({"source": "csv", "path": str(path), "date_col": "D"}).preflight()
    assert result == {"ok": False, "source": "file", "error": "missing columns: D"}
    assert FileSource({"source": "csv", "conn_macro": str(path)}).path == str(path)


@pytest.mark.parametrize(
    ("name", "write", "config"),
    [
        (
            "a.txt",
            lambda p: p.write_text("D|V\n2026-01-01|1.50\n2026-01-02|x\n"),
            {"options": {"delim": "|"}},
        ),
        (
            "a.json",
            lambda p: p.write_text('[{"D": "2026-01-01", "V": 1.5}, {"D": "2026-01-02", "V": 2}]'),
            {},
        ),
        (
            "a.jsonl",
            lambda p: p.write_text('{"D": "2026-01-01", "V": 1}\n{"D": "2026-01-02", "V": 2}\n'),
            {},
        ),
        (
            "a.parquet",
            lambda p: duckdb.sql(
                "SELECT * FROM (VALUES ('2026-01-01', 1), ('2026-01-02', 2)) t(D, V)"
            ).write_parquet(str(p)),
            {},
        ),
    ],
)
def test_file_source_reads_text_json_ndjson_and_parquet(tmp_path, name, write, config):
    write(tmp_path / name)
    source = FileSource({"source": "file", "path": str(tmp_path / name), "date_col": "D", **config})
    assert source.preflight()["ok"], source.preflight()
    assert source.partition_counts("2026-01-01", "2026-01-31") == {"2026-01-01": 1, "2026-01-02": 1}
    out = source.pull_window(["2026-01-01"], ["V"], tmp_path / "w.csv")
    assert out.read_text().splitlines()[0] == "D,V" and len(out.read_text().splitlines()) == 2


def test_raw_text_survives_csv_reads(tmp_path):
    (tmp_path / "a.txt").write_text("D|V\n2026-01-01|1.50\n2026-01-01|007\n")
    source = FileSource(
        {"path": str(tmp_path / "a.txt"), "date_col": "D", "options": {"delim": "|"}}
    )
    out = source.pull_window(["2026-01-01"], ["V"], tmp_path / "w.csv")
    assert out.read_text().splitlines()[1:] == ["2026-01-01,1.50", "2026-01-01,007"]


# >>> the patch hook: a DuckDB query over {src}, here deriving the partition from each file name <<< #
def test_query_patch_and_date_format_over_a_glob_of_files(tmp_path):
    for day in ("20260101", "20260102"):
        (tmp_path / f"orders_{day}.csv").write_text("ID,V\n1,a\n2,b\n")
    source = FileSource(
        {
            "source": "file",
            "path": str(tmp_path / "orders_*.csv"),
            "options": {"filename": True},
            "query": "SELECT *, regexp_extract(filename, '(\\d{8})', 1) AS D FROM {src}",
            "date_col": "D",
            "date_format": "%Y%m%d",
        }
    )
    assert source.config["date_type"] == "date"
    assert source.partition_counts("2026-01-02") == {"2026-01-02": 2}
    out = source.pull_window(["2026-01-01"], ["V"], tmp_path / "w.csv", key_columns=["ID"])
    assert out.read_text().splitlines() == ["D,ID,V", "2026-01-01,1,a", "2026-01-01,2,b"]


def test_file_formats_come_from_the_suffix_or_config_and_unknown_ones_fail():
    assert FileSource({"path": "a.TSV"}).format == "csv"
    assert FileSource({"path": "a.jsonl"}).format == "ndjson"
    assert FileSource({"path": "a.dat", "format": "parquet"}).format == "parquet"
    assert FileSource({"path": "x"}).format == "csv"
    with pytest.raises(ValueError, match="unsupported file format: orc"):
        FileSource({"path": "a.orc"})
    reader = FileSource(
        {"path": "a'b.csv", "options": {"header": False, "skip": 2, "names": ["A"]}}
    )
    assert reader.reader() == (
        "read_csv('a''b.csv', all_varchar = true, header = false, skip = 2, names = ['A'])"
    )


# >>> several Oracle servers: each pair names its macro, credentials resolve by that name <<< #
def test_oracle_credentials_resolve_by_conn_macro_like_the_legacy_package(monkeypatch):
    for name in ("ORACLE_USER", "ORACLE_PASSWORD", "ORACLE_DSN", "LDAP_DSN"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("pb23_USR", "u23")
    monkeypatch.setenv("pb23_pwd", "p23")
    monkeypatch.setenv("pb23_DSN", "h23:1521/S23")
    monkeypatch.setenv("pb30_USR", "u30")
    monkeypatch.setenv("pb30_PWD", "p30")
    monkeypatch.setenv("DTRACK_ORACLE_MACROS", "pb30: svc30 , broken")
    a = OracleSource({**ORACLE, "conn_macro": "pb23"})
    b = OracleSource({**ORACLE, "conn_macro": "pb30"})
    assert (a.user, a.password, a.resolve_dsn()) == ("u23", "p23", "h23:1521/S23")
    assert (b.user, b.password, b.resolve_dsn()) == ("u30", "p30", "svc30")
    monkeypatch.setenv("ORACLE_DSN", "global")
    assert OracleSource({**ORACLE, "conn_macro": "pb30"}).resolve_dsn() == "svc30"
    assert OracleSource({**ORACLE, "conn_macro": "other"}).resolve_dsn() == "global"
    monkeypatch.setenv("PCDS_USR", "shared")
    assert OracleSource({**ORACLE, "conn_macro": "other"}).user == "shared"


def test_oracle_service_goes_through_ldap_when_a_template_is_set(monkeypatch):
    seen = {}

    class Connection:
        result = "bad"

        def __init__(self, server):
            self.entries = []

        def bind(self):
            return seen.get("bind", True)

        def search(self, context, query, attributes):
            seen.update(context=context, query=query)
            descriptor = SimpleNamespace(value="(DESCRIPTION=x)")
            self.entries = [SimpleNamespace(orclNetDescString=descriptor)] if seen["hit"] else []

    fake = SimpleNamespace(ALL="all", Server=lambda host, get_info: host, Connection=Connection)
    monkeypatch.setitem(sys.modules, "ldap3", fake)
    monkeypatch.setenv("LDAP_DSN", "ldap://oid:389/{service},cn=OracleContext,dc=x")
    seen["hit"] = True
    source = OracleSource({**ORACLE, "service": "svc", "user": "u", "password": "p"})
    assert source.resolve_dsn() == "(DESCRIPTION=x)"
    assert seen["context"] == "cn=OracleContext,dc=x" and seen["query"] == "(cn=svc)"
    seen["hit"] = False
    with pytest.raises(RuntimeError, match="no orclNetDescString"):
        source.resolve_dsn()
    seen["bind"] = False
    with pytest.raises(RuntimeError, match="bind to oid:389 failed"):
        source.resolve_dsn()
    with pytest.raises(ValueError, match="unsupported LDAP_DSN"):
        sources.solve_ldap("tns:nothing")
