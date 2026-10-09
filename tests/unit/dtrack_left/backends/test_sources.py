import sys
from types import SimpleNamespace

import duckdb
import pandas as pd
import pytest
from support import FakeConnection

from dtrack_left.backends import sources
from dtrack_left.backends.sources import (
    AthenaSource,
    DuckDBSource,
    FileSource,
    HadoopSource,
    OracleSource,
    source_from_config,
    write_rows,
)
from dtrack_left.common.hashing import key_family, sample_csv
from dtrack_left.common.sampling import SamplingSqlNotSupported

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
    assert OracleSource({**ORACLE, "schema": "OWN"}).relation() == "OWN.ORDERS"
    assert OracleSource({**ORACLE, "table": "own.v_app"}).relation() == "own.v_app"
    assert OracleSource({**ORACLE, "table": 'OWN."my view"'}).relation() == 'OWN."my view"'
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
    assert connection.statements == ["SELECT * FROM ORDERS WHERE 1=0"]
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


def test_oracle_preflight_probes_and_caches_the_string_date_format(monkeypatch):
    def respond(sql):
        if "FETCH FIRST 16 ROWS ONLY" in sql:
            return ["BUSINESS_DATE"], [("20260101",), ("20260102",)]
        if "COUNT(*)" in sql:
            return ["D", "N"], [("20260101", 1)]
        return ["BUSINESS_DATE"], []

    source, connection, _ = _oracle(monkeypatch, respond, date_type="string")
    assert source.preflight()["ok"]
    assert source.config["date_type"] == {"type": "string", "format": "compact"}
    assert source.partition_counts() == {"2026-01-01": 1}
    probes = [sql for sql in connection.statements if "FETCH FIRST 16 ROWS ONLY" in sql]
    assert probes == [
        (
            "SELECT BUSINESS_DATE FROM ORDERS WHERE (BUSINESS_DATE IS NOT NULL) "
            "FETCH FIRST 16 ROWS ONLY"
        )
    ]


def test_partition_counts_groups_by_day_within_bounds_and_extra_where(monkeypatch):
    rows = [
        (pd.Timestamp("2026-01-01"), 1),
        (pd.Timestamp("2026-01-01 17:30"), 2),
        ("20260102", "4"),
    ]
    source, connection, _ = _oracle(
        monkeypatch, lambda sql: (["P", "N"], rows), where="REGION = 'EU'"
    )
    assert source.partition_counts("2026-01-01", "2026-01-31") == {
        "2026-01-01": 3,
        "2026-01-02": 4,
    }
    assert connection.statements == [
        (
            "SELECT TRUNC(BUSINESS_DATE) AS dtrack_partition, COUNT(*) AS dtrack_count FROM ORDERS "
            "WHERE (REGION = 'EU') AND (BUSINESS_DATE >= DATE '2026-01-01' AND "
            "BUSINESS_DATE < DATE '2026-02-01') GROUP BY TRUNC(BUSINESS_DATE)"
        )
    ]


def test_oracle_query_windows_render_bounds_before_join_and_keep_outer_guard(monkeypatch):
    query = (
        "WITH d1 AS (SELECT * FROM DRIVER_ONE a WHERE {{query_windows:driver_one}}), "
        "d2 AS (SELECT * FROM DRIVER_TWO b WHERE {{query_windows:driver_two}}) "
        "SELECT a.DT AS FINAL_DT, a.ID FROM d1 a LEFT JOIN d2 b "
        "ON a.ID = b.ID AND a.DT = b.ACTIVITY_DT"
    )
    source, connection, _ = _oracle(
        monkeypatch,
        lambda sql: (["D", "N"], [("2026-08-01", 1)]),
        query=query,
        date_col="FINAL_DT",
        query_windows={
            "driver_one": {"expression": "a.DT", "date_type": "date"},
            "driver_two": {"expression": "b.ACTIVITY_DT", "date_type": "date"},
        },
    )

    assert source.partition_counts("2026-08-01", "2026-08-03") == {"2026-08-01": 1}
    sql = connection.statements[-1]
    assert "a.DT >= DATE '2026-08-01' AND a.DT < DATE '2026-08-04'" in sql
    assert (
        "b.ACTIVITY_DT >= DATE '2026-08-01' AND b.ACTIVITY_DT < DATE '2026-08-04'"
        in sql
    )
    assert "FINAL_DT >= DATE '2026-08-01' AND FINAL_DT < DATE '2026-08-04'" in sql


def test_oracle_query_windows_render_exact_admitted_days_before_sampling(
    monkeypatch, tmp_path
):
    query = (
        "SELECT a.DT AS FINAL_DT, a.ID, c.VALUE FROM DRIVER_ONE a "
        "LEFT JOIN DRIVER_TWO b ON a.ID = b.ID AND a.DT = b.ACTIVITY_DT "
        "AND {{query_windows:driver_two}} "
        "LEFT JOIN ACCOUNT c ON a.ACCOUNT_ID = c.ACCOUNT_ID "
        "WHERE {{query_windows:driver_one}}"
    )
    source, connection, _ = _oracle(
        monkeypatch,
        lambda sql: (["FINAL_DT", "ID", "VALUE"], []),
        query=query,
        date_col="FINAL_DT",
        query_windows={
            "driver_one": {"expression": "a.DT"},
            "driver_two": {"expression": "b.ACTIVITY_DT"},
        },
    )

    source.pull_window(
        ["2026-08-01", "2026-08-03"],
        ["VALUE"],
        tmp_path / "sample.csv",
        key_columns=["ID"],
        sample_plan={"mode": "fraction", "threshold": 100},
        key_families=["string"],
    )

    sql = connection.statements[-1]
    assert "a.DT >= DATE '2026-08-01' AND a.DT < DATE '2026-08-02'" in sql
    assert "a.DT >= DATE '2026-08-03' AND a.DT < DATE '2026-08-04'" in sql
    assert "b.ACTIVITY_DT >= DATE '2026-08-01' AND b.ACTIVITY_DT < DATE '2026-08-02'" in sql
    assert "b.ACTIVITY_DT >= DATE '2026-08-03' AND b.ACTIVITY_DT < DATE '2026-08-04'" in sql
    assert "ACCOUNT c ON a.ACCOUNT_ID = c.ACCOUNT_ID" in sql
    assert "STANDARD_HASH" in sql
    assert "{{query_windows:" not in sql


def test_oracle_query_windows_are_neutral_for_metadata_probes(monkeypatch):
    source, connection, _ = _oracle(
        monkeypatch,
        lambda sql: (["FINAL_DT"], []),
        query=(
            "SELECT a.DT AS FINAL_DT FROM DRIVER_ONE a "
            "WHERE {{query_windows:driver_one}}"
        ),
        date_col="FINAL_DT",
        query_windows={"driver_one": {"expression": "a.DT"}},
    )

    assert source.preflight()["ok"]
    assert "WHERE 1=1) dtrack_source WHERE 1=0" in connection.statements[0]


def test_query_without_a_result_set_returns_no_rows(monkeypatch):
    source, _, _ = _oracle(monkeypatch, lambda sql: ([], [("ignored",)]))
    assert source.query("BEGIN NULL; END;") == ([], [])


def test_oracle_query_is_audited_before_cursor_execute(monkeypatch):
    events = []
    source, _, _ = _oracle(monkeypatch, lambda sql: events.append(("execute", sql)) or ([], []))
    source.set_sql_audit(
        SimpleNamespace(record=lambda sql, **meta: events.append(("audit", sql, meta)))
    )
    source.query("SELECT * FROM HUGE_TABLE")
    assert events == [
        ("audit", "SELECT * FROM HUGE_TABLE", {"backend": "oracle", "operation": "query"}),
        ("execute", "SELECT * FROM HUGE_TABLE"),
    ]


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
        "SELECT BUSINESS_DATE, ORDER_ID, AMOUNT FROM ORDERS WHERE (("
    )
    assert (
        "BUSINESS_DATE >= DATE '2026-01-03' AND BUSINESS_DATE < DATE '2026-01-04'"
        in connection.statements[0]
    )


def test_oracle_pull_window_pushes_the_fraction_hash_into_the_query(monkeypatch, tmp_path):
    source, connection, _ = _oracle(
        monkeypatch, lambda sql: (["D", "K", "A"], [("2026-01-01", "007", "x")])
    )
    source.pull_window(
        ["2026-01-01"],
        ["AMOUNT"],
        tmp_path / "sample.csv",
        key_columns=["ORDER_ID"],
        sample_plan={"mode": "fraction", "threshold": 99},
        key_families=["numeric"],
    )
    assert connection.statements[0] == "SELECT ORDER_ID FROM ORDERS WHERE (1=0)"
    sql = connection.statements[-1]
    assert "STANDARD_HASH" in sql and "TO_NUMBER" in sql and "< 99" in sql

    source.pull_window(
        ["2026-01-02"],
        ["AMOUNT"],
        tmp_path / "sample-2.csv",
        key_columns=["ORDER_ID"],
        sample_plan={"mode": "fraction", "threshold": 99},
        key_families=["numeric"],
    )
    assert connection.statements.count("SELECT ORDER_ID FROM ORDERS WHERE (1=0)") == 1


def test_unsupported_custom_sampling_sql_fails_before_pulling_a_window(synthetic, tmp_path):
    class CustomFamily(type(key_family("string"))):
        name = "source-custom-test"

        def left_transform(self, text):
            return text.removeprefix("X")

    output = tmp_path / "must-not-exist.csv"
    with pytest.raises(SamplingSqlNotSupported, match="needs duckdb SQL"):
            _orders(synthetic, date_type="string").pull_window(
            ["2026-01-01"],
            ["AMOUNT"],
            output,
            key_columns=["ORDER_ID"],
            sample_plan={"mode": "fraction", "threshold": 99},
            key_families=[CustomFamily()],
        )
    assert not output.exists()


def test_athena_and_hadoop_connect_through_their_optional_drivers(monkeypatch):
    calls = []
    fake = SimpleNamespace(connect=lambda **kwargs: calls.append(kwargs) or "conn")
    monkeypatch.setitem(sys.modules, "pyathena", fake)
    monkeypatch.setitem(sys.modules, "impala", SimpleNamespace(dbapi=fake))
    monkeypatch.setitem(sys.modules, "impala.dbapi", fake)
    athena = AthenaSource(
        {"source": "athena", "schema": "db", "s3_staging_dir": "s3://q/", "region": "us-east-1"}
    )
    assert athena.connect() == "conn"
    assert calls == [{"s3_staging_dir": "s3://q/", "region_name": "us-east-1", "schema_name": "db"}]
    with pytest.raises(NotImplementedError, match="HadoopSource is disabled"):
        HadoopSource({"source": "hadoop", "host": "hive", "port": "10001"})


def test_optional_drivers_explain_the_extra_to_install(monkeypatch):
    monkeypatch.setitem(sys.modules, "pyathena", None)
    monkeypatch.setitem(sys.modules, "impala", None)
    monkeypatch.setitem(sys.modules, "impala.dbapi", None)
    with pytest.raises(ImportError, match=r"dtrack\[athena\]"):
        AthenaSource({"source": "athena"}).connect()


@pytest.mark.parametrize(
    ("name", "cls"),
    [
        ("oracle", OracleSource),
        ("ATHENA", AthenaSource),
        ("aws", AthenaSource),
        ("csv", FileSource),
        ("file", FileSource),
    ],
)
def test_source_from_config_dispatches_on_source(name, cls):
    assert isinstance(source_from_config({"source": name}), cls)


def test_source_from_config_reaches_the_disabled_hadoop_source():
    with pytest.raises(NotImplementedError, match="HadoopSource is disabled"):
        source_from_config({"source": "hadoop"})


def test_source_from_config_rejects_unknown_sources():
    with pytest.raises(ValueError, match="unsupported Left source: sas"):
        source_from_config({"source": "sas"})


def _orders(synthetic, **config):
    path = str(synthetic.left / "ORDERS.csv")
    return FileSource({"source": "csv", "path": path, "date_col": "BUSINESS_DATE", **config})


def test_file_source_counts_every_real_partition_exactly(synthetic):
    source = _orders(synthetic, date_type="string")
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
    out = _orders(synthetic, date_type="string").pull_window(
        days, ["AMOUNT", "STATUS"], tmp_path / "x" / "w.csv", key_columns=["ORDER_ID"]
    )
    frame = pd.read_csv(synthetic.left / "ORDERS.csv", dtype=object)
    old = tmp_path / "old.csv"
    frame.loc[frame["BUSINESS_DATE"].isin(days), columns].to_csv(old, index=False)
    assert out.read_bytes() == old.read_bytes()
    assert len(pd.read_csv(out)) == 29 * 30


def test_file_source_csv_export_fetches_bounded_batches(tmp_path, monkeypatch):
    class StreamingConnection:
        description = (("A",), ("B",))

        def __init__(self):
            self.batches = [[("1", "x")], [("2", None)], []]
            self.closed = False

        def execute(self, _sql):
            return self

        def fetchmany(self, size):
            assert size == 10_000
            return self.batches.pop(0)

        def fetchall(self):
            raise AssertionError("large file exports must not call fetchall")

        def close(self):
            self.closed = True

    connection = StreamingConnection()
    source = FileSource({"source": "file", "path": "ignored.csv", "date_col": "D"})
    monkeypatch.setattr(source, "connect", lambda: connection)
    output = source.query_to_csv("SELECT A, B", tmp_path / "streamed.csv", ["A", "B"])
    assert output.read_text() == "A,B\n1,x\n2,\n"
    assert connection.closed


def test_file_source_fraction_pushdown_matches_the_python_reference(synthetic, tmp_path):
    source = _orders(synthetic, date_type="string")
    days = ["2026-01-01", "2026-01-02", "2026-01-03"]
    full = source.pull_window(days, ["AMOUNT"], tmp_path / "full.csv", key_columns=["ORDER_ID"])
    expected = sample_csv(
        full, key_columns=["ORDER_ID"], mode="fraction", rate=0.25, families=["numeric"]
    )
    pushed = source.pull_window(
        days,
        ["AMOUNT"],
        tmp_path / "pushed.csv",
        key_columns=["ORDER_ID"],
        sample_plan={"mode": "fraction", "rate": 0.25, "threshold": round(0.25 * 2**32)},
        key_families=["numeric"],
    )
    assert pd.read_csv(pushed, dtype=object).equals(pd.read_csv(expected, dtype=object))


def test_file_source_selects_fixed_keys_in_duckdb_before_writing_csv(synthetic, tmp_path):
    source = _orders(synthetic, date_type="string")
    days = ["2026-01-01", "2026-01-02", "2026-01-03"]
    full = source.pull_window(days, [], tmp_path / "all.csv", key_columns=["ORDER_ID"])
    expected = sample_csv(
        full, key_columns=["ORDER_ID"], mode="fixed", count=5, families=["numeric"]
    )
    selected = source.sample_keys(
        days,
        ["ORDER_ID"],
        tmp_path / "keys.csv",
        count=5,
        key_families=["numeric"],
    )
    got = set(pd.read_csv(selected, dtype=object)["ORDER_ID"])
    want = set(pd.read_csv(expected, dtype=object)["ORDER_ID"])
    assert got == want and len(got) == 5


def test_file_source_preflight_reports_missing_file_and_missing_date_column(tmp_path):
    missing = FileSource({"source": "csv", "path": str(tmp_path / "nope.csv"), "date_col": "D"})
    assert missing.preflight()["ok"] is False
    path = tmp_path / "a.csv"
    path.write_text("X\n1\n")
    result = FileSource({"source": "csv", "path": str(path), "date_col": "D"}).preflight()
    assert result == {"ok": False, "source": "file", "error": "missing columns: D"}
    assert FileSource({"source": "csv", "conn_macro": str(path)}).path == str(path)


def test_file_source_preflight_resolves_date_column_case_insensitively(tmp_path):
    path = tmp_path / "case.csv"
    path.write_text("TRIGGER_DT,VALUE\n2026-01-01,1\n")
    source = FileSource(
        {"source": "csv", "path": str(path), "date_col": "trigger_dt", "date_type": "date"}
    )

    health = source.preflight()

    assert health["ok"] is True
    assert source.config["date_col"] == "TRIGGER_DT"


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


def test_file_source_nullstr_converts_export_markers_before_queries(tmp_path):
    path = tmp_path / "sas.csv"
    path.write_text("D,K,V\n2026-01-01,NULL,NULL\n2026-01-01,7,x\n")
    source = FileSource(
        {
            "source": "file",
            "path": str(path),
            "date_col": "D",
            "nullstr": ["", "NULL"],
        }
    )

    assert "nullstr = ['', 'NULL']" in source.reader()
    _columns, rows = source.query(
        f'SELECT count_if("K" IS NULL), count_if("V" IS NULL) FROM {source.relation()}'
    )
    assert rows == [(1, 1)]


def test_file_source_nullstr_rejects_non_csv_and_conflicting_options():
    with pytest.raises(ValueError, match="only supported for CSV"):
        FileSource({"source": "file", "path": "a.parquet", "nullstr": "NULL"})
    with pytest.raises(ValueError, match="disagrees"):
        FileSource(
            {
                "source": "file",
                "path": "a.csv",
                "nullstr": "NULL",
                "options": {"nullstr": "NA"},
            }
        )


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


@pytest.mark.parametrize(
    ("date_type", "raw_days"),
    [
        ("string", ["20260101", "20260102"]),
        ("sas", [" 01JAN2026", "02JAN2026 "]),
        ("sas", ["24107", "24108.0"]),
        ("sas", ["01JAN2026:09:45:35", "02JAN2026:00:00:00"]),
    ],
)
def test_file_source_filters_supported_text_and_sas_dates(tmp_path, date_type, raw_days):
    path = tmp_path / f"{date_type}.csv"
    path.write_text(f"D,V\n{raw_days[0]},a\n{raw_days[1]},b\n")
    source = FileSource(
        {"source": "file", "path": str(path), "date_col": "D", "date_type": date_type}
    )
    assert source.partition_counts("2026-01-01", "2026-01-02") == {
        "2026-01-01": 1,
        "2026-01-02": 1,
    }
    pulled = source.pull_window(["2026-01-02"], ["V"], tmp_path / f"{date_type}-out.csv")
    assert pulled.read_text().splitlines() == ["D,V", f"{raw_days[1]},b"]


def test_file_source_rejects_mixed_date_formats(tmp_path):
    path = tmp_path / "mixed.csv"
    path.write_text("D,V\n01JAN2026,a\n24108,b\n")
    source = FileSource(
        {"source": "file", "path": str(path), "date_col": "D", "date_type": "sas"}
    )
    health = source.preflight()
    assert health["ok"] is False
    assert "mixed SAS date formats" in health["error"]


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


# >>> a DuckDB database file with schema fin and nine orders over three days <<< #
@pytest.fixture
def duckdb_file(tmp_path):
    import duckdb

    path = tmp_path / "sales-2026.duckdb"
    connection = duckdb.connect(str(path))
    connection.execute(
        "CREATE SCHEMA fin; CREATE TABLE fin.orders AS SELECT DATE '2026-01-01' + (i % 3)::INT "
        "AS business_date, i AS amount, 'o' || i AS order_id FROM range(9) t(i)"
    )
    connection.close()
    return path


@pytest.mark.parametrize(
    "spec",
    [
        {"table": "fin.orders"},
        {"table": "FIN.ORDERS"},
        {"table": "sales-2026.fin.orders"},
        {"catalog": "sales-2026", "schema": "fin", "table": "orders"},
        {"query": "SELECT * FROM fin.orders"},
    ],
)
def test_duckdb_source_reads_tables_by_any_name_spelling(duckdb_file, spec):
    source = source_from_config(
        {
            "source": "duckdb",
            "path": str(duckdb_file),
            "date_col": "BUSINESS_DATE",
            "date_type": "date",
            **spec,
        }
    )
    assert isinstance(source, DuckDBSource) and source.preflight()["ok"]
    assert source.partition_counts() == {"2026-01-01": 3, "2026-01-02": 3, "2026-01-03": 3}


def test_duckdb_source_pulls_a_window_and_never_writes(duckdb_file, tmp_path):
    source = DuckDBSource(
        {
            "path": str(duckdb_file),
            "table": "fin.orders",
            "date_col": "business_date",
            "date_type": "date",
        }
    )
    output = source.pull_window(
        ["2026-01-02"], ["amount"], tmp_path / "w.csv", key_columns=["order_id"]
    )
    lines = output.read_text().splitlines()
    assert lines[0] == "business_date,order_id,amount" and len(lines) == 4
    with pytest.raises(Exception, match="read-only"):
        source.query("CREATE TABLE fin.x (i INT)")


def test_duckdb_source_needs_a_path_and_reports_a_missing_database(tmp_path):
    with pytest.raises(ValueError, match="requires path"):
        DuckDBSource({"table": "t"})
    missing = DuckDBSource({"path": str(tmp_path / "nope.duckdb"), "table": "t", "date_col": "d"})
    assert missing.preflight()["ok"] is False
    assert DuckDBSource({"path": "s3://bucket/dir/sales.duckdb"}).alias == "sales"
    assert DuckDBSource({"conn_macro": "https://h/x/prod.db"}).alias == "prod"
    with pytest.raises(ValueError, match="table or query"):
        DuckDBSource({"path": "a.duckdb"}).relation()


# >>> another process holds the database read-write and has three uncheckpointed rows in its .wal <<< #
@pytest.fixture
def locked_duckdb(duckdb_file):
    import subprocess

    script = (
        "import duckdb, sys\n"
        f"c = duckdb.connect({str(duckdb_file)!r})\n"
        "c.execute(\"INSERT INTO fin.orders VALUES (DATE '2026-01-04', 9, 'o9'), "
        "(DATE '2026-01-04', 10, 'o10'), (DATE '2026-01-04', 11, 'o11')\")\n"
        "print('ready', flush=True)\n"
        "sys.stdin.read()\n"
    )
    writer = subprocess.Popen(
        [sys.executable, "-c", script], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
    )
    assert writer.stdout.readline().strip() == "ready"
    yield duckdb_file
    writer.stdin.close()
    writer.wait(timeout=30)


def test_duckdb_lock_held_by_a_writer_explains_the_snapshot_option(locked_duckdb):
    source = DuckDBSource({"path": str(locked_duckdb), "table": "fin.orders", "date_col": "d"})
    result = source.preflight()
    assert result["ok"] is False and '"snapshot": true' in result["error"]


def test_duckdb_snapshot_reads_a_copy_with_the_writers_wal_once(locked_duckdb):
    source = DuckDBSource(
        {
            "path": str(locked_duckdb),
            "table": "fin.orders",
            "date_col": "business_date",
            "date_type": "date",
            "snapshot": True,
        }
    )
    assert source.partition_counts()["2026-01-04"] == 3
    first = source.database()
    assert source.partition_counts()["2026-01-01"] == 3 and source.database() == first
    assert first != str(locked_duckdb) and first.endswith("sales-2026.duckdb")
    assert source.relation() == '"fin"."orders"'


def test_duckdb_snapshot_is_off_for_urls():
    assert DuckDBSource({"path": "s3://b/x.duckdb", "snapshot": True}).snapshot is False
    assert DuckDBSource({"path": "x.duckdb", "snapshot": True}).snapshot is True
