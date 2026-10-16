import json
import os
from typing import ClassVar

import pytest

from dtrack_left.cli.config import load_config, validate_config
from dtrack_left.common.column_types import DateColumnType, register_column_type


def pair(right="databricks", left="oracle"):
    return {
        "left": {"source": left, "table": "a", "date_col": "dt"},
        "right": {"source": right, "table": "b", "date_col": "dt"},
        "col_map": {"amount": "amount"},
    }


def test_right_side():
    with pytest.raises(ValueError, match="Right side must be databricks"):
        validate_config({"pairs": {"p": pair(right="csv")}})


@pytest.mark.parametrize(
    ("source", "error"),
    [
        ("oracle", None),
        ("athena", None),
        ("aws", None),
        ("hadoop", None),
        ("csv", None),
        ("file", None),
        ("duckdb", None),
        ("sas", "unsupported Left source"),
    ],
    ids=["oracle", "athena", "aws", "hadoop", "csv", "file", "duckdb", "sas"],
)
def test_sources(source, error):
    config = {"pairs": {"p": pair(left=source)}}
    if error:
        with pytest.raises(ValueError, match=error):
            validate_config(config)
    else:
        assert validate_config(config)


def test_duckdb_live():
    row = pair(left="duckdb")
    row["left"]["snapshot"] = True
    with pytest.raises(ValueError, match="snapshot is no longer supported"):
        validate_config({"pairs": {"p": row}})


def test_conn_macro():
    config = {
        "connections": {
            "pcds": {"source": "oracle", "dsn": "pcds:1521/PCDS", "user_env": "PCDS_USER"},
            "edw": {"source": "oracle", "dsn": "edw:1521/EDW", "user_env": "EDW_USER"},
            "dbx_main": {"source": "databricks", "server_hostname": "main.host", "http_path": "/a"},
        },
        "pairs": {
            "a": {
                "left": {"conn_macro": "pcds", "table": "A", "date_col": "dt"},
                "right": {
                    "conn_macro": "dbx_main",
                    "catalog": "c1",
                    "table": "a",
                    "date_col": "dt",
                },
                "col_map": {"x": "x"},
            },
            "b": {
                "left": {"conn_macro": "edw", "table": "B", "date_col": "dt", "dsn": "override"},
                "right": {
                    "conn_macro": "dbx_main",
                    "catalog": "c2",
                    "table": "b",
                    "date_col": "dt",
                },
                "col_map": {"x": "x"},
            },
        },
    }
    resolved = validate_config(config)["pairs"]
    assert resolved["a"]["left"]["dsn"] == "pcds:1521/PCDS"
    assert resolved["a"]["left"]["user_env"] == "PCDS_USER"
    assert resolved["b"]["left"]["dsn"] == "override"
    assert resolved["b"]["left"]["user_env"] == "EDW_USER"
    assert resolved["a"]["right"]["server_hostname"] == "main.host"
    assert resolved["a"]["right"]["catalog"] == "c1"
    assert resolved["b"]["right"]["catalog"] == "c2"
    loose = {"pairs": {"p": {**pair(), "left": {"conn_macro": "pb23", "source": "oracle"}}}}
    loose["pairs"]["p"]["left"]["date_col"] = "dt"
    assert validate_config(loose)["pairs"]["p"]["left"] == {
        "conn_macro": "pb23",
        "source": "oracle",
        "date_col": "dt",
    }


def test_config_needs():
    with pytest.raises(ValueError, match="non-empty pairs"):
        validate_config({"pairs": {}})
    with pytest.raises(TypeError, match="connections must be an object"):
        validate_config({"connections": ["pcds"], "pairs": {"p": pair()}})


def test_config_version():
    row = pair()
    row.update(
        excluded_partitions=["20260102", "2026-01-01", "2026-01-01"],
        excluded_columns=["amount", "amount"],
    )
    loaded = validate_config({"version": 2, "pairs": {"p": row}})
    assert loaded["pairs"]["p"]["version"] == 2
    assert loaded["pairs"]["p"]["excluded_partitions"] == ["2026-01-01", "2026-01-02"]
    assert loaded["pairs"]["p"]["excluded_columns"] == ["amount"]
    with pytest.raises(ValueError, match="exclusions require config version 2"):
        validate_config({"version": 1, "pairs": {"p": row}})


@pytest.mark.parametrize("version", [0, 3, "2", True])
def test_config_supports(version):
    with pytest.raises(ValueError, match="version must be 1 or 2"):
        validate_config({"version": version, "pairs": {"p": pair()}})


def test_csv_sides():
    config = {"pairs": {"p": pair(left="csv"), "off": {"skip": True}}}
    config["pairs"]["p"]["left"]["conn_macro"] = "data/a.csv"
    assert validate_config(config)["pairs"]["p"]["left"]["conn_macro"] == "data/a.csv"


@pytest.mark.parametrize(
    ("source", "nullstr", "error"),
    [
        ("file", ["", "NULL"], None),
        ("file", 1, "nullstr must be a string or a list of strings"),
        ("file", ["", 1], "nullstr must be a string or a list of strings"),
        ("oracle", "NULL", "nullstr requires source file/csv"),
    ],
    ids=["valid", "scalar-type", "list-type", "database"],
)
def test_nullstr(source, nullstr, error):
    row = pair(left=source)
    row["left"]["nullstr"] = nullstr
    config = {"pairs": {"p": row}}
    if error:
        with pytest.raises((TypeError, ValueError), match=error):
            validate_config(config)
    else:
        assert validate_config(config)["pairs"]["p"]["left"]["nullstr"] == nullstr


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (lambda p: p["right"].pop("date_col"), "both sides require date_col"),
        (lambda p: p.update(col_map={}), "col_map must be non-empty"),
        (lambda p: p["right"].update(kind="view"), "unsupported Right kind 'view'"),
        (lambda p: p["right"].update(kind="query"), "kind 'query' requires query"),
        (lambda p: p["right"].update(kind="volume_raw"), "kind 'files' requires path, format"),
        (lambda p: p["right"].update(query="SELECT * FROM {src}"), "needs kind 'files'"),
        (lambda p: p.update(col_type_overrides={"nope": "numeric"}), "col_type_overrides"),
        (lambda p: p.update(col_type_overrides={"amount": "date"}), "col_type_overrides"),
    ],
)
def test_pairs_need(change, message):
    row = pair()
    change(row)
    with pytest.raises(ValueError, match=message):
        validate_config({"pairs": {"p": row}})


@pytest.mark.parametrize(
    ("change", "error", "exception"),
    [
        (lambda p: p.update(partition_mode="all"), None, None),
        (
            lambda p: p.update(partition_mode="weekly"),
            "partition_mode must be date or all",
            ValueError,
        ),
        (lambda p: p.update(partition_mode=1), "partition_mode must be date or all", TypeError),
        (
            lambda p: p.update(partition_mode="all", fromDate="2026-01-01"),
            "partition_mode all cannot use fromDate",
            ValueError,
        ),
        (
            lambda p: p.update(partition_mode="all", vintage="month"),
            "partition_mode all cannot use vintage",
            ValueError,
        ),
        (
            lambda p: p.update(partition_mode="all", excluded_partitions=["2026-01-01"]),
            "partition_mode all cannot use excluded_partitions",
            ValueError,
        ),
    ],
    ids=["all", "invalid", "type", "bounds", "vintage", "exclusions"],
)
def test_partition_mode(change, error, exception):
    row = pair()
    row["left"].pop("date_col")
    row["right"].pop("date_col")
    change(row)
    config = {"version": 2, "pairs": {"p": row}}
    if error:
        with pytest.raises(exception, match=error):
            validate_config(config)
    else:
        loaded = validate_config(config)["pairs"]["p"]
        assert loaded["partition_mode"] == "all"
        assert "date_col" not in loaded["left"] and "date_col" not in loaded["right"]


@pytest.mark.parametrize(
    ("value", "expected", "error"),
    [
        (None, None, None),
        (["hash_sum", "n_unique"], ["n_unique", "hash_sum"], None),
        (["hash_sum", "hash_sum"], None, "must not contain duplicates"),
        (["unique_value"], None, "unsupported statistics"),
        ("hash_sum", None, "must be a non-empty list"),
        ([], None, "must be a non-empty list"),
    ],
    ids=["default", "selected", "duplicate", "unknown", "type", "empty"],
)
def test_statistics(value, expected, error):
    row = pair()
    if value is not None:
        row["statistics"] = value
    config = {"pairs": {"p": row}}
    if error:
        with pytest.raises((TypeError, ValueError), match=error):
            validate_config(config)
    else:
        loaded = validate_config(config)["pairs"]["p"]
        assert loaded.get("statistics") == expected


def test_load_config(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("DTRACK_UNIT_MARKER", raising=False)
    (tmp_path / ".env").write_text("DTRACK_UNIT_MARKER=from-dotenv\n")
    path = tmp_path / "c.json"
    path.write_text(json.dumps({"pairs": {"p": pair()}}))
    assert load_config(path)["pairs"]["p"]["left"]["source"] == "oracle"
    assert os.environ["DTRACK_UNIT_MARKER"] == "from-dotenv"


def test_right_files():
    row = pair(left="file")
    row["right"] = {"source": "databricks", "kind": "files", "path": "/Volumes/c/s/work/in/"}
    row["right"] |= {"format": "csv", "query": "SELECT * FROM {src}", "date_col": "dt"}
    assert validate_config({"pairs": {"p": row}})["pairs"]["p"]["right"]["kind"] == "files"


# >>> a config whose pair p points col_map at a file in a maps/ folder next to it <<< #
def config_with_map(tmp_path, col_map, **extra):
    (tmp_path / "maps").mkdir(exist_ok=True)
    path = tmp_path / "c.json"
    path.write_text(json.dumps({"pairs": {"p": {**pair(), "col_map": col_map, **extra}}}))
    return path


def test_col_map(tmp_path, monkeypatch):
    path = config_with_map(tmp_path, "maps/p.csv")
    (tmp_path / "maps/p.csv").write_text(
        "﻿# left side, right side\nleft, right ,type\nORDER_ID,order_id,\n\nAMT,amount,Numeric\n"
    )
    monkeypatch.chdir(tmp_path / "maps")
    loaded = load_config(path)["pairs"]["p"]
    assert loaded["col_map"] == {"ORDER_ID": "order_id", "AMT": "amount"}
    assert loaded["col_type_overrides"] == {"AMT": "numeric"}


def test_col_type(tmp_path):
    path = config_with_map(tmp_path, "maps/p.csv")
    (tmp_path / "maps/p.csv").write_text(
        "left,right,type\nSAS_ATTRIBUTE,date_attribute,sas_datetime_date\n"
    )
    loaded = load_config(path)["pairs"]["p"]
    assert loaded["col_map"] == {"SAS_ATTRIBUTE": "date_attribute"}
    assert loaded["column_types"] == {
        "SAS_ATTRIBUTE": {
            "type": "date",
            "left_date_type": {"type": "sas", "format": "datetime"},
            "right_date_type": "date",
        }
    }


def test_inline_json(tmp_path):
    path = tmp_path / "c.json"
    document = {"pairs": {"p": pair()}}
    document["pairs"]["p"]["col_map"] = {
        "SAS_ATTRIBUTE": {"right": "date_attribute", "type": "sas_datetime_date"}
    }
    path.write_text(json.dumps(document))
    loaded = load_config(path)["pairs"]["p"]
    assert loaded["col_map"] == {"SAS_ATTRIBUTE": "date_attribute"}
    assert loaded["column_types"]["SAS_ATTRIBUTE"]["right_date_type"] == "date"


def test_new_date(tmp_path):
    @register_column_type("test_compact_to_date")
    class CompactToDate(DateColumnType):
        left_date_type: ClassVar[object] = {"type": "string", "format": "compact"}
        right_date_type: ClassVar[object] = "date"

    path = tmp_path / "c.json"
    document = {"pairs": {"p": pair()}}
    document["pairs"]["p"]["col_map"] = {"D": {"right": "d", "type": "test_compact_to_date"}}
    path.write_text(json.dumps(document))
    assert load_config(path)["pairs"]["p"]["column_types"]["D"] == {
        "type": "date",
        "left_date_type": {"type": "string", "format": "compact"},
        "right_date_type": "date",
    }


def test_col_json(tmp_path):
    path = config_with_map(tmp_path, "maps/p.json", col_type_overrides={"ID": "categorical"})
    (tmp_path / "maps/p.json").write_text(json.dumps({"ID": "id", "AMT": "amount"}))
    loaded = load_config(path)["pairs"]["p"]
    assert loaded["col_map"] == {"ID": "id", "AMT": "amount"}
    assert loaded["col_type_overrides"] == {"ID": "categorical"}


def test_inline_col(tmp_path):
    path = tmp_path / "c.json"
    path.write_text(json.dumps({"pairs": {"p": pair()}}))
    assert load_config(path)["pairs"]["p"]["col_map"] == {"amount": "amount"}


def test_query_sql(tmp_path, monkeypatch):
    (tmp_path / "queries").mkdir()
    (tmp_path / "queries" / "consolidated.sql").write_text(
        "SELECT d1.dt AS FINAL_DT, d1.amount\n"
        "FROM driver_one d1 WHERE {{query_windows:driver_one}};\n"
    )
    document = {"pairs": {"p": pair()}}
    document["pairs"]["p"]["left"] = {
        "source": "oracle",
        "query": "queries/consolidated.sql",
        "date_col": "FINAL_DT",
        "date_type": "date",
        "query_windows": {"driver_one": {"expression": "d1.dt", "date_type": "date"}},
    }
    path = tmp_path / "config.json"
    path.write_text(json.dumps(document))
    monkeypatch.chdir(tmp_path / "queries")

    left = load_config(path)["pairs"]["p"]["left"]

    assert left["query"].endswith("WHERE {{query_windows:driver_one}}")
    assert left["query_windows"] == {"driver_one": {"expression": "d1.dt", "date_type": "date"}}


def test_query_file(tmp_path):
    (tmp_path / "driver.sql").write_text("SELECT dt, amount FROM driver")
    document = {
        "connections": {
            "left_query": {
                "source": "oracle",
                "query": "driver.sql",
                "date_col": "dt",
            }
        },
        "pairs": {
            "p": {
                **pair(),
                "left": {"conn_macro": "left_query"},
            }
        },
    }
    path = tmp_path / "config.json"
    path.write_text(json.dumps(document))

    assert load_config(path)["pairs"]["p"]["left"]["query"] == ("SELECT dt, amount FROM driver")


def test_right_query(tmp_path):
    (tmp_path / "queries").mkdir()
    (tmp_path / "queries" / "right.sql").write_text("SELECT * FROM main.sales.orders;\n")
    document = {"pairs": {"p": pair()}}
    document["pairs"]["p"]["right"] = {
        "source": "databricks",
        "kind": "query",
        "query": "queries/right.sql",
        "date_col": "dt",
    }
    path = tmp_path / "config.json"
    path.write_text(json.dumps(document))

    assert load_config(path)["pairs"]["p"]["right"]["query"] == ("SELECT * FROM main.sales.orders")


def test_missing_query(tmp_path):
    document = {"pairs": {"p": pair()}}
    document["pairs"]["p"]["left"]["query"] = "queries/missing.sql"
    path = tmp_path / "config.json"
    path.write_text(json.dumps(document))

    with pytest.raises(FileNotFoundError, match=r"left query file not found: .*missing\.sql"):
        load_config(path)


@pytest.mark.parametrize(
    ("query", "windows", "error"),
    [
        (
            "SELECT * FROM d WHERE {{query_windows:driver}}",
            None,
            "must be a non-empty object",
        ),
        (
            "SELECT * FROM d WHERE {{query_windows:driver}}",
            {"other": {"expression": "d.dt"}},
            "lack declarations: driver",
        ),
        (
            "SELECT * FROM d",
            {"driver": {"expression": "d.dt"}},
            "lack markers: driver",
        ),
        (
            "SELECT * FROM d WHERE {{query_windows:bad name}}",
            {"bad name": {"expression": "d.dt"}},
            "malformed",
        ),
        (
            "SELECT * FROM d WHERE {{query_windows:driver}}",
            {"driver": {"expression": ""}},
            "expression must be a non-empty string",
        ),
    ],
)
def test_query_windows(query, windows, error):
    row = pair()
    row["left"].update(query=query, query_windows=windows)
    with pytest.raises(ValueError, match=error):
        validate_config({"pairs": {"p": row}})


def test_query_oracle():
    row = pair(left="file")
    row["left"].update(
        query="SELECT * FROM {src} WHERE {{query_windows:driver}}",
        query_windows={"driver": {"expression": "dt"}},
    )
    with pytest.raises(ValueError, match="only for Oracle"):
        validate_config({"pairs": {"p": row}})


@pytest.mark.parametrize(
    ("name", "body", "error"),
    [
        ("missing.csv", None, "col_map file not found: .*missing.csv"),
        ("p.txt", "a", r"must be \.csv or \.json"),
        ("p.csv", "src,dst\na,b\n", "left,right"),
        ("p.csv", "left,right\na,\n", "needs left and right"),
        ("p.csv", "left,right\na,x\nb,x\n", "twice"),
        ("p.csv", "left,right\na,x\na,y\n", "twice"),
        ("p.json", '["a"]', "object of left: right"),
        ("p.json", '{"a": 1}', "object of left: right"),
        ("p.csv", "left,right,type\na,x,text\n", "numeric|categorical"),
    ],
)
def test_bad_col(tmp_path, name, body, error):
    path = config_with_map(tmp_path, f"maps/{name}")
    if body is not None:
        (tmp_path / "maps" / name).write_text(body)
    with pytest.raises((ValueError, FileNotFoundError), match=error):
        load_config(path)


def test_col_agree(tmp_path):
    path = config_with_map(tmp_path, "maps/p.csv", col_type_overrides={"a": "categorical"})
    (tmp_path / "maps/p.csv").write_text("left,right,type\na,x,numeric\n")
    with pytest.raises(ValueError, match=r"disagree on \['a'\]"):
        load_config(path)
