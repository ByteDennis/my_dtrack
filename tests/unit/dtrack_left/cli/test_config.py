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


def test_right_side_is_always_databricks():
    with pytest.raises(ValueError, match="Right side must be databricks"):
        validate_config({"pairs": {"p": pair(right="csv")}})


@pytest.mark.parametrize("source", ["oracle", "athena", "aws", "hadoop", "csv", "file", "duckdb"])
def test_supported_left_sources(source):
    assert validate_config({"pairs": {"p": pair(left=source)}})


def test_sas_is_out_of_scope():
    with pytest.raises(ValueError, match="unsupported Left source"):
        validate_config({"pairs": {"p": pair(left="sas")}})


def test_optional_adapters_fail_with_install_instructions_when_absent():
    from dtrack_left.backends.sources import AthenaSource, HadoopSource

    with pytest.raises(ImportError, match=r"\[athena\]"):
        AthenaSource({"source": "athena"}).connect()
    with pytest.raises(NotImplementedError, match="disabled"):
        HadoopSource({"source": "hadoop"})


def test_conn_macro_expands_named_profiles_and_pair_fields_win():
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


def test_config_needs_pairs_and_a_profile_object():
    with pytest.raises(ValueError, match="non-empty pairs"):
        validate_config({"pairs": {}})
    with pytest.raises(TypeError, match="connections must be an object"):
        validate_config({"connections": ["pcds"], "pairs": {"p": pair()}})


def test_csv_sides_may_name_a_file_as_conn_macro_and_skipped_pairs_are_not_checked():
    config = {"pairs": {"p": pair(left="csv"), "off": {"skip": True}}}
    config["pairs"]["p"]["left"]["conn_macro"] = "data/a.csv"
    assert validate_config(config)["pairs"]["p"]["left"]["conn_macro"] == "data/a.csv"


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
def test_pairs_need_date_columns_and_a_column_map(change, message):
    row = pair()
    change(row)
    with pytest.raises(ValueError, match=message):
        validate_config({"pairs": {"p": row}})


def test_load_config_reads_json_and_loads_dotenv(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("DTRACK_UNIT_MARKER", raising=False)
    (tmp_path / ".env").write_text("DTRACK_UNIT_MARKER=from-dotenv\n")
    path = tmp_path / "c.json"
    path.write_text(json.dumps({"pairs": {"p": pair()}}))
    assert load_config(path)["pairs"]["p"]["left"]["source"] == "oracle"
    assert os.environ["DTRACK_UNIT_MARKER"] == "from-dotenv"


def test_right_files_kind_with_a_src_patch_is_accepted():
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


def test_col_map_csv_is_relative_to_the_config_file_and_fills_types(tmp_path, monkeypatch):
    path = config_with_map(tmp_path, "maps/p.csv")
    (tmp_path / "maps/p.csv").write_text(
        "﻿# left side, right side\nleft, right ,type\nORDER_ID,order_id,\n\nAMT,amount,Numeric\n"
    )
    monkeypatch.chdir(tmp_path / "maps")
    loaded = load_config(path)["pairs"]["p"]
    assert loaded["col_map"] == {"ORDER_ID": "order_id", "AMT": "amount"}
    assert loaded["col_type_overrides"] == {"AMT": "numeric"}


def test_col_map_type_can_name_a_registered_asymmetric_date_type(tmp_path):
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


def test_inline_json_col_map_accepts_right_and_registered_type(tmp_path):
    path = tmp_path / "c.json"
    document = {"pairs": {"p": pair()}}
    document["pairs"]["p"]["col_map"] = {
        "SAS_ATTRIBUTE": {"right": "date_attribute", "type": "sas_datetime_date"}
    }
    path.write_text(json.dumps(document))
    loaded = load_config(path)["pairs"]["p"]
    assert loaded["col_map"] == {"SAS_ATTRIBUTE": "date_attribute"}
    assert loaded["column_types"]["SAS_ATTRIBUTE"]["right_date_type"] == "date"


def test_a_new_date_column_type_only_needs_a_registered_class(tmp_path):
    @register_column_type("test_compact_to_date")
    class CompactToDate(DateColumnType):
        left_date_type: ClassVar[object] = {"type": "string", "format": "compact"}
        right_date_type: ClassVar[object] = "date"

    path = tmp_path / "c.json"
    document = {"pairs": {"p": pair()}}
    document["pairs"]["p"]["col_map"] = {
        "D": {"right": "d", "type": "test_compact_to_date"}
    }
    path.write_text(json.dumps(document))
    assert load_config(path)["pairs"]["p"]["column_types"]["D"] == {
        "type": "date",
        "left_date_type": {"type": "string", "format": "compact"},
        "right_date_type": "date",
    }


def test_col_map_json_file_and_inline_overrides_merge(tmp_path):
    path = config_with_map(tmp_path, "maps/p.json", col_type_overrides={"ID": "categorical"})
    (tmp_path / "maps/p.json").write_text(json.dumps({"ID": "id", "AMT": "amount"}))
    loaded = load_config(path)["pairs"]["p"]
    assert loaded["col_map"] == {"ID": "id", "AMT": "amount"}
    assert loaded["col_type_overrides"] == {"ID": "categorical"}


def test_inline_col_map_is_untouched(tmp_path):
    path = tmp_path / "c.json"
    path.write_text(json.dumps({"pairs": {"p": pair()}}))
    assert load_config(path)["pairs"]["p"]["col_map"] == {"amount": "amount"}


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
def test_bad_col_map_files_fail_with_the_path(tmp_path, name, body, error):
    path = config_with_map(tmp_path, f"maps/{name}")
    if body is not None:
        (tmp_path / "maps" / name).write_text(body)
    with pytest.raises((ValueError, FileNotFoundError), match=error):
        load_config(path)


def test_col_map_type_must_agree_with_inline_overrides(tmp_path):
    path = config_with_map(tmp_path, "maps/p.csv", col_type_overrides={"a": "categorical"})
    (tmp_path / "maps/p.csv").write_text("left,right,type\na,x,numeric\n")
    with pytest.raises(ValueError, match=r"disagree on \['a'\]"):
        load_config(path)
