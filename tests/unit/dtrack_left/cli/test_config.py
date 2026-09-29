import json
import os

import pytest

from dtrack_left.cli.config import load_config, validate_config


def pair(right="databricks", left="oracle"):
    return {
        "left": {"source": left, "table": "a", "date_col": "dt"},
        "right": {"source": right, "table": "b", "date_col": "dt"},
        "col_map": {"amount": "amount"},
    }


def test_right_side_is_always_databricks():
    with pytest.raises(ValueError, match="Right side must be databricks"):
        validate_config({"pairs": {"p": pair(right="csv")}})


@pytest.mark.parametrize("source", ["oracle", "athena", "aws", "hadoop", "csv"])
def test_supported_left_sources(source):
    assert validate_config({"pairs": {"p": pair(left=source)}})


def test_sas_is_out_of_scope():
    with pytest.raises(ValueError, match="unsupported Left source"):
        validate_config({"pairs": {"p": pair(left="sas")}})


def test_optional_adapters_fail_with_install_instructions_when_absent():
    from dtrack_left.backends.sources import AthenaSource, HadoopSource

    with pytest.raises(ImportError, match=r"\[athena\]"):
        AthenaSource({"source": "athena"}).connect()
    with pytest.raises(ImportError, match=r"\[hadoop\]"):
        HadoopSource({"source": "hadoop"}).connect()


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
