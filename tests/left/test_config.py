import pytest

from dtrack_left.config import validate_config


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
    from dtrack_left.sources import AthenaSource, HadoopSource

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
    with pytest.raises(ValueError, match="unknown conn_macro"):
        validate_config(
            {
                "pairs": {
                    "p": {
                        **pair(),
                        "left": {"conn_macro": "nope", "source": "oracle", "date_col": "dt"},
                    }
                }
            }
        )
