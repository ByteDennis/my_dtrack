import pytest

from pair_recon_left.cli.config import validate_config


def pair(mode="all"):
    dates = {} if mode == "all" else {"date_col": "D"}
    return {
        "partition_mode": mode,
        "left": {"source": "oracle", "table": "L", **dates},
        "right": {"source": "databricks", "table": "R", **dates},
        "col_map": {"A": "a"},
        "statistics": ["hash_sum", "n_unique"],
    }


@pytest.mark.parametrize("mode", ["all", "date"])
def test_valid(mode):
    value = validate_config({"version": 2, "pairs": {"p": pair(mode)}})
    assert value["pairs"]["p"]["statistics"] == ["n_unique", "hash_sum"]


@pytest.mark.parametrize("source", ["csv", "hadoop", "athena", "duckdb"])
def test_source(source):
    value = pair()
    value["left"]["source"] = source
    with pytest.raises(ValueError, match="unsupported Left source"):
        validate_config({"version": 2, "pairs": {"p": value}})


def test_snapshot():
    value = pair("date")
    value["snapshot"] = True
    value["left"].pop("date_col")
    assert validate_config({"version": 2, "pairs": {"p": value}})["pairs"]["p"]["snapshot"]
