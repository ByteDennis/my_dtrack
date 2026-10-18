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


@pytest.mark.parametrize(
    ("source", "valid"),
    [("csv", True), ("hadoop", True), ("file", False), ("athena", False), ("duckdb", False)],
)
def test_source(source, valid):
    value = pair()
    value["left"]["source"] = source
    if source == "csv":
        value["left"]["path"] = "C:/data/sample.csv"
    if source == "hadoop":
        value["left"]["schema"] = "risk"
    if valid:
        assert validate_config({"version": 2, "pairs": {"p": value}})
    else:
        with pytest.raises(ValueError, match="unsupported Left source"):
            validate_config({"version": 2, "pairs": {"p": value}})


def test_hadoop():
    value = pair()
    value["left"] = {"source": "hadoop", "query": "SELECT A FROM risk.L"}
    assert validate_config({"version": 2, "pairs": {"p": value}})


@pytest.mark.parametrize("source", ["databricks", "csv"])
def test_right(source):
    value = pair()
    if source == "csv":
        value["right"] = {"source": "csv", "path": "right.csv"}
        value["right_pre_sampled"] = True
    assert validate_config({"version": 2, "pairs": {"p": value}})


def test_snapshot():
    value = pair("date")
    value["snapshot"] = True
    value["left"].pop("date_col")
    assert validate_config({"version": 2, "pairs": {"p": value}})["pairs"]["p"]["snapshot"]


@pytest.mark.parametrize("sample", [10, "10"])
def test_sample(sample):
    value = pair()
    value["left"] = {"source": "csv", "path": "sample.csv"}
    value.update(pre_sampled=True, sample=sample, key="A")
    with pytest.raises(ValueError, match="requires all or fraction"):
        validate_config({"version": 2, "pairs": {"p": value}})
