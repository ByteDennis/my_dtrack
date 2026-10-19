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


def test_query_windows_on_both_sides():
    value = pair("date")
    value["left"] |= {
        "query": "SELECT * FROM L l WHERE {{query_windows:driver}}",
        "query_windows": {"driver": {"expression": "l.D", "date_type": "date"}},
    }
    value["right"] |= {
        "query": "SELECT * FROM R r WHERE {{query_windows:driver}}",
        "query_windows": {"driver": {"expression": "r.D", "date_type": "date"}},
    }
    configured = validate_config({"version": 2, "pairs": {"p": value}})["pairs"]["p"]
    assert configured["left"]["query_windows"]["driver"]["expression"] == "l.D"
    assert configured["right"]["query_windows"]["driver"]["expression"] == "r.D"


def test_query_windows_require_date_mode_and_declarations():
    value = pair()
    value["right"] |= {"query": "SELECT * FROM R WHERE {{query_windows:driver}}"}
    with pytest.raises(ValueError, match="Right query_windows"):
        validate_config({"version": 2, "pairs": {"p": value}})

    value["right"]["query_windows"] = {"driver": {"expression": "D"}}
    with pytest.raises(ValueError, match="partition_mode all"):
        validate_config({"version": 2, "pairs": {"p": value}})


def test_vintage_selection_config():
    value = pair("date")
    value["vintage_values"] = ["2026-01-01", "2026-01-01"]
    configured = validate_config({"version": 2, "pairs": {"p": value}})["pairs"]["p"]
    assert configured["vintage_values"] == ["2026-01-01"]

    value = pair("date")
    value["vintage_sample_size"] = "3"
    configured = validate_config({"version": 2, "pairs": {"p": value}})["pairs"]["p"]
    assert configured["vintage_sample_size"] == 3


def test_vintage_selection_config_rejects_ambiguity_and_all_mode():
    value = pair("date")
    value.update(vintage_sample_size=2, vintage_values=["2026-01-01"])
    with pytest.raises(ValueError, match="not both"):
        validate_config({"version": 2, "pairs": {"p": value}})

    value = pair()
    value["vintage_sample_size"] = 2
    with pytest.raises(ValueError, match="cannot select vintages"):
        validate_config({"version": 2, "pairs": {"p": value}})

    value = pair("date")
    value["vintage_sample_size"] = 1.5
    with pytest.raises(TypeError, match="positive integer"):
        validate_config({"version": 2, "pairs": {"p": value}})


@pytest.mark.parametrize("sample", [10, "10"])
def test_sample(sample):
    value = pair()
    value["left"] = {"source": "csv", "path": "sample.csv"}
    value.update(pre_sampled=True, sample=sample, key="A")
    with pytest.raises(ValueError, match="requires all or fraction"):
        validate_config({"version": 2, "pairs": {"p": value}})
