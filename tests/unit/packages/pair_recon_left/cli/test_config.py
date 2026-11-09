import pytest

from pair_recon_left.cli.config import validate_config


def pair(partition="all"):
    dates = {} if partition == "all" else {"date_col": "D"}
    return {
        "partition": partition,
        "left": {"source": "oracle", "table": "L", **dates},
        "right": {"source": "databricks", "table": "R", **dates},
        "col_map": {"A": "a"},
        "statistics": ["hash_sum", "n_unique"],
    }


@pytest.mark.parametrize("partition", ["all", "day", "month"])
def test_valid(partition):
    value = validate_config({"pairs": {"p": pair(partition)}})
    assert value["pairs"]["p"]["statistics"] == ["n_unique", "hash_sum"]
    assert value["pairs"]["p"]["audit"] is False


def test_audit_must_be_boolean():
    value = pair()
    value["audit"] = "false"
    with pytest.raises(TypeError, match="audit must be true or false"):
        validate_config({"pairs": {"p": value}})


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
        assert validate_config({"pairs": {"p": value}})
    else:
        with pytest.raises(ValueError, match="unsupported Left source"):
            validate_config({"pairs": {"p": value}})


def test_hadoop():
    value = pair()
    value["left"] = {"source": "hadoop", "query": "SELECT A FROM risk.L"}
    assert validate_config({"pairs": {"p": value}})


@pytest.mark.parametrize("source", ["oracle", "hadoop"])
def test_sas_sample_csv(source):
    value = pair("day")
    value["sample"] = 0.1
    value["key"] = "A"
    value["left"] = {
        "source": source,
        "date_col": "D",
        "sas_sample_csv": " C:/PairRecon/samples/left.csv ",
        **({"table": "L"} if source == "oracle" else {"schema": "risk", "table": "L"}),
    }

    configured = validate_config({"pairs": {"p": value}})["pairs"]["p"]

    assert configured["left"]["sas_sample_csv"] == "C:/PairRecon/samples/left.csv"


def test_sas_sample_csv_rejects_csv_source_and_fixed_sampling():
    value = pair("day")
    value["left"] = {
        "source": "csv",
        "path": "left.csv",
        "date_col": "D",
        "sas_sample_csv": "sample.csv",
    }
    with pytest.raises(ValueError, match="Oracle or Hadoop"):
        validate_config({"pairs": {"p": value}})

    value = pair("day")
    value.update(sample=10, key="A")
    value["left"]["sas_sample_csv"] = "sample.csv"
    with pytest.raises(ValueError, match="all or fraction"):
        validate_config({"pairs": {"p": value}})


def test_modular_sampling_resolves_mapped_selector():
    value = pair("day")
    value.update(sample=0.1, key="A", use_modular="a")

    configured = validate_config({"pairs": {"p": value}})["pairs"]["p"]

    assert configured["use_modular"] == "A"


@pytest.mark.parametrize(
    ("selector", "sample", "message"),
    [
        ("missing", 0.1, "outside col_map"),
        ("A", 10, "requires percentage sampling"),
        ("", 0.1, "non-empty Left column"),
    ],
)
def test_modular_sampling_rejects_invalid_configuration(selector, sample, message):
    value = pair("day")
    value.update(sample=sample, key="A", use_modular=selector)
    with pytest.raises((TypeError, ValueError), match=message):
        validate_config({"pairs": {"p": value}})


@pytest.mark.parametrize("source", ["databricks", "csv"])
def test_right(source):
    value = pair()
    if source == "csv":
        value["right"] = {"source": "csv", "path": "right.csv"}
        value["right_pre_sampled"] = True
    assert validate_config({"pairs": {"p": value}})


def test_snapshot():
    value = pair("day")
    value["snapshot"] = True
    value["left"].pop("date_col")
    assert validate_config({"pairs": {"p": value}})["pairs"]["p"]["snapshot"]


def test_query_windows_on_both_sides():
    value = pair("day")
    value["left"] |= {
        "query": "SELECT * FROM L l WHERE {{query_windows:driver}}",
        "query_windows": {"driver": {"expression": "l.D", "date_type": "date"}},
    }
    value["right"] |= {
        "query": "SELECT * FROM R r WHERE {{query_windows:driver}}",
        "query_windows": {"driver": {"expression": "r.D", "date_type": "date"}},
    }
    configured = validate_config({"pairs": {"p": value}})["pairs"]["p"]
    assert configured["left"]["query_windows"]["driver"]["expression"] == "l.D"
    assert configured["right"]["query_windows"]["driver"]["expression"] == "r.D"


def test_query_windows_require_date_mode_and_declarations():
    value = pair()
    value["right"] |= {"query": "SELECT * FROM R WHERE {{query_windows:driver}}"}
    with pytest.raises(ValueError, match="Right query_windows"):
        validate_config({"pairs": {"p": value}})

    value["right"]["query_windows"] = {"driver": {"expression": "D"}}
    with pytest.raises(ValueError, match="partition all"):
        validate_config({"pairs": {"p": value}})


def test_vintage_selection_config():
    value = pair("day")
    value["vintage_values"] = ["2026-01-01", "2026-01-01"]
    configured = validate_config({"pairs": {"p": value}})["pairs"]["p"]
    assert configured["vintage_values"] == ["2026-01-01"]

    value = pair("day")
    value["vintage_sample_size"] = "3"
    configured = validate_config({"pairs": {"p": value}})["pairs"]["p"]
    assert configured["vintage_sample_size"] == 3


def test_month_number_date_type_normalizes_excluded_partitions():
    value = pair("day")
    value["left"]["date_type"] = {"type": "number", "format": "month"}
    value["right"]["date_type"] = {"type": "number", "format": "month"}
    value["excluded_partitions"] = ["2026-01-15", "2026-01-31", "2026-02-01"]

    configured = validate_config({"pairs": {"p": value}})["pairs"]["p"]

    assert configured["excluded_partitions"] == ["2026-01-01", "2026-02-01"]


def test_vintage_selection_config_rejects_ambiguity_and_all_mode():
    value = pair("day")
    value.update(vintage_sample_size=2, vintage_values=["2026-01-01"])
    with pytest.raises(ValueError, match="not both"):
        validate_config({"pairs": {"p": value}})

    value = pair()
    value["vintage_sample_size"] = 2
    with pytest.raises(ValueError, match="cannot select vintages"):
        validate_config({"pairs": {"p": value}})

    value = pair("day")
    value["vintage_sample_size"] = 1.5
    with pytest.raises(TypeError, match="positive integer"):
        validate_config({"pairs": {"p": value}})


@pytest.mark.parametrize("sample", [10, "10"])
def test_sample(sample):
    value = pair()
    value["left"] = {"source": "csv", "path": "sample.csv"}
    value.update(pre_sampled=True, sample=sample, key="A")
    with pytest.raises(ValueError, match="requires all or fraction"):
        validate_config({"pairs": {"p": value}})


def test_sample_partitions_preserve_preference_order_and_remove_duplicates():
    value = pair()
    value["sample_partitions"] = [" 2026-02-01 ", "2026-01-01", "2026-02-01"]
    configured = validate_config({"pairs": {"p": value}})["pairs"]["p"]
    assert configured["sample_partitions"] == ["2026-02-01", "2026-01-01"]


@pytest.mark.parametrize("selector", ["2026-02-01", 1, [""], [None]])
def test_sample_partitions_require_a_list_of_nonempty_vintage_ids(selector):
    value = pair()
    value["sample_partitions"] = selector
    with pytest.raises(TypeError, match="sample_partitions"):
        validate_config({"pairs": {"p": value}})
