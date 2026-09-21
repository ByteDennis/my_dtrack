import pytest

from dtrack_distributed.config import validate_config


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
    from dtrack_distributed.adapters.sources.athena import AthenaSource
    from dtrack_distributed.adapters.sources.hadoop import HadoopSource

    with pytest.raises(ImportError, match=r"\[athena\]"):
        AthenaSource({"source": "athena"}).connect()
    with pytest.raises(ImportError, match=r"\[hadoop\]"):
        HadoopSource({"source": "hadoop"}).connect()
