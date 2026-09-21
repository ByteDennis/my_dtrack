import tomllib
from pathlib import Path


def test_databricks_and_oracle_are_mandatory_but_athena_and_hadoop_are_optional():
    project = tomllib.loads(Path("pyproject.toml").read_text())
    dependencies = project["project"]["dependencies"]
    extras = project["project"]["optional-dependencies"]
    assert any(item.startswith("oracledb") for item in dependencies)
    assert any(item.startswith("databricks-sdk") for item in dependencies)
    assert any(item.startswith("databricks-sql-connector") for item in dependencies)
    assert not any("pyathena" in item or "impyla" in item for item in dependencies)
    assert extras["athena"] == ["pyathena>=2.0"]
    assert any(item.startswith("impyla") for item in extras["hadoop"])
    assert sorted(extras) == ["athena", "hadoop"]
    assert "sas" not in extras
