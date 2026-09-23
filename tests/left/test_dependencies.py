import tomllib
from pathlib import Path


def test_local_package_has_no_databricks_dependency_and_optional_left_adapters():
    project = tomllib.loads(Path("pyproject.toml").read_text())
    dependencies = project["project"]["dependencies"]
    extras = project["project"]["optional-dependencies"]
    assert any(item.startswith("oracledb") for item in dependencies)
    assert any(item.startswith("boto3") for item in dependencies)
    assert not any("databricks" in item for item in dependencies)
    assert not any("pyathena" in item or "impyla" in item for item in dependencies)
    assert extras["athena"] == ["pyathena>=2.0"]
    assert any(item.startswith("impyla") for item in extras["hadoop"])
    assert sorted(extras) == ["athena", "hadoop"]


def test_right_package_never_imports_the_left_package():
    cloud = Path("src/dtrack_right")
    for path in cloud.rglob("*.py"):
        assert "dtrack_left" not in path.read_text(), path
