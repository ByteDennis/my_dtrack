from pathlib import Path

import pytest

from pair_recon_left.common.model import hashing
from pair_recon_left.common.sql import sampling as left_sampling
from pair_recon_right.engine.checks import col_check as cloud_col_check
from pair_recon_right.engine.common.model import hashing as cloud_hashing
from pair_recon_right.engine.common.sql import sampling as right_sampling

SRC = Path(__file__).resolve().parents[4] / "src"
RIGHT_MODULES = {
    "model": {"attribute_checks.py", "audit.py", "hashing.py", "manifest.py", "protocol.py"},
    "sql": {"column_types.py", "date_platforms.py", "dates.py", "query_windows.py", "sampling.py"},
}
EXACT_MIRRORS = ("attribute_checks.py", "audit.py", "hashing.py", "manifest.py", "protocol.py")


def test_right():
    right = SRC / "pair_recon_right/engine/common"
    assert {path.name for path in right.iterdir() if path.name != "__pycache__"} == {
        "__init__.py",
        *RIGHT_MODULES,
    }
    for folder, expected in RIGHT_MODULES.items():
        assert {path.name for path in (right / folder).glob("*.py")} == expected


@pytest.mark.parametrize("name", EXACT_MIRRORS)
def test_module(name):
    left = (SRC / "pair_recon_left/common/model" / name).read_bytes()
    right = (SRC / "pair_recon_right/engine/common/model" / name).read_bytes()
    assert left == right


def test_query_windows_module():
    left = (SRC / "pair_recon_left/common/sql/query_windows.py").read_bytes()
    right = (SRC / "pair_recon_right/engine/common/sql/query_windows.py").read_bytes()
    assert left == right


def test_right2():
    from pair_recon_left.common.sql.date_platforms import DATE_SQL_PLATFORMS as left_platforms
    from pair_recon_right.engine.common.sql.date_platforms import (
        DATE_SQL_PLATFORMS as right_platforms,
    )

    assert set(right_platforms) == {"spark", "databricks"}
    assert set(left_platforms) == {"oracle", "spark", "databricks", "hadoop", "athena", "duckdb"}


def test_cloud():
    left = {
        "col_type": "numeric",
        "n_total": 3,
        "n_missing": 0,
        "n_unique": 3,
        "hash_sum": "10",
        "mean": 2.0,
        "std": 1.0,
        "min_val": "1",
        "max_val": "3",
    }
    same = cloud_col_check.compare_statistics(
        pair="p", window="w", column="x", left=left, right=left
    )
    changed = cloud_col_check.compare_statistics(
        pair="p", window="w", column="x", left=left, right={**left, "mean": 9.0}
    )
    assert same["verdict"] == "match" and changed["verdict"] == "diff"


def test_key():
    cases = {
        ("007", "numeric"): "7",
        ("007", "string"): "007",
        ("+007.50", "numeric"): "7.5",
        ("2.50", "numeric"): "2.5",
        ("2.0", "numeric"): "2",
        ("1e3", "numeric"): "1e3",
        ("-0.10", "numeric"): "-0.1",
        (" Abc ", "string"): "abc",
        ("", "string"): "<<NULL>>",
        ("01JAN2026", "date"): "2026-01-01",
    }
    for (value, family), expected in cases.items():
        assert cloud_hashing.key_family(family).normalize(value) == expected, (value, family)
        assert hashing.key_family(family).normalize(value) == expected, (value, family)


@pytest.mark.parametrize(
    ("plan", "key_tuples"),
    [
        ({"mode": "all"}, None),
        ({"mode": "fraction", "threshold": 123456}, None),
        ({"mode": "fixed", "count": 2}, [["007", " AbC "], ["2.0", "O'K"]]),
    ],
)
@pytest.mark.parametrize("side", ["left", "right"])
def test_sample(plan, key_tuples, side):
    arguments = (
        plan,
        ["`id`", "`label`"],
        ["numeric", "string"],
        "spark",
        side,
        key_tuples,
    )
    assert right_sampling.sample_predicate_sql(*arguments) == left_sampling.sample_predicate_sql(
        *arguments
    )
