from pathlib import Path

import pytest

from dtrack_left.common import hashing
from dtrack_left.common import sampling as left_sampling
from dtrack_right.checks import col_check as cloud_col_check
from dtrack_right.common import hashing as cloud_hashing
from dtrack_right.common import sampling as right_sampling

SRC = Path(__file__).resolve().parents[3] / "src"
RIGHT_COMMON = {
    "__init__.py",
    "audit.py",
    "column_types.py",
    "date_platforms.py",
    "dates.py",
    "hashing.py",
    "manifest.py",
    "protocol.py",
    "sampling.py",
}
EXACT_MIRRORS = ("audit.py", "hashing.py", "manifest.py", "protocol.py")


def test_right_common_contains_only_databricks_protocol_modules():
    right = SRC / "dtrack_right/common"
    assert {path.name for path in right.glob("*.py")} == RIGHT_COMMON


@pytest.mark.parametrize("name", EXACT_MIRRORS)
def test_runtime_neutral_common_modules_are_identical_on_both_sides(name):
    left = (SRC / "dtrack_left/common" / name).read_bytes()
    right = (SRC / "dtrack_right/common" / name).read_bytes()
    assert left == right


def test_right_date_sql_runtime_contains_only_databricks_platforms():
    from dtrack_left.common.date_platforms import DATE_SQL_PLATFORMS as left_platforms
    from dtrack_right.common.date_platforms import DATE_SQL_PLATFORMS as right_platforms

    assert set(right_platforms) == {"spark", "databricks"}
    assert set(left_platforms) == {"oracle", "spark", "databricks", "hadoop", "athena", "duckdb"}


def test_cloud_comparison_matches_expected_verdicts():
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


def test_key_normalisation_strips_numeric_zeros_but_string_keeps_them():
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
def test_left_and_right_compile_identical_spark_sample_predicates(plan, key_tuples, side):
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
