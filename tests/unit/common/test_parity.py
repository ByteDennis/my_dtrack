from pathlib import Path

from dtrack_left.common import hashing
from dtrack_right.checks import col_check as cloud_col_check
from dtrack_right.common import hashing as cloud_hashing

SRC = Path(__file__).resolve().parents[3] / "src"
SHARED = ["dates.py", "hashing.py", "stats.py", "manifest.py"]


def test_shared_common_modules_are_byte_identical_on_both_sides():
    left, right = SRC / "dtrack_left/common", SRC / "dtrack_right/common"
    for name in SHARED:
        assert (left / name).read_bytes() == (right / name).read_bytes(), f"{name} drifted"
    assert sorted(p.name for p in left.glob("*.py")) == sorted(p.name for p in right.glob("*.py"))


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


def test_key_normalisation_keeps_leading_zeros_and_collapses_only_pure_decimals():
    cases = {
        ("007", "numeric"): "007",
        ("2.50", "numeric"): "2.5",
        ("2.0", "numeric"): "2",
        ("1e3", "numeric"): "1e3",
        ("-0.10", "numeric"): "-0.1",
        (" Abc ", "string"): "abc",
        ("", "string"): "<<NULL>>",
        ("01JAN2026", "date"): "2026-01-01",
    }
    for (value, family), expected in cases.items():
        assert cloud_hashing.normalize_value(value, family) == expected, (value, family)
        assert hashing.normalize_value(value, family) == expected, (value, family)
