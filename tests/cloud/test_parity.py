import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dtrack_cloud import comparison as cloud_comparison
from dtrack_cloud import dates as cloud_dates
from dtrack_cloud import manifest as cloud_manifest
from dtrack_cloud import normalization as cloud_norm
from dtrack_cloud import sql as cloud_sql

from dtrack_distributed.domain import manifest
from dtrack_distributed.engine import dates, normalization, sql

PAIRS = [
    (dates, cloud_dates, ["canonical_date", "bucket_date", "apply_synonyms", "contiguous_runs"]),
    (
        normalization,
        cloud_norm,
        ["collapse_decimal", "normalize_value", "hash32", "selected_fraction"],
    ),
    (sql, cloud_sql, ["partition_where", "bounds_where", "date_literal"]),
    (manifest, cloud_manifest, ["unit_id", "sample_plan", "build_manifest", "validate_manifest"]),
]


def test_vendored_protocol_modules_are_byte_identical_functions():
    import inspect

    for local, cloud, names in PAIRS:
        for name in names:
            assert inspect.getsource(getattr(local, name)) == inspect.getsource(
                getattr(cloud, name)
            ), f"{name} drifted between local and cloud"


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
    same = cloud_comparison.compare_statistics(
        pair="p", window="w", column="x", left=left, right=left
    )
    changed = cloud_comparison.compare_statistics(
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
        assert cloud_norm.normalize_value(value, family) == expected, (value, family)
        assert normalization.normalize_value(value, family) == expected, (value, family)
