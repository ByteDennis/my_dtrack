import pytest
from dtrack_cloud.gates import evaluate_gate, reconcile_partitions

from dtrack_distributed.domain.manifest import build_manifest, sample_plan, unit_id
from dtrack_distributed.engine.sampling import sample_csv
from dtrack_distributed.engine.sql import partition_where


def test_exact_partition_barrier_excludes_mismatches_and_one_sided_partitions():
    rows = reconcile_partitions(
        {"2026-01-01": 10, "2026-01-02": 5},
        {"2026-01-01": 10, "2026-01-03": 5},
    )
    assert [rows[key]["verdict"] for key in sorted(rows)] == ["match", "left_only", "right_only"]
    assert evaluate_gate(rows, "ratio:0.67")["passed"] is True


def test_manifest_preserves_holes_in_admitted_partitions():
    pair = {
        "left": {"source": "oracle", "table": "a", "date_col": "dt"},
        "right": {"source": "databricks", "table": "b", "date_col": "dt"},
        "col_map": {"amount": "amount"},
        "vintage": "month",
    }
    stage1 = {
        "gate_decision": {"passed": True},
        "partitions": {
            "2026-01-01": {"left": 1, "right": 1, "verdict": "match"},
            "2026-01-02": {"left": 1, "right": 2, "verdict": "count_diff"},
            "2026-01-03": {"left": 1, "right": 1, "verdict": "match"},
        },
    }
    manifest = build_manifest("run", "pair", pair, stage1)
    assert manifest["windows"][0]["partitions"] == ["2026-01-01", "2026-01-03"]
    where = partition_where("dt", manifest["windows"][0]["partitions"], dialect="oracle")
    assert "2026-01-02" not in where


def test_sampling_requires_valid_values_and_unit_id_is_stable():
    assert sample_plan(0.1)["mode"] == "fraction"
    assert sample_plan(20) == {"mode": "fixed", "count": 20}
    with pytest.raises(ValueError):
        sample_plan(0)
    assert unit_id("p", "2026-01", "x") == unit_id("p", "2026-01", "x")


def test_fixed_sampling_uses_manifest_key_tuples_on_both_sides(tmp_path):
    path = tmp_path / "rows.csv"
    path.write_text("id,value\n1,a\n2.0,b\n3,c\n")
    sampled = sample_csv(
        path,
        key_columns=["id"],
        mode="fixed",
        count=1,
        key_tuples=[["2"]],
        families=["numeric"],
    )
    assert sampled.read_text().splitlines() == ["id,value", "2.0,b"]
