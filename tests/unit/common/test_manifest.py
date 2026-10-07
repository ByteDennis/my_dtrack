import copy

import pytest

PAIR = {
    "left": {"source": "oracle", "table": "A", "date_col": "DT"},
    "right": {"source": "databricks", "table": "a", "date_col": "dt"},
    "col_map": {"DT": "dt", "ID": "id", "AMOUNT": "amount"},
    "vintage": "month",
    "key": "ID",
    "key_families": {"ID": "numeric"},
    "sample": 0.5,
}
ROW_CHECK = {
    "gate_decision": {"passed": True},
    "partitions": {
        "2026-01-01": {"left": 3, "right": 3, "verdict": "match"},
        "2026-01-02": {"left": 1, "right": 2, "verdict": "count_diff"},
        "2026-01-03": {"left": 4, "right": 4, "verdict": "match"},
        "2026-02-01": {"left": 5, "right": 5, "verdict": "match"},
        "2026-02-02": {"left": 5, "right": None, "verdict": "left_only"},
    },
}


def test_unit_id_is_stable_slugged_and_collision_resistant(common):
    unit_id = common.manifest.unit_id
    assert unit_id("p", "2026-01-01", "AMOUNT") == unit_id("p", "2026-01-01", "AMOUNT")
    assert unit_id("p", "2026-01-01", "AMOUNT").startswith("p--2026-01-01--amount--")
    assert unit_id("p", "w", "a b") != unit_id("p", "w", "a_b")
    assert unit_id("p", "w", "***").split("--")[2] == "empty"
    assert len(unit_id("x" * 99, "w", "c").split("--")[0]) == 48


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, {"mode": "all"}),
        ("", {"mode": "all"}),
        (False, {"mode": "all"}),
        (20, {"mode": "fixed", "count": 20}),
        ("20", {"mode": "fixed", "count": 20}),
        (1.0, {"mode": "fraction", "rate": 1.0, "threshold": 2**32}),
        ("0.5", {"mode": "fraction", "rate": 0.5, "threshold": 2**31}),
        ("1e-9", {"mode": "fraction", "rate": 1e-9, "threshold": 4}),
    ],
)
def test_sample_plan_parses_fractions_and_counts(common, raw, expected):
    assert common.manifest.sample_plan(raw) == expected


@pytest.mark.parametrize(
    ("raw", "error"), [(True, TypeError), (1.5, ValueError), (0.0, ValueError), (0, ValueError)]
)
def test_sample_plan_rejects_invalid_values(common, raw, error):
    with pytest.raises(error):
        common.manifest.sample_plan(raw)


def test_build_manifest_windows_keep_holes_and_count_units(common):
    manifest = common.manifest.build_manifest("run", "p", PAIR, ROW_CHECK)
    assert manifest["columns"] == ["ID", "AMOUNT"]
    assert manifest["windows"] == [
        {"bucket": "2026-01-01", "partitions": ["2026-01-01", "2026-01-03"], "n_left": 7},
        {"bucket": "2026-02-01", "partitions": ["2026-02-01"], "n_left": 5},
    ]
    assert manifest["expected_units"] == 4
    assert manifest["key_cols"] == ["ID"] and manifest["right_key_cols"] == ["id"]
    assert manifest["key_families"] == ["numeric"] and manifest["sample"]["mode"] == "fraction"
    forced = common.manifest.build_manifest(
        "run", "p", _variant(col_type_overrides={"AMOUNT": "numeric"}), ROW_CHECK
    )
    assert forced["col_type_overrides"] == {"AMOUNT": "numeric"}


def _variant(**changes):
    pair = copy.deepcopy(PAIR)
    pair.update(changes)
    return pair


@pytest.mark.parametrize(
    ("pair", "row_check", "message"),
    [
        (_variant(right={"source": "csv"}), ROW_CHECK, "must be databricks"),
        (PAIR, {**ROW_CHECK, "gate_decision": {"passed": False}}, "has not passed"),
        (_variant(col_map={"DT": "dt"}), ROW_CHECK, "no mapped value columns"),
        (_variant(key_families={"ID": "blob"}), ROW_CHECK, "unsupported key families"),
        (_variant(key=""), ROW_CHECK, "sampling requires a key"),
        (_variant(col_type_overrides={"NOPE": "numeric"}), ROW_CHECK, "got NOPE"),
        (_variant(col_type_overrides={"ID": "date"}), ROW_CHECK, "got date"),
    ],
)
def test_build_manifest_refuses_invalid_pairs(common, pair, row_check, message):
    with pytest.raises(ValueError, match=message):
        common.manifest.build_manifest("run", "p", pair, row_check)


def test_build_manifest_defaults_to_daily_windows_and_full_scan(common):
    manifest = common.manifest.build_manifest(
        "run", "p", _variant(vintage=None, sample=None, key=""), ROW_CHECK
    )
    assert [w["bucket"] for w in manifest["windows"]] == ["2026-01-01", "2026-01-03", "2026-02-01"]
    assert manifest["sample"] == {"mode": "all"}


def test_date_partition_key_reuses_each_sides_date_type_for_hashing(common):
    pair = _variant(
        key="DT",
        key_families={},
        left={**PAIR["left"], "date_type": {"type": "sas", "format": "days"}},
        right={
            **PAIR["right"],
            "date_type": {"type": "string", "format": "dash"},
        },
    )
    manifest = common.manifest.build_manifest("run", "p", pair, ROW_CHECK)
    assert manifest["key_families"] == [
        {
            "family": "date_type",
            "left_date_type": {"type": "sas", "format": "days"},
            "right_date_type": {"type": "string", "format": "dash"},
        }
    ]


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (lambda m: m.pop("windows"), "missing fields: windows"),
        (lambda m: m.update(protocol_version=99), "protocol version"),
        (lambda m: m.update(right={"source": "csv"}), "must be databricks"),
        (lambda m: m.update(expected_units=1), "expected_units"),
        (lambda m: m["windows"][0].update(partitions=["b", "a"]), "sorted and unique"),
    ],
)
def test_validate_manifest_rejects_broken_documents(common, change, message):
    manifest = common.manifest.build_manifest("run", "p", PAIR, ROW_CHECK)
    change(manifest)
    with pytest.raises(ValueError, match=message):
        common.manifest.validate_manifest(manifest)
