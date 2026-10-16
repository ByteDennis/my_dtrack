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


def test_unit_id(common):
    unit_id = common.manifest.unit_id
    assert unit_id("p", "2026-01-01", "AMOUNT") == unit_id("p", "2026-01-01", "AMOUNT")
    assert unit_id("p", "2026-01-01", "AMOUNT").startswith("p--2026-01-01--amount--")
    assert unit_id("p", "w", "a b") != unit_id("p", "w", "a_b")
    assert unit_id("p", "w", "***").split("--")[2] == "empty"
    assert len(unit_id("x" * 99, "w", "c").split("--")[0]) == 48


@pytest.mark.parametrize(
    ("raw", "expected", "error"),
    [
        (None, {"mode": "all"}, None),
        ("", {"mode": "all"}, None),
        (False, {"mode": "all"}, None),
        (20, {"mode": "fixed", "count": 20}, None),
        ("20", {"mode": "fixed", "count": 20}, None),
        (1.0, {"mode": "fraction", "rate": 1.0, "threshold": 2**32}, None),
        ("0.5", {"mode": "fraction", "rate": 0.5, "threshold": 2**31}, None),
        ("1e-9", {"mode": "fraction", "rate": 1e-9, "threshold": 4}, None),
        (True, None, TypeError),
        (1.5, None, ValueError),
        (0.0, None, ValueError),
        (0, None, ValueError),
    ],
)
def test_sample(common, raw, expected, error):
    if error is None:
        assert common.manifest.sample_plan(raw) == expected
    else:
        with pytest.raises(error):
            common.manifest.sample_plan(raw)


def test_manifest_windows(common):
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
    dated = common.manifest.build_manifest(
        "run",
        "p",
        _variant(
            column_types={
                "AMOUNT": {
                    "type": "date",
                    "left_date_type": {"type": "sas", "format": "datetime"},
                    "right_date_type": "date",
                }
            }
        ),
        ROW_CHECK,
    )
    assert dated["column_types"]["AMOUNT"]["left_date_type"] == {
        "type": "sas",
        "format": "datetime",
    }


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
def test_manifest_invalid(common, pair, row_check, message):
    with pytest.raises(ValueError, match=message):
        common.manifest.build_manifest("run", "p", pair, row_check)


def test_manifest_defaults(common):
    manifest = common.manifest.build_manifest(
        "run", "p", _variant(vintage=None, sample=None, key=""), ROW_CHECK
    )
    assert [w["bucket"] for w in manifest["windows"]] == ["2026-01-01", "2026-01-03", "2026-02-01"]
    assert manifest["sample"] == {"mode": "all"}


def test_all_window(common):
    pair = {
        "partition_mode": "all",
        "left": {"source": "oracle", "table": "A"},
        "right": {"source": "databricks", "table": "a"},
        "col_map": {"ID": "id", "AMOUNT": "amount"},
        "key": "ID",
        "key_families": {"ID": "numeric"},
        "sample": 0.5,
    }
    row_check = {
        "gate_decision": {"passed": True},
        "partitions": {"all": {"left": 9, "right": 9, "verdict": "match"}},
    }
    manifest = common.manifest.build_manifest("run", "p", pair, row_check)
    assert manifest["partition_mode"] == "all"
    assert manifest["vintage"] == "all"
    assert manifest["columns"] == ["ID", "AMOUNT"]
    assert manifest["windows"] == [{"bucket": "all", "partitions": ["all"], "n_left": 9}]
    assert manifest["expected_units"] == 2


def test_version_two(common):
    pair = _variant(
        version=2,
        excluded_partitions=["2026-01-02", "2026-02-02"],
        excluded_columns=["ID"],
    )
    manifest = common.manifest.build_manifest("run", "p", pair, ROW_CHECK)
    assert manifest["version"] == 2 and manifest["plan_id"]
    assert manifest["columns"] == ["AMOUNT"]
    assert manifest["windows"] == [
        {"bucket": "2026-01-01", "partitions": ["2026-01-01", "2026-01-03"], "n_left": 7},
        {"bucket": "2026-02-01", "partitions": ["2026-02-01"], "n_left": 5},
    ]


def test_version_ids(common):
    first = common.manifest.build_manifest("run", "p", _variant(version=2), ROW_CHECK)
    second = common.manifest.build_manifest(
        "run", "p", _variant(version=2, excluded_partitions=["2026-01-02"]), ROW_CHECK
    )
    first_ids = {
        window["bucket"]: common.manifest.unit_id_for(first, window, "AMOUNT")
        for window in first["windows"]
    }
    second_ids = {
        window["bucket"]: common.manifest.unit_id_for(second, window, "AMOUNT")
        for window in second["windows"]
    }
    assert first_ids["2026-01-01"] != second_ids["2026-01-01"]
    assert first_ids["2026-02-01"] == second_ids["2026-02-01"]
    right_only = common.manifest.build_manifest(
        "run", "p", _variant(version=2, excluded_partitions=["2026-03-01"]), ROW_CHECK
    )
    assert common.manifest.planned_unit_ids(right_only) == common.manifest.planned_unit_ids(first)
    assert right_only["plan_id"] != first["plan_id"]


def test_date_key(common):
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
        (lambda m: m.update(protocol_version=1), "protocol version"),
        (lambda m: m.update(protocol_version=99), "protocol version"),
        (lambda m: m.update(right={"source": "csv"}), "must be databricks"),
        (lambda m: m.update(expected_units=1), "expected_units"),
        (lambda m: m["windows"][0].update(partitions=["b", "a"]), "sorted and unique"),
    ],
)
def test_manifest_validation(common, change, message):
    manifest = common.manifest.build_manifest("run", "p", PAIR, ROW_CHECK)
    change(manifest)
    with pytest.raises(ValueError, match=message):
        common.manifest.validate_manifest(manifest)
