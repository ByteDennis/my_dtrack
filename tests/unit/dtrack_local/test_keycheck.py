from types import SimpleNamespace

import pandas as pd
import pytest

from dtrack_local import keycheck

PAIR = {
    "left": {"source": "csv", "date_col": "EFF_DT"},
    "right": {"source": "databricks", "table": "t", "date_col": "dw_dt", "date_type": "int"},
    "key": "ACCT_ID,OPEN_DT",
    "col_map": {"ACCT_ID": "acct_id", "OPEN_DT": "open_dt", "BAL": "bal"},
    "key_families": {"ACCT_ID": "numeric", "OPEN_DT": "date"},
}


def _csvs(tmp_path, right_norm_sql=None):
    left = pd.DataFrame(
        {
            "EFF_DT": ["2026-01-31 00:00:00"] * 4,
            "ACCT_ID": ["007", "008", "ABC", "009"],
            "OPEN_DT": ["2020-01-01 00:00:00", "2020-01-02 00:00:00", "2020-01-03", "2020-01-04"],
        }
    )
    right = pd.DataFrame(
        {
            "dw_dt": ["20260131"] * 4,
            "acct_id": ["7", "8.00", "abc ", "10"],
            "open_dt": ["20200101", "20200102", "20200103", "20200105"],
        }
    )
    if right_norm_sql is not None:
        right[keycheck.NORM_SQL] = right_norm_sql
    left.to_csv(tmp_path / "left.csv", index=False)
    right.to_csv(tmp_path / "right.csv", index=False)
    return tmp_path / "left.csv", tmp_path / "right.csv"


def test_configured_families_match_differently_written_keys(tmp_path):
    left, right = _csvs(tmp_path)
    check = keycheck.compare_keys("p", PAIR, left, right, window=("2026-01-31", "2026-01-31"))
    summary = check.summary()
    assert summary["matched keys"] == 2 and summary["left only"] == 2 and summary["right only"] == 2
    assert set(check.left_only["ACCT_ID"]) == {"ABC", "009"}


def test_report_explains_each_mismatch_and_points_at_a_family(tmp_path):
    left, right = _csvs(tmp_path)
    check = keycheck.compare_keys(
        "p",
        PAIR,
        left,
        right,
        window=("2026-01-31", "2026-01-31"),
        families={"ACCT_ID": "exact", "OPEN_DT": "exact"},
    )
    text = keycheck.write_report(check, tmp_path / "out" / "keys.txt").read_text()
    assert "== LEFT ONLY: 4 key(s), 3 with a near match ==" in text
    assert "leading/trailing zeros or sign -> numeric" in text
    assert "date format -> date" in text
    assert "letter case -> string" in text
    assert "no near match on the other side" in text
    assert pd.read_csv(tmp_path / "out" / "keys.csv")["side"].value_counts().to_dict() == {
        "left_only": 4,
        "right_only": 4,
    }
    best = check.suggestions.groupby("left key").first()["family"].to_dict()
    assert best == {"ACCT_ID": "numeric", "OPEN_DT": "date"}


def test_only_keys_narrows_both_sides_and_sql_parity_is_reported(tmp_path):
    left, right = _csvs(tmp_path, right_norm_sql=["7|2020-01-01", "X", "abc|2020-01-03", "10|x"])
    check = keycheck.compare_keys(
        "p", PAIR, left, right, window=("a", "b"), only_keys=[["007", "2020-01-01"]]
    )
    assert check.summary()["matched keys"] == 1 and len(check.left) == len(check.right) == 1
    full = keycheck.compare_keys("p", PAIR, left, right, window=("a", "b"))
    assert set(full.sql_mismatch["acct_id"]) == {"8.00", "10"}


@pytest.mark.parametrize(
    ("left", "right", "reason"),
    [
        (" 7", "7", "surrounding whitespace"),
        ("Ab", "aB", "letter case"),
        ("2026-01-31", "20260131", "date format"),
        ("007", "7", "leading/trailing zeros or sign"),
        ("123-45", "12345", "punctuation around digits"),
        ("x", "y", "different value"),
    ],
)
def test_explain_names_the_difference(left, right, reason):
    assert keycheck.explain(left, right)[0] == reason


def test_pull_right_keys_selects_raw_keys_and_the_spark_composite(tmp_path):
    seen = {}
    warehouse = SimpleNamespace(
        to_csv=lambda sql, output, columns: seen.update(sql=sql, cols=columns)
    )
    target = SimpleNamespace(
        config=PAIR["right"], relation=lambda: "`t`", _where=lambda predicate: predicate
    )
    target.warehouse = warehouse
    _, right_keys, families = keycheck.key_spec(PAIR)
    keycheck.pull_right_keys(target, ["2026-01-31"], tmp_path / "r.csv", right_keys, families)
    assert seen["cols"] == ["dw_dt", "acct_id", "open_dt", keycheck.NORM_SQL]
    assert "`dw_dt` IN (20260131)" in seen["sql"] or "20260131" in seen["sql"]
    assert f"concat_ws('|', {families[0].sql('`acct_id`', 'right')}" in seen["sql"]


def test_key_spec_needs_a_key():
    with pytest.raises(ValueError, match="no key"):
        keycheck.key_spec({**PAIR, "key": ""})
