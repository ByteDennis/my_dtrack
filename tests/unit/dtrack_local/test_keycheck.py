from types import SimpleNamespace

import pandas as pd
import pytest

from dtrack_left.backends.sources import FileSource
from dtrack_local import keycheck

PAIR = {
    "left": {"source": "csv", "date_col": "EFF_DT"},
    "right": {
        "source": "databricks",
        "table": "t",
        "date_col": "dw_dt",
        "date_type": "number",
    },
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


def test_family_match(tmp_path):
    left, right = _csvs(tmp_path)
    check = keycheck.compare_keys("p", PAIR, left, right, window=("2026-01-31", "2026-01-31"))
    summary = check.summary()
    assert summary["matched keys"] == 2 and summary["left only"] == 2 and summary["right only"] == 2
    assert set(check.left_only["ACCT_ID"]) == {"ABC", "009"}


def test_mismatch_report(tmp_path):
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


def test_key_filter(tmp_path):
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
def test_explanations(left, right, reason):
    assert keycheck.explain(left, right)[0] == reason


def test_right_keys(tmp_path):
    seen = {}
    warehouse = SimpleNamespace(
        to_csv=lambda sql, output, columns, **kwargs: seen.update(
            sql=sql, cols=columns, operation=kwargs.get("operation")
        )
    )
    target = SimpleNamespace(
        config=PAIR["right"], relation=lambda: "`t`", _where=lambda predicate: predicate
    )
    target.warehouse = warehouse
    _, right_keys, families = keycheck.key_spec(PAIR)
    keycheck.pull_right_keys(target, ["2026-01-31"], tmp_path / "r.csv", right_keys, families)
    assert seen["cols"] == ["dw_dt", "acct_id", "open_dt", keycheck.NORM_SQL]
    assert seen["operation"] == "key_check"
    assert "`dw_dt` IN (20260131)" in seen["sql"] or "20260131" in seen["sql"]
    assert f"concat_ws('|', {families[0].sql('`acct_id`', 'right')}" in seen["sql"]
    assert "CAST(`dw_dt` AS STRING)" not in seen["sql"]


def test_fraction_pushdown(tmp_path):
    seen = {}

    class Source:
        def pull_window(self, partitions, columns, output, **kwargs):
            seen["left"] = (partitions, columns, kwargs)
            pd.DataFrame(
                {"EFF_DT": ["2026-01-31"], "ACCT_ID": ["007"], "OPEN_DT": ["2020-01-01"]}
            ).to_csv(output, index=False)
            return output

    warehouse = SimpleNamespace(
        to_csv=lambda sql, output, columns, **_kwargs: (
            seen.update(right_sql=sql),
            pd.DataFrame(
                {
                    "dw_dt": ["20260131"],
                    "acct_id": ["7"],
                    "open_dt": ["20200101"],
                    keycheck.NORM_SQL: ["7|2020-01-01"],
                }
            ).to_csv(output, index=False),
        )[-1]
    )
    target = SimpleNamespace(
        config=PAIR["right"],
        relation=lambda: "`t`",
        _where=lambda predicate: predicate,
        warehouse=warehouse,
    )
    pair = {**PAIR, "sample": 0.25}
    result = keycheck.run_sample_check("p", pair, Source(), target, ["2026-01-31"], tmp_path)
    left_options = seen["left"][2]
    assert left_options["sample_plan"] == result.sample
    assert left_options["side"] == "left"
    assert "conv(substr(md5(" in seen["right_sql"]
    assert result.summary()["sample mode"] == "fraction"
    assert result.summary()["ok"] is True


def test_fixed_keys(tmp_path):
    seen = {}

    class Source:
        def sample_keys(self, partitions, keys, output, **kwargs):
            seen["selection"] = (partitions, keys, kwargs)
            pd.DataFrame({"ACCT_ID": ["007"], "OPEN_DT": ["2020-01-01"]}).to_csv(
                output, index=False
            )
            return output

        def pull_window(self, partitions, columns, output, **kwargs):
            seen["left"] = kwargs
            pd.DataFrame(
                {"EFF_DT": ["2026-01-31"], "ACCT_ID": ["007"], "OPEN_DT": ["2020-01-01"]}
            ).to_csv(output, index=False)
            return output

    warehouse = SimpleNamespace(
        to_csv=lambda sql, output, columns, **_kwargs: (
            seen.update(right_sql=sql),
            pd.DataFrame(
                {
                    "dw_dt": ["20260131"],
                    "acct_id": ["7"],
                    "open_dt": ["20200101"],
                    keycheck.NORM_SQL: ["7|2020-01-01"],
                }
            ).to_csv(output, index=False),
        )[-1]
    )
    target = SimpleNamespace(
        config=PAIR["right"],
        relation=lambda: "`t`",
        _where=lambda predicate: predicate,
        warehouse=warehouse,
    )
    result = keycheck.run_sample_check(
        "p", {**PAIR, "sample": 10}, Source(), target, ["2026-01-31"], tmp_path
    )
    assert seen["selection"][2]["count"] == 10
    assert seen["left"]["key_tuples"] == [["007", "2020-01-01"]]
    assert " IN ('7|2020-01-01')" in seen["right_sql"]
    assert result.selected_keys == [["007", "2020-01-01"]]
    assert result.summary()["selected fixed keys"] == 1


def test_real_source(tmp_path):
    left_path = tmp_path / "left.csv"
    pd.DataFrame({"EFF_DT": ["2026-01-31"], "ACCT_ID": ["007"], "OPEN_DT": ["2020-01-01"]}).to_csv(
        left_path, index=False
    )
    pair = {
        **PAIR,
        "sample": 1,
        "left": {**PAIR["left"], "path": str(left_path), "date_type": "string"},
    }

    def right_csv(_sql, output, _columns, **_kwargs):
        pd.DataFrame(
            {
                "dw_dt": ["20260131"],
                "acct_id": ["7"],
                "open_dt": ["20200101"],
                keycheck.NORM_SQL: ["7|2020-01-01"],
            }
        ).to_csv(output, index=False)

    target = SimpleNamespace(
        config=pair["right"],
        relation=lambda: "`t`",
        _where=lambda predicate: predicate,
        warehouse=SimpleNamespace(to_csv=right_csv),
    )
    result = keycheck.run_sample_check(
        "p", pair, FileSource(pair["left"]), target, ["2026-01-31"], tmp_path / "out"
    )
    assert result.summary()["ok"] is True


def test_key_required():
    with pytest.raises(ValueError, match="no key"):
        keycheck.key_spec({**PAIR, "key": ""})


def test_date_family():
    pair = {
        **PAIR,
        "key": "EFF_DT",
        "left": {
            **PAIR["left"],
            "date_type": {"type": "sas", "format": "days"},
        },
        "right": {
            **PAIR["right"],
            "date_type": {"type": "string", "format": "dash"},
        },
        "col_map": {**PAIR["col_map"], "EFF_DT": "dw_dt"},
        "key_families": {},
    }
    left, right, families = keycheck.key_spec(pair)
    assert left == ["EFF_DT"] and right == ["dw_dt"]
    assert families[0].normalize("24108", "left") == "2026-01-02"
    assert families[0].normalize("2026-01-02", "right") == "2026-01-02"
