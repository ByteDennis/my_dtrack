import json

import pandas as pd
import pytest

from dtrack_left.common import stats


def _series(*values):
    return pd.Series(list(values), dtype=object)


def test_numeric_columns_hash_canonical_decimal_text():
    compute = stats.column_statistics
    a = compute(_series("7.50", "2", None))
    b = compute(_series("7.5", "2.0", None))
    assert a["hash_sum"] == b["hash_sum"] and a["n_unique"] == b["n_unique"] == 2
    assert a["col_type"] == "numeric" and a["n_missing"] == 1 and a["n_total"] == 3
    assert a["min_val"] == "2.0" and a["max_val"] == "7.5"
    assert a["mean"] == pytest.approx(4.75) and a["std"] == pytest.approx(3.889087, rel=1e-6)
    assert a["length_avg"] is None and a["top_10"] == ""


def test_integral_columns_format_extremes_as_integers():
    result = stats.column_statistics(_series("100003", "7", "42"))
    assert (result["min_val"], result["max_val"]) == ("7", "100003")


def test_single_numeric_value_has_no_sample_std():
    assert stats.column_statistics(_series("5"))["std"] is None


def test_categorical_columns_report_lengths_extremes_and_top_10():
    result = stats.column_statistics(_series("b", "aa", "b", None))
    assert result["col_type"] == "categorical"
    assert (result["min_val"], result["max_val"]) == ("aa", "b")
    assert result["length_avg"] == pytest.approx(4 / 3) and result["length_max"] == 2
    assert json.loads(result["top_10"]) == [{"value": "b", "count": 2}, {"value": "aa", "count": 1}]


def test_an_all_null_column_is_categorical_with_empty_extremes():
    result = stats.column_statistics(_series(None, None))
    assert result["col_type"] == "categorical" and result["n_missing"] == 2
    assert result["min_val"] is None and result["length_max"] is None


def test_strict_mode_folds_case_and_decimal_text():
    compute = stats.column_statistics
    loose_a, loose_b = compute(_series("Gold", "SILVER")), compute(_series("gold", "silver"))
    strict_a = compute(_series("Gold", "SILVER"), strict=True)
    strict_b = compute(_series("gold", "silver"), strict=True)
    assert loose_a["hash_sum"] != loose_b["hash_sum"]
    assert strict_a["hash_sum"] == strict_b["hash_sum"]


def test_compute_csv_statistics_reads_as_text_and_checks_columns(tmp_path):
    path = tmp_path / "w.csv"
    path.write_text("ID,AMOUNT\n007,1.10\n008,2\n")
    result = stats.compute_csv_statistics(path, ["ID", "AMOUNT"])
    assert result["ID"]["min_val"] == "7" and result["AMOUNT"]["n_unique"] == 2
    with pytest.raises(ValueError, match="missing columns: NOPE"):
        stats.compute_csv_statistics(path, ["NOPE"])


def test_statistics_see_the_injected_drift_in_the_real_files(synthetic, tmp_path):
    left = pd.read_csv(synthetic.left / "ORDERS.csv", dtype=object)
    right = pd.read_csv(synthetic.right / "orders.csv", dtype=object).rename(columns=str.upper)
    january = ["2026-01-15", "2026-01-31"]
    for name, frame in (("l", left), ("r", right)):
        mask = frame["BUSINESS_DATE"].str.startswith("2026-01") & ~frame["BUSINESS_DATE"].isin(
            january
        )
        frame[mask].to_csv(tmp_path / f"{name}.csv", index=False)
    columns = ["STATUS", "REGION", "AMOUNT"]
    left_stats = stats.compute_csv_statistics(tmp_path / "l.csv", columns)
    right_stats = stats.compute_csv_statistics(tmp_path / "r.csv", columns)
    assert left_stats["STATUS"]["n_unique"] == 4 and right_stats["STATUS"]["n_unique"] == 8
    assert left_stats["REGION"] == right_stats["REGION"]
    assert left_stats["AMOUNT"] == right_stats["AMOUNT"]
    strict = [
        stats.compute_csv_statistics(tmp_path / f"{name}.csv", ["STATUS"], strict=True)
        for name in ("l", "r")
    ]
    assert strict[0]["STATUS"]["hash_sum"] == strict[1]["STATUS"]["hash_sum"]


# >>> col_type_overrides: the pair decides, not the values or the warehouse type <<< #
def test_col_type_override_forces_numeric_or_categorical():
    compute = stats.column_statistics
    forced = compute(_series("7.50", "n/a", "2", None), col_type="numeric")
    assert forced["col_type"] == "numeric" and forced["mean"] == pytest.approx(4.75)
    assert (forced["min_val"], forced["max_val"]) == ("2.0", "7.5") and forced["top_10"] == ""
    text = compute(_series("007", "7"), col_type="categorical")
    assert text["col_type"] == "categorical" and text["n_unique"] == 2
    assert (text["min_val"], text["max_val"]) == ("007", "7")


def test_compute_csv_statistics_applies_overrides_by_column(tmp_path):
    path = tmp_path / "w.csv"
    path.write_text("ID,AMOUNT\n007,1.10\n008,2\n")
    result = stats.compute_csv_statistics(
        path, ["ID", "AMOUNT"], col_types={"ID": "categorical"}
    )
    assert result["ID"]["col_type"] == "categorical" and result["AMOUNT"]["col_type"] == "numeric"


def test_registered_date_column_type_normalizes_each_side_before_hashing(tmp_path):
    left = tmp_path / "left.csv"
    right = tmp_path / "right.csv"
    left.write_text("D\n01AUG2026:09:45:35\n02AUG2026:00:00:00\n")
    right.write_text("D\n2026-08-01\n2026-08-02\n")
    spec = {
        "type": "date",
        "left_date_type": {"type": "sas", "format": "datetime"},
        "right_date_type": "date",
    }
    left_stats = stats.compute_csv_statistics(
        left, ["D"], column_types={"D": spec}, side="left"
    )
    right_stats = stats.compute_csv_statistics(
        right, ["D"], column_types={"D": spec}, side="right"
    )
    assert left_stats == right_stats
    assert left_stats["D"]["min_val"] == "2026-08-01"
