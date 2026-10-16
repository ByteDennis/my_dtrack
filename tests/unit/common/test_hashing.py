import hashlib

import pandas as pd
import pytest

from dtrack_left import csv_sampling
from dtrack_left.common import hashing as left_hashing


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("2.50", "2.5"),
        ("2.0", "2"),
        ("-0.10", "-0.1"),
        ("10", "10"),
        ("007", "007"),
        ("1e3", "1e3"),
        ("abc", "abc"),
        ("1.2.3", "1.2.3"),
    ],
)
def test_collapse_decimal(common, text, expected):
    assert common.hashing.collapse_decimal(text) == expected


@pytest.mark.parametrize(
    ("value", "family", "expected"),
    [
        (None, "string", "<<NULL>>"),
        ("   ", "numeric", "<<NULL>>"),
        (" Abc ", "string", "abc"),
        ("CAFÉ", "string", "café"),
        ("02JAN2026", "date", "2026-01-02"),
        (" 7.50 ", "numeric", "7.5"),
        (7, "numeric", "7"),
    ],
)
def test_normalize_value(common, value, family, expected):
    assert common.hashing.key_family(family).normalize(value) == expected


def test_hash32(common):
    assert common.hashing.hash32("abc") == int(hashlib.md5(b"abc").hexdigest()[:8], 16)
    assert 0 <= common.hashing.hash32("anything") < common.hashing.HASH_BUCKET_SPACE


def test_selected_fraction(common):
    hashing = common.hashing
    value = hashing.hash32("abc|7.5")
    assert hashing.selected_fraction([" ABC", "7.50"], ["string", "numeric"], value + 1)
    assert not hashing.selected_fraction([" ABC", "7.50"], ["string", "numeric"], value)


def _keys_csv(tmp_path, rows):
    path = tmp_path / "rows.csv"
    pd.DataFrame(rows, columns=["id", "value"]).to_csv(path, index=False)
    return path


def test_sample_all(tmp_path):
    path = _keys_csv(tmp_path, [["1", "a"]])
    assert csv_sampling.sample_csv(path, key_columns=[], mode="all") == path


def test_sample_invalid(tmp_path):
    path = _keys_csv(tmp_path, [["1", "a"]])
    with pytest.raises(ValueError, match="requires key columns"):
        csv_sampling.sample_csv(path, key_columns=[], mode="fraction", rate=0.5)
    with pytest.raises(ValueError, match="unsupported sample mode"):
        csv_sampling.sample_csv(path, key_columns=["id"], mode="stratified")


def test_sample_fraction(tmp_path):
    rows = [[f"{i:05d}", "x"] for i in range(400)]
    path = _keys_csv(tmp_path, rows)
    sampled = pd.read_csv(
        csv_sampling.sample_csv(
            path, key_columns=["id"], mode="fraction", rate=0.25, families=["numeric"]
        ),
        dtype=object,
    )
    threshold = round(0.25 * 2**32)
    expected = [key for key, _ in rows if left_hashing.hash32(str(int(key))) < threshold]
    assert list(sampled["id"]) == expected and 60 < len(expected) < 140


def test_fixed_tuples(tmp_path):
    path = _keys_csv(tmp_path, [["1", "a"], ["2.0", "b"], ["3", "c"]])
    sampled = csv_sampling.sample_csv(
        path, key_columns=["id"], mode="fixed", count=1, key_tuples=[["2"]], families=["numeric"]
    )
    assert sampled.read_text().splitlines() == ["id,value", "2.0,b"]


def test_fixed_rank(
    tmp_path,
):
    path = _keys_csv(tmp_path, [["1", "a"], ["1", "b"], ["2", "c"], ["3", "d"], ["4", "e"]])
    sampled = pd.read_csv(
        csv_sampling.sample_csv(path, key_columns=["id"], mode="fixed", count=2), dtype=object
    )
    ranked = sorted("1234", key=lambda key: hashlib.md5(key.encode()).hexdigest())[:2]
    assert sorted(set(sampled["id"])) == sorted(ranked)
    assert len(sampled) == sum(2 if key == "1" else 1 for key in ranked)


def test_fraction_parity(synthetic, tmp_path):
    left = pd.read_csv(synthetic.left / "ORDERS.csv", dtype=object)
    right = pd.read_csv(synthetic.right / "orders.csv", dtype=object)
    common_days = sorted(set(left["BUSINESS_DATE"]) & set(right["business_date"]))
    left[left["BUSINESS_DATE"].isin(common_days)].to_csv(tmp_path / "l.csv", index=False)
    right[right["business_date"].isin(common_days)].rename(columns=str.upper).to_csv(
        tmp_path / "r.csv", index=False
    )
    options = {"key_columns": ["ORDER_ID"], "mode": "fraction", "rate": 0.25}
    options["families"] = ["numeric"]
    first = pd.read_csv(csv_sampling.sample_csv(tmp_path / "l.csv", **options), dtype=object)
    again = pd.read_csv(csv_sampling.sample_csv(tmp_path / "l.csv", **options), dtype=object)
    other = pd.read_csv(csv_sampling.sample_csv(tmp_path / "r.csv", **options), dtype=object)
    assert first.equals(again)
    kept = ~first["BUSINESS_DATE"].isin({"2026-01-15"})
    kept_other = ~other["BUSINESS_DATE"].isin({"2026-01-15"})
    assert set(first[kept]["ORDER_ID"]) == set(other[kept_other]["ORDER_ID"])
