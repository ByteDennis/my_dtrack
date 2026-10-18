import hashlib

import pandas as pd
import pytest

from pair_recon_left.common.model import hashing as left_hashing
from pair_recon_left.common.sql import sampling as csv_sampling


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
def test_collapse(common, text, expected):
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
def test_normalize(common, value, family, expected):
    assert common.hashing.key_family(family).normalize(value) == expected


def test_hash32(common):
    assert common.hashing.hash32("abc") == int(hashlib.md5(b"abc").hexdigest()[:8], 16)
    assert 0 <= common.hashing.hash32("anything") < common.hashing.HASH_BUCKET_SPACE


def test_selected(common):
    hashing = common.hashing
    value = hashing.hash32("abc|7.5")
    assert hashing.selected_fraction([" ABC", "7.50"], ["string", "numeric"], value + 1)
    assert not hashing.selected_fraction([" ABC", "7.50"], ["string", "numeric"], value)


def _keys_csv(tmp_path, rows):
    path = tmp_path / "rows.csv"
    pd.DataFrame(rows, columns=["id", "value"]).to_csv(path, index=False)
    return path


def test_sample(tmp_path):
    path = _keys_csv(tmp_path, [["1", "a"]])
    assert csv_sampling.sample_csv(path, key_columns=[], mode="all") == path


def test_sample2(tmp_path):
    path = _keys_csv(tmp_path, [["1", "a"]])
    with pytest.raises(ValueError, match="requires key columns"):
        csv_sampling.sample_csv(path, key_columns=[], mode="fraction", rate=0.5)
    with pytest.raises(ValueError, match="unsupported sample mode"):
        csv_sampling.sample_csv(path, key_columns=["id"], mode="stratified")


def test_sample3(tmp_path):
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


def test_fixed(tmp_path):
    path = _keys_csv(tmp_path, [["1", "a"], ["2.0", "b"], ["3", "c"]])
    sampled = csv_sampling.sample_csv(
        path, key_columns=["id"], mode="fixed", count=1, key_tuples=[["2"]], families=["numeric"]
    )
    assert sampled.read_text().splitlines() == ["id,value", "2.0,b"]


def test_fixed2(
    tmp_path,
):
    path = _keys_csv(tmp_path, [["1", "a"], ["1", "b"], ["2", "c"], ["3", "d"], ["4", "e"]])
    sampled = pd.read_csv(
        csv_sampling.sample_csv(path, key_columns=["id"], mode="fixed", count=2), dtype=object
    )
    ranked = sorted("1234", key=lambda key: hashlib.md5(key.encode()).hexdigest())[:2]
    assert sorted(set(sampled["id"])) == sorted(ranked)
    assert len(sampled) == sum(2 if key == "1" else 1 for key in ranked)
