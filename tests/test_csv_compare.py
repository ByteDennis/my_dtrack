"""Tests for dtrack.csv_compare — string-exact diff by primary key."""

import io

import pandas as pd
import pytest

from dtrack.csv_compare import compare_csvs, read_csv_as_str


def _csv(text):
    return read_csv_as_str(io.StringIO(text))


def test_identical_tables():
    df = _csv("id,amount\n1,10\n2,20\n")
    out = compare_csvs(df, df.copy(), pk_cols=["id"], compare_cols=["amount"])
    assert out["summary"]["matched"] == 2
    assert out["summary"]["only_left"] == 0
    assert out["summary"]["only_right"] == 0
    assert out["summary"]["total_mismatches"] == 0
    assert out["columns"][0]["n_unmatched"] == 0


def test_only_left_and_right():
    left  = _csv("id,amount\n1,10\n2,20\n3,30\n")
    right = _csv("id,amount\n2,20\n4,40\n")
    out = compare_csvs(left, right, pk_cols=["id"], compare_cols=["amount"])
    s = out["summary"]
    assert s["matched"] == 1
    assert s["only_left"] == 2
    assert s["only_right"] == 1
    assert {ex["id"] for ex in out["only_left_examples"]}  == {"1", "3"}
    assert {ex["id"] for ex in out["only_right_examples"]} == {"4"}


def test_value_mismatch_examples_capped():
    left  = _csv("id,v\n1,a\n2,b\n3,c\n4,d\n5,e\n")
    right = _csv("id,v\n1,A\n2,B\n3,C\n4,D\n5,E\n")
    out = compare_csvs(left, right, pk_cols=["id"], compare_cols=["v"], n_examples=3)
    col = out["columns"][0]
    assert col["n_unmatched"] == 5
    assert len(col["examples"]) == 3


def test_string_cast_keeps_10_distinct_from_10_dot_0():
    left  = _csv("id,n\n1,10\n")
    right = _csv("id,n\n1,10.0\n")
    out = compare_csvs(left, right, pk_cols=["id"], compare_cols=["n"])
    assert out["summary"]["total_mismatches"] == 1
    assert out["columns"][0]["examples"][0] == {
        "pk": {"id": "1"}, "left": "10", "right": "10.0"
    }


def test_empty_string_kept_distinct_from_zero():
    left  = _csv("id,n\n1,\n")
    right = _csv("id,n\n1,0\n")
    out = compare_csvs(left, right, pk_cols=["id"], compare_cols=["n"])
    assert out["summary"]["total_mismatches"] == 1
    ex = out["columns"][0]["examples"][0]
    assert ex["left"] == "" and ex["right"] == "0"


def test_multi_column_pk():
    left  = _csv("id,date,amt\n1,2025-01-01,10\n1,2025-01-02,11\n2,2025-01-01,20\n")
    right = _csv("id,date,amt\n1,2025-01-01,10\n1,2025-01-02,99\n2,2025-01-01,20\n")
    out = compare_csvs(left, right, pk_cols=["id", "date"], compare_cols=["amt"])
    assert out["summary"]["matched"] == 3
    assert out["summary"]["total_mismatches"] == 1
    ex = out["columns"][0]["examples"][0]
    assert ex["pk"] == {"id": "1", "date": "2025-01-02"}


def test_compare_column_missing_on_one_side_is_skipped():
    left  = _csv("id,a,b\n1,x,y\n")
    right = _csv("id,a\n1,x\n")
    out = compare_csvs(left, right, pk_cols=["id"], compare_cols=["a", "b"])
    cols_by_name = {c["name"]: c for c in out["columns"]}
    assert cols_by_name["a"]["skipped"] is False
    assert cols_by_name["b"]["skipped"] is True
    assert "missing" in cols_by_name["b"]["reason"]


def test_pk_must_exist_on_both_sides():
    left  = _csv("id,a\n1,x\n")
    right = _csv("foo,a\n1,x\n")
    with pytest.raises(ValueError, match="missing on right"):
        compare_csvs(left, right, pk_cols=["id"], compare_cols=["a"])


def test_pk_in_compare_cols_is_skipped():
    df = _csv("id,a\n1,x\n2,y\n")
    out = compare_csvs(df, df.copy(), pk_cols=["id"], compare_cols=["id", "a"])
    cols = {c["name"]: c for c in out["columns"]}
    assert cols["id"]["skipped"] is True
    assert "primary key" in cols["id"]["reason"]
