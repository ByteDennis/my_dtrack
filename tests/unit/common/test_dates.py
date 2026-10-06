from datetime import UTC, date, datetime

import pytest


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (datetime(2026, 1, 2, 13, 5, tzinfo=UTC), "2026-01-02"),
        (date(2026, 1, 2), "2026-01-02"),
        ("2026-01-02", "2026-01-02"),
        (" 20260102 ", "2026-01-02"),
        ("01/02/2026", "2026-01-02"),
        ("02JAN2026", "2026-01-02"),
        ("02-Jan-2026", "2026-01-02"),
        ("2026-01-02 00:00:00", "2026-01-02"),
        ("2026-01-02T13:05:00.123456", "2026-01-02"),
    ],
)
def test_canonical_date_accepts_every_supported_format(common, value, expected):
    assert common.dates.canonical_date(value) == expected


def test_canonical_date_rejects_unknown_text(common):
    with pytest.raises(ValueError, match="unsupported date"):
        common.dates.canonical_date("next tuesday")


@pytest.mark.parametrize(
    ("vintage", "expected"),
    [
        ("day", "2026-05-14"),
        ("week", "2026-05-11"),
        ("month", "2026-05-01"),
        ("quarter", "2026-04-01"),
        ("year", "2026-01-01"),
    ],
)
def test_bucket_date_floors_to_the_vintage(common, vintage, expected):
    assert common.dates.bucket_date("2026-05-14", vintage) == expected


def test_bucket_date_rejects_unknown_vintage(common):
    with pytest.raises(ValueError, match="unsupported vintage"):
        common.dates.bucket_date("2026-05-14", "fortnight")


def test_apply_synonyms_canonicalises_and_folds_sentinel_dates(common):
    counts = {"20260101": 2, "2026-01-01": 3, "3000-01-01": 4}
    folded = common.dates.apply_synonyms(counts, {"3000-01-01": "2026-07-01"})
    assert folded == {"2026-01-01": 5, "2026-07-01": 4}
    assert common.dates.apply_synonyms({"20260101": 1}) == {"2026-01-01": 1}


def test_contiguous_runs_splits_on_holes_and_ignores_order_and_duplicates(common):
    runs = common.dates.contiguous_runs(
        ["2026-01-05", "2026-01-01", "2026-01-02", "2026-01-02", "2026-01-03"]
    )
    assert runs == [("2026-01-01", "2026-01-03"), ("2026-01-05", "2026-01-05")]
    assert common.dates.contiguous_runs([]) == []


@pytest.mark.parametrize(
    ("name", "dialect", "expected"),
    [
        ("main.sales.orders", "spark", "`main`.`sales`.`orders`"),
        ("we`ird", "spark", "`we``ird`"),
        ("OWNER.ORDERS", "oracle", "OWNER.ORDERS"),
        ("own.v_app_analysis", "oracle", "own.v_app_analysis"),
        ('OWN."DATE"', "oracle", 'OWN."DATE"'),
        ("db.t", "hadoop", '"db"."t"'),
        ('a"b', "sql", '"a""b"'),
        ("a.b", "sql", '"a.b"'),
    ],
)
def test_quote_ident_per_dialect(common, name, dialect, expected):
    assert common.dates.quote_ident(name, dialect) == expected


@pytest.mark.parametrize(
    ("date_type", "expected"),
    [
        ("date", "DATE '2026-01-02'"),
        ("TIMESTAMP", "DATE '2026-01-02'"),
        ("number", "20260102"),
        ("string_dash", "'2026-01-02'"),
        (None, "'2026-01-02'"),
    ],
)
def test_date_literal_follows_the_column_type(common, date_type, expected):
    assert common.dates.date_literal("2026-01-02", "oracle", date_type) == expected


def test_partition_where_turns_holes_into_a_union_of_ranges(common):
    where = common.dates.partition_where(
        "dt", ["2026-01-01", "2026-01-02", "2026-01-04"], dialect="oracle", date_type="date"
    )
    assert where == (
        "((dt >= DATE '2026-01-01' AND dt < DATE '2026-01-03') OR "
        "(dt >= DATE '2026-01-04' AND dt < DATE '2026-01-05'))"
    )
    assert common.dates.partition_where("dt", [], dialect="spark") == "1=0"


def test_bounds_where_is_none_without_bounds(common):
    bounds_where = common.dates.bounds_where
    assert bounds_where("dt", dialect="spark", date_type=None, from_date=None, to_date=None) is None
    assert (
        bounds_where("dt", dialect="spark", date_type="int", from_date="2026-01-01", to_date=None)
        == "`dt` >= 20260101"
    )
    assert (
        bounds_where(
            "dt", dialect="oracle", date_type=None, from_date="2026-01-01", to_date="2026-01-31"
        )
        == "dt >= '2026-01-01' AND dt <= '2026-01-31'"
    )


@pytest.mark.parametrize(
    ("dialect", "date_type", "expected"),
    [
        ("oracle", "date", "TRUNC(DT)"),
        ("oracle", "timestamp", "TRUNC(DT)"),
        ("spark", "datetime", "to_date(`DT`)"),
        ("duckdb", "date", 'CAST("DT" AS DATE)'),
        ("oracle", "string_dash", "DT"),
        ("oracle", None, "DT"),
        ("spark", "num", "`DT`"),
    ],
)
def test_day_expr_truncates_only_columns_that_can_carry_a_time(
    common, dialect, date_type, expected
):
    assert common.dates.day_expr("DT", dialect, date_type) == expected


def test_day_bounds_are_half_open_so_times_on_the_last_day_count(common):
    where = common.dates.bounds_where(
        "dt", dialect="oracle", date_type="timestamp", from_date="2026-01-30", to_date="2026-12-31"
    )
    assert where == "dt >= DATE '2026-01-30' AND dt < DATE '2027-01-01'"
    single = common.dates.partition_where(
        "dt", ["2024-02-28", "2024-02-29"], dialect="oracle", date_type="date"
    )
    assert single == "((dt >= DATE '2024-02-28' AND dt < DATE '2024-03-01'))"
    assert common.dates.partition_where(
        "dt", ["2026-01-01"], dialect="oracle", date_type="string_dash"
    ) == ("((dt = '2026-01-01'))")
