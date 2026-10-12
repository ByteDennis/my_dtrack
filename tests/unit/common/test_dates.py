from datetime import UTC, date, datetime

import duckdb
import pytest


def require_date_platform(common, dialect):
    if common.side == "dtrack_right" and dialect not in {"spark", "databricks"}:
        pytest.skip("the Databricks package intentionally ships only its Spark adapter")


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
        ("number", "20260102"),
        ({"type": "string", "format": "dash"}, "'2026-01-02'"),
        ({"type": "string", "format": "compact"}, "'20260102'"),
        ({"type": "sas", "format": "date9"}, "DATE '2026-01-02'"),
        ({"type": "sas", "format": "days"}, "24108"),
    ],
)
def test_date_literal_follows_the_column_type(common, date_type, expected):
    assert common.dates.date_literal("2026-01-02", "oracle", date_type) == expected


def test_registry_exposes_only_four_configurable_date_type_names(common):
    assert set(common.dates.DATE_TYPES) == {"date", "string", "number", "sas"}


def test_builtin_date_types_are_registered_classes(common):
    dates = common.dates
    assert isinstance(dates.resolve_date_type("date"), dates.NativeDateType)
    assert isinstance(dates.resolve_date_type("string"), dates.StringDateType)
    assert isinstance(dates.resolve_date_type("number"), dates.NumericDateType)
    assert isinstance(dates.resolve_date_type("sas"), dates.SasDateType)


def test_parameterized_string_and_sas_formats_share_one_class_each(common):
    dates = common.dates
    compact = dates.resolve_date_type({"type": "string", "format": "compact"})
    assert isinstance(compact, dates.StringDateType)
    assert compact.format == "compact"
    assert compact.literal("2026-01-02", "oracle") == "'20260102'"
    assert compact.spec(portable=True) == {"type": "string", "format": "compact"}

    days = dates.resolve_date_type({"type": "sas", "format": "days"})
    assert isinstance(days, dates.SasDateType)
    assert days.format == "days"
    assert days.normalize("24108.75") == "2026-01-02"
    assert days.spec(portable=True) == {"type": "sas", "format": "days"}


@pytest.mark.parametrize(
    ("configured", "values", "expected"),
    [
        ("string", ["2025-04-02", "2026-08-01"], {"type": "string", "format": "dash"}),
        ("string", ["20250402", "20260801"], {"type": "string", "format": "compact"}),
        ("sas", ["01AUG2026", "02AUG2026"], {"type": "sas", "format": "date9"}),
        (
            "sas",
            ["01AUG2026:09:45:35", "02AUG2026:00:00:00"],
            {"type": "sas", "format": "datetime"},
        ),
        ("sas", ["24108.75", "24109"], {"type": "sas", "format": "days"}),
    ],
)
def test_probe_values_resolve_a_family_to_one_concrete_format(
    common, configured, values, expected
):
    detected = common.dates.resolve_date_type(configured).detect(values)
    assert detected.spec(portable=True) == expected


def test_probe_rejects_empty_or_mixed_physical_formats(common):
    with pytest.raises(ValueError, match="no non-null values"):
        common.dates.resolve_date_type("string").detect([None, ""])
    with pytest.raises(ValueError, match="mixed string date formats"):
        common.dates.resolve_date_type("string").detect(["2025-04-02", "20250403"])
    with pytest.raises(ValueError, match="mixed SAS date formats"):
        common.dates.resolve_date_type("sas").detect(["01AUG2026", "24108"])


def test_sas_datetime_normalizes_to_its_calendar_day(common):
    kind = common.dates.resolve_date_type({"type": "sas", "format": "datetime"})
    assert kind.normalize("01AUG2026:09:45:35") == "2026-08-01"


def test_oracle_string_normalizer_keeps_dashes_for_its_matching_mask(common):
    require_date_platform(common, "oracle")
    kind = common.dates.resolve_date_type({"type": "string", "format": "dash"})
    assert kind.normalize_sql("DT", "oracle") == (
        "COALESCE(TO_CHAR(TO_DATE(TRIM(TO_CHAR(DT)) DEFAULT NULL ON CONVERSION ERROR, "
        "'YYYY-MM-DD'), 'YYYY-MM-DD'), '')"
    )


def test_register_date_type_accepts_a_named_class_instance(common):
    dates = common.dates
    kind = dates.DateType(
        "test_slash",
        predicate_sql={"oracle": "TO_DATE({column}, 'YYYY/MM/DD')"},
        literal_sql="DATE '{iso}'",
        range="day",
        python_formats=("%Y/%m/%d",),
    )
    dates.register_date_type(kind)
    try:
        assert dates.resolve_date_type("test_slash") is kind
        assert dates.resolve_date_type("test_slash").normalize("2026/01/02") == "2026-01-02"
    finally:
        dates.DATE_TYPES.pop("test_slash")


@pytest.mark.parametrize(
    "removed_name",
    [
        "timestamp",
        "datetime",
        "string_dash",
        "string_compact",
        "num",
        "integer",
        "int",
        "sas_auto",
        "string_sas",
        "string_sas_date9",
        "sas_date9",
        "sas_days",
        "sas_date",
    ],
)
def test_removed_date_type_names_fail_with_the_four_supported_names(common, removed_name):
    with pytest.raises(ValueError, match="date, number, sas, string"):
        common.dates.resolve_date_type(removed_name)


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
        bounds_where(
            "dt", dialect="spark", date_type="number", from_date="2026-01-01", to_date=None
        )
        == "`dt` >= 20260101"
    )
    assert (
        bounds_where(
            "dt",
            dialect="oracle",
            date_type={"type": "string", "format": "dash"},
            from_date="2026-01-01",
            to_date="2026-01-31",
        )
        == "dt >= '2026-01-01' AND dt <= '2026-01-31'"
    )


@pytest.mark.parametrize(
    ("dialect", "date_type", "expected"),
    [
        ("oracle", "date", "TRUNC(DT)"),
        ("spark", "date", "to_date(`DT`)"),
        ("duckdb", "date", 'try_cast("DT" AS DATE)'),
        ("oracle", {"type": "string", "format": "dash"}, "DT"),
        ("oracle", {"type": "string", "format": "compact"}, "DT"),
        (
            "oracle",
            {"type": "sas", "format": "date9"},
            (
                "TO_DATE(REPLACE(TRIM(TO_CHAR(DT)), '-', '') DEFAULT NULL ON CONVERSION ERROR, "
                "'DDMONYYYY', 'NLS_DATE_LANGUAGE=English')"
            ),
        ),
        (
            "athena",
            {"type": "sas", "format": "date9"},
            "CAST(date_parse(TRIM(CAST(\"DT\" AS VARCHAR)), '%d%b%Y') AS DATE)",
        ),
        (
            "athena",
            {"type": "sas", "format": "days"},
            (
                "date_add('day', CAST(truncate(CAST(\"DT\" AS DOUBLE)) AS BIGINT), "
                "DATE '1960-01-01')"
            ),
        ),
        (
            "duckdb",
            {"type": "sas", "format": "days"},
            "DATE '1960-01-01' + CAST(trunc(CAST(\"DT\" AS DOUBLE)) AS INTEGER)",
        ),
        ("spark", "number", "`DT`"),
    ],
)
def test_day_expr_truncates_only_columns_that_can_carry_a_time(
    common, dialect, date_type, expected
):
    require_date_platform(common, dialect)
    assert common.dates.day_expr("DT", dialect, date_type) == expected


def test_day_bounds_are_half_open_so_times_on_the_last_day_count(common):
    where = common.dates.bounds_where(
        "dt", dialect="oracle", date_type="date", from_date="2026-01-30", to_date="2026-12-31"
    )
    assert where == "dt >= DATE '2026-01-30' AND dt < DATE '2027-01-01'"
    single = common.dates.partition_where(
        "dt", ["2024-02-28", "2024-02-29"], dialect="oracle", date_type="date"
    )
    assert single == "((dt >= DATE '2024-02-28' AND dt < DATE '2024-03-01'))"
    assert common.dates.partition_where(
        "dt",
        ["2026-01-01"],
        dialect="oracle",
        date_type={"type": "string", "format": "dash"},
    ) == ("((dt = '2026-01-01'))")


def test_string_compact_predicates_use_compact_literals(common):
    assert (
        common.dates.bounds_where(
            "dt",
            dialect="oracle",
            date_type={"type": "string", "format": "compact"},
            from_date="2026-01-01",
            to_date="2026-01-31",
        )
        == "dt >= '20260101' AND dt <= '20260131'"
    )
    assert (
        common.dates.partition_where(
            "dt",
            ["2026-01-01", "2026-01-02"],
            dialect="spark",
            date_type={"type": "string", "format": "compact"},
        )
        == "((`dt` BETWEEN '20260101' AND '20260102'))"
    )


def test_sas_date9_predicates_parse_before_comparing(common):
    require_date_platform(common, "oracle")
    where = common.dates.bounds_where(
        "dt",
        dialect="oracle",
        date_type={"type": "sas", "format": "date9"},
        from_date="2026-01-31",
        to_date="2026-02-01",
    )
    parsed = (
        "TO_DATE(REPLACE(TRIM(TO_CHAR(dt)), '-', '') DEFAULT NULL ON CONVERSION ERROR, "
        "'DDMONYYYY', 'NLS_DATE_LANGUAGE=English')"
    )
    assert where == f"{parsed} >= DATE '2026-01-31' AND {parsed} < DATE '2026-02-02'"


def test_sas_day_predicates_convert_iso_bounds_to_1960_offsets(common):
    require_date_platform(common, "oracle")
    where = common.dates.partition_where(
        "dt",
        ["2026-01-01", "2026-01-02"],
        dialect="oracle",
        date_type={"type": "sas", "format": "days"},
    )
    assert where == ("((TRUNC(TO_NUMBER(dt)) >= 24107 AND TRUNC(TO_NUMBER(dt)) < 24109))")


def test_inline_date_type_can_define_dialect_sql_and_day_ranges(common):
    spec = {
        "predicate_sql": {
            "oracle": "TO_DATE({column}, 'YYYY/MM/DD')",
            "spark": "to_date({column}, 'yyyy/MM/dd')",
        },
        "literal_sql": "DATE '{iso}'",
        "range": "day",
    }
    assert common.dates.day_expr("DT", "oracle", spec) == "TO_DATE(DT, 'YYYY/MM/DD')"
    assert common.dates.partition_where(
        "DT", ["2026-01-02"], dialect="spark", date_type=spec
    ) == (
        "((to_date(`DT`, 'yyyy/MM/dd') >= DATE '2026-01-02' "
        "AND to_date(`DT`, 'yyyy/MM/dd') < DATE '2026-01-03'))"
    )


def test_registered_date_type_uses_a_name_locally_and_a_portable_remote_spec(common):
    dates = common.dates
    dates.register_date_type(
        "test_slash",
        predicate_sql={"oracle": "TO_DATE({column}, 'YYYY/MM/DD')"},
        literal_sql="DATE '{iso}'",
        range="day",
    )
    try:
        registered = dates.resolve_date_type("test_slash")
        assert registered.spec() == "test_slash"
        assert registered.spec(portable=True) == {
            "predicate_sql": {"oracle": "TO_DATE({column}, 'YYYY/MM/DD')"},
            "literal_sql": "DATE '{iso}'",
            "range": "day",
        }
        assert dates.bounds_where(
            "DT",
            dialect="oracle",
            date_type="test_slash",
            from_date="2026-01-02",
            to_date="2026-01-02",
        ) == (
            "TO_DATE(DT, 'YYYY/MM/DD') >= DATE '2026-01-02' "
            "AND TO_DATE(DT, 'YYYY/MM/DD') < DATE '2026-01-03'"
        )
    finally:
        dates.DATE_TYPES.pop("test_slash")


def test_sas_date_type_family_resolves_to_one_format_before_sql(common):
    dates = common.dates
    sas = dates.resolve_date_type("sas")
    assert isinstance(sas, dates.SasDateType)
    assert sas.requires_probe
    date9 = sas.detect(["02JAN2026", "03-Jan-2026"])
    days = sas.detect(["24108", "24108.75"])
    assert date9.normalize("02JAN2026") == "2026-01-02"
    assert days.normalize("24108.75") == "2026-01-02"
    spark = dates.day_expr("DT", "spark", days)
    assert "1960-01-01" in spark
    if common.side == "dtrack_left":
        assert "DDMONYYYY" in dates.day_expr("DT", "oracle", date9)
        assert dates.partition_where(
            "DT", ["2026-01-02"], dialect="oracle", date_type=date9
        ).endswith("< DATE '2026-01-03'))")


def test_sas_fractional_days_have_python_duckdb_parity(common):
    require_date_platform(common, "duckdb")
    dates = common.dates
    kind = dates.resolve_date_type({"type": "sas", "format": "days"})
    values = ["24108.75", "24108.0", "-1.75", "-1.0"]
    expression = kind.normalize_sql("raw", "duckdb")
    rows = duckdb.sql(
        f"SELECT {expression} FROM (SELECT unnest(?) AS raw)", params=[values]
    ).fetchall()
    assert [row[0] for row in rows] == [kind.normalize(value) for value in values]


def test_sas_registered_and_inline_specs_preserve_sas_behavior(common):
    dates = common.dates
    inline = dates.resolve_date_type({"type": "sas", "format": "days"})
    assert isinstance(inline, dates.SasDateType)
    assert inline.normalize("24108.75") == "2026-01-02"
    dates.register_date_type("test_sas", inline)
    try:
        registered = dates.resolve_date_type("test_sas")
        assert isinstance(registered, dates.SasDateType)
        assert registered.spec() == "test_sas"
        portable = registered.spec(portable=True)
        assert portable == {"type": "sas", "format": "days"}
        restored = dates.resolve_date_type(portable)
        assert isinstance(restored, dates.SasDateType)
        assert restored.normalize("-1.75") == "1959-12-31"
    finally:
        dates.DATE_TYPES.pop("test_sas")


def test_registered_date_type_can_reuse_python_and_sql_normalizers(common):
    dates = common.dates
    dates.register_date_type(
        "test_slash",
        predicate_sql={"spark": "to_date({column}, 'yyyy/MM/dd')"},
        literal_sql="DATE '{iso}'",
        range="day",
        python_formats=["%Y/%m/%d"],
        normalizer_sql={
            "spark": "coalesce(date_format(to_date({column}, 'yyyy/MM/dd'), 'yyyy-MM-dd'), '')"
        },
    )
    try:
        kind = dates.resolve_date_type("test_slash")
        assert kind.normalize("2026/01/02") == "2026-01-02"
        assert kind.normalize_sql("`DT`", "spark") == (
            "coalesce(date_format(to_date(`DT`, 'yyyy/MM/dd'), 'yyyy-MM-dd'), '')"
        )
        assert kind.spec(portable=True)["python_formats"] == ["%Y/%m/%d"]
    finally:
        dates.DATE_TYPES.pop("test_slash")


@pytest.mark.parametrize(
    ("spec", "message"),
    [
        ({"format": "YYYYMMDD"}, "string date format must be"),
        ({"range": "week"}, "range must be 'value' or 'day'"),
        (7, "date type must be a name or an object"),
    ],
)
def test_bad_custom_date_types_are_refused(common, spec, message):
    with pytest.raises((TypeError, ValueError), match=message):
        common.dates.resolve_date_type(spec)
