"""Tests for date WHERE clause generation: _reformat_date, _build_date_in_clause,
_build_date_between_clause, _build_where_from_dates, _compute_date_filter."""

import os
import csv
import re
import pytest
from unittest.mock import patch, MagicMock

from dtrack.extract import (
    _reformat_date,
    _build_date_in_clause,
    _build_date_between_clause,
    _compute_date_filter,
    _qualified_name,
)


# ---------------------------------------------------------------------------
# Section 1: TestReformatDate
# ---------------------------------------------------------------------------
class TestReformatDate:
    """Test _reformat_date conversion from YYYY-MM-DD to target formats."""

    @pytest.mark.parametrize("date_str, fmt, expected", [
        ("2025-03-15", "YYYYMMDD", "20250315"),
        ("2025-03-15", "YYYYMM", "202503"),
        ("2025-03-15", "YYYY-MM-DD", "2025-03-15"),
        ("2025-03-15", "DDMONYYYY", "15MAR2025"),
        ("2025-03-15", "DD-MON-YYYY", "15-MAR-2025"),
        ("2025-03-15", "MM/DD/YYYY", "03/15/2025"),
        ("2025-03-15", None, "2025-03-15"),
        ("2025-12-25", "DDMONYYYY", "25DEC2025"),
    ], ids=[
        "yyyymmdd", "yyyymm", "yyyy-mm-dd-passthrough",
        "ddmonyyyy", "dd-mon-yyyy", "mm-dd-yyyy",
        "none-passthrough", "ddmonyyyy-dec",
    ])
    def test_format_conversion(self, date_str, fmt, expected):
        assert _reformat_date(date_str, fmt) == expected

    def test_invalid_date_returns_str(self):
        assert _reformat_date("not-a-date", "YYYYMMDD") == "not-a-date"


# ---------------------------------------------------------------------------
# Section 2: TestBuildDateInClause — full platform × dtype × format matrix
# ---------------------------------------------------------------------------
class TestBuildDateInClause:
    """Test _build_date_in_clause across all platform/dtype/format combos."""

    @pytest.mark.parametrize(
        "id, dtype, is_sas, date_format, dates, exp_col, exp_lit",
        [
            ("sas-date", "DATE", True, None,
             ["2025-03-15"], "RPT_DT IN", "'15MAR2025'd"),
            ("sas-timestamp", "TIMESTAMP", True, None,
             ["2025-03-15"], "datepart(RPT_DT) IN", "'15MAR2025'd"),
            ("sas-datetime", "DATETIME", True, None,
             ["2025-03-15"], "datepart(RPT_DT) IN", "'15MAR2025'd"),
            ("oracle-date", "DATE", False, None,
             ["2025-03-15"], "RPT_DT IN", "DATE '2025-03-15'"),
            ("oracle-timestamp", "TIMESTAMP", False, None,
             ["2025-03-15"], "TRUNC(RPT_DT) IN", "DATE '2025-03-15'"),
            ("number-yyyymmdd", "NUMBER", False, "YYYYMMDD",
             [20250315], "RPT_DT IN", "20250315"),
            ("number-yyyymm", "NUMBER", False, "YYYYMM",
             [202503], "RPT_DT IN", "202503"),
            ("integer-yyyymmdd", "INTEGER", False, "YYYYMMDD",
             [20250315], "RPT_DT IN", "20250315"),
            ("varchar-yyyymmdd", "VARCHAR", False, "YYYYMMDD",
             ["2025-03-15"], "RPT_DT IN", "'20250315'"),
            ("varchar-yyyy-mm-dd", "VARCHAR", False, "YYYY-MM-DD",
             ["2025-03-15"], "RPT_DT IN", "'2025-03-15'"),
            ("varchar-ddmonyyyy", "VARCHAR", False, "DDMONYYYY",
             ["2025-03-15"], "RPT_DT IN", "'15MAR2025'"),
            ("string-yyyymmdd", "STRING", False, "YYYYMMDD",
             ["2025-03-15"], "RPT_DT IN", "'20250315'"),
            ("char-yyyymmdd", "CHAR(8)", False, "YYYYMMDD",
             ["2025-03-15"], "RPT_DT IN", "'20250315'"),
            ("char-default", "CHAR(10)", False, None,
             ["2025-03-15"], "RPT_DT IN", "'2025-03-15'"),
        ],
        ids=lambda v: v if isinstance(v, str) and v.count("-") <= 2 and not v.startswith("20") else "",
    )
    def test_matrix(self, id, dtype, is_sas, date_format, dates, exp_col, exp_lit):
        result = _build_date_in_clause("RPT_DT", dates, dtype,
                                       is_sas=is_sas, date_format=date_format)
        assert exp_col in result, f"[{id}] Expected '{exp_col}' in '{result}'"
        assert exp_lit in result, f"[{id}] Expected '{exp_lit}' in '{result}'"

    def test_numeric_no_quotes(self):
        """Numeric dates must never be quoted."""
        result = _build_date_in_clause("DT", [20250315, 20250416], "NUMBER")
        assert "'" not in result

    def test_oracle_in_limit_chunking(self):
        """1000+ dates should be split into multiple OR groups."""
        dates = [f"2025-01-{str(i).zfill(2)}" for i in range(1, 32)] * 35  # 1085
        result = _build_date_in_clause("DT", dates, "DATE", is_sas=False)
        assert " OR " in result


# ---------------------------------------------------------------------------
# Section 3: TestBuildDateBetweenClause — same matrix as Section 2
# ---------------------------------------------------------------------------
class TestBuildDateBetweenClause:
    """Test _build_date_between_clause across all platform/dtype/format combos."""

    @pytest.mark.parametrize(
        "id, dtype, is_sas, date_format, min_d, max_d, exp_col, exp_lit",
        [
            ("sas-date", "DATE", True, None,
             "2025-03-01", "2025-03-31", "RPT_DT BETWEEN", "'01MAR2025'd"),
            ("sas-timestamp", "TIMESTAMP", True, None,
             "2025-03-01", "2025-03-31", "datepart(RPT_DT) BETWEEN", "'01MAR2025'd"),
            ("sas-datetime", "DATETIME", True, None,
             "2025-03-01", "2025-03-31", "datepart(RPT_DT) BETWEEN", "'01MAR2025'd"),
            ("oracle-date", "DATE", False, None,
             "2025-03-01", "2025-03-31", "RPT_DT BETWEEN", "DATE '2025-03-01'"),
            ("oracle-timestamp", "TIMESTAMP", False, None,
             "2025-03-01", "2025-03-31", "TRUNC(RPT_DT) BETWEEN", "DATE '2025-03-01'"),
            ("number-yyyymmdd", "NUMBER", False, "YYYYMMDD",
             20250301, 20250331, "RPT_DT BETWEEN", "20250301"),
            ("number-yyyymm", "NUMBER", False, "YYYYMM",
             202503, 202503, "RPT_DT BETWEEN", "202503"),
            ("integer-yyyymmdd", "INTEGER", False, "YYYYMMDD",
             20250301, 20250331, "RPT_DT BETWEEN", "20250301"),
            ("varchar-yyyymmdd", "VARCHAR", False, "YYYYMMDD",
             "2025-03-01", "2025-03-31", "RPT_DT BETWEEN", "'20250301'"),
            ("varchar-yyyy-mm-dd", "VARCHAR", False, "YYYY-MM-DD",
             "2025-03-01", "2025-03-31", "RPT_DT BETWEEN", "'2025-03-01'"),
            ("varchar-ddmonyyyy", "VARCHAR", False, "DDMONYYYY",
             "2025-03-01", "2025-03-31", "RPT_DT BETWEEN", "'01MAR2025'"),
            ("string-yyyymmdd", "STRING", False, "YYYYMMDD",
             "2025-03-01", "2025-03-31", "RPT_DT BETWEEN", "'20250301'"),
            ("char-yyyymmdd", "CHAR(8)", False, "YYYYMMDD",
             "2025-03-01", "2025-03-31", "RPT_DT BETWEEN", "'20250301'"),
            ("char-default", "CHAR(10)", False, None,
             "2025-03-01", "2025-03-31", "RPT_DT BETWEEN", "'2025-03-01'"),
        ],
        ids=lambda v: v if isinstance(v, str) and v.count("-") <= 2 and not v.startswith("20") else "",
    )
    def test_matrix(self, id, dtype, is_sas, date_format, min_d, max_d, exp_col, exp_lit):
        result = _build_date_between_clause("RPT_DT", min_d, max_d, dtype,
                                            is_sas=is_sas, date_format=date_format)
        assert exp_col in result, f"[{id}] Expected '{exp_col}' in '{result}'"
        assert exp_lit in result, f"[{id}] Expected '{exp_lit}' in '{result}'"

    def test_both_bounds_present(self):
        """Both min and max dates should appear in the clause."""
        result = _build_date_between_clause("DT", "2025-01-01", "2025-12-31",
                                            "DATE", is_sas=True)
        assert "'01JAN2025'd" in result
        assert "'31DEC2025'd" in result

    def test_numeric_no_quotes(self):
        """Numeric BETWEEN must not quote values."""
        result = _build_date_between_clause("DT", 202501, 202512, "NUMBER")
        assert "'" not in result


# ---------------------------------------------------------------------------
# Section 4: TestBuildWhereFromDates — integration with mocked DB
# ---------------------------------------------------------------------------
class TestBuildWhereFromDates:
    """Test _build_where_from_dates full flow from table_cfg → WHERE string."""

    def _make_table_cfg(self, name='tbl', table='SCHEMA.TBL', source='pcds',
                        date_col='RPT_DT', processed=None, where=''):
        cfg = {
            'name': name, 'table': table, 'source': source,
            'date_col': date_col, 'where': where,
        }
        if processed:
            cfg['processed'] = processed
        return cfg

    def _mock_db(self, date_dtype='DATE', date_format=None, db_path='/fake/db'):
        """Return patches for get_column_meta and get_metadata."""
        col_meta = [{'column_name': 'RPT_DT', 'data_type': date_dtype}]
        meta = {'date_format': date_format} if date_format else {}
        return (
            patch('dtrack.cli.get_column_meta', return_value=col_meta),
            patch('dtrack.cli.get_metadata', return_value=meta),
        )

    def _patch_db(self, date_dtype, date_format=None):
        """Context manager patching DB calls for _build_where_from_dates."""
        col_meta = [{'column_name': self._col, 'data_type': date_dtype}]
        meta = {'date_format': date_format} if date_format else {}
        return (
            patch('dtrack.db.get_column_meta', return_value=col_meta),
            patch('dtrack.cli.get_metadata', return_value=meta),
        )

    _col = 'RPT_DT'  # default; overridden per-test via cfg

    def test_oracle_timestamp_trunc_date_literal(self):
        """Oracle TIMESTAMP → TRUNC(col) >= DATE '...'"""
        from dtrack.cli import _build_where_from_dates
        cfg = self._make_table_cfg(date_col='EFF_DT')
        self._col = 'EFF_DT'
        p1, p2 = self._patch_db('TIMESTAMP')
        with p1, p2:
            result = _build_where_from_dates(cfg, ['2025-03-01', '2025-03-15'],
                                             ['2025-03-08'], db_path='/fake')
        assert "TRUNC(EFF_DT)" in result
        assert "DATE '2025-03-01'" in result
        assert "NOT IN" in result
        assert "DATE '2025-03-08'" in result

    def test_sas_datetime_datepart(self):
        """SAS DATETIME → datepart(col) >= '...'d"""
        from dtrack.cli import _build_where_from_dates
        cfg = self._make_table_cfg(date_col='EFF_DT', processed='$WORK.DS')
        self._col = 'EFF_DT'
        p1, p2 = self._patch_db('DATETIME')
        with p1, p2:
            result = _build_where_from_dates(cfg, ['2025-03-01', '2025-03-15'],
                                             [], db_path='/fake')
        assert "datepart(EFF_DT)" in result
        assert "'01MAR2025'd" in result

    def test_athena_varchar_yyyymmdd(self):
        """Athena VARCHAR YYYYMMDD → quoted reformatted strings."""
        from dtrack.cli import _build_where_from_dates
        cfg = self._make_table_cfg(source='aws', date_col='DW_BUS_DT')
        self._col = 'DW_BUS_DT'
        p1, p2 = self._patch_db('VARCHAR', 'YYYYMMDD')
        with p1, p2:
            result = _build_where_from_dates(cfg, ['2025-03-01', '2025-03-15'],
                                             [], db_path='/fake')
        assert "DW_BUS_DT >=" in result
        assert "'20250301'" in result

    def test_oracle_date(self):
        """Oracle DATE → col + DATE literal (non-datetime, no TRUNC)."""
        from dtrack.cli import _build_where_from_dates
        cfg = self._make_table_cfg(date_col='RPT_DT')
        self._col = 'RPT_DT'
        p1, p2 = self._patch_db('DATE')
        with p1, p2:
            result = _build_where_from_dates(cfg, ['2025-06-01', '2025-06-30'],
                                             [], db_path='/fake')
        assert "RPT_DT >=" in result
        assert "DATE '2025-06-01'" in result

    def test_number_yyyymm_bare_integers(self):
        """NUMBER YYYYMM → bare integers without quotes."""
        from dtrack.cli import _build_where_from_dates
        cfg = self._make_table_cfg(date_col='RPT_DT')
        self._col = 'RPT_DT'
        p1, p2 = self._patch_db('NUMBER', 'YYYYMM')
        with p1, p2:
            result = _build_where_from_dates(cfg, ['202503', '202504'],
                                             [], db_path='/fake')
        assert "RPT_DT >=" in result
        assert "202503" in result

    def test_existing_where_preserved(self):
        """Original WHERE from config should be AND-joined."""
        from dtrack.cli import _build_where_from_dates
        cfg = self._make_table_cfg(where="STATUS = 'A'")
        self._col = 'RPT_DT'
        p1, p2 = self._patch_db('DATE')
        with p1, p2:
            result = _build_where_from_dates(cfg, ['2025-01-01', '2025-01-31'],
                                             [], db_path='/fake')
        assert "(STATUS = 'A')" in result
        assert " AND " in result


# ---------------------------------------------------------------------------
# Section 5: TestComputeDateFilter — vintage × filter_type
# ---------------------------------------------------------------------------
class TestComputeDateFilter:
    """Test _compute_date_filter with various vintages and mocked DB."""

    def _setup_db(self, tmp_path, n_dates=30):
        """Create a minimal DB with 30 matching dates (2025-01-01 to 2025-01-30)."""
        from dtrack.db import init_database, insert_column_meta, save_row_comparison
        from dtrack.db import register_table_pair
        from dtrack.loader import load_row_counts

        dates = [f"2025-{(i // 30) + 1:02d}-{(i % 30) + 1:02d}" for i in range(n_dates)]
        # Generate 30 dates spread across Jan-Mar 2025
        dates = []
        for m in range(1, 4):
            for d in range(1, 11):
                dates.append(f"2025-{m:02d}-{d:02d}")
        dates = sorted(dates)[:n_dates]

        db_path = str(tmp_path / 'test.db')
        init_database(db_path)

        qname = 'pcds_test_tbl'

        # Write CSV and load
        csv_path = tmp_path / 'row.csv'
        with open(csv_path, 'w', newline='') as f:
            w = csv.writer(f)
            w.writerow(['RPT_DT', 'row_count'])
            for d in dates:
                w.writerow([d, 100])
        load_row_counts(db_path, str(csv_path), qname, source='pcds', date_col='RPT_DT')

        insert_column_meta(db_path, qname, {'RPT_DT': 'DATE', 'AMT': 'NUMBER'})
        register_table_pair(db_path, 'test_pair', qname, qname)
        save_row_comparison(db_path, 'test_pair', min(dates), max(dates), dates, [])

        return db_path, dates

    def _make_cfg(self):
        return {
            'name': 'test_tbl',
            'source': 'pcds',
            'table': 'SCHEMA.TEST_TBL',
            'date_col': 'RPT_DT',
        }

    def test_vintage_all(self, tmp_path):
        """vintage='all' → no filter applied."""
        db_path, dates = self._setup_db(tmp_path)
        cfg = self._make_cfg()
        result = _compute_date_filter(cfg, db_path, 'all')
        assert result['filter_type'] == 'none'
        assert result['n_buckets'] == 0

    @pytest.mark.parametrize("vintage, expected_filter", [
        ("day", "between"),
        ("week", "between"),
        ("month", "between"),
        ("quarter", "between"),
        ("year", "between"),
    ])
    def test_vintage_between(self, tmp_path, vintage, expected_filter):
        """Standard vintages → between filter with correct bucket counts."""
        db_path, dates = self._setup_db(tmp_path)
        cfg = self._make_cfg()
        result = _compute_date_filter(cfg, db_path, vintage)
        assert result['filter_type'] == expected_filter
        assert result['n_matching'] == len(dates)
        assert result['n_buckets'] >= 1
        assert result['min_date'] == min(dates)
        assert result['max_date'] == max(dates)

    def test_vintage_day_buckets_equal_dates(self, tmp_path):
        """Day vintage: each date gets its own bucket."""
        db_path, dates = self._setup_db(tmp_path)
        cfg = self._make_cfg()
        result = _compute_date_filter(cfg, db_path, 'day')
        assert result['n_buckets'] == len(dates)

    def test_vintage_month_buckets(self, tmp_path):
        """Month vintage: 30 dates across Jan-Mar → 3 buckets."""
        db_path, dates = self._setup_db(tmp_path)
        cfg = self._make_cfg()
        result = _compute_date_filter(cfg, db_path, 'month')
        assert result['n_buckets'] == 3

    def test_sample_at_10(self, tmp_path):
        """sample@10 → in_list filter with 10 dates."""
        db_path, dates = self._setup_db(tmp_path)
        cfg = self._make_cfg()
        result = _compute_date_filter(cfg, db_path, 'sample@10')
        assert result['filter_type'] == 'in_list'
        assert result['n_matching'] == 10
        assert result['n_buckets'] == 1
        assert result['vintage'] == 'sample'

    def test_sample_at_50_caps_at_total(self, tmp_path):
        """sample@50 with 30 dates → uses all 30."""
        db_path, dates = self._setup_db(tmp_path)
        cfg = self._make_cfg()
        result = _compute_date_filter(cfg, db_path, 'sample@50')
        assert result['filter_type'] == 'in_list'
        assert result['n_matching'] == 30  # all dates, since 30 < 50

    def test_date_dtype_populated(self, tmp_path):
        """date_dtype should be populated from column metadata."""
        db_path, dates = self._setup_db(tmp_path)
        cfg = self._make_cfg()
        result = _compute_date_filter(cfg, db_path, 'day')
        assert result['date_dtype'] == 'DATE'
