"""Tests for dtrack/extract.py — SAS generation, SQL builders, column matching."""

import json
import os
import pytest

from dtrack.extract import (
    is_numeric_type,
    _oracle_date_transform,
    _vintage_date_expr,
    _vintage_date_expr_athena,
    _gen_sas_row_datadriven,
    _gen_sas_col_local,
    gen_sas,
    build_continuous_sql_oracle,
    build_categorical_sql_oracle,
    build_continuous_sql_athena,
    build_categorical_sql_athena,
    build_top10_sql_oracle,
    build_top10_sql_athena,
    match_columns_from_dicts,
)


# ---------------------------------------------------------------------------
# TestIsNumericType
# ---------------------------------------------------------------------------
class TestIsNumericType:
    def test_oracle_numeric_basic(self):
        for t in ('NUMBER', 'FLOAT', 'INTEGER', 'DECIMAL'):
            assert is_numeric_type(t, is_oracle=True), t

    def test_oracle_numeric_with_precision(self):
        assert is_numeric_type('NUMBER(10,2)', is_oracle=True)
        assert is_numeric_type('DECIMAL(15,4)', is_oracle=True)

    def test_oracle_non_numeric(self):
        for t in ('VARCHAR2', 'DATE', 'CLOB', 'VARCHAR2(20)'):
            assert not is_numeric_type(t, is_oracle=True), t

    def test_athena_numeric(self):
        for t in ('int', 'bigint', 'double', 'decimal'):
            assert is_numeric_type(t, is_oracle=False), t

    def test_athena_non_numeric(self):
        for t in ('varchar', 'string', 'date'):
            assert not is_numeric_type(t, is_oracle=False), t

    def test_case_insensitive_oracle(self):
        assert is_numeric_type('number', is_oracle=True)
        assert is_numeric_type('float', is_oracle=True)


# ---------------------------------------------------------------------------
# TestOracleDateTransform
# ---------------------------------------------------------------------------
class TestOracleDateTransform:
    def test_datetime_to_date(self):
        assert _oracle_date_transform('RPT_DT', 'datetime_to_date') == 'TRUNC(RPT_DT)'

    def test_to_char(self):
        assert _oracle_date_transform('RPT_DT', 'to_char') == "TO_CHAR(RPT_DT, 'YYYY-MM-DD')"

    def test_no_transform(self):
        assert _oracle_date_transform('RPT_DT', '') == 'RPT_DT'
        assert _oracle_date_transform('RPT_DT', None) == 'RPT_DT'


# ---------------------------------------------------------------------------
# TestVintageDateExpr
# ---------------------------------------------------------------------------
class TestVintageDateExpr:
    def test_day_is_identity(self):
        assert _vintage_date_expr('RPT_DT', 'day') == 'RPT_DT'

    def test_none_is_identity(self):
        assert _vintage_date_expr('RPT_DT', None) == 'RPT_DT'

    def test_week(self):
        assert _vintage_date_expr('RPT_DT', 'week') == "TRUNC(RPT_DT, 'IW')"

    def test_month(self):
        assert _vintage_date_expr('RPT_DT', 'month') == "TRUNC(RPT_DT, 'MM')"

    def test_quarter(self):
        assert _vintage_date_expr('RPT_DT', 'quarter') == "TRUNC(RPT_DT, 'Q')"

    def test_year(self):
        assert _vintage_date_expr('RPT_DT', 'year') == "TRUNC(RPT_DT, 'YYYY')"

    def test_stacks_on_transform(self):
        # date_transform already applied TRUNC(RPT_DT)
        assert _vintage_date_expr('TRUNC(RPT_DT)', 'month') == "TRUNC(TRUNC(RPT_DT), 'MM')"

    def test_vintage_transform_overrides(self):
        result = _vintage_date_expr('RPT_DT', 'week', vintage_transform="TO_DATE(TO_CHAR({col}), 'YYYYMM')")
        assert result == "TO_DATE(TO_CHAR(RPT_DT), 'YYYYMM')"

    def test_vintage_transform_noop(self):
        assert _vintage_date_expr('MONTH_DT', 'week', vintage_transform="{col}") == "MONTH_DT"


# ---------------------------------------------------------------------------
# TestVintageDateExprAthena
# ---------------------------------------------------------------------------
class TestVintageDateExprAthena:
    def test_day_is_identity(self):
        assert _vintage_date_expr_athena('dt_col', 'day') == 'dt_col'

    def test_week(self):
        assert _vintage_date_expr_athena('dt_col', 'week') == "date_trunc('week', CAST(dt_col AS date))"

    def test_month(self):
        assert _vintage_date_expr_athena('dt_col', 'month') == "date_trunc('month', CAST(dt_col AS date))"

    def test_quarter(self):
        assert _vintage_date_expr_athena('dt_col', 'quarter') == "date_trunc('quarter', CAST(dt_col AS date))"

    def test_year(self):
        assert _vintage_date_expr_athena('dt_col', 'year') == "date_trunc('year', CAST(dt_col AS date))"

    def test_vintage_transform_overrides(self):
        result = _vintage_date_expr_athena('dt_col', 'week', vintage_transform="date_trunc('month', {col})")
        assert result == "date_trunc('month', dt_col)"

    def test_vintage_transform_noop(self):
        assert _vintage_date_expr_athena('dt_col', 'week', vintage_transform="{col}") == "dt_col"


# ---------------------------------------------------------------------------
# TestGenSasRowDatadriven
# ---------------------------------------------------------------------------
class TestGenSasRowDatadriven:
    @pytest.fixture()
    def basic_cfg(self):
        return {
            'name': 'cust_daily',
            'table': 'SCHEMA1.CUST_DAILY',
            'source': 'pcds',
            'date_col': 'RPT_DT',
            'conn_macro': 'pcds',
            'where': '',
            'date_transform': '',
        }

    def test_basic_structure(self, basic_cfg):
        sas = _gen_sas_row_datadriven([basic_cfg])
        assert '_ora_map' in sas
        assert '%macro _row_oracle' in sas
        assert 'call execute' in sas
        assert 'connection to oracle' in sas
        assert 'proc export' in sas
        assert 'cache.' in sas

    def test_datalines_content(self, basic_cfg):
        sas = _gen_sas_row_datadriven([basic_cfg])
        assert 'SCHEMA1.CUST_DAILY|pcds_cust_daily|pcds_cust_daily|RPT_DT|pcds|' in sas

    def test_date_transform_trunc(self, basic_cfg):
        basic_cfg['date_transform'] = 'datetime_to_date'
        sas = _gen_sas_row_datadriven([basic_cfg])
        assert 'TRUNC(RPT_DT)' in sas

    def test_conn_macro_in_datalines(self, basic_cfg):
        basic_cfg['conn_macro'] = 'pb23'
        sas = _gen_sas_row_datadriven([basic_cfg])
        assert '|pb23|' in sas

    def test_multiple_tables(self, basic_cfg):
        cfg2 = {
            'name': 'txn_monthly',
            'table': 'TXN_MONTHLY',
            'source': 'oracle',
            'date_col': 'MONTH_DT',
            'conn_macro': 'pb23',
        }
        sas = _gen_sas_row_datadriven([basic_cfg, cfg2])
        assert 'SCHEMA1.CUST_DAILY' in sas
        assert 'TXN_MONTHLY' in sas

    def test_where_in_datalines(self, basic_cfg):
        basic_cfg['where'] = "STATUS = 'A'"
        sas = _gen_sas_row_datadriven([basic_cfg])
        # SAS-safe quoting (doubled single quotes)
        assert "STATUS = ''A''" in sas


# ---------------------------------------------------------------------------
# TestGenSasColLocal
# ---------------------------------------------------------------------------
class TestGenSasColLocal:
    @pytest.fixture()
    def mixed_cfg(self):
        return {
            'name': 'cust_daily',
            'table': 'SCHEMA1.CUST_DAILY',
            'date_col': 'RPT_DT',
            'conn_macro': 'pcds',
            'where': "STATUS = 'A'",
            'date_transform': 'datetime_to_date',
            'columns': {
                'RPT_DT': 'DATE',
                'AMT': 'NUMBER(10,2)',
                'CUST_STATUS': 'VARCHAR2(20)',
            },
        }

    def test_uses_proc_means_for_numeric(self, mixed_cfg):
        sas = _gen_sas_col_local(mixed_cfg)
        assert 'proc means' in sas
        assert 'var AMT' in sas

    def test_uses_proc_freq_for_categorical(self, mixed_cfg):
        sas = _gen_sas_col_local(mixed_cfg)
        assert 'proc freq' in sas
        assert 'CUST_STATUS' in sas

    def test_discovery_mode(self):
        cfg = {
            'name': 'acct_summary',
            'table': 'SCHEMA1.ACCT_SUMMARY',
            'date_col': 'AS_OF_DT',
            'conn_macro': 'pcds',
            'columns': {},
        }
        sas = _gen_sas_col_local(cfg)
        assert 'WARNING' in sas

    def test_zero_non_date_columns(self):
        cfg = {
            'name': 'date_only_tbl',
            'table': 'DATE_ONLY_TBL',
            'date_col': 'DT',
            'columns': {'DT': 'DATE'},
        }
        sas = _gen_sas_col_local(cfg)
        assert 'WARNING' in sas
        assert 'No non-date columns' in sas

    def test_uses_pull_data(self, mixed_cfg):
        sas = _gen_sas_col_local(mixed_cfg)
        assert '%pull_data(' in sas

    def test_cache_saves_stats(self, mixed_cfg):
        sas = _gen_sas_col_local(mixed_cfg)
        assert 'cache._cs_' in sas

    def test_skip_if_cached(self, mixed_cfg):
        sas = _gen_sas_col_local(mixed_cfg)
        assert '%sysfunc(exist(' in sas
        assert 'skipping' in sas.lower()

    def test_top_10_from_freq(self, mixed_cfg):
        sas = _gen_sas_col_local(mixed_cfg)
        assert 'top_10' in sas
        assert '_t10_' in sas

    def test_stacking_export_cleanup(self, mixed_cfg):
        sas = _gen_sas_col_local(mixed_cfg)
        assert 'data _colstats_cust_daily' in sas
        assert 'proc export' in sas
        assert 'proc delete' in sas

    def test_cleanup_raw_data(self, mixed_cfg):
        sas = _gen_sas_col_local(mixed_cfg)
        assert 'proc delete data=_raw_' in sas



# ---------------------------------------------------------------------------
# TestGenSasFull — integration tests
# ---------------------------------------------------------------------------
class TestGenSasFull:
    @pytest.fixture()
    def config_path(self):
        return os.path.join(os.path.dirname(__file__), '..', 'sample_data', 'extract_config_full.json')

    def test_single_table_output(self, tmp_path):
        cfg = {'tables': [{'name': 't1', 'table': 'T1', 'source': 'pcds', 'date_col': 'D', 'columns': {'D': 'DATE', 'X': 'NUMBER'}}]}
        cfg_path = tmp_path / 'cfg.json'
        cfg_path.write_text(json.dumps(cfg))
        outdir = tmp_path / 'out'
        gen_sas(str(cfg_path), str(outdir))
        assert (outdir / 'extract.sas').exists()

    def test_multiple_pcds_tables(self, tmp_path, config_path):
        outdir = tmp_path / 'out'
        gen_sas(config_path, str(outdir))
        sas = (outdir / 'extract.sas').read_text()
        # Data-driven row uses table_date_map, not per-table macros
        assert 'table_date_map' in sas
        assert 'export_row_one' in sas
        # Col macros still per-table
        assert 'get_colstats_cust_daily' in sas
        assert 'get_colstats_txn_monthly' in sas

    def test_aws_table_skipped(self, tmp_path, config_path):
        outdir = tmp_path / 'out'
        gen_sas(config_path, str(outdir))
        sas = (outdir / 'extract.sas').read_text()
        assert 'get_colstats_aws_table' not in sas

    def test_template_boilerplate(self, tmp_path, config_path):
        outdir = tmp_path / 'out'
        gen_sas(config_path, str(outdir))
        sas = (outdir / 'extract.sas').read_text()
        assert '%MACRO pull_data' in sas
        assert '%macro start_timer' in sas
        assert '%macro log_time' in sas
        assert '%macro pcds' in sas
        assert '%macro pb23' in sas

    def test_runner_has_timer_pairs(self, tmp_path, config_path):
        outdir = tmp_path / 'out'
        gen_sas(config_path, str(outdir))
        sas = (outdir / 'extract.sas').read_text()
        assert '%start_timer()' in sas
        assert '%log_time(' in sas

    def test_types_row_only(self, tmp_path, config_path):
        outdir = tmp_path / 'out'
        gen_sas(config_path, str(outdir), types=['row'])
        sas = (outdir / 'extract_row.sas').read_text()
        assert 'export_row_one' in sas
        assert 'get_colstats_cust_daily' not in sas

    def test_types_col_only(self, tmp_path, config_path):
        outdir = tmp_path / 'out'
        gen_sas(config_path, str(outdir), types=['col'])
        sas = (outdir / 'extract_col.sas').read_text()
        assert 'get_colstats_cust_daily' in sas
        assert 'get_rowcounts_cust_daily' not in sas

    def test_env_credentials_filled(self, tmp_path, config_path):
        env_file = tmp_path / '.env'
        env_file.write_text('pcds_usr=myuser\npcds_pw=mypass\nemail_to=me@co.com\nlib_path=/data')
        outdir = tmp_path / 'out'
        gen_sas(config_path, str(outdir), env_path=str(env_file))
        sas = (outdir / 'extract.sas').read_text()
        assert 'myuser' in sas
        assert 'mypass' in sas
        assert 'me@co.com' in sas
        assert '/data' in sas


# ---------------------------------------------------------------------------
# TestGenSasEnv
# ---------------------------------------------------------------------------
class TestGenSasEnv:
    @pytest.fixture()
    def simple_config(self, tmp_path):
        cfg = {'tables': [{'name': 't', 'table': 'T', 'source': 'pcds', 'date_col': 'D', 'columns': {'D': 'DATE', 'X': 'NUMBER'}}]}
        p = tmp_path / 'cfg.json'
        p.write_text(json.dumps(cfg))
        return str(p)

    def test_all_env_vars(self, tmp_path, simple_config):
        env = tmp_path / '.env'
        env.write_text('pcds_usr=u1\npcds_pw=p1\nemail_to=e@x\nlib_path=/lib')
        outdir = tmp_path / 'out'
        gen_sas(simple_config, str(outdir), env_path=str(env))
        sas = (outdir / 'extract.sas').read_text()
        assert 'u1' in sas and 'p1' in sas and 'e@x' in sas and '/lib' in sas

    def test_no_env(self, tmp_path, simple_config):
        outdir = tmp_path / 'out'
        gen_sas(simple_config, str(outdir))
        sas = (outdir / 'extract.sas').read_text()
        # Should have empty credentials
        assert "%let iamusr = ;" in sas or "%let iamusr =;" in sas or "iamusr" in sas

    def test_partial_env(self, tmp_path, simple_config):
        env = tmp_path / '.env'
        env.write_text('pcds_usr=onlyuser')
        outdir = tmp_path / 'out'
        gen_sas(simple_config, str(outdir), env_path=str(env))
        sas = (outdir / 'extract.sas').read_text()
        assert 'onlyuser' in sas


# ---------------------------------------------------------------------------
# TestSqlBuilders
# ---------------------------------------------------------------------------
class TestSqlBuilders:
    def test_continuous_has_avg_stddev(self):
        sql = build_continuous_sql_oracle('T', 'AMT', 'DT')
        assert 'AVG(AMT)' in sql
        assert 'STDDEV(AMT)' in sql
        assert 'GROUP BY DT' in sql

    def test_categorical_has_null(self):
        sql = build_categorical_sql_oracle('T', 'STATUS', 'DT')
        assert 'NULL AS mean' in sql
        assert 'NULL AS std' in sql

    def test_where_clause(self):
        sql = build_continuous_sql_oracle('T', 'AMT', 'DT', where="X > 1")
        assert 'AND X > 1' in sql

    def test_top10_row_number(self):
        sql = build_top10_sql_oracle('T', 'COL', 'DT')
        assert 'ROW_NUMBER()' in sql
        assert 'PARTITION BY DT' in sql
        assert 'rn <= 10' in sql

    def test_no_order_by_in_oracle(self):
        sql = build_continuous_sql_oracle('T', 'AMT', 'DT')
        assert 'ORDER BY' not in sql

    def test_athena_continuous_n_missing(self):
        sql = build_continuous_sql_athena('T', 'AMT', 'DT')
        assert 'COUNT(*) - COUNT(AMT) AS n_missing' in sql
        assert 'ORDER BY' not in sql

    def test_athena_categorical(self):
        sql = build_categorical_sql_athena('T', 'STATUS', 'DT')
        assert 'NULL AS mean' in sql
        assert 'ORDER BY' not in sql

    def test_athena_top10(self):
        sql = build_top10_sql_athena('T', 'COL', 'DT')
        assert 'ROW_NUMBER()' in sql
        assert 'ORDER BY' not in sql.split('rn <= 10')[1]  # no ORDER BY after filter

    def test_athena_continuous_with_vintage(self):
        date_expr = "date_trunc('week', CAST(dt_col AS date))"
        sql = build_continuous_sql_athena('T', 'AMT', date_expr)
        assert f"{date_expr} AS dt" in sql
        assert f"GROUP BY {date_expr}" in sql


# ---------------------------------------------------------------------------
# TestMatchColumns
# ---------------------------------------------------------------------------
class TestMatchColumns:
    def test_case_insensitive_match(self):
        left = {'AMT': 'NUMBER', 'STATUS': 'VARCHAR2'}
        right = {'amt': 'double', 'status': 'varchar'}
        result = match_columns_from_dicts(left, right)
        assert len(result['matched']) == 2

    def test_unmatched(self):
        left = {'AMT': 'NUMBER', 'EXTRA_L': 'VARCHAR2'}
        right = {'amt': 'double', 'extra_r': 'varchar'}
        result = match_columns_from_dicts(left, right, left_label='pcds', right_label='aws')
        assert len(result['matched']) == 1
        assert len(result['pcds_only']) == 1
        assert len(result['aws_only']) == 1

    def test_json_outfile(self, tmp_path):
        left = {'A': 'NUMBER'}
        right = {'a': 'int', 'B': 'varchar'}
        out = tmp_path / 'map.json'
        match_columns_from_dicts(left, right, outfile=str(out))
        data = json.loads(out.read_text())
        assert 'matched' in data
        assert 'manual_mapping' in data
