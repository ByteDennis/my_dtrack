
import os
import csv
from datetime import datetime

from .base import (
    qualified_name,
    quote_ident,
    build_date_in_clause,
    build_date_range_with_gaps,
    resolve_date_format,
    apply_pair_date_bounds,
    compute_date_filter,
    set_cache_db,
    cache_db_path,
    cache_get,
    cache_put,
    load_tables_from_config,
    fill_columns_from_meta,
    resolve_config_columns,
    inject_where_from_config,
    tag_generated_sql,
)
from ..logmgr import get_logger, log_exc

# >>> persistent DuckDB file holding CREATE TABLE materializations of each CSV <<< #
_CSV_DUCKDB_FILE = None

# >>> point the runtime at a persistent DuckDB db file (or None for in-memory) <<< #
def set_csv_duckdb(path):
    global _CSV_DUCKDB_FILE
    _CSV_DUCKDB_FILE = path if (path and os.path.exists(path)) else None

# >>> regenerable cache/state dir for a csv outdir (materialized db, query cache) <<< #
def csv_state_dir(outdir):
    return os.path.join(outdir, ".state")

# >>> default location of the materialized CSV database (under the state dir) <<< #
def csv_duckdb_path(outdir):
    return os.path.join(csv_state_dir(outdir), "csv.duckdb")

# >>> Return an in-memory DuckDB connection. Raises a helpful error when <<< #
def duckdb_connect():
    try:
        import duckdb
    except ImportError as exc:
        raise RuntimeError(
            "CSV-source pairs require DuckDB. Install with:\n"
            "  pip install 'dtrack[csv]'\n"
            "or directly:\n"
            "  pip install duckdb>=0.10.0"
        ) from exc
    return duckdb.connect(database=':memory:')

# >>> Replace `FROM '<csv_path>'` literals with `FROM read_csv_auto(...)` <<< #
def _rewrite_csv_from_clauses(sql):
    import re
    def _repl(match):
        keyword = match.group(1)
        path = match.group(2)
        return f"{keyword} read_csv_auto('{path}', header=True)"
    return re.sub(
        r"(\bFROM|\bJOIN)\s+'([^']+\.csv)'",
        _repl, sql, flags=re.IGNORECASE,
    )

# >>> Execute SQL against DuckDB; return list[dict] (lowercased cols) <<< #
def _query_duckdb(sql, conn_macro):
    log = get_logger()
    cached = cache_get(sql, conn_macro)
    if cached is not None:
        return cached

    if _CSV_DUCKDB_FILE:
        import duckdb
        log.info("csv: live read from materialized DB %s", _CSV_DUCKDB_FILE)
        conn = duckdb.connect(database=_CSV_DUCKDB_FILE, read_only=True)
        effective_sql = sql
    else:
        conn = duckdb_connect()
        if conn is None:
            return []
        log.info("csv: live read (in-memory read_csv)")
        effective_sql = _rewrite_csv_from_clauses(sql)
    try:
        cur = conn.execute(effective_sql)
        cols = [d[0].lower() for d in cur.description] if cur.description else []
        rows = cur.fetchall() or []
        result = [dict(zip(cols, r)) for r in rows]
    finally:
        try:
            conn.close()
        except Exception as e:
            log_exc("duckdb conn close", e, level="debug")

    cache_put(sql, conn_macro, result, row_count=len(result))
    return result

# >>> read_csv(...) expr for DESCRIBE, honoring a custom quote/escape <<< #
def _csv_describe_from(csv_path, quote='"', escape=''):
    if (quote or '"') == '"' and not escape:
        return f"read_csv_auto('{csv_path}', header=True)"
    opts = ["header=true", f"quote={_csv_str_literal(quote or chr(34))}"]
    if escape:
        opts.append(f"escape={_csv_str_literal(escape)}")
    return f"read_csv('{csv_path}', {', '.join(opts)})"

# >>> Read the CSV header + a sample of rows, return {col_name: dtype} <<< #
def discover_csv_columns(csv_path, quote='"', escape=''):
    try:
        import duckdb  # noqa: F401
        conn = duckdb_connect()
        if conn is not None:
            try:
                cur = conn.execute(
                    f"DESCRIBE SELECT * FROM {_csv_describe_from(csv_path, quote, escape)} LIMIT 0"
                )
                out = {}
                for row in cur.fetchall():
                    out[row[0]] = row[1]
                return out
            finally:
                try:
                    conn.close()
                except Exception as e:
                    log_exc("duckdb conn close", e, level="debug")
    except ImportError:
        pass

    try:
        import pandas as pd
        df = pd.read_csv(csv_path, nrows=1000)
    except Exception:
        return {}
    out = {}
    for col, dt in df.dtypes.items():
        if pd.api.types.is_integer_dtype(dt):
            out[col] = 'BIGINT'
        elif pd.api.types.is_float_dtype(dt):
            out[col] = 'DOUBLE'
        elif pd.api.types.is_datetime64_any_dtype(dt):
            out[col] = 'TIMESTAMP'
        elif pd.api.types.is_bool_dtype(dt):
            out[col] = 'BOOLEAN'
        else:
            out[col] = 'VARCHAR'
    return out

def csv_sql_filename(extract_type):
    return f"extract_{extract_type}.sql"

# >>> map an original-DB type annotation to the closest DuckDB read type <<< #
def _duckdb_type(decl):
    raw = (decl or '').upper().strip()
    base = raw.split('(')[0].strip()
    if base in ('NUMBER', 'NUMERIC', 'DECIMAL', 'DEC'):
        import re as _re
        m = _re.search(r'\(\s*\d+\s*,\s*(\d+)\s*\)', raw)
        return 'BIGINT' if (m and int(m.group(1)) == 0) else 'DOUBLE'
    if base in ('INT', 'INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT', 'LONG', 'INT64', 'INT32'):
        return 'BIGINT'
    if base in ('DOUBLE', 'FLOAT', 'REAL', 'BINARY_DOUBLE', 'BINARY_FLOAT', 'FLOAT64'):
        return 'DOUBLE'
    if base in ('VARCHAR', 'VARCHAR2', 'CHAR', 'NCHAR', 'NVARCHAR', 'NVARCHAR2',
                'STRING', 'TEXT', 'CLOB', 'CHARACTER'):
        return 'VARCHAR'
    if base in ('DATE',):
        return 'DATE'
    if base in ('TIMESTAMP', 'DATETIME'):
        return 'TIMESTAMP'
    return 'VARCHAR'

# >>> effective read types: declared (config) wins, inferred BOOLEAN -> VARCHAR; returns (all_types, overrides) <<< #
def _resolve_csv_types(inferred, declared):
    decl_lower = {k.lower(): v for k, v in (declared or {}).items()}
    effective, overrides = {}, {}
    for col, inf in inferred.items():
        if col.lower() in decl_lower:
            ddl = _duckdb_type(decl_lower[col.lower()])
            effective[col] = ddl
            overrides[col] = ddl
        elif (inf or '').upper().startswith('BOOL'):
            effective[col] = 'VARCHAR'
            overrides[col] = 'VARCHAR'
        else:
            effective[col] = inf
    return effective, overrides

# >>> emit a DuckDB single-quoted string literal, doubling embedded quotes <<< #
def _csv_str_literal(s):
    return "'" + str(s).replace("'", "''") + "'"

# >>> resolve per-source quote char (default ") and optional escape <<< #
def _csv_quote_escape(tbl_cfg):
    quote = tbl_cfg.get('quote')
    if quote is None or quote == '':
        quote = '"'
    escape = tbl_cfg.get('escape') or ''
    return quote, escape

# >>> sanitized DuckDB table name for a csv-source table (materialized form) <<< #
def _csv_table_name(tbl_cfg):
    import re as _re
    return "t_" + _re.sub(r'[^0-9a-zA-Z]+', '_', qualified_name(tbl_cfg)).strip('_').lower()

# >>> Return the FROM-clause table reference for a csv-source pair <<< #
def _csv_from_clause(tbl_cfg):
    materialized = tbl_cfg.get('_csv_table')
    if materialized:
        return materialized
    path = tbl_cfg.get('conn_macro', '') or ''
    if not path:
        return tbl_cfg.get('table', '') or tbl_cfg.get('name', 'unknown')
    types = tbl_cfg.get('_read_types') or {}
    quote, escape = _csv_quote_escape(tbl_cfg)
    if not types and quote == '"' and not escape:
        return f"'{path}'"
    opts = ["header=true"]
    if types:
        type_map = ", ".join(f"'{c}': '{t}'" for c, t in types.items())
        opts.append(f"types={{{type_map}}}")
    opts.append(f"quote={_csv_str_literal(quote)}")
    if escape:
        opts.append(f"escape={_csv_str_literal(escape)}")
    return f"read_csv('{path}', {', '.join(opts)})"

# >>> format a canonical YYYY-MM-DD as a DuckDB literal matching the date column's stored type <<< #
def _csv_date_bound_literal(canon_date, dtype, date_type, custom_date_types=None):
    dt = (dtype or '').upper()
    if dt.startswith(('BIGINT', 'INT', 'HUGEINT', 'DECIMAL', 'DOUBLE', 'FLOAT', 'NUMERIC', 'NUMBER')):
        return canon_date.replace('-', '')
    if dt.startswith(('DATE', 'TIMESTAMP')):
        return f"DATE '{canon_date}'"
    if not (date_type or '').strip():
        return f"'{canon_date}'"
    from .base import format_date_bound_literal
    return format_date_bound_literal(canon_date, date_type, custom_date_types=custom_date_types)

# >>> DuckDB row-count SQL for one csv-source table <<< #
def _build_row_sql_csv(tbl_cfg, db_path=None, vintage=None):
    date_col = tbl_cfg['date_col']
    transform = tbl_cfg.get('date_transform', '') or ''
    from_ref = _csv_from_clause(tbl_cfg)

    parts = []
    base_where = '' if tbl_cfg.get('_where_from_config') else (tbl_cfg.get('where', '') or '').strip()
    date_bounds = (tbl_cfg.get('_date_bounds', '') or '').strip()
    if date_bounds:
        parts.append(date_bounds)
    if base_where:
        parts.append(base_where)
    wc = ' AND '.join(f"({p})" for p in parts) if parts else '1=1'

    if transform and '{col}' in transform:
        dt_expr = transform.replace('{col}', date_col)
    else:
        dt_expr = date_col

    return (
        f"SELECT {dt_expr} AS dt, COUNT(*) AS row_count\n"
        f"FROM {from_ref} WHERE {wc}\n"
        f"GROUP BY {dt_expr}"
    )

# >>> extract_row.sql for csv-source tables. One block per table <<< #
def _write_combined_row_sql_csv(csv_tables, outdir, db_path=None, vintage=None):
    os.makedirs(outdir, exist_ok=True)
    blocks = [
        "-- dtrack CSV row extraction queries (DuckDB-evaluated)",
        f"-- Generated: {datetime.now().isoformat()}",
        "",
    ]
    for tbl in csv_tables:
        qname = qualified_name(tbl)
        try:
            sql = _build_row_sql_csv(tbl, db_path=db_path, vintage=vintage)
        except Exception as e:
            blocks.append(f"-- {qname}")
            blocks.append(f"-- SKIP: SQL build failed: {e}")
            blocks.append("")
            continue
        blocks.append(f"-- {qname}")
        blocks.append("/* @conn_macro=csv */")
        blocks.append(sql)
        blocks.append(";")
        blocks.append("")
    sql_path = os.path.join(outdir, csv_sql_filename("row"))
    with open(sql_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(blocks))
    return sql_path

def _build_col_sql_csv(tbl_cfg, col, dtype, full_table, cte_prefix="",
                       vintage=None, dt_label=None, coarse=False, preset=None,
                       stats_override=None):
    from .base import build_col_sql
    return build_col_sql('duckdb', tbl_cfg, col, dtype, full_table,
                          cte_prefix=cte_prefix, vintage=vintage,
                          dt_label=dt_label, coarse=coarse, preset=preset,
                          stats_override=stats_override)

# >>> extract_col.sql for csv-source tables. Per-col emission with <<< #
def _write_combined_col_sql_csv(csv_tables, outdir, db_path=None,
                                  vintage=None, coarse=False, preset=None,
                                  stats_override=None):

    os.makedirs(outdir, exist_ok=True)
    label = "coarse col" if coarse else "col"
    blocks = [
        f"-- dtrack CSV {label} extraction queries (DuckDB-evaluated)",
        f"-- Generated: {datetime.now().isoformat()}",
        "",
    ]
    for tbl in csv_tables:
        qname = qualified_name(tbl)
        date_col = tbl.get('date_col', '')
        full_table = _csv_from_clause(tbl)
        cte_prefix = ""

        columns = tbl.get('columns', {})
        if not columns:
            blocks.append(f"-- {qname}")
            blocks.append("-- SKIP: no columns defined")
            blocks.append("")
            continue
        non_date_cols = [(c, d) for c, d in columns.items()
                         if c.lower() != date_col.lower()]
        from .hadoop import _apply_col_filter
        non_date_cols = _apply_col_filter(non_date_cols, tbl)
        if not non_date_cols:
            blocks.append(f"-- {qname}")
            blocks.append("-- SKIP: no non-date columns (col_filter may have left zero)")
            blocks.append("")
            continue

        table_vintage = tbl.get('vintage', vintage)
        date_filter = compute_date_filter(tbl, db_path, table_vintage)
        resolve_date_format(date_filter, tbl)
        effective_vintage = date_filter.get('vintage', 'all')
        date_dtype = date_filter.get('date_dtype')
        date_format = date_filter.get('date_format')
        orig_where = tbl.get('where', '') or ''
        date_bounds = tbl.get('_date_bounds', '') or ''

        exclude_dates = tbl.get('_exclude_dates', []) or []
        exclude_clause = ""
        if exclude_dates:
            in_clause = build_date_in_clause(
                date_col, exclude_dates, date_dtype,
                date_format=date_format,
            )
            exclude_clause = f"NOT ({in_clause})"

        def _and_exclude(wc):
            if not exclude_clause:
                return wc
            if not wc or not wc.strip():
                return exclude_clause
            return f"{wc} AND {exclude_clause}"

        if date_filter['filter_type'] == 'between':
            dates = date_filter['dates']
            full_range = build_date_range_with_gaps(
                date_col, dates, date_dtype, date_format=date_format)
            bucket_where = f"{full_range} AND ({orig_where})" if orig_where else full_range
            per_col_where = _and_exclude(bucket_where)
            per_col_vintage = effective_vintage
            per_col_dt_label = None
        elif date_filter['filter_type'] == 'in_list':
            dw = build_date_in_clause(
                date_col, date_filter['dates'], date_dtype,
                date_format=date_format)
            bucket_where = f"{dw} AND ({orig_where})" if orig_where else dw
            per_col_where = _and_exclude(bucket_where)
            per_col_vintage = 'all'
            per_col_dt_label = 'sample'
        elif date_bounds:
            bw = f"{date_bounds} AND ({orig_where})" if orig_where else date_bounds
            per_col_where = _and_exclude(bw)
            per_col_vintage = 'all'
            per_col_dt_label = 'all'
        else:
            per_col_where = _and_exclude(orig_where)
            per_col_vintage = 'all'
            per_col_dt_label = 'all'

        for col, dtype in non_date_cols:
            extract_cfg = dict(tbl)
            extract_cfg['where'] = per_col_where
            try:
                sql, _ = _build_col_sql_csv(
                    extract_cfg, col, dtype, full_table, cte_prefix,
                    vintage=per_col_vintage, dt_label=per_col_dt_label,
                    coarse=coarse, preset=preset,
                    stats_override=stats_override)
            except Exception as e:
                blocks.append(f"-- {qname}/{col}")
                blocks.append(f"-- SKIP: SQL build failed: {e}")
                blocks.append("")
                continue
            blocks.append(f"-- {qname}/{col}")
            blocks.append("/* @conn_macro=csv */")
            blocks.append(sql.strip())
            blocks.append(";")
            blocks.append("")

    sql_path = os.path.join(outdir, csv_sql_filename("col"))
    with open(sql_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(blocks))
    return sql_path

def parse_csv_sql_file(sql_path):
    from .base import parse_extract_sql_file
    return parse_extract_sql_file(sql_path)

# >>> Thin wrapper over base.run_extract_sql_file with platform='csv' <<< #
def run_csv_sql_file(sql_path, outdir, max_workers=None, db_path=None,
                     on_progress=None, resume=False, cancel_event=None):
    from .base import run_extract_sql_file
    set_cache_db(cache_db_path(outdir))
    set_csv_duckdb(csv_duckdb_path(outdir))
    try:
        return run_extract_sql_file(
            sql_path, outdir,
            query_fn=_query_duckdb,
            platform='csv',
            max_workers=max_workers,
            on_progress=on_progress,
            cancel_event=cancel_event,
        )
    finally:
        set_cache_db(None)
        set_csv_duckdb(None)

# >>> Resolve the full {col: duckdb_type} map for one csv table. Config-defined <<< #
# >>> columns (inline or columns_file) win wholesale and skip DESCRIBE. <<< #
def _resolve_full_csv_types(tbl, outdir):
    cfg = resolve_config_columns(tbl)
    if cfg:
        return {c: _duckdb_type(t) for c, t in cfg.items()}, True
    path = tbl.get('conn_macro', '')
    q, esc = _csv_quote_escape(tbl)
    inferred = discover_csv_columns(path, q, esc)
    if not inferred:
        return {}, False
    from ..loader import read_columns_sidecar
    sidecar = read_columns_sidecar(os.path.join(outdir, f"{qualified_name(tbl)}_columns.csv"))
    declared = {**sidecar, **(tbl.get('columns') or {})}
    cols, _ = _resolve_csv_types(inferred, declared)
    return cols, bool(sidecar)

# >>> Materialize every csv-source table as a typed CREATE TABLE in a single <<< #
# >>> DuckDB file so queries read `FROM <table>` (consistent types, short SQL). <<< #
def build_csv_duckdb(csv_tables, outdir, db_path=None):
    from ..db import insert_column_meta
    log = get_logger()
    os.makedirs(outdir, exist_ok=True)

    try:
        import duckdb
    except ImportError as e:
        log_exc("csv: DuckDB unavailable — skipping materialization", e)
        return None

    db_file = csv_duckdb_path(outdir)
    os.makedirs(os.path.dirname(db_file), exist_ok=True)
    conn = duckdb.connect(database=db_file)
    try:
        for tbl in csv_tables:
            qname = qualified_name(tbl)
            path = tbl.get('conn_macro', '')
            if not path or not os.path.exists(path):
                log.warning("%s: conn_macro (csv path) missing or not found (%r); "
                            "skipping materialization", qname, path)
                continue
            types, from_def = _resolve_full_csv_types(tbl, outdir)
            if not types:
                log.warning("%s: no columns resolved for %s", qname, path)
                continue
            tname = _csv_table_name(tbl)
            quote, escape = _csv_quote_escape(tbl)
            type_map = ", ".join(f"'{c}': '{t}'" for c, t in types.items())
            opts = ["header=true", f"types={{{type_map}}}",
                    f"quote={_csv_str_literal(quote)}"]
            if escape:
                opts.append(f"escape={_csv_str_literal(escape)}")
            read_expr = f"read_csv('{path}', {', '.join(opts)})"
            try:
                conn.execute(f"CREATE OR REPLACE TABLE {tname} AS SELECT * FROM {read_expr}")
            except Exception as e:
                log_exc(f"{qname}: CREATE TABLE from {path} failed", e)
                continue
            tbl['_csv_table'] = tname
            tbl['columns'] = types
            if db_path:
                insert_column_meta(db_path, qname, types, source='csv')
            col_csv = os.path.join(outdir, f"{qname}_columns.csv")
            if os.path.exists(col_csv):
                log.info("%s: materialized %s (%d cols); kept existing %s_columns.csv",
                         qname, tname, len(types), qname)
            else:
                with open(col_csv, 'w', newline='') as f:
                    w = csv.writer(f)
                    w.writerow(['column_name', 'data_type'])
                    for name, dt in types.items():
                        w.writerow([name, dt])
                log.info("%s: materialized %s (%d cols, %s) -> %s", qname, tname,
                         len(types), "config-defined" if from_def else "discovered", col_csv)
    finally:
        conn.close()
    return db_file

# >>> CLI entrypoint: discover -> write extract_{type}.csv.sql -> (optionally) run via DuckDB <<< #
def extract_csv(config_path, outdir, types=None, max_workers=None, db_path=None,
                vintage=None, force=False, from_date=None, to_date=None,
                preset=None, stats_override=None, gen_only=False):
    if types is None:
        types = ["row", "col"]

    from ..config import load_unified_config
    config = load_unified_config(config_path)
    if db_path:
        from ..db import get_pair_col_map_from_db, get_pair_col_filter
        for pname, pcfg in config.get("pairs", {}).items():
            if not pcfg.get("col_map"):
                db_map = get_pair_col_map_from_db(db_path, pname)
                if db_map:
                    pcfg["col_map"] = db_map
            if not pcfg.get("col_filter"):
                cf = get_pair_col_filter(db_path, pname)
                if cf.get("include") or cf.get("exclude"):
                    pcfg["col_filter"] = cf

    os.makedirs(outdir, exist_ok=True)

    all_tables = load_tables_from_config(config)
    csv_tables = [t for t in all_tables
                  if (t.get('source') or '').lower() == 'csv']
    if not csv_tables:
        print("No CSV tables found in config")
        return

    for tbl in csv_tables:
        if vintage:
            tbl['vintage'] = vintage
        elif 'vintage' not in tbl:
            tbl['vintage'] = 'all'
    inject_where_from_config(csv_tables, config)

    build_csv_duckdb(csv_tables, outdir, db_path)

    def _csv_bounds(tbl, date_col, eff_from, eff_to):
        cols = tbl.get('columns') or {}
        dtype = next((v for k, v in cols.items()
                      if k.lower() == date_col.lower()), '')
        if not dtype:
            path = tbl.get('conn_macro', '') or ''
            if path and os.path.exists(path):
                q, esc = _csv_quote_escape(tbl)
                disc = discover_csv_columns(path, q, esc)
                dtype = next((v for k, v in disc.items()
                              if k.lower() == date_col.lower()), '')
        date_type = tbl.get('date_type')
        custom = config.get('date_types')
        date_col_q = quote_ident(date_col, "duckdb")
        bounds = []
        if eff_from:
            bounds.append(f"{date_col_q} >= {_csv_date_bound_literal(eff_from, dtype, date_type, custom)}")
        if eff_to:
            bounds.append(f"{date_col_q} <= {_csv_date_bound_literal(eff_to, dtype, date_type, custom)}")
        return " AND ".join(bounds)
    apply_pair_date_bounds(csv_tables, config, from_date, to_date, _csv_bounds)

    def _gen(t):
        if t == "row":
            _write_combined_row_sql_csv(csv_tables, outdir,
                                        db_path=db_path, vintage=vintage)
        else:
            if db_path:
                fill_columns_from_meta(csv_tables, db_path)
            _write_combined_col_sql_csv(csv_tables, outdir,
                                        db_path=db_path, vintage=vintage,
                                        preset=preset, stats_override=stats_override)

    if gen_only:
        for t in types:
            _gen(t)
            tag_generated_sql(outdir, t, "csv")
        print(f"[gen-only] wrote CSV extract SQL to {outdir} (no extraction)")
        return

    for t in types:
        _gen(t)
        sql_path = os.path.join(outdir, csv_sql_filename(t))
        run_csv_sql_file(sql_path, outdir, max_workers=max_workers,
                         db_path=db_path, resume=not force)
