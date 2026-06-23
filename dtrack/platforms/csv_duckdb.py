"""CSV-as-source platform powered by DuckDB.

Lets dtrack run row_gen / col_gen against local CSV files without any
engine credentials. Useful for:

  - Validating a CSV export against another (e.g., compare what an
    Oracle batch wrote vs what an Athena pipeline wrote, both dumped to
    CSV before this point).
  - Air-gapped environments where the user has CSVs but no direct
    engine access.
  - Smoke-testing the pipeline end-to-end without paying for engine
    queries.

Layered on top of base.run_extract_sql_file + base.build_col_sql with
the new 'duckdb' dialect — every piece of the runner skeleton (parser,
ThreadPool dispatch, lifeboat retry, per-qname CSV flush, cancel,
on_progress events, .cache.db) is reused unchanged. Only the per-block
query function and the column-discovery helper are dialect-specific.

The CSV file path is read from `tbl_cfg['conn_macro']` (every source
carries its locator there; csv has no DSN, so conn_macro IS the path).
Empty / missing leaves the FROM clause as the bare table name, which
DuckDB resolves against its catalog (so a caller can pre-register
tables under a different name if desired).
"""

import os
import csv
import threading
from datetime import datetime
from typing import Optional

from .base import (
    qualified_name,
    resolve_table,
    quote_ident,
    apply_null_sentinels,
    is_numeric_type,
    build_date_in_clause,
    build_date_range_with_gaps,
    resolve_date_format,
    compute_date_filter,
    set_cache_db,
    cache_get,
    cache_put,
    load_tables_from_config,
    fill_columns_from_meta,
    inject_where_from_config,
    get_mock_dir,
    _extract_mock_tables,
    tag_generated_sql,
)


# ---------------------------------------------------------------------------
# Connection — one in-memory DuckDB connection per query. DuckDB is in-process
# so there's no real "connection" cost vs Oracle/Athena, but keeping the
# usage shape parallel to the other platforms means run_extract_sql_file
# doesn't need a per-platform branch for connection lifecycle.
# ---------------------------------------------------------------------------

def duckdb_connect():
    """Return an in-memory DuckDB connection. Raises a helpful error when
    duckdb isn't installed (it's an optional dep — see pyproject.toml
    [project.optional-dependencies].csv).

    Mock mode (DTRACK_MOCK / DTRACK_CSV_MOCK) returns None so unit tests
    can stub the platform without installing duckdb.
    """
    if os.environ.get('DTRACK_MOCK') or os.environ.get('DTRACK_CSV_MOCK'):
        print("[mock] Skipping DuckDB connection")
        return None
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


def _rewrite_csv_from_clauses(sql):
    """Replace `FROM '<csv_path>'` literals with `FROM read_csv_auto(...)`.

    DuckDB accepts a bare quoted path as a table reference, but Python's
    SQL builder emits `FROM '/path/to/file.csv'` which DuckDB parses as
    a string literal in some contexts. Substituting with the explicit
    read_csv_auto() form is unambiguous and works in every clause.

    Idempotent — calls that contain `read_csv_auto(...)` already pass
    through unchanged. Only matches paths ending in `.csv`.

    Per-block work: DuckDB re-parses the CSV on each call. For multi-GB
    files that's slow; a future optimization is to load each CSV into
    a temporary DuckDB table once per worker via a per-thread
    connection pool. Today's per-block-conn flow keeps the runner
    contract identical to the other platforms and is fine for
    validation-sized fixtures.
    """
    import re
    def _repl(match):
        keyword = match.group(1)
        path = match.group(2)
        return f"{keyword} read_csv_auto('{path}', header=True)"
    return re.sub(
        r"(\bFROM|\bJOIN)\s+'([^']+\.csv)'",
        _repl, sql, flags=re.IGNORECASE,
    )


def _query_duckdb(sql, conn_macro):
    """Execute SQL against DuckDB; return list[dict] (lowercased cols).

    `conn_macro` is unused for DuckDB (in-process, no DSN) but kept in
    the signature to match the runner contract. Mock mode returns [].
    """
    cached = cache_get(sql, conn_macro)
    if cached is not None:
        return cached

    conn = duckdb_connect()
    if conn is None:
        return []
    try:
        # Rewrite FROM '<path>.csv' -> FROM read_csv_auto('<path>',header=True)
        # for unambiguous parsing in every clause position.
        effective_sql = _rewrite_csv_from_clauses(sql)
        cur = conn.execute(effective_sql)
        cols = [d[0].lower() for d in cur.description] if cur.description else []
        rows = cur.fetchall() or []
        result = [dict(zip(cols, r)) for r in rows]
    finally:
        # DuckDB connections are cheap but holding views in memory across
        # the run is the point — we close here per-block. To avoid that
        # cost the runner can switch to a per-worker connection pool
        # later; for now this matches the other platforms' shape.
        try:
            conn.close()
        except Exception:
            pass

    cache_put(sql, conn_macro, result, row_count=len(result))
    return result


# ---------------------------------------------------------------------------
# Column discovery — auto-populate _column_meta from CSV via DuckDB's
# DESCRIBE, so the col-stats SQL builder can pick sentinel families
# without the user manually declaring every column.
# ---------------------------------------------------------------------------

def discover_csv_columns(csv_path):
    """Read the CSV header + a sample of rows, return {col_name: dtype}
    where dtype is the inferred SQL-ish type (BIGINT / DOUBLE / VARCHAR /
    DATE / TIMESTAMP / ...). Empty {} if the file can't be read.

    Tries DuckDB first (richer inference: distinguishes BIGINT/DOUBLE/DATE/
    TIMESTAMP/VARCHAR via its own CSV sampler). Falls back to pandas
    inference (BIGINT for ints, DOUBLE for floats, TIMESTAMP for parsed
    dates, VARCHAR otherwise) when DuckDB isn't installed -- so column
    metadata is still populated and the col_mapping page works without
    the optional dep. The downstream sentinel-family logic only cares
    about the broad numeric-vs-string distinction, which both paths get
    right.

    Called by the gen-time discovery helper before SQL emission so the
    same `_column_meta`-driven sentinel logic the engine platforms use
    works transparently for CSV-sourced pairs.
    """
    # DuckDB path: richest inference.
    if not (os.environ.get('DTRACK_MOCK') or os.environ.get('DTRACK_CSV_MOCK')):
        try:
            import duckdb  # noqa: F401
            conn = duckdb_connect()
            if conn is not None:
                try:
                    cur = conn.execute(
                        f"DESCRIBE SELECT * FROM read_csv_auto('{csv_path}', header=True) LIMIT 0"
                    )
                    out = {}
                    for row in cur.fetchall():
                        out[row[0]] = row[1]
                    return out
                finally:
                    try: conn.close()
                    except Exception: pass
        except ImportError:
            pass  # fall through to pandas

    # Pandas fallback: SAS-style categorical-vs-numeric distinction.
    # Samples the first 1000 rows to keep huge-file inference cheap.
    # Maps pandas dtypes to a coarse SQL-ish label that the col-stats
    # builder uses for sentinel selection (numeric vs string).
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


# ---------------------------------------------------------------------------
# Generated-file format. Mirrors oracle / hadoop block format so
# base.parse_extract_sql_file + base.run_extract_sql_file work unchanged.
# ---------------------------------------------------------------------------

def csv_sql_filename(extract_type):
    return f"extract_{extract_type}.sql"


def _csv_from_clause(tbl_cfg):
    """Return the FROM-clause table reference for a csv-source pair.

    Prefers tbl_cfg['conn_macro'] (the CSV file path, quoted as a string
    literal -- DuckDB accepts this directly; the runner's
    _rewrite_csv_from_clauses swaps in read_csv_auto at execution time).
    Falls back to tbl_cfg['table'] when conn_macro is empty, which lets a
    caller pre-register tables under a different name in the catalog.
    """
    path = tbl_cfg.get('conn_macro', '') or ''
    if path:
        return f"'{path}'"
    return tbl_cfg.get('table', '') or tbl_cfg.get('name', 'unknown')


def _build_row_sql_csv(tbl_cfg, db_path=None, vintage=None):
    """DuckDB row-count SQL for one csv-source table.

    Mirrors Hadoop/Oracle row-gen: bare date_col AS dt (with optional
    date_transform), GROUP BY date_col. Layers `_date_bounds` from the
    config into WHERE since row gen has no other date filter (matches
    the fix applied to the other platforms).
    """
    date_col = tbl_cfg['date_col']
    transform = tbl_cfg.get('date_transform', '') or ''
    from_ref = _csv_from_clause(tbl_cfg)

    parts = []
    base_where = (tbl_cfg.get('where', '') or '').strip()
    date_bounds = (tbl_cfg.get('_date_bounds', '') or '').strip()
    if base_where:
        parts.append(base_where)
    if date_bounds:
        parts.append(date_bounds)
    wc = ' AND '.join(f"({p})" for p in parts) if parts else '1=1'

    # date_transform is a DuckDB SQL expression like "CAST({col} AS DATE)"
    # if the user wants a coercion. Default: bare column.
    if transform and '{col}' in transform:
        dt_expr = transform.replace('{col}', date_col)
    else:
        dt_expr = date_col

    return (
        f"SELECT {dt_expr} AS dt, COUNT(*) AS row_count\n"
        f"FROM {from_ref} WHERE {wc}\n"
        f"GROUP BY {dt_expr}"
    )


def _write_combined_row_sql_csv(csv_tables, outdir, db_path=None, vintage=None):
    """extract_row.sql for csv-source tables. One block per table.

    Block format matches oracle/hadoop: `-- {qname}` / `/* @conn_macro=
    csv */` (csv as marker -- not actually used by the runner since
    DuckDB is in-process) / SQL / `;`.
    """
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


def _write_combined_col_sql_csv(csv_tables, outdir, db_path=None,
                                  vintage=None, coarse=False, preset=None,
                                  stats_override=None):
    """extract_col.sql for csv-source tables. Per-col emission with
    server-side GROUP BY vintage_expr (matches the Hadoop approach --
    minimal block count, DuckDB returns all buckets in one query)."""
    from ..date_utils import bucket_date

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
        # Honor col_filter selection (same plumbing as Hadoop/Oracle).
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

        # Resolve WHERE + per-col vintage. Same logic as Hadoop's
        # per-col emission: WHERE narrows to the matching-date range,
        # GROUP BY vintage_expr lives inside _build_col_sql_csv.
        if date_filter['filter_type'] == 'between':
            dates = date_filter['dates']
            full_range = build_date_range_with_gaps(
                date_col, dates, date_dtype, date_format=date_format)
            bucket_where = f"({orig_where}) AND {full_range}" if orig_where else full_range
            per_col_where = _and_exclude(bucket_where)
            per_col_vintage = effective_vintage
            per_col_dt_label = None
        elif date_filter['filter_type'] == 'in_list':
            dw = build_date_in_clause(
                date_col, date_filter['dates'], date_dtype,
                date_format=date_format)
            bucket_where = f"({orig_where}) AND {dw}" if orig_where else dw
            per_col_where = _and_exclude(bucket_where)
            per_col_vintage = 'all'
            per_col_dt_label = 'sample'
        elif date_bounds:
            bw = f"({orig_where}) AND {date_bounds}" if orig_where else date_bounds
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


# ---------------------------------------------------------------------------
# Runner — same shape as oracle/hadoop wrappers. base.run_extract_sql_file
# does the heavy lifting (parse, dispatch, lifeboat, per-qname CSV flush,
# cancel, on_progress, set_cache_db lifecycle).
# ---------------------------------------------------------------------------

def parse_csv_sql_file(sql_path):
    from .base import parse_extract_sql_file
    return parse_extract_sql_file(sql_path)


def run_csv_sql_file(sql_path, outdir, max_workers=None, db_path=None,
                     on_progress=None, resume=False, cancel_event=None):
    """Thin wrapper over base.run_extract_sql_file with platform='csv'.

    Wraps the call with the SQL-result cache lifecycle so re-running the
    same extract_*.sql skips blocks already in .cache.db (matches
    Oracle and Hadoop behavior). DuckDB is in-process so there's no
    DSN -- conn_macro is just a label.
    """
    from .base import run_extract_sql_file
    set_cache_db(os.path.join(outdir, ".cache.db"))
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


def _discover_and_write_csv_columns(csv_tables, outdir, db_path):
    """For each csv-source table, run DESCRIBE against the CSV via
    DuckDB, write the discovered columns to _column_meta + a sidecar
    {qname}_columns.csv (matches the other platforms' discovery shape).

    Called by /api/generate before col SQL emission so the col-stats
    builder can pick sentinel families and the col_mapping page sees
    the same columns the engine platforms would.
    """
    from ..db import insert_column_meta
    os.makedirs(outdir, exist_ok=True)
    for tbl in csv_tables:
        path = tbl.get('conn_macro', '')
        if not path or not os.path.exists(path):
            print(f"  {qualified_name(tbl)}: conn_macro (csv path) missing or not found "
                  f"({path!r}); skipping discovery")
            continue
        qname = qualified_name(tbl)
        cols = discover_csv_columns(path)
        if not cols:
            print(f"  {qname}: DuckDB returned no columns for {path}")
            continue
        if db_path:
            insert_column_meta(db_path, qname, cols, source='csv')
        # Sidecar CSV so the user can inspect / load via the same path
        # the engine platforms use.
        col_csv = os.path.join(outdir, f"{qname}_columns.csv")
        with open(col_csv, 'w', newline='') as f:
            w = csv.writer(f)
            w.writerow(['column_name', 'data_type'])
            for name, dt in cols.items():
                w.writerow([name, dt])
        print(f"  {qname}: discovered {len(cols)} columns -> {col_csv}")


# >>> CLI entrypoint: discover -> write extract_{type}.csv.sql -> (optionally) run via DuckDB <<< #
def extract_csv(config_path, outdir, types=None, max_workers=None, db_path=None,
                vintage=None, force=False, from_date=None, to_date=None,
                preset=None, stats_override=None, gen_only=False):
    """Extract row/col CSVs for csv-source pairs using DuckDB.

    Mirrors extract_oracle's shape: filter csv tables, discover columns,
    apply CLI date bounds, then either emit extract_{type}.csv.sql
    (gen_only) or generate-and-run via run_csv_sql_file. The plain
    extract_{type}.sql is renamed to extract_{type}.csv.sql by
    tag_generated_sql so run_sql_all picks it up.
    """
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

    if from_date or to_date:
        for tbl in csv_tables:
            date_col = tbl.get('date_col', '')
            if not date_col:
                continue
            date_col_q = quote_ident(date_col, "duckdb")
            bounds = []
            if from_date:
                bounds.append(f"{date_col_q} >= DATE '{from_date}'")
            if to_date:
                bounds.append(f"{date_col_q} <= DATE '{to_date}'")
            if bounds:
                tbl['_date_bounds'] = " AND ".join(bounds)
            tbl['_from_date'] = from_date
            tbl['_to_date'] = to_date

    _discover_and_write_csv_columns(csv_tables, outdir, db_path)

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

    mock_dir = get_mock_dir()
    if mock_dir:
        _extract_mock_tables(csv_tables, outdir, types, mock_dir)
        return

    for t in types:
        _gen(t)
        sql_path = os.path.join(outdir, csv_sql_filename(t))
        run_csv_sql_file(sql_path, outdir, max_workers=max_workers,
                         db_path=db_path, resume=not force)
