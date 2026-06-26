
import csv
import os
from datetime import datetime

from .base import (
    qualified_name,
    build_col_sql,
    build_date_in_clause,
    build_date_range_with_gaps,
    resolve_date_format,
    compute_date_filter,
    load_tables_from_config,
    fill_columns_from_meta,
    inject_where_from_config,
    quote_ident,
    set_cache_db,
    cache_get,
    cache_put,
    _mask_stats_in_rows,
)

# >>> Spark-SQL flavor of athena._athena_portable_hash16 (same 16-bit MD5 recipe, Databricks dialect) <<< #
def _databricks_portable_hash16(value_sql):
    return (
        f"CAST(conv(SUBSTR(LOWER(md5(TRIM({value_sql}))), 1, 4), "
        f"16, 10) AS BIGINT)"
    )

# >>> normalize a conn_macro into "catalog.schema" using DB_DEFAULT_CATALOG when only schema is given <<< #
def _resolve_catalog_schema(conn_macro):
    if not conn_macro:
        return None
    cm = conn_macro.strip()
    if not cm:
        return None
    if "." in cm:
        return cm
    default_catalog = (os.environ.get("DB_DEFAULT_CATALOG") or "").strip()
    if default_catalog:
        return f"{default_catalog}.{cm}"
    return cm

# >>> open a Databricks SQL connection via OAuth + databricks-sdk Config <<< #
def databricks_connect(catalog_schema=None):
    try:
        from databricks import sql as dbx_sql
        from databricks.sdk.core import Config
    except ImportError as e:
        raise ImportError(
            "databricks-sql-connector + databricks-sdk required: "
            "pip install 'dtrack[databricks]'"
        ) from e

    _usr = os.environ.get("AWS_USR")
    _pwd = os.environ.get("AWS_PWD")
    _host = os.environ.get("AWS_HOST")
    if _usr and _pwd and _host:
        os.environ["HTTPS_PROXY"] = f"http://{_usr}:{_pwd}@{_host}:8080"

    profile = os.environ.get("DB_PROFILE")
    http_path = os.environ.get("DB_HTTPS")
    if not profile or not http_path:
        raise RuntimeError(
            "Databricks requires DB_PROFILE and DB_HTTPS env vars "
            "(see .env / `dtrack doctor`)."
        )
    cfg = Config(profile=profile)
    token = cfg.authenticate()["Authorization"].replace("Bearer ", "")
    server_hostname = cfg.host.replace("https://", "").rstrip("/")

    kwargs = {
        "server_hostname": server_hostname,
        "http_path": http_path,
        "access_token": token,
    }
    resolved = _resolve_catalog_schema(catalog_schema)
    if resolved and "." in resolved:
        kwargs["catalog"], kwargs["schema"] = resolved.split(".", 1)
    return dbx_sql.connect(**kwargs)

# >>> run one SQL block and return rows of dicts keyed by lowercase column name <<< #
def _query_databricks(sql, catalog_schema=None):
    cached = cache_get(sql, catalog_schema)
    if cached is not None:
        return cached
    conn = databricks_connect(catalog_schema)
    try:
        cur = conn.cursor()
        out = []
        for stmt in (s.strip() for s in sql.split(";")):
            if not stmt:
                continue
            cur.execute(stmt)
            if cur.description:
                cols = [d[0].lower() for d in cur.description]
                out = [dict(zip(cols, row)) for row in cur.fetchall()]
        cur.close()
        cache_put(sql, catalog_schema, out, row_count=len(out))
        return out
    finally:
        try:
            conn.close()
        except Exception:
            pass

# >>> discover column_name + data_type via DESCRIBE catalog.schema.table (col 0 = name, col 1 = type) <<< #
def _discover_columns_databricks(cursor, catalog, schema, table):
    parts = [p for p in (catalog, schema, table) if p]
    full = ".".join(f"`{p}`" for p in parts)
    cursor.execute(f"DESCRIBE {full}")
    cols = {}
    for row in cursor.fetchall():
        name = (row[0] or "").strip() if row[0] is not None else ""
        if not name or name.startswith("#"):
            break
        cols[name] = (row[1] or "").strip() if len(row) > 1 and row[1] is not None else ""
    return cols

# >>> resolve "schema.table" (catalog is set per-block via USE CATALOG, not qualified inline) <<< #
def _full_table(tbl_cfg):
    cs = _resolve_catalog_schema(tbl_cfg.get("conn_macro"))
    table = tbl_cfg.get("table") or ""
    parts = [p for p in (cs or "").split(".") if p]
    schema = parts[-1] if parts else ""
    out = []
    if schema:
        out.append(quote_ident(schema, "databricks"))
    if table:
        out.append(quote_ident(table, "databricks"))
    return ".".join(out)

# >>> "USE CATALOG `cat`;\nUSE SCHEMA `sch`;" prefix from conn_macro (catalog always backticked) <<< #
def _use_catalog_prefix(conn_macro):
    cs = _resolve_catalog_schema(conn_macro)
    parts = [p for p in (cs or "").split(".") if p]
    lines = []
    if len(parts) >= 2:
        cat = parts[0] if parts[0].startswith("`") else f"`{parts[0]}`"
        lines.append(f"USE CATALOG {cat};")
        lines.append(f"USE SCHEMA {quote_ident(parts[-1], 'databricks')};")
    elif len(parts) == 1:
        lines.append(f"USE SCHEMA {quote_ident(parts[0], 'databricks')};")
    return ("\n".join(lines) + "\n") if lines else ""

# >>> build per-table row-count SQL block <<< #
def _build_row_sql_databricks(tbl_cfg):
    date_col = tbl_cfg["date_col"]
    date_col_q = quote_ident(date_col, "databricks")
    full_table = _full_table(tbl_cfg)
    where = (tbl_cfg.get("where") or "").strip()
    bounds = (tbl_cfg.get("_date_bounds") or "").strip()
    parts = []
    if where:
        parts.append(f"({where})")
    if bounds:
        parts.append(bounds)
    wc = " AND ".join(parts) if parts else "1=1"
    return (
        f"SELECT {date_col_q} AS dt, COUNT(*) AS row_count\n"
        f"FROM {full_table}\n"
        f"WHERE {wc}\n"
        f"GROUP BY {date_col_q}\n"
        f"ORDER BY {date_col_q}"
    )

# >>> aggregate extract_row.sql for every Databricks table — mirrors the Athena/Hadoop shape so previewed = saved <<< #
def _write_combined_row_sql_databricks(databricks_tables, outdir):
    import os as _os
    from datetime import datetime as _dt
    _os.makedirs(outdir, exist_ok=True)
    blocks = [
        "-- dtrack Databricks row extraction queries",
        f"-- Generated: {_dt.now().isoformat()}",
        "",
    ]
    for tbl in databricks_tables:
        qname = qualified_name(tbl)
        conn_macro = tbl.get("conn_macro", "") or ""
        try:
            sql = _build_row_sql_databricks(tbl)
        except Exception as e:
            blocks.append(f"-- {qname}")
            blocks.append(f"-- SKIP: SQL build failed: {e}")
            blocks.append("")
            continue
        blocks.append(f"-- {qname}")
        blocks.append(f"/* @conn_macro={conn_macro} */")
        blocks.append(_use_catalog_prefix(conn_macro) + sql + ";")
        blocks.append("")
    sql_path = _os.path.join(outdir, "extract_row.sql")
    with open(sql_path, "w", encoding="utf-8") as f:
        f.write("\n".join(blocks))
    return sql_path

# >>> compose WHERE + base.build_col_sql dt params (dt_label/vintage) for a table's col window <<< #
def _col_where_and_label_databricks(tbl_cfg, db_path, vintage):
    table_vintage = tbl_cfg.get("vintage", vintage)
    date_filter = compute_date_filter(tbl_cfg, db_path, table_vintage)
    resolve_date_format(date_filter, tbl_cfg)
    date_col = tbl_cfg["date_col"]
    orig_where = (tbl_cfg.get("where") or "").strip()
    bounds = (tbl_cfg.get("_date_bounds") or "").strip()
    eff_dtype = date_filter.get("date_dtype")
    eff_fmt = date_filter.get("date_format")
    buckets = table_vintage in ("week", "month", "quarter", "year")
    if date_filter["filter_type"] == "between":
        dw = build_date_range_with_gaps(
            date_col, date_filter["dates"], eff_dtype, date_format=eff_fmt)
        where = f"({orig_where}) AND {dw}" if orig_where else dw
        dt_label, ret_vintage = (None, table_vintage) if buckets else ("all", "all")
    elif date_filter["filter_type"] == "in_list":
        dw = build_date_in_clause(
            date_col, date_filter["dates"], eff_dtype, date_format=eff_fmt)
        where = f"({orig_where}) AND {dw}" if orig_where else dw
        dt_label, ret_vintage = "sample", "all"
    elif bounds:
        where = f"({orig_where}) AND {bounds}" if orig_where else bounds
        dt_label, ret_vintage = (None, table_vintage) if buckets else ("all", "all")
    else:
        where = orig_where
        dt_label, ret_vintage = (None, table_vintage) if buckets else ("all", "all")
    return where, dt_label, ret_vintage, eff_dtype, eff_fmt

# >>> aggregate extract_col.sql for every Databricks table — one block per (table, column) <<< #
def _write_combined_col_sql_databricks(databricks_tables, outdir, db_path=None,
                                       vintage=None, coarse=False, preset=None,
                                       stats_override=None):
    os.makedirs(outdir, exist_ok=True)
    blocks = [
        "-- dtrack Databricks col extraction queries",
        f"-- Generated: {datetime.now().isoformat()}",
        "",
    ]
    for tbl in databricks_tables:
        qname = qualified_name(tbl)
        conn_macro = tbl.get("conn_macro", "") or ""
        full_table = _full_table(tbl)
        date_col = tbl.get("date_col", "")
        columns = tbl.get("columns", {})
        if not columns:
            blocks.append(f"-- {qname}")
            blocks.append("-- SKIP: no columns defined")
            blocks.append("")
            continue
        non_date_cols = [(c, d) for c, d in columns.items()
                         if c.lower() != date_col.lower()]
        if not non_date_cols:
            blocks.append(f"-- {qname}")
            blocks.append("-- SKIP: no non-date columns")
            blocks.append("")
            continue
        try:
            where, dt_label, vtg, dtype_d, fmt_d = _col_where_and_label_databricks(tbl, db_path, vintage)
        except Exception as e:
            blocks.append(f"-- {qname}")
            blocks.append(f"-- SKIP: date filter build failed: {e}")
            blocks.append("")
            continue
        tbl_sql = dict(tbl)
        tbl_sql["where"] = where
        for col, dtype in non_date_cols:
            try:
                sql, _ = build_col_sql("databricks", tbl_sql, col, dtype, full_table,
                                       vintage=vtg, date_dtype=dtype_d, date_format=fmt_d,
                                       dt_label=dt_label, preset=preset,
                                       stats_override=stats_override)
            except Exception as e:
                blocks.append(f"-- {qname}/{col}")
                blocks.append(f"-- SKIP: SQL build failed: {e}")
                blocks.append("")
                continue
            blocks.append(f"-- {qname}/{col}")
            blocks.append(f"/* @conn_macro={conn_macro} */")
            blocks.append(_use_catalog_prefix(conn_macro) + sql.strip() + ";")
            blocks.append("")
    sql_path = os.path.join(outdir, "extract_col.sql")
    with open(sql_path, "w", encoding="utf-8") as f:
        f.write("\n".join(blocks))
    return sql_path

# >>> parse + execute extract_{row,col}.sql via databricks_connect (gen-then-run, mirrors run_hadoop_sql_file) <<< #
def run_databricks_sql_file(sql_path, outdir, max_workers=None, db_path=None,
                            on_progress=None, resume=False, cancel_event=None,
                            preset=None, stats_override=None):
    from .base import run_extract_sql_file

    def _qf(sql, conn_macro):
        rows = _query_databricks(sql, conn_macro)
        if preset is not None or stats_override:
            _mask_stats_in_rows(rows, preset, stats_override)
        return rows

    set_cache_db(os.path.join(outdir, ".cache.db"))
    try:
        return run_extract_sql_file(
            sql_path, outdir,
            query_fn=_qf,
            platform="databricks",
            max_workers=max_workers,
            on_progress=on_progress,
            cancel_event=cancel_event,
        )
    finally:
        set_cache_db(None)

# >>> discover columns (DESCRIBE) for any databricks table missing them; seed _column_meta + columns.csv <<< #
def _ensure_columns_databricks(databricks_tables, outdir, db_path):
    from ..db import insert_column_meta, get_column_meta
    cursors_by_cs = {}
    try:
        for tbl in databricks_tables:
            if tbl.get("columns"):
                continue
            qname = qualified_name(tbl)
            raw_table = tbl.get("table", "")
            cs = _resolve_catalog_schema(tbl.get("conn_macro"))
            cols = None
            if db_path:
                existing = get_column_meta(db_path, qname)
                if existing:
                    cols = {c["column_name"]: c.get("data_type", "") for c in existing}
            if cols is None:
                if not cs or "." not in cs:
                    print(f"  {qname}: SKIP discovery — conn_macro is not catalog.schema")
                    continue
                catalog, schema = cs.split(".", 1)
                if cs not in cursors_by_cs:
                    print(f"  {qname}: connecting to Databricks ({cs})...")
                    conn = databricks_connect(cs)
                    cursors_by_cs[cs] = (conn, conn.cursor())
                _, cur = cursors_by_cs[cs]
                cols = _discover_columns_databricks(cur, catalog, schema, raw_table)
                if cols and db_path:
                    insert_column_meta(db_path, qname, cols, source="databricks")
                    print(f"  {qname}: discovered {len(cols)} columns from Databricks")
            if cols:
                tbl["columns"] = dict(cols)
                col_csv = os.path.join(outdir, f"{qname}_columns.csv")
                with open(col_csv, "w", newline="") as f:
                    w = csv.writer(f)
                    w.writerow(["COLUMN_NAME", "DATA_TYPE"])
                    for cn, ct in sorted(cols.items()):
                        w.writerow([cn, ct])
    finally:
        for cs, (conn, cur) in cursors_by_cs.items():
            try:
                cur.close()
                conn.close()
            except Exception:
                pass

# >>> top-level entry: extract row + col CSVs for every databricks-source table <<< #
def extract_databricks(config_path, outdir, types=None, max_workers=None,
                       db_path=None, vintage=None, force=False,
                       from_date=None, to_date=None,
                       preset=None, stats_override=None, gen_only=False):
    if types is None:
        types = ["row", "col"]

    mock_dir = (os.environ.get("DTRACK_MOCK")
                or os.environ.get("DTRACK_DATABRICKS_MOCK"))

    from ..config import load_unified_config
    config = load_unified_config(config_path)
    if db_path:
        from ..db import get_pair_col_map_from_db, get_pair_col_filter
        for pname, pcfg in config.get("pairs", {}).items():
            if not pcfg.get("col_map"):
                m = get_pair_col_map_from_db(db_path, pname)
                if m:
                    pcfg["col_map"] = m
            if not pcfg.get("col_filter"):
                cf = get_pair_col_filter(db_path, pname)
                if cf.get("include") or cf.get("exclude"):
                    pcfg["col_filter"] = cf

    os.makedirs(outdir, exist_ok=True)
    all_tables = load_tables_from_config(config)
    dbx_tables = [t for t in all_tables
                  if (t.get("source") or "").lower() == "databricks"]

    if not dbx_tables:
        print("No Databricks tables found in config")
        return

    for tbl in dbx_tables:
        if vintage:
            tbl["vintage"] = vintage
        elif "vintage" not in tbl:
            tbl["vintage"] = "all"
    inject_where_from_config(dbx_tables, config)

    if from_date or to_date:
        for tbl in dbx_tables:
            date_col = tbl.get("date_col", "")
            if not date_col:
                continue
            date_col_q = quote_ident(date_col, "databricks")
            date_type = (tbl.get("date_type") or "").lower()
            from .athena import _format_athena_date_bound
            bounds = []
            if from_date:
                bounds.append(f"{date_col_q} >= "
                              f"{_format_athena_date_bound(from_date, date_type, False)}")
            if to_date:
                bounds.append(f"{date_col_q} <= "
                              f"{_format_athena_date_bound(to_date, date_type, True)}")
            if bounds:
                tbl["_date_bounds"] = " AND ".join(bounds)
            tbl["_from_date"] = from_date
            tbl["_to_date"] = to_date

    if gen_only:
        from .base import tag_generated_sql
        if "row" in types:
            _write_combined_row_sql_databricks(dbx_tables, outdir)
            tag_generated_sql(outdir, "row", "databricks")
        if "col" in types:
            if db_path:
                fill_columns_from_meta(dbx_tables, db_path)
            _write_combined_col_sql_databricks(dbx_tables, outdir, db_path=db_path,
                                               vintage=vintage, preset=preset,
                                               stats_override=stats_override)
            tag_generated_sql(outdir, "col", "databricks")
        print(f"[gen-only] wrote Databricks extract SQL to {outdir} (no extraction)")
        return

    if mock_dir:
        from .base import _extract_mock_tables
        _extract_mock_tables(dbx_tables, outdir, types, mock_dir)
        return

    if "col" in types:
        if db_path:
            fill_columns_from_meta(dbx_tables, db_path)
        _ensure_columns_databricks(dbx_tables, outdir, db_path)

    if "row" in types:
        print(f"\n--- Databricks row extraction ({len(dbx_tables)} tables) ---")
        row_path = _write_combined_row_sql_databricks(dbx_tables, outdir)
        run_databricks_sql_file(row_path, outdir, max_workers=max_workers,
                                db_path=db_path)
    if "col" in types:
        print(f"\n--- Databricks col extraction ({len(dbx_tables)} tables) ---")
        col_path = _write_combined_col_sql_databricks(
            dbx_tables, outdir, db_path=db_path, vintage=vintage,
            preset=preset, stats_override=stats_override)
        run_databricks_sql_file(col_path, outdir, max_workers=max_workers,
                                db_path=db_path, preset=preset,
                                stats_override=stats_override)

    print(f"\nExtraction complete. Output in: {outdir}")
