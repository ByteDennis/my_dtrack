"""Databricks SQL extraction platform.

Mirrors the shape of athena.py: row + col extraction into per-qname CSV
files matching the 14-field schema, with mock-mode fallback via
_extract_mock_tables. The conn_macro slot holds the catalog.schema pair
(dot-separated); the per-table `table` field is the unqualified leaf,
giving `{conn_macro}.{table}` as the full reference. When conn_macro
omits the dot (just a schema), the DB_DEFAULT_CATALOG env var is
prepended so `{DB_DEFAULT_CATALOG}.{conn_macro}.{table}` is what hits
Databricks. An empty/missing conn_macro falls through to the
warehouse's default catalog.schema.

Real-mode auth is OAuth via databricks.sdk Config + the DB_PROFILE /
DB_HTTPS env vars (loaded from .env). The workspace sits behind the same
corporate proxy as Athena, so databricks_connect sets HTTPS_PROXY from
AWS_USR / AWS_PWD / AWS_HOST -- but it does NOT run the full
aws_creds_renew() AWS token dance, which Databricks doesn't need and whose
TTL-gated proxy mutation otherwise made runs flaky.

Col extraction defers to the Athena SQL builder (Spark SQL on Databricks
shares enough of Trino's dialect -- date_parse/date_trunc/SUBSTR/
approx_count_distinct -- that the same per-column template runs without
edits). If a dialect-specific divergence shows up, override the SQL by
passing a vintage_transform on the pair.
"""

import csv
import os
from datetime import datetime

from .base import (
    qualified_name,
    is_numeric_type,
    build_date_in_clause,
    build_date_range_with_gaps,
    resolve_date_format,
    compute_date_filter,
    load_tables_from_config,
    fill_columns_from_meta,
    inject_where_from_config,
    quote_ident,
    apply_null_sentinels,
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

    # Databricks needs only two things from the environment: DB_PROFILE +
    # DB_HTTPS (loaded from .env), and the corporate HTTPS proxy. It does NOT
    # need the AWS token renewal that aws_creds_renew() performs — only the
    # proxy that function also sets. Calling the full aws_creds_renew() routed
    # Databricks through the AWS token dance and (intermittently, because it's
    # TTL-gated) was the reason a chunk could run in the playground yet fail
    # from the col_gen runner. So set just the proxy here, the same way
    # aws_creds_renew() does, from AWS_USR / AWS_PWD / AWS_HOST when present.
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
    # Consult the shared SQL-result cache first (set by run_databricks_sql_file)
    # so re-running the same extract_*.sql skips blocks that already succeeded
    # — mirrors the Oracle / Hadoop runners' resume behavior.
    cached = cache_get(sql, catalog_schema)
    if cached is not None:
        return cached
    conn = databricks_connect(catalog_schema)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        cols = [d[0].lower() for d in (cur.description or [])]
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
    # Backtick-quote each part: Spark identifiers can't start with a digit
    # (e.g. "062108..._catalog") and DESCRIBE needs a valid 3-part reference.
    parts = [p for p in (catalog, schema, table) if p]
    full = ".".join(f"`{p}`" for p in parts)
    cursor.execute(f"DESCRIBE {full}")
    cols = {}
    for row in cursor.fetchall():
        name = (row[0] or "").strip() if row[0] is not None else ""
        # Spark's DESCRIBE appends a blank row then a "# Partition Information"
        # / "# col_name" section for partitioned tables -- stop at the first
        # blank or comment marker so partition keys aren't double-counted.
        if not name or name.startswith("#"):
            break
        cols[name] = (row[1] or "").strip() if len(row) > 1 and row[1] is not None else ""
    return cols


# >>> resolve "catalog.schema.table"; backtick only the parts that aren't valid bare Spark identifiers <<< #
def _full_table(tbl_cfg):
    cs = _resolve_catalog_schema(tbl_cfg.get("conn_macro"))
    table = tbl_cfg.get("table") or ""
    parts = []
    if cs:
        parts.extend(p for p in cs.split(".") if p)
    if table:
        parts.append(table)
    # quote_ident leaves `main`/`warehouse`/`trades` bare but backticks a
    # digit-leading catalog like `062108..._catalog` so the FROM clause is valid.
    return ".".join(quote_ident(p, "databricks") for p in parts)


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
        blocks.append(sql + ";")
        blocks.append("")
    sql_path = _os.path.join(outdir, "extract_row.sql")
    with open(sql_path, "w", encoding="utf-8") as f:
        f.write("\n".join(blocks))
    return sql_path


# >>> per-column stats SQL for one Databricks column — emits all 14 COL_CSV_HEADERS columns <<< #
def _build_col_sql_databricks(col, dtype, full_table, where, dt_label="all"):
    numeric = is_numeric_type(dtype, is_oracle=False)
    col_q = quote_ident(col, "databricks")
    wc = (where or "").strip() or "(1=1)"
    col_lit = col.replace("'", "''")
    dt_lit = (dt_label or "all").replace("'", "''")
    if numeric:
        ce = apply_null_sentinels(col_q, dtype or "numeric", "databricks")
        return (
            f"SELECT '{dt_lit}' AS dt, '{col_lit}' AS column_name, "
            f"'numeric' AS col_type, "
            f"COUNT(*) AS n_total, COUNT(*) - COUNT({ce}) AS n_missing, "
            f"COUNT(DISTINCT {ce}) AS n_unique, "
            f"AVG(CAST({ce} AS DOUBLE)) AS mean, "
            f"STDDEV_SAMP(CAST({ce} AS DOUBLE)) AS std, "
            f"CAST(MIN({ce}) AS STRING) AS min_val, "
            f"CAST(MAX({ce}) AS STRING) AS max_val, "
            f"CAST(NULL AS STRING) AS top_10, "
            f"CAST(NULL AS STRING) AS length_avg, "
            f"CAST(NULL AS STRING) AS length_max, "
            f"CAST(NULL AS STRING) AS hash_sum\n"
            f"FROM {full_table} WHERE {wc}")
    ce = apply_null_sentinels(col_q, dtype or "categorical", "databricks")
    return (
        f"SELECT '{dt_lit}' AS dt, '{col_lit}' AS column_name, "
        f"'categorical' AS col_type, "
        f"COUNT(*) AS n_total, COUNT(*) - COUNT({ce}) AS n_missing, "
        f"COUNT(DISTINCT {ce}) AS n_unique, "
        f"CAST(NULL AS STRING) AS mean, CAST(NULL AS STRING) AS std, "
        f"MIN(CAST({ce} AS STRING)) AS min_val, "
        f"MAX(CAST({ce} AS STRING)) AS max_val, "
        f"CAST(NULL AS STRING) AS top_10, "
        f"CAST(AVG(LENGTH(CAST({ce} AS STRING))) AS STRING) AS length_avg, "
        f"CAST(MAX(LENGTH(CAST({ce} AS STRING))) AS STRING) AS length_max, "
        f"CAST(NULL AS STRING) AS hash_sum\n"
        f"FROM {full_table} WHERE {wc}")


# >>> compose the effective WHERE + dt label for a table's col extraction window <<< #
def _col_where_and_label_databricks(tbl_cfg, db_path, vintage):
    table_vintage = tbl_cfg.get("vintage", vintage)
    date_filter = compute_date_filter(tbl_cfg, db_path, table_vintage)
    resolve_date_format(date_filter, tbl_cfg)
    date_col = tbl_cfg["date_col"]
    orig_where = (tbl_cfg.get("where") or "").strip()
    bounds = (tbl_cfg.get("_date_bounds") or "").strip()
    eff_dtype = date_filter.get("date_dtype")
    eff_fmt = date_filter.get("date_format")
    dt_label = "all"
    if date_filter["filter_type"] == "between":
        dw = build_date_range_with_gaps(
            date_col, date_filter["dates"], eff_dtype, date_format=eff_fmt)
        where = f"({orig_where}) AND {dw}" if orig_where else dw
    elif date_filter["filter_type"] == "in_list":
        dw = build_date_in_clause(
            date_col, date_filter["dates"], eff_dtype, date_format=eff_fmt)
        where = f"({orig_where}) AND {dw}" if orig_where else dw
        dt_label = "sample"
    elif bounds:
        where = f"({orig_where}) AND {bounds}" if orig_where else bounds
    else:
        where = orig_where
    return where, dt_label


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
            where, dt_label = _col_where_and_label_databricks(tbl, db_path, vintage)
        except Exception as e:
            blocks.append(f"-- {qname}")
            blocks.append(f"-- SKIP: date filter build failed: {e}")
            blocks.append("")
            continue
        for col, dtype in non_date_cols:
            try:
                sql = _build_col_sql_databricks(col, dtype, full_table, where, dt_label)
            except Exception as e:
                blocks.append(f"-- {qname}/{col}")
                blocks.append(f"-- SKIP: SQL build failed: {e}")
                blocks.append("")
                continue
            blocks.append(f"-- {qname}/{col}")
            blocks.append(f"/* @conn_macro={conn_macro} */")
            blocks.append(sql + ";")
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
                       preset=None, stats_override=None):
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

    if mock_dir:
        from .base import _extract_mock_tables
        _extract_mock_tables(dbx_tables, outdir, types, mock_dir)
        return

    # Gen-then-run: write the same aggregate extract_{row,col}.sql the other
    # platforms produce, then execute the saved file block-by-block. This makes
    # what's previewed == what's saved == what's run (no in-memory rebuild),
    # matching Athena / Hadoop / Oracle.
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
