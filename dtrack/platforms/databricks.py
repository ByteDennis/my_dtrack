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

Real-mode auth re-uses the corporate AWS credential renewer
(aws_creds_renew) before opening a databricks.sql connection -- the
workspace sits behind the same proxy as Athena. OAuth is handled via
databricks.sdk Config + the DB_PROFILE / DB_HTTPS env vars.

Col extraction defers to the Athena SQL builder (Spark SQL on Databricks
shares enough of Trino's dialect -- date_parse/date_trunc/SUBSTR/
approx_count_distinct -- that the same per-column template runs without
edits). If a dialect-specific divergence shows up, override the SQL by
passing a vintage_transform on the pair.
"""

import csv
import os
import threading
from datetime import datetime

from .base import (
    qualified_name,
    is_numeric_type,
    resolve_table,
    build_date_in_clause,
    build_date_range_with_gaps,
    resolve_date_format,
    compute_date_filter,
    load_tables_from_config,
    fill_columns_from_meta,
    inject_where_from_config,
    quote_ident,
    set_cache_db,
    _mask_stats_in_rows,
)


_sql_log_file = None
_sql_log_lock = threading.Lock()


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

    from .athena import aws_creds_renew
    aws_creds_renew()

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


# >>> append `sql` to the per-qname log file used by _extract_cols_for_table_databricks <<< #
def _log_sql(label, sql):
    if not _sql_log_file:
        return
    with _sql_log_lock:
        with open(_sql_log_file, "a", encoding="utf-8") as f:
            f.write(f"\n-- {label}\n{sql.rstrip()};\n")


# >>> run one SQL block and return rows of dicts keyed by lowercase column name <<< #
def _query_databricks(sql, catalog_schema=None):
    conn = databricks_connect(catalog_schema)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        cols = [d[0].lower() for d in (cur.description or [])]
        out = [dict(zip(cols, row)) for row in cur.fetchall()]
        cur.close()
        return out
    finally:
        try:
            conn.close()
        except Exception:
            pass


# >>> discover column_name + data_type from information_schema <<< #
def _discover_columns_databricks(cursor, catalog, schema, table):
    # >>> backtick-quote the catalog because Spark identifiers can't start with a digit (e.g. "062108..._catalog") <<< #
    cat_q = f"`{catalog}`" if catalog else catalog
    sql = f"""
        SELECT column_name, data_type
        FROM {cat_q}.information_schema.columns
        WHERE table_schema = '{schema}' AND table_name = '{table}'
        ORDER BY ordinal_position
    """
    cursor.execute(sql)
    return {row[0]: row[1] for row in cursor.fetchall()}


# >>> resolve "catalog.schema.table" with column quoting for SQL emission <<< #
def _full_table(tbl_cfg):
    cs = _resolve_catalog_schema(tbl_cfg.get("conn_macro"))
    table = tbl_cfg.get("table") or ""
    return f"{cs}.{table}" if cs else table


# >>> build per-table row-count SQL block <<< #
def _build_row_sql_databricks(tbl_cfg):
    date_col = tbl_cfg["date_col"]
    date_col_q = quote_ident(date_col, "athena")
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
        f"SELECT {date_col_q} AS date_value, COUNT(*) AS row_count\n"
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


# >>> extract row counts for one Databricks table to {qname}_row.csv <<< #
def _extract_rows_for_table_databricks(tbl_cfg, outdir):
    qname = qualified_name(tbl_cfg)
    sql = _build_row_sql_databricks(tbl_cfg)

    rows = _query_databricks(sql, tbl_cfg.get("conn_macro"))

    csv_path = os.path.join(outdir, f"{qname}_row.csv")
    with open(csv_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date_value", "row_count"])
        for r in rows:
            w.writerow([r.get("date_value"), r.get("row_count")])
    print(f"  Row counts: {csv_path} ({len(rows)} dates)")


# >>> extract per-column stats for one Databricks table -> {qname}_col.csv <<< #
def _extract_cols_for_table_databricks(tbl_cfg, outdir, max_workers=None,
                                       db_path=None, vintage=None,
                                       preset=None, stats_override=None):
    from .athena import _extract_col_athena
    from concurrent.futures import ThreadPoolExecutor, as_completed

    name = tbl_cfg["name"]
    date_col = tbl_cfg["date_col"]
    full_table = _full_table(tbl_cfg)
    cte_prefix = ""

    columns = tbl_cfg.get("columns", {})
    if not columns:
        print(f"  WARNING: No columns specified for {name}. "
              f"Use 'dtrack load-columns' or provide columns in config.")
        return None

    non_date_cols = [(c, d) for c, d in columns.items()
                     if c.lower() != date_col.lower()]

    table_vintage = tbl_cfg.get("vintage", vintage)
    date_filter = compute_date_filter(tbl_cfg, db_path, table_vintage)
    resolve_date_format(date_filter, tbl_cfg)

    qname = qualified_name(tbl_cfg)
    log_path = os.path.join(outdir, f"{qname}_col.sql")
    with open(log_path, "w", encoding="utf-8") as f:
        f.write(f"-- SQL queries for {qname} col extraction\n"
                f"-- {datetime.now().isoformat()}\n\n")
    global _sql_log_file
    _sql_log_file = log_path

    set_cache_db(os.path.join(outdir, ".cache.db"))
    start = datetime.now()

    extract_cfg = dict(tbl_cfg)
    orig_where = (tbl_cfg.get("where") or "").strip()
    bounds = (tbl_cfg.get("_date_bounds") or "").strip()
    eff_vintage = date_filter.get("vintage", "all")
    eff_dtype = date_filter.get("date_dtype")
    eff_fmt = date_filter.get("date_format")

    if date_filter["filter_type"] == "between":
        dw = build_date_range_with_gaps(
            date_col, date_filter["dates"], eff_dtype, date_format=eff_fmt)
        extract_cfg["where"] = f"({orig_where}) AND {dw}" if orig_where else dw
    elif date_filter["filter_type"] == "in_list":
        dw = build_date_in_clause(
            date_col, date_filter["dates"], eff_dtype, date_format=eff_fmt)
        extract_cfg["where"] = f"({orig_where}) AND {dw}" if orig_where else dw
        eff_vintage = "all"; eff_dtype = None
    elif bounds:
        extract_cfg["where"] = f"({orig_where}) AND {bounds}" if orig_where else bounds

    dt_label = "sample" if date_filter["filter_type"] == "in_list" else None

    workers = max_workers or int(os.environ.get("DATABRICKS_MAX_WORKERS") or 4)
    workers = max(1, min(workers, len(non_date_cols)))

    all_stats = []

    def _one(col_dtype):
        c, dt = col_dtype
        return _extract_col_athena(
            extract_cfg, c, dt, full_table, cte_prefix,
            database=tbl_cfg.get("conn_macro"),
            vintage=eff_vintage, date_dtype=eff_dtype,
            date_format=eff_fmt, dt_label=dt_label,
            _query=lambda s, data_base=None: _query_databricks(
                s, data_base or tbl_cfg.get("conn_macro")),
        ) if _extract_col_athena_accepts_query() else _query_databricks_fallback(
            extract_cfg, c, dt, full_table, eff_vintage, eff_dtype, eff_fmt,
            dt_label, tbl_cfg.get("conn_macro"))

    if workers <= 1:
        for cd in non_date_cols:
            try:
                all_stats.extend(_one(cd))
            except Exception as e:
                print(f"  Warning: Failed to extract {cd[0]}: {e}")
    else:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(_one, cd): cd[0] for cd in non_date_cols}
            for fut in as_completed(futs):
                try:
                    all_stats.extend(fut.result())
                except Exception as e:
                    print(f"  Warning: Failed to extract {futs[fut]}: {e}")

    _sql_log_file = None
    set_cache_db(None)
    elapsed = (datetime.now() - start).total_seconds()

    if not all_stats:
        return None

    _mask_stats_in_rows(all_stats, preset, stats_override)

    csv_path = os.path.join(outdir, f"{qname}_col.csv")
    fieldnames = [
        "column_name", "dt", "col_type", "n_total", "n_missing",
        "n_unique", "mean", "std", "min_val", "max_val", "top_10",
        "length_avg", "length_max", "hash_sum",
    ]
    with open(csv_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for s in sorted(all_stats, key=lambda x: (x["column_name"], x["dt"])):
            w.writerow({k: s.get(k, "") for k in fieldnames})

    print(f"  Column stats: {csv_path} ({len(all_stats)} rows, {elapsed:.1f}s)")
    return {"table": name, "step": "col",
            "start": start.isoformat(), "end": "",
            "elapsed_sec": elapsed}


# >>> probe whether the linked _extract_col_athena exposes a _query hook <<< #
def _extract_col_athena_accepts_query():
    from inspect import signature
    from .athena import _extract_col_athena
    return "_query" in signature(_extract_col_athena).parameters


# >>> fallback shim when _extract_col_athena cannot be cleanly reused <<< #
def _query_databricks_fallback(extract_cfg, col, dtype, full_table,
                               vintage, date_dtype, date_format, dt_label,
                               catalog_schema):
    from .base import apply_null_sentinels
    numeric = is_numeric_type(dtype, is_oracle=False)
    col_q = quote_ident(col, "athena")
    where = (extract_cfg.get("where") or "").strip() or "(1=1)"
    if dt_label is not None:
        dt_select = f"'{dt_label}' AS dt"
        group_by = ""
    elif vintage == "all" or not vintage or vintage == "day":
        dt_select = "'all' AS dt"
        group_by = ""
    else:
        dt_select = "'all' AS dt"
        group_by = ""
    if numeric:
        ce = apply_null_sentinels(col_q, dtype or "numeric", "athena")
        sql = (f"SELECT {dt_select}, '{dtype}' AS col_type, "
               f"COUNT(*) AS n_total, COUNT(*) - COUNT({ce}) AS n_missing, "
               f"COUNT(DISTINCT {ce}) AS n_unique, AVG(CAST({ce} AS DOUBLE)) AS mean, "
               f"STDDEV_SAMP(CAST({ce} AS DOUBLE)) AS std, "
               f"MIN({ce}) AS min_val, MAX({ce}) AS max_val "
               f"FROM {full_table} WHERE {where}{group_by}")
    else:
        ce = apply_null_sentinels(col_q, dtype or "categorical", "athena")
        sql = (f"SELECT {dt_select}, '{dtype}' AS col_type, "
               f"COUNT(*) AS n_total, COUNT(*) - COUNT({ce}) AS n_missing, "
               f"COUNT(DISTINCT {ce}) AS n_unique, "
               f"CAST(NULL AS DOUBLE) AS mean, CAST(NULL AS DOUBLE) AS std, "
               f"MIN(CAST({ce} AS STRING)) AS min_val, "
               f"MAX(CAST({ce} AS STRING)) AS max_val, "
               f"AVG(LENGTH(CAST({ce} AS STRING))) AS length_avg, "
               f"MAX(LENGTH(CAST({ce} AS STRING))) AS length_max "
               f"FROM {full_table} WHERE {where}{group_by}")
    _log_sql(col, sql)
    rows = _query_databricks(sql, catalog_schema)
    out = []
    for r in rows:
        out.append({
            "dt": str(r.get("dt", "all")),
            "column_name": col,
            "col_type": "numeric" if numeric else "categorical",
            "n_total": str(r.get("n_total") or 0),
            "n_missing": str(r.get("n_missing") or 0),
            "n_unique": str(r.get("n_unique") or 0),
            "mean": "" if r.get("mean") is None else str(r["mean"]),
            "std": "" if r.get("std") is None else str(r["std"]),
            "min_val": "" if r.get("min_val") is None else str(r["min_val"]),
            "max_val": "" if r.get("max_val") is None else str(r["max_val"]),
            "top_10": "",
            "length_avg": "" if r.get("length_avg") is None else str(r["length_avg"]),
            "length_max": "" if r.get("length_max") is None else str(r["length_max"]),
            "hash_sum": "",
        })
    return out


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
            date_col_q = quote_ident(date_col, "athena")
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

    if db_path and "col" in types:
        fill_columns_from_meta(dbx_tables, db_path)

    timing = []
    for tbl_cfg in dbx_tables:
        name = tbl_cfg["name"]
        print(f"\nExtracting: {name} ({_full_table(tbl_cfg)})")
        if "row" in types:
            try:
                _extract_rows_for_table_databricks(tbl_cfg, outdir)
            except Exception as e:
                print(f"  ERROR row extraction for {name}: {e}")
                timing.append({"table": name, "step": "row", "status": str(e)})
        if "col" in types:
            try:
                t = _extract_cols_for_table_databricks(
                    tbl_cfg, outdir, max_workers=max_workers,
                    db_path=db_path, vintage=vintage,
                    preset=preset, stats_override=stats_override)
                if t:
                    timing.append(t)
            except NotImplementedError as e:
                print(f"  SKIP col for {name}: {e}")
            except Exception as e:
                print(f"  ERROR col extraction for {name}: {e}")

    if timing:
        timing_path = os.path.join(outdir, "_timing.csv")
        with open(timing_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["table", "step", "start",
                                              "end", "elapsed_sec", "status"])
            w.writeheader()
            for r in timing:
                w.writerow({k: r.get(k, "") for k in
                            ("table", "step", "start", "end",
                             "elapsed_sec", "status")})
        print(f"\nTiming log: {timing_path}")

    print(f"\nExtraction complete. Output in: {outdir}")
