
import csv
import os
import threading
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
    apply_pair_date_bounds,
    set_cache_db,
    cache_db_path,
    cache_get,
    cache_put,
    statement_id_get,
    statement_id_put,
    statement_id_delete,
    _mask_stats_in_rows,
)

# >>> int env knob with a safe default; shared by the Databricks resilience knobs <<< #
def _env_int(name, default):
    try:
        return int(os.environ.get(name, str(default)))
    except ValueError:
        return default

# >>> per-request socket timeout (s); a stuck poll raises instead of hanging <<< #
def _dbx_socket_timeout():
    return _env_int("DTRACK_DATABRICKS_SOCKET_TIMEOUT", 600)

# >>> server-side + poll deadline (s); 0 disables <<< #
def _dbx_timeout():
    return _env_int("DTRACK_DATABRICKS_TIMEOUT", 1800)

# >>> transient-failure retry budget for REST calls / link fetches <<< #
def _dbx_retries():
    return _env_int("DTRACK_DATABRICKS_RETRIES", 2)

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

_DBX_AUTH_LOCK = threading.Lock()
_DBX_AUTH = None  # cached (server_hostname, http_path, token); set once, thread-safe

# >>> authenticate ONCE (proxy env + OAuth token); cached + locked so concurrent <<< #
# >>> col workers don't race on os.environ / cfg.authenticate() (which can hang) <<< #
def _databricks_auth(force=False):
    global _DBX_AUTH
    with _DBX_AUTH_LOCK:
        if force:
            _DBX_AUTH = None
        if _DBX_AUTH is not None:
            return _DBX_AUTH
        from databricks.sdk.core import Config
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
        _DBX_AUTH = (server_hostname, http_path, token)
        return _DBX_AUTH

# >>> open a Databricks SQL connection via OAuth + databricks-sdk Config <<< #
def databricks_connect(catalog_schema=None):
    try:
        from databricks import sql as dbx_sql
    except ImportError as e:
        raise ImportError(
            "databricks-sql-connector + databricks-sdk required: "
            "pip install 'dtrack[databricks]'"
        ) from e

    server_hostname, http_path, token = _databricks_auth()
    kwargs = {
        "server_hostname": server_hostname,
        "http_path": http_path,
        "access_token": token,
    }
    # socket timeout so a submitted statement whose poll stops responding
    # raises instead of hanging forever (the run can then reconnect/retry).
    sock = _dbx_socket_timeout()
    if sock > 0:
        kwargs["_socket_timeout"] = sock
    resolved = _resolve_catalog_schema(catalog_schema)
    if resolved and "." in resolved:
        kwargs["catalog"], kwargs["schema"] = resolved.split(".", 1)
    conn = dbx_sql.connect(**kwargs)
    # server-side query timeout so a long/stuck statement fails instead of
    # hanging the run forever. DTRACK_DATABRICKS_TIMEOUT seconds (0 disables).
    secs = _dbx_timeout()
    if secs > 0:
        try:
            c = conn.cursor()
            c.execute(f"SET STATEMENT_TIMEOUT = {secs}")
            c.close()
        except Exception as e:
            from ..logmgr import log_exc
            log_exc("databricks: SET STATEMENT_TIMEOUT failed", e, level="debug")
    return conn

# >>> run one SQL block and return rows of dicts keyed by lowercase column name <<< #
# >>> split on ';' but NOT inside single-quoted string literals (top_10 uses '; ') <<< #
def _split_statements(sql):
    out, buf, in_str = [], [], False
    i, n = 0, len(sql)
    while i < n:
        ch = sql[i]
        if ch == "'":
            buf.append(ch)
            if in_str and i + 1 < n and sql[i + 1] == "'":
                buf.append(sql[i + 1])
                i += 2
                continue
            in_str = not in_str
        elif ch == ";" and not in_str:
            out.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
        i += 1
    if buf:
        out.append("".join(buf))
    return [s.strip() for s in out if s.strip()]


# >>> pull the SQL-warehouse id out of an http_path like /sql/1.0/warehouses/<id> <<< #
def _warehouse_id_from_http_path(http_path):
    import re
    m = re.search(r"warehouses/([0-9a-zA-Z]+)", http_path or "")
    return m.group(1) if m else None

_DBX_NUMERIC_TYPES = {
    "BYTE", "SHORT", "INT", "INTEGER", "LONG", "BIGINT", "SMALLINT", "TINYINT",
}
_DBX_FLOAT_TYPES = {"FLOAT", "DOUBLE", "REAL", "DECIMAL"}

# >>> coerce a JSON_ARRAY string cell to int/float per the manifest column type <<< #
def _dbx_coerce(value, type_name):
    if value is None:
        return None
    t = (type_name or "").upper()
    try:
        if t in _DBX_NUMERIC_TYPES:
            return int(value)
        if t in _DBX_FLOAT_TYPES:
            return float(value)
    except (ValueError, TypeError):
        return value
    return value

# >>> one Databricks REST call (statements API); returns parsed JSON or raises <<< #
# >>> one REST call; token fetched (cached) per call, refreshed on 401/403, retried on transient errors <<< #
def _rest_request(method, host, path, body=None):
    import json
    import time
    import urllib.error
    import urllib.request
    url = f"https://{host}{path}"
    data = json.dumps(body).encode() if body is not None else None
    timeout = _dbx_socket_timeout()
    tries = _dbx_retries()
    from ..logmgr import log_exc
    last = None
    for attempt in range(tries + 1):
        token = _databricks_auth()[2]
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Authorization", f"Bearer {token}")
        req.add_header("Content-Type", "application/json")
        req.add_header("User-Agent", "data-reconciliation")
        try:
            with urllib.request.urlopen(req, timeout=max(timeout, 60)) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            last = e
            if e.code in (401, 403):
                _databricks_auth(force=True)          # expired/invalid token -> refresh
            elif e.code not in (429, 500, 502, 503, 504):
                raise
            log_exc(f"databricks REST {method} {path} HTTP {e.code} "
                    f"(attempt {attempt + 1}/{tries + 1})", e, level="warning")
        except urllib.error.URLError as e:
            last = e
            log_exc(f"databricks REST {method} {path} network error "
                    f"(attempt {attempt + 1}/{tries + 1})", e, level="warning")
        if attempt < tries:
            time.sleep(2 ** attempt)
    raise last

# >>> default result disposition; EXTERNAL_LINKS avoids the 25 MiB INLINE cap on big pulls <<< #
def _dbx_disposition():
    d = (os.environ.get("DTRACK_DATABRICKS_DISPOSITION") or "EXTERNAL_LINKS").strip().upper()
    return "INLINE" if d == "INLINE" else "EXTERNAL_LINKS"

# >>> fetch one EXTERNAL_LINKS presigned chunk (plain GET, NO auth header) -> JSON_ARRAY rows <<< #
def _fetch_external_link(url):
    import json
    import time
    import urllib.error
    import urllib.request
    timeout = _dbx_socket_timeout()
    tries = _dbx_retries()
    from ..logmgr import log_exc
    last = None
    for attempt in range(tries + 1):
        try:
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(req, timeout=max(timeout, 60)) as resp:
                return json.loads(resp.read().decode())
        except (urllib.error.URLError, urllib.error.HTTPError) as e:
            last = e
            log_exc(f"databricks external_link fetch failed "
                    f"(attempt {attempt + 1}/{tries + 1})", e, level="warning")
            if attempt < tries:
                time.sleep(2 ** attempt)
    raise last

# >>> stream every result chunk (inline data_array or EXTERNAL_LINKS) as row-dict batches, one batch per chunk (never the whole result in memory) <<< #
def _rest_iter_rows(host, statement_id, payload):
    manifest = payload.get("manifest") or {}
    schema = (manifest.get("schema") or {}).get("columns") or []
    cols = [(c.get("name", "").lower(), c.get("type_name", "")) for c in schema]

    def _rows(data_array):
        return [{cols[i][0]: _dbx_coerce(v, cols[i][1]) for i, v in enumerate(arr)}
                for arr in (data_array or [])]

    # >>> batches + next chunk idx for one ResultData (EXTERNAL_LINKS often sets the idx only on the links) <<< #
    def _emit(result):
        result = result or {}
        batches = []
        nxt = result.get("next_chunk_index")
        if result.get("data_array") is not None:
            batches.append(_rows(result.get("data_array")))
        for link in result.get("external_links") or []:
            url = link.get("external_link")
            if url:
                batches.append(_rows(_fetch_external_link(url)))
            ln = link.get("next_chunk_index")
            if ln is not None:
                nxt = ln if nxt is None else max(nxt, ln)
        return batches, nxt

    batches, next_idx = _emit(payload.get("result") or {})
    for b in batches:
        if b:
            yield b
    seen = set()
    while next_idx is not None and next_idx not in seen:
        seen.add(next_idx)
        chunk = _rest_request(
            "GET", host,
            f"/api/2.0/sql/statements/{statement_id}/result/chunks/{next_idx}")
        batches, next_idx = _emit(chunk)
        for b in batches:
            if b:
                yield b

# >>> gather every result chunk into one row-dict list (non-streaming callers) <<< #
def _rest_collect_rows(host, statement_id, payload):
    rows = []
    for b in _rest_iter_rows(host, statement_id, payload):
        rows.extend(b)
    return rows

_DBX_TERMINAL = {"SUCCEEDED", "FAILED", "CANCELED", "CLOSED"}

# >>> poll a submitted statement to a terminal state and return its final payload <<< #
def _rest_poll(host, statement_id, payload):
    import time
    deadline = _dbx_timeout()
    waited = 0
    state = ((payload.get("status") or {}).get("state") or "").upper()
    while state not in _DBX_TERMINAL:
        if deadline > 0 and waited >= deadline:
            raise TimeoutError(
                f"databricks statement {statement_id} still {state} after {waited}s")
        time.sleep(5)
        waited += 5
        payload = _rest_request(
            "GET", host, f"/api/2.0/sql/statements/{statement_id}")
        state = ((payload.get("status") or {}).get("state") or "").upper()
    return payload

# >>> submit (or resume-by-ID) a statement + poll to SUCCEEDED; return (host, statement_id, terminal_payload) — shared by the list + streaming executors <<< #
def _rest_submit_and_poll(sql, catalog_schema=None):
    server_hostname, http_path, token = _databricks_auth()
    warehouse_id = _warehouse_id_from_http_path(http_path)
    if not warehouse_id:
        raise RuntimeError(
            f"DB_HTTPS http_path '{http_path}' is not a SQL warehouse path; "
            "the resume-by-ID REST executor needs /sql/.../warehouses/<id> "
            "(or set DTRACK_DATABRICKS_REST=0 to use the Thrift connector).")

    resolved = _resolve_catalog_schema(catalog_schema)
    catalog = schema = None
    if resolved and "." in resolved:
        catalog, schema = resolved.split(".", 1)
    elif resolved:
        schema = resolved

    stmts = [s for s in _split_statements(sql)
             if not s.lstrip().upper().startswith("USE ")]
    if not stmts:
        return server_hostname, None, None
    statement = stmts[-1]

    from ..logmgr import log_exc, get_logger
    existing = statement_id_get(sql, catalog_schema)
    payload = None
    if existing:
        try:
            payload = _rest_request(
                "GET", server_hostname,
                f"/api/2.0/sql/statements/{existing}")
            state = ((payload.get("status") or {}).get("state") or "").upper()
            if state in ("FAILED", "CANCELED", "CLOSED"):
                payload = None
            else:
                get_logger().info(
                    "databricks: resuming statement %s (state=%s) — not re-running",
                    existing, state or "?")
        except Exception as e:
            # 404 / expired statement: fall through to a fresh submit
            log_exc(f"databricks: cannot resume statement {existing}; resubmitting",
                    e, level="warning")
            payload = None
        if payload is None:
            statement_id_delete(sql, catalog_schema)

    if payload is None:
        body = {
            "warehouse_id": warehouse_id,
            "statement": statement,
            "wait_timeout": "0s",
            "disposition": _dbx_disposition(),
            "format": "JSON_ARRAY",
        }
        if catalog:
            body["catalog"] = catalog
        if schema:
            body["schema"] = schema
        payload = _rest_request(
            "POST", server_hostname, "/api/2.0/sql/statements/", body)
        statement_id = payload.get("statement_id")
        if statement_id:
            statement_id_put(sql, catalog_schema, statement_id)

    statement_id = payload.get("statement_id")
    payload = _rest_poll(server_hostname, statement_id, payload)
    status = payload.get("status") or {}
    state = (status.get("state") or "").upper()
    if state != "SUCCEEDED":
        statement_id_delete(sql, catalog_schema)
        err = (status.get("error") or {}).get("message") or state
        raise RuntimeError(f"databricks statement {statement_id} {state}: {err}")
    return server_hostname, statement_id, payload

# >>> resume-by-ID Databricks executor: submit via statements API, persist the <<< #
# >>> statement_id, and re-fetch by ID on retry instead of re-running the query <<< #
def _query_databricks_rest(sql, catalog_schema=None):
    cached = cache_get(sql, catalog_schema)
    if cached is not None:
        return cached
    server_hostname, statement_id, payload = _rest_submit_and_poll(sql, catalog_schema)
    if payload is None:
        return []
    out = _rest_collect_rows(server_hostname, statement_id, payload)
    cache_put(sql, catalog_schema, out, row_count=len(out))
    statement_id_delete(sql, catalog_schema)
    return out

# >>> streaming REST executor: submit/poll then yield EXTERNAL_LINKS chunks as batches (no cache; bulk pulls stay off the cache DB) <<< #
def _query_databricks_rest_iter(sql, catalog_schema=None):
    server_hostname, statement_id, payload = _rest_submit_and_poll(sql, catalog_schema)
    if payload is None:
        return
    try:
        yield from _rest_iter_rows(server_hostname, statement_id, payload)
    finally:
        statement_id_delete(sql, catalog_schema)

# >>> streaming sibling of _query_databricks: REST chunks by default, else Thrift fetchmany(arraysize); no cache <<< #
def _iter_databricks(sql, catalog_schema=None, arraysize=50000):
    if os.environ.get("DTRACK_DATABRICKS_REST", "1") != "0":
        yield from _query_databricks_rest_iter(sql, catalog_schema)
        return
    conn = databricks_connect(catalog_schema)
    try:
        cur = conn.cursor()
        try:
            for stmt in _split_statements(sql):
                if not stmt:
                    continue
                cur.execute(stmt)
                if cur.description:
                    cols = [d[0].lower() for d in cur.description]
                    while True:
                        rows = cur.fetchmany(arraysize)
                        if not rows:
                            break
                        yield [dict(zip(cols, row)) for row in rows]
        finally:
            cur.close()
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _query_databricks(sql, catalog_schema=None):
    if os.environ.get("DTRACK_DATABRICKS_REST", "1") != "0":
        return _query_databricks_rest(sql, catalog_schema)
    cached = cache_get(sql, catalog_schema)
    if cached is not None:
        return cached
    retries = _dbx_retries()
    last_err = None
    for attempt in range(retries + 1):
        conn = databricks_connect(catalog_schema)
        try:
            cur = conn.cursor()
            out = []
            for stmt in _split_statements(sql):
                if not stmt:
                    continue
                cur.execute(stmt)
                if cur.description:
                    cols = [d[0].lower() for d in cur.description]
                    out = [dict(zip(cols, row)) for row in cur.fetchall()]
            cur.close()
            cache_put(sql, catalog_schema, out, row_count=len(out))
            return out
        except Exception as e:
            # a socket-timeout / dropped poll surfaces here: reconnect + retry
            last_err = e
            from ..logmgr import log_exc
            log_exc(f"databricks query attempt {attempt + 1}/{retries + 1} failed; "
                    f"reconnecting", e, level="warning")
        finally:
            try:
                conn.close()
            except Exception:
                pass
    raise last_err

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
    if bounds:
        parts.append(bounds)
    if where:
        parts.append(f"({where})")
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
    base_where = (tbl_cfg.get("_base_where", orig_where) or "").strip()
    bounds = (tbl_cfg.get("_date_bounds") or "").strip()
    eff_dtype = date_filter.get("date_dtype")
    eff_fmt = date_filter.get("date_format")
    buckets = table_vintage in ("day", "week", "month", "quarter", "year")
    if date_filter["filter_type"] == "between":
        dw = build_date_range_with_gaps(
            date_col, date_filter["dates"], eff_dtype, date_format=eff_fmt)
        where = f"{dw} AND ({base_where})" if base_where else dw
        dt_label, ret_vintage = (None, table_vintage) if buckets else ("all", "all")
    elif date_filter["filter_type"] == "in_list":
        dw = build_date_in_clause(
            date_col, date_filter["dates"], eff_dtype, date_format=eff_fmt)
        where = f"{dw} AND ({base_where})" if base_where else dw
        dt_label, ret_vintage = "sample", "all"
    elif bounds:
        where = f"{bounds} AND ({orig_where})" if orig_where else bounds
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

    set_cache_db(cache_db_path(outdir))
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

    def _dbx_bounds(tbl, date_col, eff_from, eff_to):
        date_col_q = quote_ident(date_col, "databricks")
        date_type = (tbl.get("date_type") or "").lower()
        custom = config.get('date_types')
        from .athena import _format_athena_date_bound
        bounds = []
        if eff_from:
            bounds.append(f"{date_col_q} >= "
                          f"{_format_athena_date_bound(eff_from, date_type, False, custom_date_types=custom)}")
        if eff_to:
            bounds.append(f"{date_col_q} <= "
                          f"{_format_athena_date_bound(eff_to, date_type, True, custom_date_types=custom)}")
        return " AND ".join(bounds)
    apply_pair_date_bounds(dbx_tables, config, from_date, to_date, _dbx_bounds)

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
