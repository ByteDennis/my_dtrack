
import contextlib
import csv as _csv
import json
import os
import shutil
import tempfile
from concurrent.futures import ThreadPoolExecutor

from .config import load_unified_config, get_col_type_overrides, get_ignored_columns
from .date_utils import bucket_date, parse_date
from .platforms.base import qualified_name, quote_ident, build_date_bounds_clause
from .compare import (
    _dialect, _full_table_and_cte, _run_query,
    compare_column_stats, _has_col_differences, compare_row_counts,
    _key_in_clause, _hash16_expr, _limit,
    _hash32_expr, HASH_BUCKET_SPACE,
)
from .db import (
    get_column_meta, get_metadata, get_col_stats, get_row_counts,
    get_pair_col_map_from_db, save_col_comparison, register_table_pair,
    get_compare_csv_sig, set_compare_csv_sig, delete_col_stats_for_tables,
    upsert_row_counts, save_row_comparison,
    save_sampled_keys, get_sampled_keys, refresh_metadata_from_data,
    apply_date_synonyms, normalize_date_synonyms,
)
from .loader import load_precomputed_col_stats
from .extract import extract_all

_CSV_SUPPORTED = ("aws", "oracle", "databricks", "csv", "hadoop")

_VERBOSE = False

# >>> run a backend SQL: concise rows+secs line by default; full SQL (stdout + log) only with --verbose <<< #
def _q(tbl, sql, label, total=None):
    import time
    from .logmgr import get_logger
    src = (tbl.get("source") or "").lower()
    log = get_logger()
    if _VERBOSE:
        print(f"[compare-csv] {label} [{src}:{qualified_name(tbl)}]:\n{sql}")
        log.info("[compare-csv] %s [%s]: %s", label, qualified_name(tbl), sql)
    t0 = time.time()
    rows = _run_query(tbl, sql)
    cnt = f"{len(rows):,} rows" + (f" / {total:,} total" if total is not None else "")
    msg = f"[compare-csv] {label} [{src}]: {cnt} in {time.time() - t0:.1f}s"
    print(msg)
    if not _VERBOSE:
        log.info(msg)
    return rows

# >>> fallback column-name discovery for any source: one row -> its keys are the cols <<< #
def _discover_names(tbl):
    src = (tbl.get("source") or "").lower()
    ft, cte = _full_table_and_cte(tbl)
    base = (tbl.get("where") or "").strip()
    where = f" WHERE {base}" if base else ""
    sql = f"{cte}SELECT * FROM {ft}{where} {_limit(src, 1)}"
    try:
        rows = _q(tbl, sql, "discover columns")
    except Exception:
        return []
    return list(rows[0].keys()) if rows else []

# >>> discover columns+TYPES (DESCRIBE, all 5 sources) into _column_meta for tables missing it <<< #
def _ensure_column_meta(project_db, tables, outdir):
    from .db import insert_column_meta
    need = [t for t in tables if not _col_types(project_db, qualified_name(t))]
    if not need:
        return
    by_src = {}
    for t in need:
        by_src.setdefault((t.get("source") or "").lower(), []).append(t)
    try:
        relational = [t for s in ("aws", "oracle", "hadoop") for t in by_src.get(s, [])]
        if relational:
            from .platforms.base import discover_and_write_columns
            discover_and_write_columns(relational, outdir, project_db)
        if by_src.get("databricks"):
            from .platforms.databricks import _ensure_columns_databricks
            _ensure_columns_databricks(by_src["databricks"], outdir, project_db)
        for t in by_src.get("csv", []):
            from .platforms.csv_duckdb import discover_csv_columns
            cols = discover_csv_columns(t.get("conn_macro", "") or "")
            if cols:
                insert_column_meta(project_db, qualified_name(t), cols, source="csv")
    except Exception as e:
        print(f"[compare-csv] column discovery warning: {e}")

# >>> auto-match columns by name (from _column_meta, else a SELECT* probe) and persist column_mapping.csv <<< #
def _ensure_col_map(project_db, config, config_path, pname, left, right):
    from .platforms.base import match_columns_from_dicts
    from .config import save_unified_config
    tl, tr = qualified_name(left), qualified_name(right)
    lcols = {k: v for k, v in _col_types(project_db, tl).items()} \
        or {c: "" for c in _discover_names(left)}
    rcols = {k: v for k, v in _col_types(project_db, tr).items()} \
        or {c: "" for c in _discover_names(right)}
    if not lcols or not rcols:
        print(f"[compare-csv] {pname}: could not discover columns "
              f"(L={len(lcols)} R={len(rcols)}); no col_map generated")
        return {}
    res = match_columns_from_dicts(lcols, rcols, tl, tr)
    col_map = dict(res.get("matched", {}))
    config.setdefault("pairs", {}).setdefault(pname, {})["col_map"] = col_map
    if config_path:
        try:
            save_unified_config(config, config_path)
        except Exception as e:
            print(f"[compare-csv] {pname}: could not persist col_map: {e}")
    print(f"[compare-csv] {pname}: auto-matched {len(col_map)} columns "
          f"({len(lcols)}L/{len(rcols)}R discovered) -> column_mapping.csv")
    return col_map

# >>> {col_lower: TYPE} recorded for a table in _column_meta (empty if none) <<< #
def _col_types(project_db, qname):
    out = {}
    try:
        for cm in (get_column_meta(project_db, qname) or []):
            c = cm.get("column_name")
            if c:
                out[c.lower()] = (cm.get("data_type") or "").upper()
    except Exception:
        pass
    return out

# >>> resolve (date_dtype, date_format) from the side's date_type (e2e semantics), else _column_meta/metadata <<< #
def _date_dtype_fmt(project_db, tbl):
    from .constants import DATE_TYPE_FORMATS, DATE_TYPE_TO_CANONICAL_DTYPE
    dt = (tbl.get("date_type") or "").lower()
    dfmt = DATE_TYPE_FORMATS.get(dt)
    ddtype = DATE_TYPE_TO_CANONICAL_DTYPE.get(dt)
    dc = (tbl.get("date_col") or "").lower()
    if not ddtype:
        ddtype = (_col_types(project_db, qualified_name(tbl)).get(dc)
                  or ("VARCHAR" if dt.startswith("string") else None))
    if not dfmt:
        dfmt = (get_metadata(project_db, qualified_name(tbl)) or {}).get("date_format")
    return ddtype, dfmt

# >>> live per-date COUNT(*) on a side (cheap; no data download): {canonical_dt: count} <<< #
def _date_counts(project_db, tbl, from_date, to_date):
    src = (tbl.get("source") or "").lower()
    d = _dialect(src)
    ft, cte = _full_table_and_cte(tbl)
    dc = tbl.get("date_col", "")
    ddtype, dfmt = _date_dtype_fmt(project_db, tbl)
    date_q = quote_ident(dc, d)
    base = (tbl.get("where") or "").strip()
    bounds = (build_date_bounds_clause(date_q, from_date, to_date, ddtype, date_format=dfmt)
              if (from_date or to_date) else "")
    where = " AND ".join(f"({p})" for p in (base, bounds) if p)
    sql = (f"{cte}SELECT {date_q} AS dt, COUNT(*) AS cnt FROM {ft}"
           + (f" WHERE {where}" if where else "")
           + f" GROUP BY {date_q}")
    out = {}
    for r in _q(tbl, sql, "date counts"):
        v = r.get("dt")
        if v is None:
            continue
        try:
            k = parse_date(str(v))
        except Exception:
            k = str(v)
        out[k] = out.get(k, 0) + int(r.get("cnt") or 0)
    return out

# >>> count-MATCH dates for a pair; reuse _row_counts if loaded, else live COUNT(*) (persisted) <<< #
def _matched_dates(project_db, pname, left, right, tl, tr, from_date, to_date, synonyms=None):
    rl = {dt: c for dt, c in get_row_counts(project_db, tl, from_date, to_date, date_synonyms=synonyms) if dt}
    rr = {dt: c for dt, c in get_row_counts(project_db, tr, from_date, to_date, date_synonyms=synonyms) if dt}
    origin = "_row_counts"
    if not rl or not rr:
        raw_l = _date_counts(project_db, left, from_date, to_date)
        raw_r = _date_counts(project_db, right, from_date, to_date)
        origin = "live COUNT(*)"
        upsert_row_counts(project_db, tl, [(dt, c) for dt, c in raw_l.items()])
        upsert_row_counts(project_db, tr, [(dt, c) for dt, c in raw_r.items()])
        rl = apply_date_synonyms(raw_l, synonyms)
        rr = apply_date_synonyms(raw_r, synonyms)
    common = set(rl) & set(rr)
    matched = sorted(dt for dt in common if rl[dt] == rr[dt])
    print(f"[compare-csv] {pname}: dates from {origin} -> "
          f"{len(matched)} count-match, {len(common) - len(matched)} count-diff, "
          f"{len(set(rl) ^ set(rr))} single-side")
    return matched, rl, rr

# >>> persist the pair's row-count comparison (_row_comparison) so the row report renders, like e2e <<< #
def _save_row_compare(project_db, pname, tl, tr, rl, rr, from_date, to_date, synonyms=None):
    rc = compare_row_counts(project_db, tl, tr, from_date, to_date, date_synonyms=synonyms)
    ov_start = ov_end = None
    if rl and rr:
        ov_start = max(min(rl), min(rr))
        ov_end = min(max(rl), max(rr))
        if ov_start > ov_end:
            ov_start = ov_end = None
    matching = [dt for dt, _ in rc["matching"]]
    non_matching = [dt for dt, _, _ in rc["mismatched"]]
    excluded = [dt for dt, _ in rc["only_left"]] + [dt for dt, _ in rc["only_right"]]
    save_row_comparison(project_db, pname, ov_start, ov_end, matching, excluded,
                        non_matching_dates=non_matching)
    print(f"[compare-csv] {pname}: row compare -> {len(matching)} match, "
          f"{len(non_matching)} diff, {len(excluded)} single-side")

# >>> collapse a date list into maximal calendar-contiguous [start, end] runs (gaps split runs) <<< #
def _contiguous_runs(dates):
    from datetime import datetime, timedelta
    ds = sorted(set(dates))
    try:
        parsed = [datetime.strptime(x, "%Y-%m-%d") for x in ds]
    except (ValueError, TypeError):
        return [(x, x) for x in ds]
    runs = []
    start = prev = 0
    for i in range(1, len(parsed)):
        if parsed[i] - parsed[prev] == timedelta(days=1):
            prev = i
        else:
            runs.append((ds[start], ds[prev]))
            start = prev = i
    runs.append((ds[start], ds[prev]))
    return runs

# >>> (base where AND bucket date-window) for a side: union of count-matching date runs, skipping gaps <<< #
def _window_where(project_db, tbl, date_col, dates, synonyms=None):
    d = _dialect((tbl.get("source") or "").lower())
    ddtype, dfmt = _date_dtype_fmt(project_db, tbl)
    date_q = quote_ident(date_col, d)
    runs = [build_date_bounds_clause(date_q, lo, hi, ddtype, date_format=dfmt)
            for lo, hi in _contiguous_runs(dates)]
    # a source may store a sentinel (e.g. 3000-01-01) that stands in for a real
    # date in this window; pull those raw rows too so they land in the window.
    syn = normalize_date_synonyms(synonyms)
    if syn:
        targets = set(dates)
        for sentinel, target in syn.items():
            if target in targets:
                runs.append(build_date_bounds_clause(date_q, sentinel, sentinel,
                                                     ddtype, date_format=dfmt))
    runs = [r for r in runs if r]
    if not runs:
        bounds = ""
    elif len(runs) == 1:
        bounds = runs[0]
    else:
        bounds = "(" + " OR ".join(f"({r})" for r in runs) + ")"
    base = (tbl.get("where") or "").strip()
    return " AND ".join(f"({p})" for p in (base, bounds) if p)

# >>> canonical key form UPPER(TRIM(CAST(col AS str))) so differing source formats align <<< #
def _norm_key_expr(dialect, col_q):
    d = dialect or ""
    if d == "oracle":
        s = f"TO_CHAR({col_q})"
    elif d in ("hadoop", "hive", "databricks", "spark"):
        s = f"CAST({col_q} AS STRING)"
    else:
        s = f"CAST({col_q} AS VARCHAR)"
    return f"UPPER(TRIM({s}))"

# >>> count distinct NORMALIZED composite-key tuples on a side within the window <<< #
def _distinct_key_count(tbl, key_cols, where):
    d = _dialect((tbl.get("source") or "").lower())
    ft, cte = _full_table_and_cte(tbl)
    keys_n = ", ".join(_norm_key_expr(d, quote_ident(k, d)) for k in key_cols)
    sql = (f"{cte}SELECT COUNT(*) AS c FROM "
           f"(SELECT DISTINCT {keys_n} FROM {ft}"
           + (f" WHERE {where}" if where else "") + ") t")
    rows = _q(tbl, sql, "distinct key count")
    return int(rows[0].get("c") or 0) if rows else 0

# >>> deterministic fixed-N hash-spread sample of composite-key tuples from the LEFT, window-scoped <<< #
def _window_keys(project_db, left, key_cols, dates, sample, normalize=False, synonyms=None):
    src = (left.get("source") or "").lower()
    d = _dialect(src)
    where = _window_where(project_db, left, left.get("date_col", ""), dates, synonyms)
    kexprs = [(_norm_key_expr(d, quote_ident(k, d)) if normalize
               else quote_ident(k, d)) for k in key_cols]
    if isinstance(sample, float):
        total = _distinct_key_count(left if where == "" else {**left, "where": ""},
                                    key_cols, where)
        n = max(1, round(sample * total))
    else:
        n = int(sample)
    ft, cte = _full_table_and_cte(left)
    notnull = " AND ".join(f"{quote_ident(k, d)} IS NOT NULL" for k in key_cols)
    w = " AND ".join(f"({p})" for p in (where, notnull) if p) or "1=1"
    sel = ", ".join(f"{e} AS k{i}" for i, e in enumerate(kexprs))
    sql = (f"{cte}SELECT {sel}, {_hash16_expr(src, kexprs[0])} AS kh "
           f"FROM {ft} WHERE {w} "
           f"GROUP BY {', '.join(kexprs)} "
           f"ORDER BY kh {_limit(src, n)}")
    rows = _q(left, sql, "sample keys")
    return [tuple(r.get(f"k{i}") for i in range(len(key_cols))) for r in rows]

# >>> per-side composite-key IN (...) for the sampled tuples (raw by default, normalized opt-in) <<< #
def _key_filters(left, right, key_cols, right_key_cols, key_tuples, normalize=False):
    dl = _dialect((left.get("source") or "").lower())
    dr = _dialect((right.get("source") or "").lower())
    if normalize:
        lq = [_norm_key_expr(dl, quote_ident(k, dl)) for k in key_cols]
        rq = [_norm_key_expr(dr, quote_ident(rk, dr)) for rk in right_key_cols]
    else:
        lq = [quote_ident(k, dl) for k in key_cols]
        rq = [quote_ident(rk, dr) for rk in right_key_cols]
    return _key_in_clause(lq, key_tuples), _key_in_clause(rq, key_tuples)

# >>> STRICT value family (date|numeric|string) per key col, taken from the LEFT side so both sides match <<< #
def _sample_families(project_db, left, key_cols):
    from .platforms.base import _strict_family, _is_numeric_col_type
    types = _col_types(project_db, qualified_name(left))
    fams = []
    for k in key_cols:
        dtype = types.get(k.lower(), "")
        fams.append(_strict_family(dtype, _is_numeric_col_type(dtype)))
    return fams

# >>> pushdown predicate `hash32(normalized composite key) < threshold` (same recipe both sides) <<< #
def _hash_bucket_where(tbl, key_cols, fams, threshold):
    from .platforms.base import _strict_normalize
    d = _dialect((tbl.get("source") or "").lower())
    norm = [_strict_normalize(d, quote_ident(k, d), fam)
            for k, fam in zip(key_cols, fams)]
    if len(norm) == 1:
        comp = norm[0]
    elif d == "oracle":
        comp = " || '|' || ".join(norm)
    else:
        comp = "concat_ws('|', " + ", ".join(norm) + ")"
    return f"{_hash32_expr(d, comp)} < {threshold}"

# >>> filesystem-safe form of a qualified table name <<< #
def _safe_name(qname):
    import re
    return re.sub(r"[^A-Za-z0-9._-]", "_", str(qname))

# >>> pull one side's raw mapped-column rows for `dates` -> CSV at out_csv + inline column types <<< #
def _pull_side(project_db, tbl, value_cols, date_col, dates, out_csv, tag,
               extra_where=None, total=None, synonyms=None, dryrun=False):
    from .platforms.base import resolve_config_columns
    src = (tbl.get("source") or "").lower()
    d = _dialect(src)
    ft, cte = _full_table_and_cte(tbl)
    qname = qualified_name(tbl)
    declared = {k.lower(): v for k, v in (resolve_config_columns(tbl) or {}).items()}
    types = {**_col_types(project_db, qname), **declared}

    date_q = quote_ident(date_col, d)
    cols_q = [date_q] + [quote_ident(c, d) for c in value_cols]
    win = _window_where(project_db, tbl, date_col, dates, synonyms)
    where = " AND ".join(f"({p})" for p in (win, extra_where) if p) or "1=1"
    sql = f"{cte}SELECT {', '.join(cols_q)} FROM {ft} WHERE {where}"
    header = [date_col] + list(value_cols)
    if dryrun:
        from .logmgr import get_logger
        get_logger().info("[compare-csv][DRY RUN] would pull %s (%s):\n%s", tag, qname, sql)
        return out_csv, header, None, 0
    rows = _q(tbl, sql, f"download {tag}", total=total)

    with open(out_csv, "w", newline="") as f:
        w = _csv.writer(f)
        w.writerow(header)
        for r in rows:
            dv = r.get(date_col.lower())
            try:
                dv = parse_date(str(dv))
            except Exception:
                dv = "" if dv is None else str(dv)
            w.writerow([dv] + [r.get(c.lower()) for c in value_cols])

    vt = {c: types[c.lower()] for c in value_cols if types.get(c.lower())}
    columns = ({date_col: "VARCHAR", **vt}
               if value_cols and len(vt) == len(value_cols) else None)
    return out_csv, header, columns, len(rows)

# >>> overwrite every dt cell in a col CSV with the window's bucket key <<< #
def _stamp_dt(col_csv, dt_label):
    with open(col_csv, newline="") as f:
        reader = _csv.DictReader(f)
        fields = reader.fieldnames or []
        rows = [dict(r) for r in reader]
    for r in rows:
        r["dt"] = dt_label
    with open(col_csv, "w", newline="") as f:
        w = _csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)

# >>> swallow extract_all's banner/per-block stdout+stderr into run.log unless --verbose <<< #
@contextlib.contextmanager
def _quiet_extract(tag):
    if _VERBOSE:
        yield
        return
    import io
    import logging
    import time
    from .logmgr import get_logger
    log = get_logger()
    buf = io.StringIO()
    prev = log.level
    log.setLevel(logging.WARNING)
    t0 = time.time()
    try:
        with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
            yield
    except Exception:
        log.setLevel(prev)
        text = buf.getvalue().strip()
        if text:
            log.info("[compare-csv] local stats %s FAILED:\n%s", tag, text)
        raise
    log.setLevel(prev)
    log.info("[compare-csv] local stats %s: done in %.1fs", tag, time.time() - t0)

# >>> col stats over a pulled CSV in the transient scratch csv/ (fresh .state), upsert into project.db <<< #
def _extract_and_load(project_db, src_tbl, csv_path, columns, real_qname,
                      date_col, dt_label, vintage, preset, stats_override,
                      scratch_csv, tag, workers=None, strict=False):
    csv_cfg = {
        "name": tag, "table": tag, "source": "csv",
        "conn_macro": csv_path, "date_col": date_col,
        "date_type": "string_dash", "vintage": "all",
    }
    if strict:
        csv_cfg["strict"] = True
        csv_cfg["strict_numeric_strings"] = True
    if columns:
        csv_cfg["columns"] = columns
    temp_cfg = {"pairs": {"_w": {"left": csv_cfg, "right": csv_cfg}}, "metadata": {}}
    os.makedirs(scratch_csv, exist_ok=True)
    cfg_path = os.path.join(scratch_csv, f"{tag}.config.json")
    with open(cfg_path, "w") as f:
        json.dump(temp_cfg, f)
    with _quiet_extract(tag):
        extract_all(config_path=cfg_path, outdir=scratch_csv, csv_outdir=scratch_csv,
                    types=["col"], db_path=None, vintage="all", preset=preset,
                    stats_override=stats_override, max_workers=workers)
    col_csv = os.path.join(scratch_csv, f"{qualified_name(csv_cfg)}_col.csv")
    _stamp_dt(col_csv, dt_label)
    return load_precomputed_col_stats(
        db_path=project_db, csv_path=col_csv, table_name=real_qname,
        mode="upsert", source=src_tbl.get("source"),
        db_name=src_tbl.get("conn_macro"), vintage=vintage)

# >>> bucket-keys already present in _col_stats for a table (skip-cache) <<< #
def _loaded_buckets(project_db, qname, vintage):
    out = set()
    for s in get_col_stats(project_db, qname):
        dt = s.get("dt")
        if dt and dt not in ("all", "ALL"):
            try:
                out.add(bucket_date(dt, vintage))
            except Exception:
                out.add(dt)
    return out

_CLEAN_LEVELS = ("marks", "cache", "all")

# >>> 3-tier reset: marks=clear run signature; cache=+drop skip-driving db rows/stats/keys/caches; all=+delete project.db & --data-dir <<< #
def _clean_compare_csv(project_db, config, targets, level, data_dir):
    import glob
    from .db import (delete_compare_csv_sig, delete_col_stats_for_tables,
                     delete_row_counts_for_tables, delete_comparisons_for_pair,
                     delete_sampled_keys, init_database)
    pairs = config.get("pairs", {})
    proj_dir = os.path.dirname(os.path.abspath(project_db)) or "."
    bases = {b for b in (data_dir, proj_dir) if b}

    if os.path.exists(project_db):
        for pname in targets:
            delete_compare_csv_sig(project_db, pname)
    for b in bases:
        for f in glob.glob(os.path.join(b, ".state*", "*.done")):
            try:
                os.remove(f)
            except OSError:
                pass
    print(f"[compare-csv] clean[{level}]: cleared run marks ({len(targets)} pair(s))")
    if level == "marks":
        return

    if os.path.exists(project_db):
        for pname in targets:
            p = pairs.get(pname)
            if not p:
                continue
            tl, tr = qualified_name(p["left"]), qualified_name(p["right"])
            delete_col_stats_for_tables(project_db, [tl, tr])
            delete_row_counts_for_tables(project_db, [tl, tr])
            delete_comparisons_for_pair(project_db, pname)
            delete_sampled_keys(project_db, pname)
            vcsv = os.path.join(proj_dir, f"{_safe_name(pname)}_compare_col.csv")
            if os.path.exists(vcsv):
                os.remove(vcsv)
    for b in bases:
        shutil.rmtree(os.path.join(b, ".state"), ignore_errors=True)
    print(f"[compare-csv] clean[{level}]: dropped col/row stats, comparisons, "
          f"sampled keys, caches")
    if level == "cache":
        return

    for suf in ("", "-wal", "-shm"):
        try:
            os.remove(project_db + suf)
        except OSError:
            pass
    init_database(project_db)
    if data_dir and os.path.isdir(data_dir):
        for f in glob.glob(os.path.join(data_dir, "*")):
            shutil.rmtree(f, ignore_errors=True) if os.path.isdir(f) else \
                os.remove(f)
    print("[compare-csv] clean[all]: removed project.db (re-init) + --data-dir scratch")

# >>> per-vintage-window pull -> local DuckDB col stats -> upsert, then compare+save <<< #
def run_compare_csv(project_db, config_path, pair=None, vintage=None,
                    from_date=None, to_date=None, preset=None,
                    stats_override=None, keep_csv=False, resume=False,
                    sample=None, key=None, data_dir=None, workers=None,
                    force=False, verbose=False, clean=None, strict=False,
                    dryrun=False):
    from .extract import _cfg_max_workers
    from .config import backup_config
    import logging as _logging
    from .logmgr import get_logger
    global _VERBOSE
    _VERBOSE = verbose
    get_logger().setLevel(_logging.DEBUG if verbose else _logging.INFO)
    backup_config(config_path)
    config = load_unified_config(config_path)
    if data_dir and not os.path.isdir(data_dir):
        raise ValueError(f"--data-dir does not exist: {data_dir!r} "
                         f"(create it first; it is not a temp folder)")
    pairs = config.get("pairs", {})
    targets = ([pair] if pair else
               [n for n, p in pairs.items() if not p.get("skip")])

    if clean:
        if clean not in _CLEAN_LEVELS:
            raise ValueError(f"--clean must be one of {_CLEAN_LEVELS}; got {clean!r}")
        _clean_compare_csv(project_db, config, targets, clean, data_dir)

    cli_key_cols = [k.strip() for k in str(key).split(",") if k.strip()] if key else []

    for pname in targets:
        if pname not in pairs:
            print(f"[compare-csv] pair not found: {pname}")
            continue
        pcfg = pairs[pname]
        left, right = pcfg["left"], pcfg["right"]
        tl, tr = qualified_name(left), qualified_name(right)
        for src in (left.get("source"), right.get("source")):
            if (src or "").lower() not in _CSV_SUPPORTED:
                print(f"[compare-csv] {pname}: source {src!r} unsupported "
                      f"({', '.join(_CSV_SUPPORTED)}); skipping")
                break
        else:
            cols_dir = os.path.dirname(os.path.abspath(config_path)) or "."
            _ensure_column_meta(project_db, [left, right], cols_dir)
            col_map = (pcfg.get("col_map")
                       or get_pair_col_map_from_db(project_db, pname) or {})
            if not col_map:
                col_map = _ensure_col_map(project_db, config, config_path,
                                          pname, left, right)
            register_table_pair(project_db, pname, tl, tr,
                                source_left=left.get("source"),
                                source_right=right.get("source"),
                                col_mappings=col_map or None)
            nworkers = _cfg_max_workers(config, (left.get("source") or "").lower(),
                                        workers) or 1
            _run_pair(project_db, pname, pcfg, tl, tr, col_map, config,
                      vintage, from_date, to_date, preset, stats_override,
                      keep_csv, sample, cli_key_cols, data_dir, nworkers, force,
                      strict, dryrun)

    if not dryrun:
        refresh_metadata_from_data(project_db)

# >>> resolve a pair's key cols (per-pair `key` else CLI --key) + right names via col_map; raise if unmapped <<< #
def _resolve_key(pname, pcfg, cli_key_cols, col_map):
    k = pcfg.get("key")
    if k:
        cols = [str(c).strip() for c in
                (k.split(",") if isinstance(k, str) else k) if str(c).strip()]
    else:
        cols = list(cli_key_cols or [])
    if not cols:
        return [], []
    cm_lower = {kk.lower(): (kk, vv) for kk, vv in (col_map or {}).items()}
    key_cols, right_cols = [], []
    for c in cols:
        ent = cm_lower.get(c.lower())
        if ent is None:
            raise ValueError(
                f"{pname}: key column {c!r} is not in col_map "
                f"(left cols: {sorted(col_map or {})}). Map it via match-columns "
                f"or fix the pair 'key'.")
        key_cols.append(ent[0])
        right_cols.append(ent[1])
    return key_cols, right_cols

# >>> whether the fixed-N IN-list key should be normalized: explicit `key_normalize` else auto when key types differ <<< #
def _resolve_key_normalize(project_db, pcfg, left, right, key_cols, right_key_cols):
    v = pcfg.get("key_normalize")
    if v is not None:
        return bool(v)
    lt = _col_types(project_db, qualified_name(left))
    rt = _col_types(project_db, qualified_name(right))
    return any(lt.get(lk.lower(), "") != rt.get(rk.lower(), "")
               for lk, rk in zip(key_cols, right_key_cols))

_VINTAGE_CHOICES = ("day", "week", "month", "quarter", "year")

# >>> resolve a pair's vintage: pair-level `vintage`, else either side's table-level `vintage`, else CLI, else day <<< #
# >>> compare-csv buckets BOTH sides at ONE granularity, so conflicting side vintages are an error <<< #
def _resolve_vintage(pname, pcfg, cli_vintage):
    pair_v = pcfg.get("vintage")
    lv = (pcfg.get("left") or {}).get("vintage")
    rv = (pcfg.get("right") or {}).get("vintage")
    if not pair_v and lv and rv and \
            str(lv).strip().lower() != str(rv).strip().lower():
        raise ValueError(
            f"{pname}: left vintage={lv!r} != right vintage={rv!r}; compare-csv "
            f"compares both sides at ONE granularity. Set a pair-level \"vintage\" "
            f"or make the two sides match.")
    v = (pair_v or lv or rv or cli_vintage or "day")
    v = str(v).strip().lower()
    if v not in _VINTAGE_CHOICES:
        raise ValueError(f"{pname}: vintage must be one of {_VINTAGE_CHOICES}; got {v!r}")
    return v

# >>> resolve a pair's sample (per-pair `sample` else CLI --sample): int=#keys, float in (0,1]=fraction, None=all <<< #
def _resolve_sample(pname, pcfg, cli_sample):
    s = pcfg.get("sample", None)
    if s is None:
        s = cli_sample
    if s is None or isinstance(s, bool):
        return None
    if isinstance(s, str):
        try:
            s = float(s) if "." in s else int(s)
        except ValueError:
            raise ValueError(f"{pname}: sample must be an integer or a fraction; got {s!r}")
    if isinstance(s, float) and not (0 < s <= 1):
        raise ValueError(f"{pname}: sample as a fraction must be in (0, 1]")
    return s

# >>> drive one pair: enumerate matched-date buckets, pull/load each, then compare <<< #
# >>> apply a pair's col_filter include/exclude (fnmatch, lowercase) to left value cols <<< #
def _apply_pair_col_filter(cols, pcfg):
    cf = pcfg.get("col_filter") or {}
    inc = [p.strip().lower() for p in (cf.get("include") or []) if p and p.strip()]
    exc = [p.strip().lower() for p in (cf.get("exclude") or []) if p and p.strip()]
    if not inc and not exc:
        return cols
    from fnmatch import fnmatch
    out = cols
    if inc:
        out = [c for c in out if any(fnmatch(c.lower(), p) for p in inc)]
    if exc:
        out = [c for c in out if not any(fnmatch(c.lower(), p) for p in exc)]
    return out

# >>> signature of the params that change a window's CONTENT (excludes from/to dates) <<< #
def _run_signature(vintage, sample, key_cols, value_cols, preset, stats_override,
                   key_normalize=False):
    import hashlib
    payload = json.dumps([
        vintage, str(sample), sorted(c.lower() for c in (key_cols or [])),
        sorted(c.lower() for c in (value_cols or [])), preset or "",
        json.dumps(stats_override or {}, sort_keys=True), bool(key_normalize),
    ], sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()

# >>> "source:table@conn (date=col)" — identifies a side in the start banner <<< #
def _side_desc(tbl):
    return (f"{(tbl.get('source') or '?').lower()}:"
            f"{tbl.get('table') or tbl.get('name') or '?'}"
            f"@{tbl.get('conn_macro') or '-'} "
            f"(date={tbl.get('date_col') or '?'})")

def _run_pair(project_db, pname, pcfg, tl, tr, col_map, config,
              vintage, from_date, to_date, preset, stats_override,
              keep_csv, cli_sample=None, cli_key_cols=None,
              data_dir=None, workers=1, force=False, strict=False, dryrun=False):
    left, right = pcfg["left"], pcfg["right"]
    from .logmgr import get_logger
    _banner = (f"[compare-csv] {pname}:\n"
               f"    LEFT  {_side_desc(left)}\n"
               f"    RIGHT {_side_desc(right)}")
    print(_banner)
    get_logger().info(_banner)
    dc_left = left.get("date_col", "")
    dc_right = right.get("date_col", "")
    left_cols = _apply_pair_col_filter(
        [c for c in col_map if c.lower() != dc_left.lower()], pcfg)
    right_cols = [col_map[c] for c in left_cols]

    eff_from = from_date or ((pcfg.get("fromDate") or "").strip() or None)
    eff_to = to_date or ((pcfg.get("toDate") or "").strip() or None)
    syn = pcfg.get("date_synonyms")
    mode = (pcfg.get("mode") or "incremental").strip().lower()
    force = force or mode == "full"
    vintage = _resolve_vintage(pname, pcfg, vintage)
    sample = _resolve_sample(pname, pcfg, cli_sample)
    key_cols, right_key_cols = _resolve_key(pname, pcfg, cli_key_cols, col_map)
    if sample is not None and not key_cols:
        raise ValueError(f"{pname}: sample needs a key — set a per-pair 'key' "
                         f"in the config or pass --key")
    frac_sample = isinstance(sample, float)
    key_normalize = (_resolve_key_normalize(project_db, pcfg, left, right,
                                            key_cols, right_key_cols)
                     if (sample is not None and not frac_sample) else False)
    fams = (_sample_families(project_db, left, key_cols)
            if frac_sample else None)
    threshold = (max(1, int(round(sample * HASH_BUCKET_SPACE)))
                 if frac_sample else None)

    sdesc = ("all rows (no sample)" if sample is None else
             (f"{sample:g} fraction (hash-bucket pushdown)" if frac_sample
              else f"{int(sample)} keys/window (fixed-N IN-list)"))
    kdesc = ",".join(key_cols) if key_cols else "-"
    if key_cols and [k.lower() for k in right_key_cols] != [k.lower() for k in key_cols]:
        kdesc = f"{kdesc} -> {','.join(right_key_cols)}"
    cfg_parts = [f"vintage={vintage}", f"mode={mode}", f"sample={sdesc}", f"key=[{kdesc}]"]
    if key_cols and not frac_sample:
        cfg_parts.append(f"key_normalize={'on' if key_normalize else 'off'}")
    if preset:
        cfg_parts.append(f"preset={preset}")
    if stats_override:
        cfg_parts.append(f"stats_override={stats_override}")
    if eff_from or eff_to:
        cfg_parts.append(f"dates={eff_from or '-'}..{eff_to or '-'}")
    cfg_parts.append(f"value_cols={len(left_cols)}")
    cfg_parts.append(f"col_workers={workers}")
    cfg_line = f"[compare-csv] {pname} config: " + "  ".join(cfg_parts)
    print(cfg_line)
    get_logger().info(cfg_line)

    run_sig = _run_signature(vintage, sample, key_cols, left_cols, preset,
                             stats_override, key_normalize)
    cached = get_compare_csv_sig(project_db, pname)
    params_changed = cached is not None and cached != run_sig
    fresh = force or params_changed
    if fresh and cached is not None:
        delete_col_stats_for_tables(project_db, [tl, tr])
        why = "mode=full" if mode == "full" else ("--force" if force else "params changed")
        print(f"[compare-csv] {pname}: {why} -> clearing cache, re-pulling all")

    matched, counts, rcounts = _matched_dates(project_db, pname, left, right, tl, tr,
                                              eff_from, eff_to, syn)
    _save_row_compare(project_db, pname, tl, tr, counts, rcounts, eff_from, eff_to, syn)
    if not matched:
        print(f"[compare-csv] {pname}: no count-matching dates; skipping")
        if not dryrun:
            set_compare_csv_sig(project_db, pname, run_sig)
        return

    buckets = {}
    for dt in matched:
        buckets.setdefault(bucket_date(dt, vintage), []).append(dt)

    skip_done = not fresh
    done_l = _loaded_buckets(project_db, tl, vintage) if skip_done else set()
    done_r = _loaded_buckets(project_db, tr, vintage) if skip_done else set()

    proj_dir = os.path.dirname(os.path.abspath(project_db)) or "."
    scratch = data_dir or tempfile.mkdtemp(prefix="dtrack_ccsv_")
    cleanup_scratch = not data_dir and not keep_csv
    scratch_csv = os.path.join(scratch, "csv")
    state_dir = os.path.join(scratch, ".state")

    overrides = get_col_type_overrides(config, pname)
    ignore = get_ignored_columns(config, pname)
    vintage_csv = os.path.join(proj_dir, f"{_safe_name(pname)}_compare_col.csv")
    if os.path.exists(vintage_csv):
        os.remove(vintage_csv)

    print(f"[compare-csv] {pname}: {len(matched)} count-match dates -> "
          f"{len(buckets)} {vintage} window(s) "
          f"({len(left_cols)} value col(s), col-workers={workers})")

    for bkey in sorted(buckets):
        dates = buckets[bkey]
        span = f"{min(dates)}..{max(dates)}"
        if skip_done and bkey in done_l and bkey in done_r:
            print(f"  {bkey} [{span}]: already loaded; skip (--force to re-pull)")
        else:
            total = sum(counts.get(dt, 0) for dt in dates)
            key_in_l = key_in_r = None
            nkeys = None
            sample_label = None
            if frac_sample:
                key_in_l = _hash_bucket_where(left, key_cols, fams, threshold)
                key_in_r = _hash_bucket_where(right, right_key_cols, fams, threshold)
                sample_label = f"~{sample:g} hash-bucket"
            elif sample is not None and key_cols and dryrun:
                sample_label = f"{int(sample)} keys/window (fixed-N; key sample skipped in dry-run)"
            elif sample is not None and key_cols:
                saved = None if fresh else get_sampled_keys(project_db, pname, bkey)
                if saved is not None:
                    ktuples = saved
                    print(f"  {bkey} [{span}]: reusing {len(ktuples)} sampled "
                          f"key(s) from project.db")
                else:
                    ktuples = _window_keys(project_db, left, key_cols, dates, sample,
                                           normalize=key_normalize, synonyms=syn)
                    save_sampled_keys(project_db, pname, bkey, ktuples)
                nkeys = len(ktuples)
                key_in_l, key_in_r = _key_filters(left, right, key_cols,
                                                   right_key_cols, ktuples,
                                                   normalize=key_normalize)
            lc = os.path.join(scratch, f"{_safe_name(tl)}.csv")
            rc = os.path.join(scratch, f"{_safe_name(tr)}.csv")
            if dryrun:
                _pull_side(project_db, left, left_cols, dc_left, dates, lc,
                           "left", key_in_l, total, syn, dryrun=True)
                _pull_side(project_db, right, right_cols, dc_right, dates, rc,
                           "right", key_in_r, total, syn, dryrun=True)
                print(f"  {bkey} [{span}]: [DRY RUN] {len(dates)} date(s), "
                      f"sample={sample_label or 'all rows'} -> logged pull SQL "
                      f"(both sides); no data pulled, no compare")
                continue
            # both sides hit independent backends -> download concurrently (the slow
            # part); the local DuckDB extract stays sequential (shared per-window state)
            with ThreadPoolExecutor(max_workers=2) as ex:
                fl = ex.submit(_pull_side, project_db, left, left_cols, dc_left,
                               dates, lc, "left", key_in_l, total, syn)
                fr = ex.submit(_pull_side, project_db, right, right_cols, dc_right,
                               dates, rc, "right", key_in_r, total, syn)
                _, _, lcols, pl = fl.result()
                _, _, rcols, pr = fr.result()
            shutil.rmtree(state_dir, ignore_errors=True)
            nl = _extract_and_load(project_db, left, lc, lcols, tl, dc_left, bkey,
                                   vintage, preset, stats_override, scratch_csv,
                                   "left", workers, strict)
            nr = _extract_and_load(project_db, right, rc, rcols, tr, dc_right, bkey,
                                   vintage, preset, stats_override, scratch_csv,
                                   "right", workers, strict)
            keyinfo = (f" on {nkeys} sampled key(s)" if nkeys is not None
                       else (f" via {sample_label}" if sample_label else ""))
            print(f"  {bkey} [{span}]: {len(dates)} date(s) | records sample "
                  f"{pl}/{total} (L) {pr}/{total} (R){keyinfo} | "
                  f"col-stats {nl}/{nr}")
        if not dryrun:
            _vintage_compare(project_db, pname, tl, tr, col_map, bkey,
                             overrides, ignore, vintage_csv)

    if cleanup_scratch:
        shutil.rmtree(scratch, ignore_errors=True)
    elif not data_dir:
        print(f"    kept scratch: {scratch}")

    if dryrun:
        print(f"[compare-csv] {pname}: [DRY RUN] plan + pull SQL logged; "
              f"no data pulled, no compare, skip-cache untouched")
        return
    _compare_and_save(project_db, config, pname, tl, tr, col_map,
                      set(buckets), eff_from, eff_to)
    set_compare_csv_sig(project_db, pname, run_sig)

_VINTAGE_COMPARE_FIELDS = [
    "pair", "vintage", "column", "col_type",
    "n_total_left", "n_total_right", "n_total_diff",
    "n_missing_left", "n_missing_right", "n_missing_diff",
    "n_unique_left", "n_unique_right", "n_unique_diff",
    "min_match", "max_match", "hash_sum_match", "verdict",
]

# >>> which stats differ on a comparison row -> "min(1 != 101) mean(5.5 != 105.5)" <<< #
def _diff_detail(c):
    from .compare import _DIFF_PREDICATES
    toks = []
    for k, pred in _DIFF_PREDICATES.items():
        if pred(c):
            toks.append(f"{k}({c.get(f'{k}_left')} != {c.get(f'{k}_right')})")
    return " ".join(toks)

# >>> stats that passed only via the precision/overflow tolerance -> "mean(a != b)" <<< #
def _warn_detail(c):
    toks = []
    for k in ("mean", "std"):
        if c.get(f"{k}_warn"):
            toks.append(f"{k}({c.get(f'{k}_left')} != {c.get(f'{k}_right')})")
    return " ".join(toks)

# >>> compare one vintage bucket's col stats, append per-column rows to the per-vintage CSV + one-line summary <<< #
def _vintage_compare(project_db, pname, tl, tr, col_map, bkey, overrides, ignore,
                     vintage_csv):
    from .logmgr import get_logger
    result = compare_column_stats(
        project_db, tl, tr, col_mappings=col_map,
        matched_dates={bkey}, col_type_overrides=overrides)
    if not result:
        return
    exists = os.path.exists(vintage_csv)
    n_match, diff_cols = 0, []
    with open(vintage_csv, "a", newline="") as f:
        w = _csv.DictWriter(f, fieldnames=_VINTAGE_COMPARE_FIELDS,
                            extrasaction="ignore")
        if not exists:
            w.writeheader()
        for col in sorted(result):
            if col in ignore:
                continue
            for c in result[col]:
                is_diff = _has_col_differences(c)
                if is_diff:
                    diff_cols.append(col)
                    line = f"  {bkey}/{col}: DIFF {_diff_detail(c)}"
                    print(line)
                    get_logger().info(line)
                else:
                    n_match += 1
                    warn = _warn_detail(c)
                    if warn:
                        get_logger().debug(
                            "  %s/%s: precision-pass %s -- low-order digits, "
                            "likely float precision/overflow; passed as match",
                            bkey, col, warn)
                w.writerow({**c, "pair": pname, "vintage": bkey, "column": col,
                            "verdict": "diff" if is_diff else "match"})
    extra = f" ({', '.join(sorted(set(diff_cols))[:4])})" if diff_cols else ""
    print(f"  {bkey}: {n_match} match / {len(diff_cols)} diff{extra}")

# >>> run the standard col-stats compare over the accumulated _col_stats and persist <<< #
def _compare_and_save(project_db, config, pname, tl, tr, col_map,
                      matched_dates, from_date, to_date):
    overrides = get_col_type_overrides(config, pname)
    ignore = get_ignored_columns(config, pname)
    result = compare_column_stats(
        project_db, tl, tr, col_mappings=col_map,
        from_date=from_date, to_date=to_date,
        matched_dates=matched_dates, col_type_overrides=overrides)
    if not result:
        print(f"[compare-csv] {pname}: no comparable columns")
        return
    matched_cols, diff_cols = [], []
    for col_name, comps in result.items():
        if col_name in ignore:
            continue
        (diff_cols if any(_has_col_differences(c) for c in comps)
         else matched_cols).append(col_name)
    save_col_comparison(project_db, pname,
                        columns_compared=list(result.keys()),
                        matched_columns=matched_cols, diff_columns=diff_cols,
                        comparison_details=result)
    print(f"[compare-csv] {pname}: {len(matched_cols)} match, "
          f"{len(diff_cols)} diff"
          + (f" ({', '.join(sorted(diff_cols)[:4])})" if diff_cols else ""))
