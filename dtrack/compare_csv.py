
import csv as _csv
import json
import os
import shutil
import tempfile

from .config import load_unified_config, get_col_type_overrides, get_ignored_columns
from .date_utils import bucket_date, parse_date
from .platforms.base import qualified_name, quote_ident, build_date_bounds_clause
from .compare import (
    _dialect, _full_table_and_cte, _run_query,
    compare_column_stats, _has_col_differences,
    _key_in_clause, _hash16_expr, _limit,
)
from .db import (
    get_column_meta, get_metadata, get_col_stats, get_row_counts,
    get_pair_col_map_from_db, save_col_comparison, register_table_pair,
)
from .loader import load_precomputed_col_stats
from .extract import extract_all

_CSV_SUPPORTED = ("aws", "oracle", "databricks", "csv", "hadoop")

# >>> fallback column-name discovery for any source: one row -> its keys are the cols <<< #
def _discover_names(tbl):
    src = (tbl.get("source") or "").lower()
    ft, cte = _full_table_and_cte(tbl)
    base = (tbl.get("where") or "").strip()
    where = f" WHERE {base}" if base else ""
    sql = f"{cte}SELECT * FROM {ft}{where} {_limit(src, 1)}"
    try:
        rows = _run_query(tbl, sql)
    except Exception:
        return []
    return list(rows[0].keys()) if rows else []

# >>> discover columns+TYPES (DESCRIBE) into _column_meta for tables missing it, <<< #
# >>> covering all 5 sources by reusing each platform's own discovery <<< #
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

# >>> when no col_map exists yet, auto-match columns by name (from _column_meta, <<< #
# >>> else a SELECT* name probe) and persist column_mapping.csv -> self-contained <<< #
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

# >>> live per-date COUNT(*) on a side (cheap; no data download): {canonical_dt: count} <<< #
def _date_counts(project_db, tbl, from_date, to_date):
    src = (tbl.get("source") or "").lower()
    d = _dialect(src)
    ft, cte = _full_table_and_cte(tbl)
    qname = qualified_name(tbl)
    types = _col_types(project_db, qname)
    dc = tbl.get("date_col", "")
    ddtype = types.get(dc.lower())
    dfmt = (get_metadata(project_db, qname) or {}).get("date_format")
    date_q = quote_ident(dc, d)
    base = (tbl.get("where") or "").strip()
    bounds = (build_date_bounds_clause(date_q, from_date, to_date, ddtype, date_format=dfmt)
              if (from_date or to_date) else "")
    where = " AND ".join(f"({p})" for p in (base, bounds) if p)
    sql = (f"{cte}SELECT {date_q} AS dt, COUNT(*) AS cnt FROM {ft}"
           + (f" WHERE {where}" if where else "")
           + f" GROUP BY {date_q}")
    out = {}
    for r in _run_query(tbl, sql):
        v = r.get("dt")
        if v is None:
            continue
        try:
            k = parse_date(str(v))
        except Exception:
            k = str(v)
        out[k] = out.get(k, 0) + int(r.get("cnt") or 0)
    return out

# >>> count-MATCH dates for a pair; reuse _row_counts if loaded, else live COUNT(*) <<< #
def _matched_dates(project_db, pname, left, right, tl, tr, from_date, to_date):
    rl = {dt: c for dt, c in get_row_counts(project_db, tl, from_date, to_date) if dt}
    rr = {dt: c for dt, c in get_row_counts(project_db, tr, from_date, to_date) if dt}
    origin = "_row_counts"
    if not rl or not rr:
        rl = _date_counts(project_db, left, from_date, to_date)
        rr = _date_counts(project_db, right, from_date, to_date)
        origin = "live COUNT(*)"
    common = set(rl) & set(rr)
    matched = sorted(dt for dt in common if rl[dt] == rr[dt])
    print(f"[compare-csv] {pname}: dates from {origin} -> "
          f"{len(matched)} count-match, {len(common) - len(matched)} count-diff, "
          f"{len(set(rl) ^ set(rr))} single-side")
    return matched, rl

# >>> (base where AND date-window bounds) for a side, '' if none <<< #
def _window_where(project_db, tbl, date_col, dates):
    src = (tbl.get("source") or "").lower()
    d = _dialect(src)
    qname = qualified_name(tbl)
    types = _col_types(project_db, qname)
    ddtype = types.get((date_col or "").lower())
    dfmt = (get_metadata(project_db, qname) or {}).get("date_format")
    date_q = quote_ident(date_col, d)
    bounds = build_date_bounds_clause(date_q, min(dates), max(dates), ddtype,
                                      date_format=dfmt)
    base = (tbl.get("where") or "").strip()
    return " AND ".join(f"({p})" for p in (base, bounds) if p)

# >>> canonical key form so differing source representations align: <<< #
# >>> UPPER(TRIM(CAST(col AS <string>))) -- fixes numeric-vs-string + CHAR padding + case <<< #
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
    rows = _run_query(tbl, sql)
    return int(rows[0].get("c") or 0) if rows else 0

# >>> deterministic hash-sample of NORMALIZED composite-key tuples from the LEFT, window-scoped <<< #
def _window_keys(project_db, left, key_cols, dates, sample):
    src = (left.get("source") or "").lower()
    d = _dialect(src)
    where = _window_where(project_db, left, left.get("date_col", ""), dates)
    nexprs = [_norm_key_expr(d, quote_ident(k, d)) for k in key_cols]
    if isinstance(sample, float):
        total = _distinct_key_count(left if where == "" else {**left, "where": ""},
                                    key_cols, where)
        n = max(1, round(sample * total))
    else:
        n = int(sample)
    ft, cte = _full_table_and_cte(left)
    notnull = " AND ".join(f"{quote_ident(k, d)} IS NOT NULL" for k in key_cols)
    w = " AND ".join(f"({p})" for p in (where, notnull) if p) or "1=1"
    sel = ", ".join(f"{e} AS k{i}" for i, e in enumerate(nexprs))
    sql = (f"{cte}SELECT {sel} FROM {ft} WHERE {w} "
           f"GROUP BY {', '.join(nexprs)} "
           f"ORDER BY {_hash16_expr(src, nexprs[0])} {_limit(src, n)}")
    rows = _run_query(left, sql)
    return [tuple(r.get(f"k{i}") for i in range(len(key_cols))) for r in rows]

# >>> per-side NORMALIZED composite-key IN (...) for the sampled tuples <<< #
def _key_filters(left, right, key_cols, right_key_cols, key_tuples):
    dl = _dialect((left.get("source") or "").lower())
    dr = _dialect((right.get("source") or "").lower())
    lq = [_norm_key_expr(dl, quote_ident(k, dl)) for k in key_cols]
    rq = [_norm_key_expr(dr, quote_ident(rk, dr)) for rk in right_key_cols]
    return _key_in_clause(lq, key_tuples), _key_in_clause(rq, key_tuples)

# >>> pull one side's raw mapped-column rows for `dates` -> temp CSV + inline column types <<< #
def _pull_side(project_db, tbl, value_cols, date_col, dates, tmpdir, tag,
               extra_where=None):
    src = (tbl.get("source") or "").lower()
    d = _dialect(src)
    ft, cte = _full_table_and_cte(tbl)
    qname = qualified_name(tbl)
    types = _col_types(project_db, qname)

    date_q = quote_ident(date_col, d)
    cols_q = [date_q] + [quote_ident(c, d) for c in value_cols]
    win = _window_where(project_db, tbl, date_col, dates)
    where = " AND ".join(f"({p})" for p in (win, extra_where) if p) or "1=1"
    sql = f"{cte}SELECT {', '.join(cols_q)} FROM {ft} WHERE {where}"
    rows = _run_query(tbl, sql)

    out_csv = os.path.join(tmpdir, f"{tag}.csv")
    header = [date_col] + list(value_cols)
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

    # faithful types from _column_meta when ALL value cols are known; otherwise
    # leave columns=None so DuckDB auto-infers (keeps numeric cols numeric on a
    # cold project.db, where no col extraction has recorded types yet)
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

# >>> compute col stats locally over a pulled CSV (whole window = one bucket) and <<< #
# >>> upsert them under the real qname, stamped with the window's bucket date <<< #
def _extract_and_load(project_db, src_tbl, csv_path, columns, real_qname,
                      date_col, dt_label, vintage, preset, stats_override, tmpdir, tag):
    csv_cfg = {
        "name": tag, "table": tag, "source": "csv",
        "conn_macro": csv_path, "date_col": date_col,
        "date_type": "string_dash", "vintage": "all",
    }
    if columns:
        csv_cfg["columns"] = columns
    temp_cfg = {"pairs": {"_w": {"left": csv_cfg, "right": csv_cfg}}, "metadata": {}}
    cfg_path = os.path.join(tmpdir, f"{tag}.config.json")
    with open(cfg_path, "w") as f:
        json.dump(temp_cfg, f)
    extract_all(config_path=cfg_path, outdir=tmpdir, types=["col"],
                db_path=None, vintage="all", preset=preset,
                stats_override=stats_override)
    col_csv = os.path.join(tmpdir, "csv", f"{qualified_name(csv_cfg)}_col.csv")
    _stamp_dt(col_csv, dt_label)
    return load_precomputed_col_stats(
        db_path=project_db, csv_path=col_csv, table_name=real_qname,
        mode="upsert", source=src_tbl.get("source"),
        db_name=src_tbl.get("conn_macro"), vintage=vintage)

# >>> bucket-keys already present in _col_stats for a table (for --resume) <<< #
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

# >>> per-vintage-window pull -> local DuckDB col stats -> upsert, then compare+save <<< #
def run_compare_csv(project_db, config_path, pair=None, vintage="day",
                    from_date=None, to_date=None, preset=None,
                    stats_override=None, keep_csv=False, resume=False,
                    sample=None, key=None):
    config = load_unified_config(config_path)
    pairs = config.get("pairs", {})
    targets = ([pair] if pair else
               [n for n, p in pairs.items() if not p.get("skip")])

    # CLI --key/--sample are defaults; per-pair "key"/"sample" fields override
    # them (resolved per pair in _run_pair, validated against that pair's col_map)
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
            # discover column types into _column_meta if missing (so date bounds +
            # typing are faithful) -- no prior `e2e row` / load-columns needed
            tmpd = tempfile.mkdtemp(prefix="dtrack_ccsv_meta_")
            try:
                _ensure_column_meta(project_db, [left, right], tmpd)
            finally:
                shutil.rmtree(tmpd, ignore_errors=True)
            col_map = (pcfg.get("col_map")
                       or get_pair_col_map_from_db(project_db, pname) or {})
            if not col_map:
                # never generated -> discover + auto-match + persist column_mapping.csv
                col_map = _ensure_col_map(project_db, config, config_path,
                                          pname, left, right)
            register_table_pair(project_db, pname, tl, tr,
                                source_left=left.get("source"),
                                source_right=right.get("source"),
                                col_mappings=col_map or None)
            _run_pair(project_db, pname, pcfg, tl, tr, col_map, config,
                      vintage, from_date, to_date, preset, stats_override,
                      keep_csv, resume, sample, cli_key_cols)

# >>> resolve a pair's key cols (per-pair `key` field, else CLI --key) and their <<< #
# >>> right-side names via col_map; RAISE if a key col isn't mapped <<< #
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

# >>> resolve a pair's sample (per-pair `sample` field, else CLI --sample); <<< #
# >>> int = fixed #keys, float in (0,1] = fraction of keys; None = pull all <<< #
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
def _run_pair(project_db, pname, pcfg, tl, tr, col_map, config,
              vintage, from_date, to_date, preset, stats_override,
              keep_csv, resume, cli_sample=None, cli_key_cols=None):
    left, right = pcfg["left"], pcfg["right"]
    dc_left = left.get("date_col", "")
    dc_right = right.get("date_col", "")
    left_cols = [c for c in col_map if c.lower() != dc_left.lower()]
    right_cols = [col_map[c] for c in left_cols]

    sample = _resolve_sample(pname, pcfg, cli_sample)
    key_cols, right_key_cols = _resolve_key(pname, pcfg, cli_key_cols, col_map)
    if sample is not None and not key_cols:
        raise ValueError(f"{pname}: sample needs a key — set a per-pair 'key' "
                         f"in the config or pass --key")

    matched, counts = _matched_dates(project_db, pname, left, right, tl, tr,
                                     from_date, to_date)
    if not matched:
        print(f"[compare-csv] {pname}: no count-matching dates; skipping")
        return

    buckets = {}
    for dt in matched:
        buckets.setdefault(bucket_date(dt, vintage), []).append(dt)

    done_l = _loaded_buckets(project_db, tl, vintage) if resume else set()
    done_r = _loaded_buckets(project_db, tr, vintage) if resume else set()

    print(f"[compare-csv] {pname}: {len(matched)} count-match dates -> "
          f"{len(buckets)} {vintage} window(s)")
    for bkey in sorted(buckets):
        dates = buckets[bkey]
        if resume and bkey in done_l and bkey in done_r:
            print(f"  {bkey}: already loaded; skip (--resume)")
            continue
        tmpdir = tempfile.mkdtemp(prefix=f"dtrack_ccsv_{bkey}_")
        try:
            total = sum(counts.get(dt, 0) for dt in dates)
            key_in_l = key_in_r = None
            nkeys = None
            if sample is not None and key_cols:
                ktuples = _window_keys(project_db, left, key_cols, dates, sample)
                nkeys = len(ktuples)
                key_in_l, key_in_r = _key_filters(left, right, key_cols,
                                                   right_key_cols, ktuples)
            lc, _, lcols, pl = _pull_side(project_db, left, left_cols, dc_left,
                                          dates, tmpdir, "left", extra_where=key_in_l)
            rc, _, rcols, pr = _pull_side(project_db, right, right_cols, dc_right,
                                          dates, tmpdir, "right", extra_where=key_in_r)
            nl = _extract_and_load(project_db, left, lc, lcols, tl, dc_left,
                                   bkey, vintage, preset, stats_override, tmpdir, "left")
            nr = _extract_and_load(project_db, right, rc, rcols, tr, dc_right,
                                   bkey, vintage, preset, stats_override, tmpdir, "right")
            keyinfo = f" on {nkeys} sampled key(s)" if nkeys is not None else ""
            print(f"  {bkey}: {len(dates)} date(s) | records sample "
                  f"{pl}/{total} (L) {pr}/{total} (R){keyinfo} | "
                  f"col-stats {nl}/{nr}")
            if keep_csv:
                print(f"    kept tmp: {tmpdir}")
        finally:
            if not keep_csv:
                shutil.rmtree(tmpdir, ignore_errors=True)

    _compare_and_save(project_db, config, pname, tl, tr, col_map,
                      set(matched), from_date, to_date)

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
