
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
)
from .db import (
    get_column_meta, get_metadata, get_col_stats, get_row_counts,
    get_pair_col_map_from_db, save_col_comparison, register_table_pair,
)
from .loader import load_precomputed_col_stats
from .extract import extract_all

_CSV_SUPPORTED = ("aws", "oracle", "databricks", "csv", "hadoop")

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
    return matched

# >>> pull one side's raw mapped-column rows for `dates` -> temp CSV + inline column types <<< #
def _pull_side(project_db, tbl, value_cols, date_col, dates, tmpdir, tag):
    src = (tbl.get("source") or "").lower()
    d = _dialect(src)
    ft, cte = _full_table_and_cte(tbl)
    qname = qualified_name(tbl)
    types = _col_types(project_db, qname)
    ddtype = types.get((date_col or "").lower())
    dfmt = (get_metadata(project_db, qname) or {}).get("date_format")

    date_q = quote_ident(date_col, d)
    cols_q = [date_q] + [quote_ident(c, d) for c in value_cols]
    bounds = build_date_bounds_clause(date_q, min(dates), max(dates), ddtype,
                                      date_format=dfmt)
    base = (tbl.get("where") or "").strip()
    where = " AND ".join(f"({p})" for p in (base, bounds) if p) or "1=1"
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
    vt = {c: types[c.lower()] for c in value_cols if c.lower() in types}
    columns = ({date_col: "VARCHAR", **vt}
               if value_cols and len(vt) == len(value_cols) else None)
    return out_csv, header, columns

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
                    stats_override=None, keep_csv=False, resume=False):
    config = load_unified_config(config_path)
    pairs = config.get("pairs", {})
    targets = ([pair] if pair else
               [n for n, p in pairs.items() if not p.get("skip")])

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
            col_map = (pcfg.get("col_map")
                       or get_pair_col_map_from_db(project_db, pname) or {})
            register_table_pair(project_db, pname, tl, tr,
                                source_left=left.get("source"),
                                source_right=right.get("source"),
                                col_mappings=col_map or None)
            _run_pair(project_db, pname, pcfg, tl, tr, col_map, config,
                      vintage, from_date, to_date, preset, stats_override,
                      keep_csv, resume)

# >>> drive one pair: enumerate matched-date buckets, pull/load each, then compare <<< #
def _run_pair(project_db, pname, pcfg, tl, tr, col_map, config,
              vintage, from_date, to_date, preset, stats_override,
              keep_csv, resume):
    left, right = pcfg["left"], pcfg["right"]
    dc_left = left.get("date_col", "")
    dc_right = right.get("date_col", "")
    left_cols = [c for c in col_map if c.lower() != dc_left.lower()]
    right_cols = [col_map[c] for c in left_cols]

    matched = _matched_dates(project_db, pname, left, right, tl, tr,
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
            lc, _, lcols = _pull_side(project_db, left, left_cols, dc_left,
                                      dates, tmpdir, "left")
            rc, _, rcols = _pull_side(project_db, right, right_cols, dc_right,
                                      dates, tmpdir, "right")
            nl = _extract_and_load(project_db, left, lc, lcols, tl, dc_left,
                                   bkey, vintage, preset, stats_override, tmpdir, "left")
            nr = _extract_and_load(project_db, right, rc, rcols, tr, dc_right,
                                   bkey, vintage, preset, stats_override, tmpdir, "right")
            print(f"  {bkey}: {len(dates)} date(s) -> L {nl} / R {nr} col-stat rows")
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
