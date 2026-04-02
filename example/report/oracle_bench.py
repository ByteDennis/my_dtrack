#!/usr/bin/env python3
"""Oracle SQL Performance Experiment Framework.

Runs experiments against a real Oracle DB (read-only access),
collecting Python-side timing and row counts. No v$sql or DBA access required.

Usage:
    python oracle_bench.py --list
    python oracle_bench.py --config bench_config.json --experiments index_where --table SCHEMA.MY_TABLE --date-col LOAD_DT --num-col AMOUNT
    python oracle_bench.py --config bench_config.json --experiments dtrack_row_count --table SCHEMA.MY_TABLE --date-col LOAD_DT
    python oracle_bench.py --config bench_config.json --iterations 3
"""

import argparse
import json
import os
import sys
import time

import oracledb


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def load_config(path):
    with open(path) as f:
        cfg = json.load(f)
    for key in ("user", "password", "dsn"):
        val = cfg.get(key, "")
        if isinstance(val, str) and val.startswith("ENV:"):
            env_var = val[4:]
            cfg[key] = os.environ.get(env_var, "")
            if not cfg[key]:
                print(f"WARNING: env var {env_var} is not set")
    return cfg


def get_connection(cfg):
    return oracledb.connect(user=cfg["user"], password=cfg["password"], dsn=cfg["dsn"])


# ---------------------------------------------------------------------------
# Timing helper
# ---------------------------------------------------------------------------

def run_timed(conn, sql, params=None, arraysize=5000, fetch=True):
    """Execute SQL, fetch all rows, return elapsed seconds and row count."""
    cur = conn.cursor()
    cur.arraysize = arraysize
    start = time.perf_counter()
    cur.execute(sql, params or {})
    if fetch:
        rows = cur.fetchall()
        elapsed = time.perf_counter() - start
        row_count = len(rows)
    else:
        elapsed = time.perf_counter() - start
        row_count = cur.rowcount
    cur.close()
    return {"elapsed_sec": round(elapsed, 6), "rows": row_count}


# ---------------------------------------------------------------------------
# Runtime table/column context — set via CLI args, used by all experiments
# ---------------------------------------------------------------------------

class Ctx:
    """Mutable context shared by all experiments."""
    table = ""       # e.g. SCHEMA.TABLE_NAME
    date_col = ""    # e.g. LOAD_DT
    num_col = ""     # e.g. AMOUNT (numeric column for stats)
    cat_col = ""     # e.g. STATUS (categorical column for top10)
    pk_col = ""      # e.g. ID (primary key column for bind var test)
    where = ""       # optional WHERE filter


CTX = Ctx()


# ---------------------------------------------------------------------------
# Experiment definitions
# ---------------------------------------------------------------------------

EXPERIMENTS = {}


def experiment(name, description=""):
    def decorator(cls):
        cls.name = name
        cls.description = description
        EXPERIMENTS[name] = cls()
        return cls
    return decorator


class BaseExperiment:
    name = ""
    description = ""

    def variants(self, ctx):
        """Return list of {name, sql, params?, arraysize?}."""
        raise NotImplementedError


# ---- 1: index_where — TRUNC(date) vs range ----

@experiment("index_where", "WHERE: TRUNC(date_col) vs date range")
class IndexWhereExperiment(BaseExperiment):
    def variants(self, ctx):
        t, d = ctx.table, ctx.date_col
        return [
            {"name": "slow_trunc", "sql": f"""
                SELECT COUNT(*) FROM {t}
                WHERE TRUNC({d}) = DATE '2024-01-15'
            """},
            {"name": "fast_range", "sql": f"""
                SELECT COUNT(*) FROM {t}
                WHERE {d} >= DATE '2024-01-15' AND {d} < DATE '2024-01-16'
            """},
        ]


# ---- 2: index_groupby — GROUP BY TRUNC vs direct ----

@experiment("index_groupby", "GROUP BY: TRUNC(date_col) vs direct column")
class IndexGroupByExperiment(BaseExperiment):
    def variants(self, ctx):
        t, d = ctx.table, ctx.date_col
        return [
            {"name": "slow_trunc_groupby", "sql": f"""
                SELECT TRUNC({d}) AS dt, COUNT(*)
                FROM {t} GROUP BY TRUNC({d}) ORDER BY 1
            """},
            {"name": "fast_direct_groupby", "sql": f"""
                SELECT {d} AS dt, COUNT(*)
                FROM {t} GROUP BY {d} ORDER BY 1
            """},
        ]


# ---- 3: cte_materialize — MATERIALIZE hint ----

@experiment("cte_materialize", "CTE with vs without MATERIALIZE hint")
class CTEMaterializeExperiment(BaseExperiment):
    def variants(self, ctx):
        t, d, n = ctx.table, ctx.date_col, ctx.num_col or "1"
        return [
            {"name": "no_hint", "sql": f"""
                WITH daily_avg AS (
                    SELECT {d}, AVG({n}) as avg_val FROM {t} GROUP BY {d}
                )
                SELECT d.{d}, d.avg_val, COUNT(*) as cnt
                FROM daily_avg d JOIN {t} s ON s.{d} = d.{d}
                GROUP BY d.{d}, d.avg_val
            """},
            {"name": "materialize_hint", "sql": f"""
                WITH daily_avg AS (
                    SELECT /*+ MATERIALIZE */ {d}, AVG({n}) as avg_val FROM {t} GROUP BY {d}
                )
                SELECT d.{d}, d.avg_val, COUNT(*) as cnt
                FROM daily_avg d JOIN {t} s ON s.{d} = d.{d}
                GROUP BY d.{d}, d.avg_val
            """},
        ]


# ---- 4: cte_simple — CTE overhead for simple queries ----

@experiment("cte_simple", "Simple aggregation: CTE vs direct query")
class CTESimpleExperiment(BaseExperiment):
    def variants(self, ctx):
        t, d, n = ctx.table, ctx.date_col, ctx.num_col or "1"
        return [
            {"name": "with_cte", "sql": f"""
                WITH means AS (SELECT AVG({n}) as avg_val, COUNT(*) as cnt FROM {t})
                SELECT * FROM means
            """},
            {"name": "direct", "sql": f"""
                SELECT AVG({n}) as avg_val, COUNT(*) as cnt FROM {t}
            """},
        ]


# ---- 5: parallel_query — serial vs PARALLEL ----

@experiment("parallel_query", "Serial vs PARALLEL(4) execution")
class ParallelQueryExperiment(BaseExperiment):
    def variants(self, ctx):
        t, d, n = ctx.table, ctx.date_col, ctx.num_col or "1"
        return [
            {"name": "serial", "sql": f"""
                SELECT /*+ NO_PARALLEL */ {d}, COUNT(*), AVG({n}) AS avg_val
                FROM {t} GROUP BY {d}
            """},
            {"name": "parallel_4", "sql": f"""
                SELECT /*+ PARALLEL(t, 4) */ {d}, COUNT(*), AVG({n}) AS avg_val
                FROM {t} t GROUP BY {d}
            """},
        ]


# ---- 6: arraysize — Python fetch tuning ----

@experiment("arraysize", "Python fetch arraysize 100 vs 5000")
class ArraysizeExperiment(BaseExperiment):
    def variants(self, ctx):
        t = ctx.table
        return [
            {"name": "arraysize_100", "sql": f"SELECT * FROM {t} WHERE ROWNUM <= 200000",
             "arraysize": 100},
            {"name": "arraysize_5000", "sql": f"SELECT * FROM {t} WHERE ROWNUM <= 200000",
             "arraysize": 5000},
        ]


# ---- 7: bind_variables — literal vs bind ----

@experiment("bind_variables", "Literal values vs bind parameters (500 lookups)")
class BindVariablesExperiment(BaseExperiment):
    def variants(self, ctx):
        return [
            {"name": "literals", "sql": "__literal_loop__"},
            {"name": "bind_vars", "sql": "__bind_loop__"},
        ]

    def custom_run(self, conn, variant_name, ctx, arraysize):
        t, pk = ctx.table, ctx.pk_col or "ROWID"
        elapsed_total = 0.0
        row_total = 0
        if pk == "ROWID":
            # Fetch 500 rowids to use as lookup keys
            cur = conn.cursor()
            cur.execute(f"SELECT ROWID FROM {t} WHERE ROWNUM <= 500")
            rowids = [r[0] for r in cur.fetchall()]
            cur.close()
            if variant_name == "literals":
                for rid in rowids:
                    r = run_timed(conn, f"SELECT * FROM {t} WHERE ROWID = '{rid}'",
                                  arraysize=arraysize)
                    elapsed_total += r["elapsed_sec"]
                    row_total += r["rows"]
            else:
                for rid in rowids:
                    r = run_timed(conn, f"SELECT * FROM {t} WHERE ROWID = :rid",
                                  params={"rid": rid}, arraysize=arraysize)
                    elapsed_total += r["elapsed_sec"]
                    row_total += r["rows"]
        else:
            if variant_name == "literals":
                for i in range(1, 501):
                    r = run_timed(conn, f"SELECT * FROM {t} WHERE {pk} = {i}",
                                  arraysize=arraysize)
                    elapsed_total += r["elapsed_sec"]
                    row_total += r["rows"]
            else:
                for i in range(1, 501):
                    r = run_timed(conn, f"SELECT * FROM {t} WHERE {pk} = :id",
                                  params={"id": i}, arraysize=arraysize)
                    elapsed_total += r["elapsed_sec"]
                    row_total += r["rows"]
        return {"elapsed_sec": round(elapsed_total, 6), "rows": row_total}


# ---- 8: partition_pruning — TO_CHAR vs date range ----

@experiment("partition_pruning", "TO_CHAR filter vs date range")
class PartitionPruningExperiment(BaseExperiment):
    def variants(self, ctx):
        t, d = ctx.table, ctx.date_col
        return [
            {"name": "slow_to_char", "sql": f"""
                SELECT COUNT(*) FROM {t}
                WHERE TO_CHAR({d}, 'YYYY-MM') = '2024-01'
            """},
            {"name": "fast_date_range", "sql": f"""
                SELECT COUNT(*) FROM {t}
                WHERE {d} >= DATE '2024-01-01' AND {d} < DATE '2024-02-01'
            """},
        ]


# ---- 9: result_cache — RESULT_CACHE hint ----

@experiment("result_cache", "With vs without RESULT_CACHE hint")
class ResultCacheExperiment(BaseExperiment):
    def variants(self, ctx):
        t, d, n = ctx.table, ctx.date_col, ctx.num_col or "1"
        return [
            {"name": "no_cache", "sql": f"""
                SELECT {d}, COUNT(*), AVG({n}) AS avg_val FROM {t} GROUP BY {d}
            """},
            {"name": "result_cache", "sql": f"""
                SELECT /*+ RESULT_CACHE */ {d}, COUNT(*), AVG({n}) AS avg_val FROM {t} GROUP BY {d}
            """},
        ]


# ---- 10: fetch_batch — Python row-by-row vs fetchmany (bulk collect equivalent) ----

@experiment("fetch_batch", "Python fetchone() vs fetchmany(5000) — bulk collect equivalent")
class FetchBatchExperiment(BaseExperiment):
    def variants(self, ctx):
        return [
            {"name": "fetchone_loop", "sql": "__fetchone__"},
            {"name": "fetchmany_5000", "sql": "__fetchmany__"},
        ]

    def custom_run(self, conn, variant_name, ctx, arraysize):
        t = ctx.table
        sql = f"SELECT * FROM {t} WHERE ROWNUM <= 100000"
        cur = conn.cursor()
        start = time.perf_counter()
        cur.execute(sql)
        row_count = 0
        if variant_name == "fetchone_loop":
            cur.arraysize = 1
            while True:
                row = cur.fetchone()
                if row is None:
                    break
                row_count += 1
        else:
            cur.arraysize = 5000
            while True:
                batch = cur.fetchmany(5000)
                if not batch:
                    break
                row_count += len(batch)
        elapsed = time.perf_counter() - start
        cur.close()
        return {"elapsed_sec": round(elapsed, 6), "rows": row_count}


# ---- 11: dtrack_row_count — dtrack row count SQL variants ----

@experiment("dtrack_row_count", "dtrack row count: TRUNC GROUP BY vs direct vs parallel")
class DtrackRowCountExperiment(BaseExperiment):
    def variants(self, ctx):
        t, d = ctx.table, ctx.date_col
        w = f"WHERE {ctx.where}" if ctx.where else ""
        return [
            {"name": "trunc_groupby", "sql": f"""
                SELECT TRUNC({d}) AS date_value, COUNT(*) AS row_count
                FROM {t} {w} GROUP BY TRUNC({d})
            """},
            {"name": "direct_groupby", "sql": f"""
                SELECT {d} AS date_value, COUNT(*) AS row_count
                FROM {t} {w} GROUP BY {d}
            """},
            {"name": "parallel_groupby", "sql": f"""
                SELECT /*+ PARALLEL(t, 4) */ TRUNC({d}) AS date_value, COUNT(*) AS row_count
                FROM {t} t {w} GROUP BY TRUNC({d})
            """},
        ]


# ---- 12: dtrack_col_stats — dtrack column statistics SQL ----

@experiment("dtrack_col_stats", "dtrack column stats: serial vs parallel")
class DtrackColStatsExperiment(BaseExperiment):
    def variants(self, ctx):
        t, d = ctx.table, ctx.date_col
        n = ctx.num_col
        if not n:
            return [{"name": "skip", "sql": "SELECT 1 FROM dual"}]
        w = f"AND {ctx.where}" if ctx.where else ""
        return [
            {"name": "serial_stats", "sql": f"""
                SELECT {d} AS dt, '{n}' AS column_name, 'numeric' AS col_type,
                    COUNT(*) AS n_total,
                    SUM(CASE WHEN {n} IS NULL THEN 1 ELSE 0 END) AS n_missing,
                    COUNT(DISTINCT {n}) AS n_unique,
                    AVG({n}) AS mean, STDDEV({n}) AS std,
                    MIN({n}) AS min_val, MAX({n}) AS max_val
                FROM {t} WHERE 1=1 {w} GROUP BY {d}
            """},
            {"name": "parallel_stats", "sql": f"""
                SELECT /*+ PARALLEL(t, 4) */ {d} AS dt, '{n}' AS column_name, 'numeric' AS col_type,
                    COUNT(*) AS n_total,
                    SUM(CASE WHEN {n} IS NULL THEN 1 ELSE 0 END) AS n_missing,
                    COUNT(DISTINCT {n}) AS n_unique,
                    AVG({n}) AS mean, STDDEV({n}) AS std,
                    MIN({n}) AS min_val, MAX({n}) AS max_val
                FROM {t} t WHERE 1=1 {w} GROUP BY {d}
            """},
        ]


# ---- 13: dtrack_top10 — dtrack top-10 frequency SQL ----

@experiment("dtrack_top10", "dtrack top-10 frequency: serial vs parallel")
class DtrackTop10Experiment(BaseExperiment):
    def variants(self, ctx):
        t, d = ctx.table, ctx.date_col
        c = ctx.cat_col or ctx.num_col
        if not c:
            return [{"name": "skip", "sql": "SELECT 1 FROM dual"}]
        w = f"AND {ctx.where}" if ctx.where else ""
        return [
            {"name": "serial_top10", "sql": f"""
                SELECT dt, val, cnt FROM (
                    SELECT {d} AS dt, CAST({c} AS VARCHAR(200)) AS val, COUNT(*) AS cnt,
                           ROW_NUMBER() OVER (PARTITION BY {d} ORDER BY COUNT(*) DESC) AS rn
                    FROM {t} WHERE {c} IS NOT NULL {w}
                    GROUP BY {d}, {c}
                ) WHERE rn <= 10
            """},
            {"name": "parallel_top10", "sql": f"""
                SELECT dt, val, cnt FROM (
                    SELECT /*+ PARALLEL(t, 4) */ {d} AS dt, CAST({c} AS VARCHAR(200)) AS val,
                           COUNT(*) AS cnt,
                           ROW_NUMBER() OVER (PARTITION BY {d} ORDER BY COUNT(*) DESC) AS rn
                    FROM {t} t WHERE {c} IS NOT NULL {w}
                    GROUP BY {d}, {c}
                ) WHERE rn <= 10
            """},
        ]


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run_experiment(conn, exp, ctx, iterations, arraysize):
    results = []
    variants = exp.variants(ctx)
    for v in variants:
        vname = v["name"]
        v_arraysize = v.get("arraysize", arraysize)
        for run_idx in range(1, iterations + 1):
            if hasattr(exp, "custom_run") and v["sql"].startswith("__"):
                result = exp.custom_run(conn, vname, ctx, v_arraysize)
            else:
                result = run_timed(conn, v["sql"], arraysize=v_arraysize)

            results.append({
                "experiment": exp.name,
                "variant": vname,
                "run": run_idx,
                "elapsed_sec": result["elapsed_sec"],
                "rows": result["rows"],
                "table": ctx.table,
            })
            print(f"  {vname} run {run_idx}: {result['elapsed_sec']:.4f}s ({result['rows']} rows)")
    return results


def main():
    parser = argparse.ArgumentParser(description="Oracle SQL Performance Benchmark (read-only)")
    parser.add_argument("--config", default="bench_config.json", help="Config file path")
    parser.add_argument("--experiments", help="Comma-separated experiment names (default: all)")
    parser.add_argument("--iterations", type=int, help="Runs per variant (overrides config)")
    parser.add_argument("--output", default="bench_results.json", help="Output JSON file")
    parser.add_argument("--list", action="store_true", help="List available experiments")

    # Table/column args — swap these to test different tables
    parser.add_argument("--table", required=False, help="Table name, e.g. SCHEMA.MY_TABLE")
    parser.add_argument("--date-col", required=False, help="Date column name, e.g. LOAD_DT")
    parser.add_argument("--num-col", default="", help="Numeric column for stats, e.g. AMOUNT")
    parser.add_argument("--cat-col", default="", help="Categorical column for top10, e.g. STATUS")
    parser.add_argument("--pk-col", default="", help="Primary key column for bind var test, e.g. ID")
    parser.add_argument("--where", default="", help="Extra WHERE filter (without WHERE keyword)")
    parser.add_argument("--append", action="store_true",
                        help="Append to existing output file instead of overwriting")
    args = parser.parse_args()

    if args.list:
        print("Available experiments:")
        for name, exp in sorted(EXPERIMENTS.items()):
            print(f"  {name:25s} {exp.description}")
        print("\nRequired args: --table TABLE --date-col COL")
        print("Optional args: --num-col COL --cat-col COL --pk-col COL --where FILTER")
        return

    if not args.table or not args.date_col:
        parser.error("--table and --date-col are required (use --list to see experiments)")

    CTX.table = args.table
    CTX.date_col = args.date_col
    CTX.num_col = args.num_col
    CTX.cat_col = args.cat_col
    CTX.pk_col = args.pk_col
    CTX.where = args.where

    cfg = load_config(args.config)
    iterations = args.iterations or cfg.get("iterations", 5)
    arraysize = cfg.get("arraysize", 5000)

    if args.experiments:
        names = [n.strip() for n in args.experiments.split(",")]
        for n in names:
            if n not in EXPERIMENTS:
                print(f"ERROR: unknown experiment '{n}'")
                sys.exit(1)
    else:
        names = list(EXPERIMENTS.keys())

    conn = get_connection(cfg)
    print(f"Connected to {cfg['dsn']}")
    print(f"Table: {CTX.table}  date_col: {CTX.date_col}  num_col: {CTX.num_col or '(none)'}  "
          f"cat_col: {CTX.cat_col or '(none)'}  pk_col: {CTX.pk_col or '(none)'}")

    all_results = []
    for name in names:
        exp = EXPERIMENTS[name]
        print(f"\n=== {name}: {exp.description} ===")
        results = run_experiment(conn, exp, CTX, iterations, arraysize)
        all_results.extend(results)

    conn.close()

    # Append or overwrite
    if args.append and os.path.exists(args.output):
        with open(args.output) as f:
            existing = json.load(f)
        existing.extend(all_results)
        all_results = existing

    with open(args.output, "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"\nResults saved to {args.output} ({len(all_results)} records)")


if __name__ == "__main__":
    main()
