"""Reusable source->target migration-parity harness for the container matrix.

Each engine adapter seeds a table with a given dataset and runs dtrack's own
row + col extraction SQL (build_col_sql / _build_row_sql_*) on the real engine,
returning canonical row/col stat rows. The harness loads both sides into a temp
project.db and runs compare_row_counts / compare_column_stats, so a migration
LEFT(source) -> RIGHT(target) is validated exactly as dtrack would in prod.
"""

import csv as _csv
import os

from dtrack.constants import COL_CSV_HEADERS, ROW_CSV_HEADERS
from dtrack.platforms.base import build_col_sql
from dtrack.platforms.oracle import _build_row_sql_oracle
from dtrack.platforms.hadoop import _build_row_sql_hadoop
from dtrack.platforms.athena import _build_row_sql_athena
from dtrack.platforms.databricks import _build_row_sql_databricks
from dtrack.platforms.csv_duckdb import _build_row_sql_csv
from dtrack.loader import load_row_counts, load_precomputed_col_stats
from dtrack.compare import compare_row_counts, compare_column_stats


# >>> columns extracted on every side: amt (numeric), ccy (categorical) <<< #
COLS = [("amt", "numeric"), ("ccy", "categorical")]


# >>> canonical col-stat dict (COL_CSV_HEADERS schema) from an engine result row <<< #
def _col_record(col, col_type, rec):
    g = lambda k: rec.get(k)
    return {
        "dt": "all", "column_name": col, "col_type": col_type,
        "n_total": g("n_total"), "n_missing": g("n_missing"),
        "n_unique": g("n_unique"), "mean": g("mean") or "", "std": g("std") or "",
        "min_val": g("min_val") if g("min_val") is not None else "",
        "max_val": g("max_val") if g("max_val") is not None else "",
        "top_10": g("top_10") or "", "length_avg": g("length_avg") or "",
        "length_max": g("length_max") or "", "hash_sum": g("hash_sum") or "",
    }


# >>> base adapter: subclasses provide seed() + the two select helpers <<< #
class EngineSide:
    dialect = None
    date_type = "string_dash"
    amt_dtype = "BIGINT"
    ccy_dtype = "VARCHAR"

    def full_table(self, table):
        return table

    def cfg(self, table):
        return {"name": table, "table": table, "source": self.dialect,
                "conn_macro": "", "date_col": "rpt_dt", "date_type": self.date_type}

    # >>> seed + extract row counts and per-col stats for one dataset <<< #
    def extract(self, table, data, strict):
        self.seed(table, data)
        cfg = self.cfg(table)
        ft = self.full_table(table)
        row_rows = self.select_rows(self.row_sql(cfg))
        row_canon = [(self._dt(dt), int(cnt)) for dt, cnt in row_rows]
        col_canon = []
        for col, col_type in COLS:
            dtype = self.amt_dtype if col == "amt" else self.ccy_dtype
            sql, _ = build_col_sql(self.dialect, cfg, col, dtype, ft,
                                   vintage="all", strict=strict)
            rec = self.select_dicts(sql)[0]
            col_canon.append(_col_record(col, col_type, rec))
        return row_canon, col_canon

    def row_sql(self, cfg):
        raise NotImplementedError

    def _dt(self, dt):
        return str(dt)


# >>> run a cursor SELECT, return list[tuple] <<< #
def _cur_rows(cur, sql):
    cur.execute(sql)
    return cur.fetchall()


# >>> run a cursor SELECT, return list[dict] keyed by lowercased column <<< #
def _cur_dicts(cur, sql):
    cur.execute(sql)
    cols = [d[0].split('.')[-1].lower() for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


class OracleSide(EngineSide):
    dialect = "oracle"
    amt_dtype = "NUMBER"
    ccy_dtype = "VARCHAR2"

    def __init__(self, cur):
        self.cur = cur

    def seed(self, table, data):
        self.cur.execute(f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {table}'; "
                         f"EXCEPTION WHEN OTHERS THEN NULL; END;")
        self.cur.execute(f"CREATE TABLE {table} "
                         f"(rpt_dt VARCHAR2(20), amt NUMBER, ccy VARCHAR2(20))")
        self.cur.executemany(
            f"INSERT INTO {table} (rpt_dt, amt, ccy) VALUES (:1, :2, :3)", data)
        self.cur.connection.commit()

    def row_sql(self, cfg):
        return _build_row_sql_oracle(cfg)

    def select_rows(self, sql):
        return _cur_rows(self.cur, sql)

    def select_dicts(self, sql):
        return _cur_dicts(self.cur, sql)


class HiveSide(EngineSide):
    dialect = "hadoop"

    def __init__(self, cur):
        self.cur = cur

    def seed(self, table, data):
        self.cur.execute(f"DROP TABLE IF EXISTS {table}")
        self.cur.execute(f"CREATE TABLE {table} "
                         f"(rpt_dt STRING, amt BIGINT, ccy STRING)")
        values = ", ".join(
            f"('{d}', {a}, '{c}')" for d, a, c in data)
        self.cur.execute(f"INSERT INTO {table} VALUES {values}")

    def row_sql(self, cfg):
        return _build_row_sql_hadoop(cfg)

    def select_rows(self, sql):
        return _cur_rows(self.cur, sql)

    def select_dicts(self, sql):
        return _cur_dicts(self.cur, sql)


class TrinoSide(EngineSide):
    dialect = "athena"

    def __init__(self, cur):
        self.cur = cur

    def full_table(self, table):
        return f"memory.default.{table}"

    def cfg(self, table):
        c = super().cfg(table)
        c["source"] = "aws"
        c["conn_macro"] = "memory.default"
        return c

    def seed(self, table, data):
        ft = self.full_table(table)
        self._run(f"DROP TABLE IF EXISTS {ft}")
        self._run(f"CREATE TABLE {ft} (rpt_dt varchar, amt integer, ccy varchar)")
        values = ", ".join(f"('{d}', {a}, '{c}')" for d, a, c in data)
        self._run(f"INSERT INTO {ft} VALUES {values}")

    def _run(self, sql):
        self.cur.execute(sql)
        self.cur.fetchall()

    def row_sql(self, cfg):
        return _build_row_sql_athena(cfg)

    def select_rows(self, sql):
        return _cur_rows(self.cur, sql)

    def select_dicts(self, sql):
        return _cur_dicts(self.cur, sql)


class SparkSide(EngineSide):
    dialect = "databricks"

    def __init__(self, spark):
        self.spark = spark

    def seed(self, table, data):
        df = self.spark.createDataFrame(
            [(str(d), int(a), str(c)) for d, a, c in data],
            ["rpt_dt", "amt", "ccy"])
        df.createOrReplaceTempView(table)

    def row_sql(self, cfg):
        return _build_row_sql_databricks(cfg)

    def select_rows(self, sql):
        return [(r[0], r[1]) for r in self.spark.sql(sql).collect()]

    def select_dicts(self, sql):
        return [{k.lower(): v for k, v in r.asDict().items()}
                for r in self.spark.sql(sql).collect()]


class DuckdbSide(EngineSide):
    dialect = "duckdb"

    def __init__(self, tmpdir):
        self.tmpdir = tmpdir

    def full_table(self, table):
        return self._path

    def cfg(self, table):
        c = super().cfg(table)
        c["source"] = "csv"
        c["conn_macro"] = self._path
        c["_read_types"] = {"rpt_dt": "VARCHAR", "amt": "BIGINT", "ccy": "VARCHAR"}
        return c

    def seed(self, table, data):
        self._path = os.path.join(self.tmpdir, f"{table}.csv")
        with open(self._path, "w", newline="") as f:
            w = _csv.writer(f)
            w.writerow(["rpt_dt", "amt", "ccy"])
            for d, a, c in data:
                w.writerow([d, a, c])

    def _conn(self):
        import duckdb
        return duckdb.connect(":memory:")

    def full_table_expr(self, table):
        from dtrack.platforms.csv_duckdb import _csv_from_clause
        return _csv_from_clause(self.cfg(table))

    def extract(self, table, data, strict):
        self.seed(table, data)
        cfg = self.cfg(table)
        from dtrack.platforms.csv_duckdb import _csv_from_clause
        ft = _csv_from_clause(cfg)
        con = self._conn()
        try:
            rrows = con.execute(_build_row_sql_csv(cfg)).fetchall()
            row_canon = [(str(dt), int(cnt)) for dt, cnt in rrows]
            col_canon = []
            for col, col_type in COLS:
                dtype = self.amt_dtype if col == "amt" else self.ccy_dtype
                sql, _ = build_col_sql("duckdb", cfg, col, dtype, ft,
                                       vintage="all", strict=strict)
                cur = con.execute(sql)
                cols = [d[0].lower() for d in cur.description]
                rec = dict(zip(cols, cur.fetchall()[0]))
                col_canon.append(_col_record(col, col_type, rec))
            return row_canon, col_canon
        finally:
            con.close()


# >>> write canonical row/col CSVs and load both sides into a temp project.db <<< #
def _load_side(db, outdir, name, row_canon, col_canon):
    rp = os.path.join(outdir, f"{name}_row.csv")
    with open(rp, "w", newline="") as f:
        w = _csv.writer(f)
        w.writerow(ROW_CSV_HEADERS)
        for dt, cnt in row_canon:
            w.writerow([dt, cnt])
    cp = os.path.join(outdir, f"{name}_col.csv")
    with open(cp, "w", newline="") as f:
        w = _csv.DictWriter(f, fieldnames=COL_CSV_HEADERS)
        w.writeheader()
        for r in col_canon:
            w.writerow({k: r.get(k, "") for k in COL_CSV_HEADERS})
    load_row_counts(db, rp, name, source="csv")
    load_precomputed_col_stats(db, cp, name, source="csv", vintage="all")


# >>> seed+extract both sides, load, compare; return (row_cmp, col_cmp) <<< #
def run_parity(tmp_path, left_side, right_side, left_data, right_data, strict):
    from dtrack.db import init_database
    db = os.path.join(str(tmp_path), f"proj_{int(strict)}.db")
    init_database(db)
    outdir = os.path.join(str(tmp_path), f"out_{int(strict)}")
    os.makedirs(outdir, exist_ok=True)

    l_row, l_col = left_side.extract("mig_src", left_data, strict)
    r_row, r_col = right_side.extract("mig_tgt", right_data, strict)
    _load_side(db, outdir, "mig_src", l_row, l_col)
    _load_side(db, outdir, "mig_tgt", r_row, r_col)

    row_cmp = compare_row_counts(db, "mig_src", "mig_tgt")
    col_cmp = compare_column_stats(db, "mig_src", "mig_tgt", level="fine")
    return row_cmp, col_cmp
