"""Constants for dtrack UI configuration."""

# ==========================================================================
# Extract CSV / SQL-block format. Shared across all platforms (Oracle,
# Hadoop, Athena) so the gen-first runners and the load-row / load-col
# downstream parsers see the same shape regardless of source.
# ==========================================================================

# Header row written at the top of every {qname}_col.csv. The runners
# write rows in this exact column order; load-col reads by name so order
# is purely cosmetic, but keeping it canonical helps cross-platform diffs.
COL_CSV_HEADERS = [
    'dt', 'column_name', 'col_type', 'n_total', 'n_missing',
    'n_unique', 'mean', 'std', 'min_val', 'max_val', 'top_10',
    'length_avg', 'length_max', 'hash_sum',
]

ROW_CSV_HEADERS = ['dt', 'row_count']

# Marker comments embedded in extract_{row,col}.sql blocks. The runners
# strip these out before sending SQL to the engine; they only carry
# metadata for the dispatcher.
SQL_MARKER_CONN_MACRO  = '@conn_macro'
SQL_MARKER_DEGRADE_SQL = '@degrade_sql'
SQL_MARKER_END_DEGRADE = '@end_degrade'

# Stats presets — drives the col_gen page's preset radio and the
# /api/generate `preset` parameter. Each preset is a set of stat
# names that the col SQL builder includes; stats not in the set are
# emitted as '' in the result CSV so the canonical 14-column schema
# stays consistent across presets.
#
# Tradeoffs (Hadoop-relevant):
#   - quick: flat aggregates, NO freq table -> cheapest, partition-prune
#     friendly. Categorical min/max are scalar (alphabetic MIN/MAX of
#     the column), NOT the "val=cnt" form that requires the freq table.
#   - standard: adds n_unique (COUNT DISTINCT) + hash_sum (portable
#     md5-truncated-to-int16; single-pass SUM(hash16(col)) -- still
#     no freq table). Cross-engine compare gets a meaningful signal.
#   - detailed: adds top_10 + the "val=cnt" min/max form. Requires
#     the WITH-chain freq table. This is the only preset where Hive
#     emits a GROUP BY <col_expr>.
#
# Mean/std (rank-weighted from the freq table) are NOT included in any
# preset -- empirical signal-to-noise was poor and they doubled the
# query cost. Kept '' in every emitted row so the CSV schema doesn't
# need to change.
STATS_PRESETS = {
    'quick': {
        'n_total':     True,
        'n_missing':   True,
        'n_unique':    False,
        'min_max':     True,  # scalar MIN/MAX (no =cnt suffix)
        'length':      True,  # length_avg + length_max
        'hash_sum':    False,
        'top_10':      False,
    },
    'standard': {
        'n_total':     True,
        'n_missing':   True,
        'n_unique':    True,
        'min_max':     True,  # scalar
        'length':      True,
        'hash_sum':    True,
        'top_10':      False,
    },
    'detailed': {
        'n_total':     True,
        'n_missing':   True,
        'n_unique':    True,
        'min_max':     True,  # =cnt form (freq-table-derived)
        'length':      True,
        'hash_sum':    True,
        'top_10':      True,
    },
}

# Per-platform runner config. Drives the generic run_extract_sql_file
# helper: which env var caps worker count, what label to write into the
# 'partial' CSV column for lifeboat-fallback rows, and which error
# substrings should trigger the @degrade_sql retry instead of failing
# the block. Add a new platform by appending a row here -- no runner
# code changes needed.
PLATFORM_RUNNER = {
    'oracle': {
        'worker_env':       'ORACLE_MAX_WORKERS',
        'worker_default':   2,
        'degraded_label':   'oracle_degraded',
        'lifeboat_errors':  ['ORA-01652', 'ORA-04030'],
    },
    'hadoop': {
        'worker_env':       'HADOOP_MAX_WORKERS',
        'worker_default':   4,
        'degraded_label':   'hadoop_degraded',
        # Trigger the @degrade_sql lifeboat for Hive memory / executor /
        # container exhaustion patterns. Broad on purpose: Hive surfaces
        # the same root cause (mapper/reducer ran out of resources) under
        # several different driver / Tez / MR error strings, and an over-
        # narrow list silently fails the block instead of falling back to
        # the streaming aggregate. Watch for false positives though --
        # genuine SQL errors (column not found, syntax) shouldn't match
        # any of these substrings.
        'lifeboat_errors':  [
            'OutOfMemoryError',
            'memory limit',
            'GC overhead',
            'Container killed',
            'beyond physical memory',
            'beyond virtual memory',
            'Java heap space',
            'spill failed',
        ],
    },
    'athena': {
        'worker_env':       'ATHENA_MAX_WORKERS',
        'worker_default':   8,
        'degraded_label':   'athena_degraded',
        'lifeboat_errors':  ['Query exhausted resources'],
    },
    # CSV-as-source: powered by DuckDB in-process. Workers are CPU-bound
    # (in-memory query exec) so default to a few; no real per-conn
    # concurrency limit but writing CSV results back to disk creates I/O
    # contention. Lifeboat trigger covers DuckDB's "out of memory" /
    # "could not allocate" patterns -- rare on small fixtures, possible
    # when reading multi-GB CSVs into a single connection's table cache.
    'csv': {
        'worker_env':       'CSV_MAX_WORKERS',
        'worker_default':   4,
        'degraded_label':   'csv_degraded',
        'lifeboat_errors':  ['Out of Memory', 'could not allocate', 'OutOfMemory'],
    },
}

# Decimal precision for mean/std match keys in column comparison.
# Two values match iff `round(float(v), STAT_ROUND_DECIMALS)` is equal as a
# string with an "m_"/"s_" prefix — bypasses float-equality drift.
STAT_ROUND_DECIMALS = 4

# Sentinel values that should be coerced to NULL before computing stats.
# Phase-1 coarse comparison is meant to flag columns whose distribution
# truly drifts between platforms; if one side stores -999 for missing and
# the other stores NULL, min/max/mean/n_unique will diverge for reasons
# that have nothing to do with the underlying data. Translating these
# values to NULL up front collapses the difference and keeps the signal
# focused on real drift.
#
# Per-platform so each engine can use its native types/literals.
# Categories:
#   numeric  — sentinel ints/floats (-999, -99999999, etc.)
#   string   — sentinel strings ('NA', '#N/A', '.', etc.). Blank/empty
#              are handled separately by the missing_cond rule.
#   date     — sentinel date literals. "infinity" (e.g. 9999-12-31),
#              "missing" (e.g. 0001-01-01), and the "today-placeholder"
#              (a fixed far-future date some pipelines write when the
#              row's date is unknown).
#
# Edit this dict to add or remove sentinels for your environment.
NULL_SENTINELS = {
    "oracle": {
        "numeric": [-999, -9999, -99999999],
        "string":  ["NA", "N/A", "#N/A", ".", "NULL", "null"],
        "date":    ["9999-12-31", "4712-12-31", "0001-01-01", "1900-01-01"],
    },
    "hadoop": {
        "numeric": [-999, -9999, -99999999],
        "string":  ["NA", "N/A", "#N/A", ".", "NULL", "null"],
        "date":    ["9999-12-31", "0001-01-01", "1900-01-01"],
    },
    "aws": {
        "numeric": [-999, -9999, -99999999],
        "string":  ["NA", "N/A", "#N/A", ".", "NULL", "null"],
        "date":    ["9999-12-31", "0001-01-01", "1900-01-01"],
    },
    "sas": {
        "numeric": [-999, -9999, -99999999],
        "string":  ["NA", "N/A", "#N/A", ".", "NULL", "null"],
        "date":    ["9999-12-31", "0001-01-01", "1900-01-01"],
    },
}

# Data source types
DATA_SOURCES = [
    {"value": "oracle", "label": "SAS/Oracle"},
    {"value": "hadoop", "label": "SAS/Hadoop"},
    {"value": "sas", "label": "SAS Dataset"},
    {"value": "aws", "label": "AWS/Athena"},
    {"value": "csv", "label": "CSV"},
]

# Date column types — dropdown options shown in the pairs UI. When adding a
# new format, also add it to DATE_TYPE_FORMATS in platforms/base.py and to
# reformat_date()'s fmt_map there.
DATE_COLUMN_TYPES = [
    {"value": "date", "label": "Date"},
    {"value": "timestamp", "label": "Timestamp"},
    {"value": "datetime", "label": "DateTime"},
    {"value": "num", "label": "Number (YYYYMMDD)"},
    {"value": "num_yyyymm", "label": "Number (YYYYMM)"},
    {"value": "string_dash", "label": "String (YYYY-MM-DD)"},
    {"value": "string_compact", "label": "String (YYYYMMDD)"},
    {"value": "string_mon", "label": "String (DDMONYYYY)"},
    {"value": "string_mon_dash", "label": "String (DD-MON-YYYY)"},
    {"value": "string_us", "label": "String (MM/DD/YYYY)"},
]

# ==========================================================================
# DATE TYPE REGISTRY — single source of truth for how `date_type` (the
# user's semantic config in tbl_cfg.date_type) maps to:
#   1. the canonical strftime-like format label used by reformat_date()
#      to convert min/max date strings into the column's stored shape,
#   2. the canonical raw DB type that the WHERE-clause builders branch
#      on to choose between bare numerics, quoted strings, DATE literals,
#      etc.
#
# To add a new format:
#   1. Add an entry to DATE_TYPE_FORMATS (date_type -> format label).
#   2. Add a matching entry to DATE_TYPE_TO_CANONICAL_DTYPE.
#   3. Add a strftime pattern to reformat_date()'s fmt_map (platforms/base.py).
#   4. Add a dropdown entry to DATE_COLUMN_TYPES above.
#
# All platforms (SAS, Oracle, Hadoop, Athena) pick these up via
# resolve_date_format() — no per-platform changes needed.
# ==========================================================================
DATE_TYPE_FORMATS = {
    # String types
    'string_compact':  'YYYYMMDD',    # '20260115'
    'string_dash':     'YYYY-MM-DD',  # '2026-01-15'
    'string':          'YYYY-MM-DD',  # '2026-01-15'
    'string_mon':      'DDMONYYYY',   # '15JAN2026'
    'string_mon_dash': 'DD-MON-YYYY', # '15-JAN-2026'
    'string_us':       'MM/DD/YYYY',  # '01/15/2026'
    # Numeric types (no quotes in SQL)
    'num':             'YYYYMMDD',    # 20260115
    'integer':         'YYYYMMDD',    # 20260115
    'int':             'YYYYMMDD',    # 20260115
    'number':          'YYYYMMDD',    # 20260115
    'num_yyyymm':      'YYYYMM',      # 202601
    # Native date/timestamp
    'date':            'YYYY-MM-DD',  # DATE '2026-01-15'
    'timestamp':       'YYYY-MM-DD',  # TIMESTAMP '2026-01-15 ...'
    'datetime':        'YYYY-MM-DD',  # TIMESTAMP '2026-01-15 ...'
}

# Map the user's semantic date_type to a canonical RAW DB type label.
# Downstream WHERE-clause builders branch on this dtype to pick the
# literal form (number, quoted string, DATE 'X', etc.). When the user
# configures date_type explicitly, this overrides _column_meta even if
# the metadata says something different (stale / missing / wrong).
DATE_TYPE_TO_CANONICAL_DTYPE = {
    # Native date / timestamp -- TRUNC + DATE 'YYYY-MM-DD' literals.
    'date':            'DATE',
    'timestamp':       'TIMESTAMP',
    'datetime':        'TIMESTAMP',
    # Numeric: bare integer literals like 20260115, no quotes, no TRUNC.
    'num':             'NUMBER',
    'integer':         'NUMBER',
    'int':             'NUMBER',
    'number':          'NUMBER',
    'num_yyyymm':      'NUMBER',
    # String: quoted literals in the column's stored format
    # ('2026-01-15', '20260115', '15JAN2026', '15-JAN-2026', '01/15/2026').
    'string':          'VARCHAR2',
    'string_dash':     'VARCHAR2',
    'string_compact':  'VARCHAR2',
    'string_mon':      'VARCHAR2',
    'string_mon_dash': 'VARCHAR2',
    'string_us':       'VARCHAR2',
}

# Connection macros by source type
CONNECTION_MACROS = {
    "oracle": ["pb23", "pb30"],
    "hadoop": ["hdp", "hadoop_prod"],
    "sas": ["work", "sasuser"],
    "aws": ["analytics_db", "warehouse_db", "mydb"],
    "csv": [],
}

# Hadoop libname / PROC SQL CONNECT profiles. Each method gets its own dict
# because the two statements take different params on this user's box. The
# Playground SAS engine shows the appropriate dropdown depending on which
# method is selected.
#
# Add or rename profiles here without touching JS/HTML — the UI fetches
# both dicts via /api/constants on page load and switches based on method.
HADOOP_LIBNAME_PROFILES = {
    "default": {
        "server":      "hadoop.example.com",
        "host":        "REALM.EXAMPLE.COM",
        "schema":      "default",
        "port":        10000,
        "jar_path":    "/sas/bin/Software/Hadoop/OSH/Jars",
        "config_path": "/sas/bin/Software/Hadoop/OSH/PROD",
        # "truststore": "/sas/bin/Software/Hadoop/OSH/PROD/truststore.jks",
    },
}

HADOOP_PROCSQL_PROFILES = {
    "default": {
        "server":      "hadoop.example.com",
        "host":        "REALM.EXAMPLE.COM",
        "schema":      "default",
        "port":        10000,
        "jar_path":    "/sas/bin/Software/Hadoop/OSH/Jars",
        "config_path": "/sas/bin/Software/Hadoop/OSH/PROD",
    },
}

# Vintage presets for different platforms
VINTAGE_PRESETS = {
    "hadoop": {
        "day": "{col}",
        "week": "TRUNC({col}, 'IW')",
        "month": "TRUNC({col}, 'MM')",
        "quarter": "TRUNC({col}, 'Q')",
        "year": "TRUNC({col}, 'YYYY')",
    },
    "oracle": {
        "day": "{col}",
        "week": "TRUNC({col}, 'IW')",
        "month": "TRUNC({col}, 'MM')",
        "quarter": "TRUNC({col}, 'Q')",
        "year": "TRUNC({col}, 'YYYY')",
    },
    "aws": {
        "day": "{col}",
        "week": "DATE_TRUNC('week', {col})",
        "month": "DATE_TRUNC('month', {col})",
        "quarter": "DATE_TRUNC('quarter', {col})",
        "year": "DATE_TRUNC('year', {col})",
    },
    "sas": {
        "day": "{col}",
        "week": "intnx('week.2', {col}, 0, 'b')",
        "month": "intnx('month', {col}, 0, 'b')",
        "quarter": "intnx('qtr', {col}, 0, 'b')",
        "year": "intnx('year', {col}, 0, 'b')",
    },
}
